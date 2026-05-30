"""TPS strategy: multi-timeframe Thùng Phá Sảnh.

Per the source method's "CÁC BƯỚC ĐỂ VÀO LỆNH":

  1. On each timeframe, check whether price is fully on one side of EMA34/EMA89.
     If price sits *between* the two EMAs on M15/M30/H1/H4/D1 -> sit out.
  2. Higher TFs (H4, D1) set the long-term bias.
  3. A TPS entry on the entry timeframe requires, in the same direction:
        TREND BREAK (mandatory) + DOW BREAK + EMA ALIGNED.
     (DOW-break / EMA-aligned may happen in either order.)
  4. Entry at the break close; SL at the XTC2 wick (opposite recent pivot);
     TP at the nearest hard S/R, else 1R (IR) from the XTC1 swing size.
  5. Conflict handling between H4/D1 and the lower TFs (VGT fallback / sit out).

The strategy is *stateless* across calls: ``decide`` consumes a snapshot of every
timeframe's ``TimeframeState`` and returns ``StrategyDecision`` order intents.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd

from strategies.base_strategy import BaseStrategy, OrderIntent, StrategyDecision
from strategies import indicators as ind


@dataclass
class TimeframeState:
    timeframe: str
    df: pd.DataFrame
    bias: ind.Bias = "NEUTRAL"
    ema_fast: float = 0.0
    ema_slow: float = 0.0
    pivots: List[ind.Pivot] = field(default_factory=list)
    support_line: Optional[ind.Trendline] = None
    resistance_line: Optional[ind.Trendline] = None
    sr: dict = field(default_factory=lambda: {"resistance": [], "support": []})
    close: float = 0.0


class TPSStrategy(BaseStrategy):
    name = "TPS"

    REQUIRED = (
        "ema_fast", "ema_slow", "pivot_window", "sr_lookback",
        "sideway_window", "sideway_crosses", "tp_mode", "min_rr",
        "risk_amount_per_position",
    )

    def required_param_names(self) -> list[str]:
        return list(self.REQUIRED)

    def validate_params(self) -> None:
        for k in self.REQUIRED:
            if k not in self.params:
                raise ValueError(f"TPS missing param: {k}")
        if self.params["ema_fast"] >= self.params["ema_slow"]:
            raise ValueError("ema_fast must be < ema_slow")
        if self.params["risk_amount_per_position"] <= 0:
            raise ValueError("risk_amount_per_position must be > 0")
        if self.params["tp_mode"] not in ("ir", "hard_sr"):
            raise ValueError("tp_mode must be 'ir' or 'hard_sr'")

    # ------------------------------------------------------------------ #
    # Per-timeframe analysis
    # ------------------------------------------------------------------ #

    def min_bars(self) -> int:
        """Minimum candles required before a timeframe can be analysed."""
        return int(self.params["ema_slow"]) + 2 * int(self.params["pivot_window"]) + 2

    def analyze_timeframe(self, timeframe: str, df: pd.DataFrame) -> Optional[TimeframeState]:
        if df is None or len(df) < self.min_bars():
            return None
        fast = ind.ema(df["close"], int(self.params["ema_fast"]))
        slow = ind.ema(df["close"], int(self.params["ema_slow"]))
        pivots = ind.swing_pivots(df, int(self.params["pivot_window"]))
        bias = ind.ema_bias(
            df, fast, slow,
            int(self.params["sideway_window"]),
            int(self.params["sideway_crosses"]),
        )
        return TimeframeState(
            timeframe=timeframe,
            df=df,
            bias=bias,
            ema_fast=float(fast.iloc[-1]),
            ema_slow=float(slow.iloc[-1]),
            pivots=pivots,
            support_line=ind.trendline(pivots, "support"),
            resistance_line=ind.trendline(pivots, "resistance"),
            sr=ind.hard_sr_levels(pivots, int(self.params["sr_lookback"])),
            close=float(df["close"].iloc[-1]),
        )

    def current_risk(self) -> float:
        return float(self.params["risk_amount_per_position"])

    # ------------------------------------------------------------------ #
    # TPS trigger on a single (entry) timeframe
    # ------------------------------------------------------------------ #

    def _tps_signal_direction(self, st: TimeframeState) -> Optional[str]:
        """Return 'BUY'/'SELL' if a full TPS setup fired on this timeframe.

        TPS = TREND BREAK (mandatory) + DOW BREAK + EMA ALIGNED, same direction.
        """
        for direction in ("BUY", "SELL"):
            if st.bias != direction:                       # EMA aligned
                continue
            line = st.resistance_line if direction == "BUY" else st.support_line
            if not ind.trendline_break(st.df, line, direction):   # mandatory
                continue
            if ind.dow_break(st.df, st.pivots, direction) is None:  # DOW break
                continue
            return direction
        return None

    def _vgt_signal_direction(self, st: TimeframeState, bias: str) -> Optional[str]:
        """VGT (value-zone) fallback: price poked through an EMA but the last
        candle closed back on the ``bias`` side. Used when H4/D1 conflict with
        the very low TFs (Case 1)."""
        if len(st.df) < 2:
            return None
        prev_low = float(st.df["low"].iloc[-2])
        prev_high = float(st.df["high"].iloc[-2])
        close = st.close
        if bias == "BUY" and prev_low < st.ema_fast <= close and st.ema_fast > st.ema_slow:
            return "BUY"
        if bias == "SELL" and prev_high > st.ema_fast >= close and st.ema_fast < st.ema_slow:
            return "SELL"
        return None

    # ------------------------------------------------------------------ #
    # Entry / SL / TP construction
    # ------------------------------------------------------------------ #

    def _build_intent(
        self,
        st: TimeframeState,
        direction: str,
        reason: str,
    ) -> Optional[OrderIntent]:
        entry = st.close
        pivots = st.pivots
        if direction == "BUY":
            xtc2 = ind.last_pivot(pivots, "low")     # SL = opposite (XTC2) wick
            if xtc2 is None or xtc2.price >= entry:
                return None
            stop_loss = xtc2.price
            risk_dist = entry - stop_loss
            tp = self._take_profit(st, entry, direction, risk_dist)
            side = "BUY"
        else:
            xtc2 = ind.last_pivot(pivots, "high")
            if xtc2 is None or xtc2.price <= entry:
                return None
            stop_loss = xtc2.price
            risk_dist = stop_loss - entry
            tp = self._take_profit(st, entry, direction, risk_dist)
            side = "SELL"

        if risk_dist <= 0:
            return None
        reward = abs(tp - entry)
        if reward / risk_dist < float(self.params["min_rr"]):
            return None

        risk_usdt = self.current_risk()
        qty = risk_usdt / risk_dist
        tag = "tps_buy" if direction == "BUY" else "tps_sell"
        return OrderIntent(
            side=side,
            order_type="STOP",                  # stop-entry at the break level
            qty=qty,
            limit_price=entry,
            trigger_price=entry,
            current_price=entry,
            take_profit=tp,
            stop_loss=stop_loss,
            tag=tag,
            meta={
                "timeframe": st.timeframe,
                "reason": reason,
                "risk": risk_usdt,
                "rr": reward / risk_dist,
            },
        )

    def _take_profit(self, st: TimeframeState, entry: float, direction: str, risk_dist: float) -> float:
        """TP at the nearest hard S/R when tp_mode == 'hard_sr', else 1R (IR)."""
        if self.params["tp_mode"] == "hard_sr":
            if direction == "BUY":
                lv = ind.nearest_level_above(st.sr["resistance"], entry)
                if lv is not None:
                    return lv
            else:
                lv = ind.nearest_level_below(st.sr["support"], entry)
                if lv is not None:
                    return lv
        # IR / 1R fallback: project the risk distance in the trade direction.
        return entry + risk_dist if direction == "BUY" else entry - risk_dist

    # ------------------------------------------------------------------ #
    # Multi-timeframe orchestration
    # ------------------------------------------------------------------ #

    def _higher_bias(self, states: Dict[str, TimeframeState]) -> Optional[str]:
        """Agreed bias of the configured higher TFs (H4, D1). None if they
        disagree or are not clearly trending."""
        votes = set()
        for tf in self.params.get("bias_timeframes", ["4h", "1d"]):
            st = states.get(tf)
            if st is None or st.bias not in ("BUY", "SELL"):
                return None
            votes.add(st.bias)
        if len(votes) == 1:
            return votes.pop()
        return None

    def _ema_gate_blocked(self, states: Dict[str, TimeframeState]) -> bool:
        """Sit out when price is between the two EMAs (NEUTRAL) on any gated TF."""
        for tf in self.params.get("ema_gate_timeframes", []):
            st = states.get(tf)
            if st is not None and st.bias == "NEUTRAL":
                return True
        return False

    def decide(
        self,
        states: Dict[str, TimeframeState],
        entry_timeframe: str,
        has_open_position: bool = False,
    ) -> StrategyDecision:
        decision = StrategyDecision()
        if has_open_position:
            return decision

        entry_st = states.get(entry_timeframe)
        if entry_st is None:
            return decision

        # Step 1: EMA gate — between EMAs on a gated TF => sit out.
        if self._ema_gate_blocked(states):
            return decision

        higher = self._higher_bias(states)

        # Primary path: a clean TPS setup on the entry timeframe.
        direction = self._tps_signal_direction(entry_st)

        if direction is not None:
            # If higher TFs have a clear bias, only trade *with* it.
            if higher is not None and direction != higher:
                # Case 2: H4/D1 conflict with M30/H1 -> sit out (no counter-trend).
                return decision
            intent = self._build_intent(entry_st, direction, reason="TPS")
            if intent is not None:
                decision.place.append(intent)
            return decision

        # Conflict Case 1: H4/D1 trend clear but entry TF gave no TPS signal ->
        # look for a with-trend VGT entry on the entry timeframe.
        if higher is not None:
            vgt_dir = self._vgt_signal_direction(entry_st, higher)
            if vgt_dir is not None:
                intent = self._build_intent(entry_st, vgt_dir, reason="VGT")
                if intent is not None:
                    decision.place.append(intent)
        return decision
