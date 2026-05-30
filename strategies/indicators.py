"""Pure price-action primitives for the TPS method.

All functions operate on an OHLCV ``pandas.DataFrame`` indexed by open_time with
columns ``open, high, low, close, volume`` (the shape returned by
``data.data_store.load_ohlcv``). Everything here is deterministic and reused by
both the live websocket path and the backtester.

The three TPS pillars map to functions below:
  * EMA34/EMA89  -> ``ema`` + ``ema_bias``
  * DOW theory   -> ``swing_pivots`` (XTC1/XTC2 structure) + ``dow_break``
  * Trendline    -> ``trendline`` + ``trendline_break``
Hard support/resistance levels come from the pivot history (``hard_sr_levels``).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Literal, Optional

import pandas as pd


Bias = Literal["BUY", "SELL", "NEUTRAL", "SIDEWAY"]


@dataclass
class Pivot:
    idx: int          # positional index into the dataframe
    price: float      # high (for a swing high) or low (for a swing low)
    kind: Literal["high", "low"]


@dataclass
class Trendline:
    # price(x) = slope * x + intercept, x = positional bar index
    slope: float
    intercept: float
    kind: Literal["support", "resistance"]
    p1: Pivot
    p2: Pivot

    def value_at(self, idx: int) -> float:
        return self.slope * idx + self.intercept


# --------------------------------------------------------------------------- #
# EMA + trend bias
# --------------------------------------------------------------------------- #

def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def ema_bias(
    df: pd.DataFrame,
    fast: pd.Series,
    slow: pd.Series,
    sideway_window: int,
    sideway_crosses: int,
) -> Bias:
    """Classify the latest candle per the doc's "TRICK VỚI EMA" rules.

    BUY     -> candle body fully above both EMAs and ema_fast > ema_slow
    SELL    -> candle body fully below both EMAs and ema_fast < ema_slow
    SIDEWAY -> price crossed the EMAs >= sideway_crosses times recently
    NEUTRAL -> anything else (price between the two EMAs => sit out)
    """
    if len(df) < 2:
        return "NEUTRAL"

    # Sideway detection: count how often the close flips sides of ema_slow.
    window = min(sideway_window, len(df))
    recent_close = df["close"].iloc[-window:].to_numpy()
    recent_slow = slow.iloc[-window:].to_numpy()
    side = recent_close > recent_slow
    crosses = int((side[1:] != side[:-1]).sum())
    if crosses >= sideway_crosses:
        return "SIDEWAY"

    o = float(df["open"].iloc[-1])
    c = float(df["close"].iloc[-1])
    body_low, body_high = min(o, c), max(o, c)
    f = float(fast.iloc[-1])
    s = float(slow.iloc[-1])

    if body_low > f and body_low > s and f > s:
        return "BUY"
    if body_high < f and body_high < s and f < s:
        return "SELL"
    return "NEUTRAL"


# --------------------------------------------------------------------------- #
# DOW structure: swing pivots
# --------------------------------------------------------------------------- #

def swing_pivots(df: pd.DataFrame, window: int) -> List[Pivot]:
    """Fractal swing highs/lows: a bar is a pivot high when its high is the
    strict max of the ``window`` bars on each side (and symmetric for lows).

    Only *confirmed* pivots are returned — i.e. those with ``window`` bars to the
    right — which matches the doc's rule that a break needs candle closes to the
    side of the structure before it counts.
    """
    highs = df["high"].to_numpy()
    lows = df["low"].to_numpy()
    n = len(df)
    pivots: List[Pivot] = []
    for i in range(window, n - window):
        left_h = highs[i - window:i]
        right_h = highs[i + 1:i + 1 + window]
        if highs[i] > left_h.max() and highs[i] >= right_h.max():
            pivots.append(Pivot(idx=i, price=float(highs[i]), kind="high"))
            continue
        left_l = lows[i - window:i]
        right_l = lows[i + 1:i + 1 + window]
        if lows[i] < left_l.min() and lows[i] <= right_l.min():
            pivots.append(Pivot(idx=i, price=float(lows[i]), kind="low"))
    return pivots


def last_pivot(pivots: List[Pivot], kind: Literal["high", "low"]) -> Optional[Pivot]:
    for p in reversed(pivots):
        if p.kind == kind:
            return p
    return None


def dow_break(df: pd.DataFrame, pivots: List[Pivot], direction: Literal["BUY", "SELL"]) -> Optional[Pivot]:
    """Return the broken pivot if the latest close breaks DOW structure.

    BUY  -> latest close > most-recent confirmed swing HIGH (XTC1 continues up)
    SELL -> latest close < most-recent confirmed swing LOW  (XTC1 continues down)
    """
    close = float(df["close"].iloc[-1])
    if direction == "BUY":
        ph = last_pivot(pivots, "high")
        if ph is not None and close > ph.price:
            return ph
    else:
        pl = last_pivot(pivots, "low")
        if pl is not None and close < pl.price:
            return pl
    return None


# --------------------------------------------------------------------------- #
# Trendline + hard S/R
# --------------------------------------------------------------------------- #

def trendline(pivots: List[Pivot], kind: Literal["support", "resistance"]) -> Optional[Trendline]:
    """Build a trendline through the last two pivot lows (support) or pivot
    highs (resistance) — the doc connects the bottoms/tops of XTC1 + XTC2."""
    pk = "low" if kind == "support" else "high"
    pts = [p for p in pivots if p.kind == pk]
    if len(pts) < 2:
        return None
    p1, p2 = pts[-2], pts[-1]
    if p2.idx == p1.idx:
        return None
    slope = (p2.price - p1.price) / (p2.idx - p1.idx)
    intercept = p1.price - slope * p1.idx
    return Trendline(slope=slope, intercept=intercept, kind=kind, p1=p1, p2=p2)


def trendline_break(df: pd.DataFrame, line: Optional[Trendline], direction: Literal["BUY", "SELL"]) -> bool:
    """A break = the latest candle *closes* through the projected trendline.

    SELL breaks a support line (close below it); BUY breaks a resistance line
    (close above it).
    """
    if line is None:
        return False
    idx = len(df) - 1
    proj = line.value_at(idx)
    close = float(df["close"].iloc[-1])
    if direction == "SELL" and line.kind == "support":
        return close < proj
    if direction == "BUY" and line.kind == "resistance":
        return close > proj
    return False


def hard_sr_levels(pivots: List[Pivot], lookback: int) -> dict:
    """Recent pivot highs/lows act as hard resistance / support (TP targets)."""
    recent = pivots[-lookback:] if lookback else pivots
    return {
        "resistance": sorted({p.price for p in recent if p.kind == "high"}),
        "support": sorted({p.price for p in recent if p.kind == "low"}),
    }


def nearest_level_above(levels: List[float], price: float) -> Optional[float]:
    above = [lv for lv in levels if lv > price]
    return min(above) if above else None


def nearest_level_below(levels: List[float], price: float) -> Optional[float]:
    below = [lv for lv in levels if lv < price]
    return max(below) if below else None
