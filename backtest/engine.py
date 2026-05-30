"""Event-driven multi-timeframe backtester for TPS.

Replays the entry timeframe bar by bar. At each entry-bar close it reconstructs
every timeframe's view using only candles that have *already closed* (no
lookahead), runs the exact same ``TPSStrategy.decide`` used live, and simulates
the resulting STOP order against subsequent entry-bar highs/lows.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from data.binance_fetcher import _TF_TO_MS
from data.data_store import load_ohlcv
from strategies.tps_strategy import TPSStrategy


@dataclass
class BacktestResult:
    trades: List[dict] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)


def _tf_delta(tf: str) -> timedelta:
    return timedelta(milliseconds=_TF_TO_MS[tf])


def _compute_metrics(trades: List[dict], starting_equity: float = 1000.0) -> dict:
    n = len(trades)
    if n == 0:
        return {"n_trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
                "total_pnl": 0.0, "avg_pnl": 0.0, "profit_factor": 0.0,
                "expectancy": 0.0, "max_drawdown": 0.0}
    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    equity = starting_equity
    peak = equity
    max_dd = 0.0
    for p in pnls:
        equity += p
        peak = max(peak, equity)
        max_dd = min(max_dd, equity - peak)
    return {
        "n_trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / n,
        "total_pnl": sum(pnls),
        "avg_pnl": sum(pnls) / n,
        "profit_factor": (gross_win / gross_loss) if gross_loss else float("inf"),
        "expectancy": sum(pnls) / n,
        "max_drawdown": max_dd,
    }


def run_tps_backtest(
    strategy: TPSStrategy,
    symbol: str,
    timeframes: List[str],
    entry_timeframe: str,
    starting_equity: float = 1000.0,
) -> BacktestResult:
    # Load every timeframe once.
    data: Dict[str, pd.DataFrame] = {}
    for tf in timeframes:
        df = load_ohlcv(symbol, tf)
        if df.empty:
            logger.warning(f"No data for {symbol} {tf}; run `fetch` first.")
        data[tf] = df

    entry_df = data.get(entry_timeframe)
    if entry_df is None or entry_df.empty:
        return BacktestResult(metrics=_compute_metrics([], starting_equity))

    entry_delta = _tf_delta(entry_timeframe)
    min_bars = strategy.min_bars()
    trades: List[dict] = []
    open_trade: Optional[dict] = None

    entry_times = entry_df.index
    for i in range(min_bars, len(entry_df) - 1):
        bar = entry_df.iloc[i]
        bar_open_time = entry_times[i]
        bar_close_time = bar_open_time + entry_delta

        # 1) Manage an open trade against THIS bar's range first.
        if open_trade is not None:
            hit = _check_exit(open_trade, bar)
            if hit is not None:
                exit_price, reason = hit
                pnl = _pnl(open_trade, exit_price)
                trades.append({**open_trade, "exit_price": exit_price,
                               "exit_time": bar_open_time, "reason": reason, "pnl": pnl})
                open_trade = None
            else:
                continue  # still in a trade; do not stack new entries

        # 2) Build a no-lookahead snapshot of every timeframe.
        tf_states = {}
        for tf, df in data.items():
            if df.empty:
                continue
            closed = df[df.index + _tf_delta(tf) <= bar_close_time]
            if len(closed) < min_bars:
                continue
            st = strategy.analyze_timeframe(tf, closed.tail(500))
            if st is not None:
                tf_states[tf] = st

        if entry_timeframe not in tf_states:
            continue

        # 3) Ask the strategy for a decision and open at the next bar.
        decision = strategy.decide(tf_states, entry_timeframe=entry_timeframe,
                                   has_open_position=False)
        if not decision.place:
            continue
        intent = decision.place[0]
        open_trade = {
            "side": intent.side,
            "entry_price": float(intent.limit_price),
            "tp": intent.take_profit,
            "sl": intent.stop_loss,
            "qty": intent.qty,
            "entry_time": bar_close_time,
            "timeframe": intent.meta.get("timeframe"),
            "reason_in": intent.meta.get("reason"),
        }

    return BacktestResult(trades=trades, metrics=_compute_metrics(trades, starting_equity))


def _check_exit(trade: dict, bar) -> Optional[tuple]:
    """Return (exit_price, reason) if SL or TP is touched this bar.

    When both levels fall inside one bar we assume SL first (conservative)."""
    high, low = float(bar["high"]), float(bar["low"])
    if trade["side"] == "BUY":
        if low <= trade["sl"]:
            return trade["sl"], "SL"
        if high >= trade["tp"]:
            return trade["tp"], "TP"
    else:
        if high >= trade["sl"]:
            return trade["sl"], "SL"
        if low <= trade["tp"]:
            return trade["tp"], "TP"
    return None


def _pnl(trade: dict, exit_price: float) -> float:
    diff = exit_price - trade["entry_price"]
    if trade["side"] == "SELL":
        diff = -diff
    return diff * trade["qty"]
