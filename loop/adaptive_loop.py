"""Coordination layer for the TPS bot.

``LoopState`` holds a rolling OHLCV frame per timeframe plus the latest analysed
``TimeframeState`` for each. The websocket appends each closed candle, re-analyses
that timeframe, and (on the entry timeframe) asks the strategy for a decision.

Jobs:
- ``on_candle_close``   : append a closed candle, refresh that TF's state, and on
                          the entry timeframe run the multi-TF decision engine.
- ``refresh_history_job``: periodically re-pull history so higher TFs stay warm.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from config import TPS_DEFAULT_PARAMS, settings
from data.binance_fetcher import update_history
from data.data_store import init_db, load_ohlcv
from execution.binance_executor import BinanceExecutor
from execution.portfolio import Portfolio
from strategies.tps_strategy import TPSStrategy, TimeframeState


# How many candles to keep in memory per timeframe.
_MAX_BARS = 500


@dataclass
class LoopState:
    symbol: str
    timeframes: List[str]
    entry_timeframe: str
    strategy: TPSStrategy
    portfolio: Portfolio
    executor: BinanceExecutor
    candles: Dict[str, pd.DataFrame] = field(default_factory=dict)
    tf_states: Dict[str, TimeframeState] = field(default_factory=dict)
    halted: bool = False


def _load_strategy() -> TPSStrategy:
    params = {**TPS_DEFAULT_PARAMS,
              "risk_amount_per_position": settings.default_risk_usdt}
    return TPSStrategy(params)


def make_default_state(paper: bool = True) -> LoopState:
    init_db()
    strategy = _load_strategy()
    state = LoopState(
        symbol=settings.tps_symbol,
        timeframes=list(settings.tps_timeframes),
        entry_timeframe=settings.tps_entry_timeframe,
        strategy=strategy,
        portfolio=Portfolio(),
        executor=BinanceExecutor(symbol=settings.tps_symbol, paper=paper),
    )
    return state


def seed_history(state: LoopState, lookback_days: int = 120) -> None:
    """Pull and cache enough candles per timeframe to make EMA89 + pivots valid."""
    for tf in state.timeframes:
        try:
            update_history(state.symbol, tf, lookback_days=lookback_days)
        except Exception as e:  # pragma: no cover - network
            logger.warning(f"seed_history fetch failed for {tf}: {e}")
        df = load_ohlcv(state.symbol, tf)
        if not df.empty:
            state.candles[tf] = df.tail(_MAX_BARS).copy()
            st = state.strategy.analyze_timeframe(tf, state.candles[tf])
            if st is not None:
                state.tf_states[tf] = st
    logger.info(f"Seeded history for {len(state.candles)} timeframes "
                f"({', '.join(state.candles)})")


def append_closed_candle(state: LoopState, timeframe: str, kline: dict) -> None:
    """Append one *closed* kline dict (Binance 'k' payload) to the rolling frame."""
    open_time = pd.to_datetime(int(kline["t"]), unit="ms", utc=True)
    row = pd.DataFrame(
        [{
            "open": float(kline["o"]), "high": float(kline["h"]),
            "low": float(kline["l"]), "close": float(kline["c"]),
            "volume": float(kline["v"]),
        }],
        index=pd.DatetimeIndex([open_time], name="open_time"),
    )
    df = state.candles.get(timeframe)
    if df is None or df.empty:
        df = row
    else:
        df = pd.concat([df, row])
        df = df[~df.index.duplicated(keep="last")].sort_index().tail(_MAX_BARS)
    state.candles[timeframe] = df
    st = state.strategy.analyze_timeframe(timeframe, df)
    if st is not None:
        state.tf_states[timeframe] = st


def evaluate_and_act(state: LoopState) -> None:
    """Run the multi-TF decision engine and place/publish any resulting orders."""
    if state.halted:
        return
    has_pos = state.portfolio.has_position()
    decision = state.strategy.decide(
        state.tf_states,
        entry_timeframe=state.entry_timeframe,
        has_open_position=has_pos,
    )
    if not decision.place:
        return
    for intent in decision.place:
        logger.info(f"TPS signal: {intent.side} {intent.tag} entry={intent.limit_price} "
                    f"sl={intent.stop_loss} tp={intent.take_profit} meta={intent.meta}")
        if settings.send_copy_trade:
            state.executor._push_to_rabbitmq(intent)
        placed = state.executor.place(intent)
        state.portfolio.pending_orders[intent.tag] = placed.broker_order_id


def refresh_history_job(state: LoopState) -> None:
    """Periodic warm-up so D1/H4 levels stay accurate even between websocket ticks."""
    try:
        seed_history(state)
    except Exception as e:  # pragma: no cover
        logger.error(f"refresh_history_job failed: {e}")


def build_scheduler(state: LoopState, refresh_hours: int = 6) -> AsyncIOScheduler:
    sched = AsyncIOScheduler(timezone=pytz.timezone("UTC"))
    sched.add_job(
        refresh_history_job, IntervalTrigger(hours=refresh_hours),
        args=[state], id="refresh_history",
    )
    return sched
