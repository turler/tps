"""CLI entry point for the TPS (Thùng Phá Sảnh) bot.

Usage:
  python main.py fetch     [--symbol BTCUSDT] [--days 120]
  python main.py backtest  [--symbol BTCUSDT]
  python main.py live      [--paper|--live]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

from loguru import logger

from config import settings


def cmd_fetch(args) -> int:
    from data.binance_fetcher import update_history
    total = 0
    for tf in settings.tps_timeframes:
        n = update_history(args.symbol, tf, lookback_days=args.days)
        print(f"Inserted {n} rows for {args.symbol} {tf}")
        total += n
    print(f"Total inserted: {total}")
    return 0


def cmd_backtest(args) -> int:
    from backtest.engine import run_tps_backtest
    from loop.adaptive_loop import _load_strategy

    strategy = _load_strategy()
    result = run_tps_backtest(
        strategy,
        symbol=args.symbol,
        timeframes=settings.tps_timeframes,
        entry_timeframe=settings.tps_entry_timeframe,
    )
    print(json.dumps(result.metrics, indent=2, default=str))
    return 0


def cmd_live(args) -> int:
    from loop.adaptive_loop import build_scheduler, make_default_state
    from strategies.tps_websocket import TPSWebSocket

    paper = True
    if args.live:
        paper = False
    elif args.paper:
        paper = True
    else:
        paper = settings.execution_mode == "paper"

    state = make_default_state(paper=paper)
    sched = build_scheduler(state)
    logger.info(f"TPS live loop starting (paper={paper}, entry_tf={state.entry_timeframe}, "
                f"timeframes={state.timeframes}). Ctrl-C to stop.")

    async def _run():
        sched.start()
        ws = TPSWebSocket(
            state,
            settings.binance_api_key,
            settings.binance_api_secret,
            settings.binance_is_demo,
            timeframes=state.timeframes,
        )
        await ws.start(symbol=state.symbol)

    asyncio.run(_run())
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="tps")
    sub = p.add_subparsers(dest="command", required=True)

    f = sub.add_parser("fetch")
    f.add_argument("--symbol", default=settings.tps_symbol)
    f.add_argument("--days", type=int, default=120)
    f.set_defaults(func=cmd_fetch)

    b = sub.add_parser("backtest")
    b.add_argument("--symbol", default=settings.tps_symbol)
    b.set_defaults(func=cmd_backtest)

    l = sub.add_parser("live")
    l.add_argument("--paper", action="store_true")
    l.add_argument("--live", action="store_true")
    l.set_defaults(func=cmd_live)

    return p


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
