"""Smoke test: load history and run TPS analysis on every timeframe.

    python test.py            # uses whatever is already in the DB
    python main.py fetch      # populate the DB first

Asserts the indicator pipeline computes without error and prints the current
per-timeframe bias + structure so a human can sanity-check against the chart.
"""
from __future__ import annotations

from config import settings
from data.data_store import load_ohlcv
from loop.adaptive_loop import _load_strategy


def main() -> int:
    strategy = _load_strategy()
    states = {}
    for tf in settings.tps_timeframes:
        df = load_ohlcv(settings.tps_symbol, tf)
        st = strategy.analyze_timeframe(tf, df)
        if st is None:
            print(f"{tf:>4}: not enough data ({len(df)} bars, need {strategy.min_bars()})")
            continue
        states[tf] = st
        sup = round(st.support_line.value_at(len(st.df) - 1), 2) if st.support_line else None
        res = round(st.resistance_line.value_at(len(st.df) - 1), 2) if st.resistance_line else None
        print(f"{tf:>4}: bias={st.bias:<7} close={st.close:<12} "
              f"ema34={st.ema_fast:.2f} ema89={st.ema_slow:.2f} "
              f"pivots={len(st.pivots)} support_tl={sup} resistance_tl={res}")

    if settings.tps_entry_timeframe in states:
        decision = strategy.decide(states, entry_timeframe=settings.tps_entry_timeframe)
        print("\nDecision on entry timeframe "
              f"{settings.tps_entry_timeframe}: "
              f"{[ (i.side, i.tag, i.meta.get('reason')) for i in decision.place ] or 'no signal'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
