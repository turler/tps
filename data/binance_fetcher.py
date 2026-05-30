"""Fetch historical and live klines from Binance using ccxt."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import ccxt
import pandas as pd
from loguru import logger

from data.data_store import init_db, load_ohlcv, save_ohlcv, delete_ohlcv


_TF_TO_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000, "3d": 259_200_000,
    "1w": 604_800_000,
}


def _make_exchange() -> ccxt.binance:
    """Exchange instance for *market data*.

    Market data (klines, tickers) is always fetched from Binance production —
    the testnet exposes only a tiny window of synthetic candles, which yields
    near-empty datasets and unrealistic OHLC ranges. Testnet mode is honored
    by the order-placement layer (`execution/binance_executor.py`) instead.
    """
    params = {
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
    }
    # Public klines do not require auth; only attach keys if you actually want
    # higher rate limits via signed requests for private data.
    return ccxt.binance(params)


def _to_ccxt_symbol(symbol: str) -> str:
    # BTCUSDT -> BTC/USDT (assume USDT quote)
    if "/" in symbol:
        return symbol
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}/USDT"
    if symbol.endswith("BUSD"):
        return f"{symbol[:-4]}/BUSD"
    return symbol


def fetch_klines(
    symbol: str,
    timeframe: str = "1d",
    since: Optional[datetime] = None,
    limit_per_call: int = 1000,
) -> pd.DataFrame:
    """Fetch OHLCV from Binance starting at `since` until now. Returns DataFrame."""
    ex = _make_exchange()
    market_symbol = _to_ccxt_symbol(symbol)
    tf_ms = _TF_TO_MS[timeframe]
    if since is None:
        since = datetime.now(timezone.utc) - timedelta(days=365)
    since_ms = int(since.timestamp() * 1000)

    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    start_of_today = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    start_of_today_ms = int(start_of_today.timestamp() * 1000)
    end_ms = start_of_today_ms + (now_ms - start_of_today_ms)//tf_ms*tf_ms - 1

    all_rows: list[list] = []
    cursor = since_ms
    while cursor < end_ms:
        batch = ex.fetch_ohlcv(market_symbol, timeframe=timeframe, since=cursor, limit=limit_per_call, params={'until': end_ms})
        if not batch:
            break
        all_rows.extend(batch)
        last = batch[-1][0]
        next_cursor = last + tf_ms
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(batch) < limit_per_call:
            break

    if not all_rows:
        return pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(all_rows, columns=["open_time", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset="open_time").sort_values("open_time")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert(None)
    return df.reset_index(drop=True)


def update_history(symbol: str, timeframe: str = "1d", lookback_days: int = 365) -> int:
    """Fetch and persist klines into the DB. Returns number of rows inserted."""
    init_db()
    existing = load_ohlcv(symbol, timeframe)
    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    since = since.replace(tzinfo=None)
    should_delete=False
    if not existing.empty and existing.index.min() < since:
        last_ts = existing.index.max()
        since = last_ts.to_pydatetime() + timedelta(milliseconds=_TF_TO_MS[timeframe])
    else:
        should_delete=True
        logger.info("lookback_days deeper than current db, try to fetch, delete old and save new")
        since = datetime(since.year, since.month, since.day, tzinfo=timezone.utc)
        since = since.replace(tzinfo=None)
    logger.info(f"Fetching {symbol} {timeframe} since {since}")
    df = fetch_klines(symbol, timeframe, since=since)
    if df.empty:
        logger.info("No new rows.")
        return 0
    rows = df.to_dict(orient="records")
    if should_delete:
        delete_ohlcv(symbol, timeframe)
        logger.info(f"Deleted old rows for {symbol} {timeframe}")
    n = save_ohlcv(symbol, timeframe, rows)
    logger.info(f"Saved {n} rows for {symbol} {timeframe}")
    return n


def fetch_latest_price(symbol: str) -> float:
    ex = _make_exchange()
    ticker = ex.fetch_ticker(_to_ccxt_symbol(symbol))
    return float(ticker["last"])
