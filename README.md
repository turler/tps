# TPS — Thùng Phá Sảnh strategy bot

A multi-timeframe price-action bot implementing the **TPS** method (DOW theory +
EMA34/EMA89 + Trendline) from `../tps_ocr/combined_wsl.txt`. On every Binance
websocket **candle close** it evaluates a signal, publishes it to **RabbitMQ**, and
places orders on Binance futures.

---

## Quick start

```bash
cp .env.example .env          # fill in your keys / broker URL
pip install -r requirements.txt

python main.py fetch          # pull OHLCV for all timeframes
python test.py                # print current per-TF bias (sanity check)
python main.py backtest       # run event-driven backtest, print metrics
python main.py live --paper   # paper-trade; publishes signals to RabbitMQ
python main.py live --live    # place real Binance futures orders
```

---

## Commands

### `fetch` — pull OHLCV history

Downloads candles for **every configured timeframe** and stores them in `var/tps.db`.
Run this before `backtest` or `live`.

```
python main.py fetch [--symbol SYMBOL] [--days DAYS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--symbol` | `BTCUSDT` (from `TPS_SYMBOL` env) | Futures symbol to fetch |
| `--days` | `120` | Lookback in calendar days |

**Example**
```bash
python main.py fetch --symbol ETHUSDT --days 365
```

---

### `backtest` — event-driven multi-TF backtest

Replays closed candles bar-by-bar on the entry timeframe, reconstructing every
higher TF's state with no lookahead, and simulates STOP entries with SL/TP exits.
Prints JSON metrics to stdout.

```
python main.py backtest [--symbol SYMBOL]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--symbol` | `BTCUSDT` | Symbol to backtest (must be fetched first) |

The timeframes and entry timeframe are taken from the `.env` /
`TPS_TIMEFRAMES` / `TPS_ENTRY_TIMEFRAME` settings.

**Example output**
```json
{
  "n_trades": 42,
  "wins": 24,
  "losses": 18,
  "win_rate": 0.571,
  "total_pnl": 87.5,
  "avg_pnl": 2.08,
  "profit_factor": 1.75,
  "expectancy": 2.08,
  "max_drawdown": -22.0
}
```

---

### `live` — live / paper trading

Connects to Binance websocket, subscribes to all configured timeframes, and runs
the TPS signal engine on every closed candle.

```
python main.py live [--paper | --live]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--paper` | *(from `EXECUTION_MODE` env)* | Simulate orders in memory; no real orders |
| `--live` | | Place real Binance futures orders |

If neither flag is given, `EXECUTION_MODE` env var decides (`paper` by default).

**Examples**
```bash
python main.py live --paper          # safe simulation, publishes to RabbitMQ if SEND_COPY_TRADE=true
python main.py live --live           # real orders, requires API keys
```

---

### `test.py` — indicator smoke-check (not a pytest)

Reads the DB and prints the current EMA bias, EMA values, pivot count, and trendline
levels for every timeframe. Also runs `decide()` and shows whether a signal would
fire right now. No network calls.

```bash
python test.py
```

Sample output:
```
  1m: bias=BUY     close=65120.4    ema34=64980.12 ema89=64502.33 pivots=41 support_tl=64800.0 resistance_tl=65400.0
  5m: bias=BUY     close=65120.4    ...
 15m: bias=NEUTRAL close=65120.4    ...
 ...
Decision on entry timeframe 15m: no signal
```

---

## Environment variables (`.env`)

All settings are loaded from `tps/.env` (copy from `.env.example`).

### Binance connection

| Variable | Default | Description |
|----------|---------|-------------|
| `BINANCE_API_KEY` | *(empty)* | Required only for `--live` |
| `BINANCE_API_SECRET` | *(empty)* | Required only for `--live` |
| `BINANCE_TESTNET` | `false` | Route order placement to Binance testnet |
| `BINANCE_IS_DEMO` | `true` | Use Binance demo/paper account for order placement |

> Market data (klines) is always fetched from Binance production regardless of these flags.

### RabbitMQ / messaging

| Variable | Default | Description |
|----------|---------|-------------|
| `CELERY_BROKER_URL` | `amqp://rabbitmq` | RabbitMQ connection string |
| `SEND_COPY_TRADE` | `false` | Set `true` to publish signals to the `strategy_signals` exchange |

### Trading

| Variable | Default | Description |
|----------|---------|-------------|
| `TPS_SYMBOL` | `BTCUSDT` | Futures symbol to trade |
| `TPS_ENTRY_TIMEFRAME` | `15m` | Timeframe on which signals are emitted |
| `TPS_TIMEFRAMES` | `["1m","5m","15m","30m","1h","4h","1d"]` | All timeframes to subscribe and analyse |
| `TPS_NEWS_FILTER` | `false` | Stub (v1): flatten before red USD news events |
| `DEFAULT_RISK_USDT` | `5.0` | USDT risked per trade (`risk_amount_per_position`) |
| `EXECUTION_MODE` | `paper` | `paper` (in-memory) or `live` (real orders) |

### Database

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `sqlite:///var/tps.db` | SQLAlchemy DB URL; change to PostgreSQL for production |

---

## Strategy parameters (`TPS_DEFAULT_PARAMS` in `config.py`)

These control the signal logic. Edit `config.py` directly to change defaults; they
are not currently exposed as CLI flags.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ema_fast` | `34` | Fast EMA period (EMA34 in the doc) |
| `ema_slow` | `89` | Slow EMA period (EMA89 in the doc) |
| `pivot_window` | `2` | Fractal swing-pivot neighbour window — bars each side of a high/low that must be lower/higher for it to qualify as a pivot (XTC1/XTC2 structure) |
| `sr_lookback` | `60` | Number of recent pivots kept as hard support/resistance levels (TP targets) |
| `sideway_window` | `12` | Candle window used to count EMA crosses for sideway detection |
| `sideway_crosses` | `3` | If price crosses EMA89 ≥ this many times in `sideway_window` → SIDEWAY → sit out |
| `tp_mode` | `"ir"` | Take-profit mode: `"ir"` = 1R (distance = XTC1 swing size); `"hard_sr"` = nearest hard S/R pivot level |
| `min_rr` | `1.0` | Minimum reward-to-risk ratio required to emit a signal; signals with R:R below this are discarded |
| `risk_amount_per_position` | `5.0` | USDT risked per trade. Position size = `risk / (entry - stop_loss)` |
| `bias_timeframes` | `["4h","1d"]` | Timeframes whose EMA bias sets the long-term trend direction (must agree for a directional bias to be established) |
| `ema_gate_timeframes` | `["15m","30m","1h","4h","1d"]` | If price is between EMA34 and EMA89 (NEUTRAL) on **any** of these TFs → sit out entirely |

### TPS signal logic (summary)

A signal fires on the **entry timeframe** close when all three hold in the same direction:

1. **Trendline break** *(mandatory)* — candle closes through the projected support/resistance trendline
2. **DOW break** — candle closes beyond the last confirmed swing high (BUY) or low (SELL)
3. **EMA aligned** — candle body is fully above both EMAs with EMA34 > EMA89 (BUY), or fully below with EMA34 < EMA89 (SELL)

When H4/D1 agree on a trend but no TPS setup fires on the entry TF, the bot falls back to a **VGT** (value-zone) entry: price poked through EMA34 but the last candle closed back on the trend side.

| Level | Action |
|-------|--------|
| Entry | Break-close price (STOP order at candle close) |
| Stop-loss | Opposite XTC2 pivot wick |
| Take-profit | Nearest hard S/R (`tp_mode=hard_sr`) **or** 1R from the XTC1 swing (`tp_mode=ir`) |

---

## RabbitMQ signal schema

When `SEND_COPY_TRADE=true`, every signal is published to exchange `strategy_signals`
(topic, durable) with routing key `TPS`. Consumers bind `*.TPS` or `TPS` to receive.

```json
{
  "symbol":        "BTCUSDT",
  "side":          "BUY",
  "order_type":    "STOP",
  "qty":           0.008,
  "limit_price":   65000.0,
  "trigger_price": 65000.0,
  "take_profit":   65800.0,
  "stop_loss":     64000.0,
  "tag":           "tps_buy",
  "strategy":      "TPS",
  "meta": {
    "timeframe": "15m",
    "reason":    "TPS",
    "risk":      5.0,
    "rr":        0.8
  }
}
```

Cancel messages use `{"action":"CANCEL","tag":"tps_buy","strategy":"TPS"}`.
Force-close messages use `{"action":"FORCE_CLOSE","tag":"tps_buy","qty":0.008,"symbol":"BTCUSDT","strategy":"TPS"}`.

---

## Project layout

```
tps/
  config.py                 Settings (env vars) + TPS_DEFAULT_PARAMS + TPS_SEARCH_SPACE
  main.py                   CLI: fetch / backtest / live
  test.py                   Indicator smoke-check (reads DB, no network)
  .env.example              Copy to .env and fill in credentials
  requirements.txt

  strategies/
    indicators.py           Pure functions: EMA, swing pivots, trendline, S/R, bias
    tps_strategy.py         TimeframeState + multi-TF decide() (the signal engine)
    tps_websocket.py        Websocket → decision → publish + place orders
    base_strategy.py        OrderIntent / StrategyDecision contracts (shared with mint)

  data/
    websocket.py            Multi-TF kline streams + user stream (asyncio)
    binance_fetcher.py      ccxt OHLCV fetcher + fetch_latest_price
    data_store.py           SQLAlchemy models: OHLCV / Trade / StrategyParams

  execution/
    binance_executor.py     Order placement (paper/live) + _push_to_rabbitmq
    portfolio.py            In-memory + DB position / trade tracker

  loop/
    adaptive_loop.py        LoopState, candle ingestion, evaluate_and_act, scheduler

  backtest/
    engine.py               No-lookahead multi-TF event-driven backtester

  var/
    tps.db                  SQLite database (auto-created on first run)
```

---

## Notes

- Market data is always fetched from Binance **production** (testnet klines are too sparse).
  Order placement respects `BINANCE_TESTNET` / `BINANCE_IS_DEMO`.
- `TPS_NEWS_FILTER` is stubbed in v1 — the interface exists but does nothing.
- To register `tps/` as a real git submodule, create a remote repo and run:
  ```bash
  git submodule add <remote-url> tps
  ```
