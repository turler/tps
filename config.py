"""Centralized configuration for the TPS (Thùng Phá Sảnh) trading bot.

TPS combines three classic price-action pillars from the source method doc:
  * DOW theory        -> swing-structure (pivot) breaks
  * EMA34 / EMA89     -> trend filter + VGT (value-zone) entries
  * Trendline         -> hard support/resistance breaks

Signals are computed on Binance websocket *candle close* across multiple
timeframes, then published to RabbitMQ and (optionally) placed on Binance
futures — mirroring the sibling `mint` bot.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "var"
DATA_DIR.mkdir(exist_ok=True)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ROOT_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Binance
    binance_api_key: str = ""
    binance_api_secret: str = ""
    binance_testnet: bool = False
    binance_is_demo: bool = True

    # Database / messaging
    database_url: str = f"sqlite:///{DATA_DIR / 'tps.db'}"
    celery_broker_url: str = Field("amqp://rabbitmq", alias="CELERY_BROKER_URL")
    send_copy_trade: bool = False

    # Trading defaults
    tps_symbol: str = "BTCUSDC"
    # Full multi-timeframe set: M1 / M5 / M15 / M30 / H1 / H4 / D1
    tps_timeframes: List[str] = ["15m", "30m", "1h", "4h", "1d"]
    tps_entry_timeframe: str = "15m"   # primary timeframe a signal is emitted on
    tps_news_filter: bool = False       # stubbed v1; flatten before red USD news
    default_risk_usdt: float = 5.0

    # Scheduler
    optimize_every_n_days: int = 30
    oos_days: int = 30

    # Execution mode
    execution_mode: Literal["paper", "live"] = "paper"

    # back-compat aliases used by copied mint infra (executor reads default_symbol)
    @property
    def default_symbol(self) -> str:
        return self.tps_symbol

    @property
    def default_timeframe(self) -> str:
        return self.tps_entry_timeframe


settings = Settings()


# Default TPS parameters (applied to every timeframe's analysis).
TPS_DEFAULT_PARAMS: dict = {
    "ema_fast": 34,                 # fast EMA period
    "ema_slow": 89,                 # slow EMA period
    "pivot_window": 2,              # fractal neighbours each side for swing pivots
    "sr_lookback": 60,              # candles kept for hard S/R level detection
    "sideway_window": 12,           # window to count EMA crosses -> sideway
    "sideway_crosses": 3,           # >= this many crosses in window => sideway, sit out
    "tp_mode": "ir",               # "ir" (1R from XTC1 size) | "hard_sr"
    "min_rr": 1.0,                  # minimum reward:risk to accept a signal
    "risk_amount_per_position": 5.0,
    # Higher TFs that set the long-term bias (per "CÁC BƯỚC ĐỂ VÀO LỆNH").
    "bias_timeframes": ["4h", "1d"],
    # TFs where price between the two EMAs forces a sit-out.
    "ema_gate_timeframes": ["15m", "30m", "1h", "4h", "1d"],
}


# Optuna-style search space (kept for parity with mint; optimizer is optional).
TPS_SEARCH_SPACE: dict = {
    "pivot_window": {"type": "int", "low": 2, "high": 5, "step": 1},
    "sideway_window": {"type": "int", "low": 6, "high": 20, "step": 2},
    "sideway_crosses": {"type": "int", "low": 2, "high": 5, "step": 1},
    "min_rr": {"type": "float", "low": 1.0, "high": 3.0, "step": 0.5},
    "tp_mode": {"type": "categorical", "choices": ["ir", "hard_sr"]},
}
