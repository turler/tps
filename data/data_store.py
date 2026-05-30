"""Persistence layer: SQLAlchemy models + helpers for OHLCV, trades, params."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterable, Iterator

import pandas as pd
from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy import delete

from config import settings


class Base(DeclarativeBase):
    pass


# SQLite only auto-increments columns declared exactly as `INTEGER PRIMARY KEY`
# (ROWID alias). A BIGINT primary key on SQLite enforces NOT NULL but is NOT
# auto-populated, which causes "NOT NULL constraint failed" on insert. Using
# `with_variant` keeps BIGINT on PostgreSQL while degrading to INTEGER on SQLite.
PK_BIGINT = BigInteger().with_variant(Integer(), "sqlite")


class OHLCV(Base):
    __tablename__ = "ohlcv"
    id = Column(PK_BIGINT, primary_key=True, autoincrement=True)
    symbol = Column(String(32), nullable=False)
    timeframe = Column(String(8), nullable=False)
    open_time = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint("symbol", "timeframe", "open_time", name="uq_ohlcv"),
        Index("ix_ohlcv_lookup", "symbol", "timeframe", "open_time"),
    )


class StrategyParams(Base):
    __tablename__ = "strategy_params"
    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy = Column(String(64), nullable=False)
    symbol = Column(String(32), nullable=False)
    timeframe = Column(String(8), nullable=False)
    params = Column(JSON, nullable=False)
    metric = Column(Float, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    is_active = Column(Integer, default=0, nullable=False)
    __table_args__ = (
        Index("ix_params_active", "strategy", "symbol", "timeframe", "is_active"),
    )


class Trade(Base):
    __tablename__ = "trades"
    id = Column(PK_BIGINT, primary_key=True, autoincrement=True)
    strategy = Column(String(64), nullable=False)
    symbol = Column(String(32), nullable=False)
    side = Column(String(8), nullable=False)  # BUY / SELL
    entry_time = Column(DateTime, nullable=False)
    entry_price = Column(Float, nullable=False)
    exit_time = Column(DateTime, nullable=True)
    exit_price = Column(Float, nullable=True)
    qty = Column(Float, nullable=False)
    pnl = Column(Float, nullable=True)
    reason = Column(String(32), nullable=True)  # TP / SL / CANCEL
    meta = Column(JSON, nullable=True)


_engine = create_engine(settings.database_url, future=True)
SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False, autoflush=False)


def init_db() -> None:
    Base.metadata.create_all(_engine)


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _upsert_dialect():
    return pg_insert if _engine.dialect.name == "postgresql" else sqlite_insert


# SQLite caps bound parameters per statement (SQLITE_MAX_VARIABLE_NUMBER:
# 999 on legacy builds, 32766 on modern ones). With 8 columns per OHLCV row
# a single multi-VALUES insert blows past the legacy limit at ~125 rows and
# the modern limit at ~4095 rows, which breaks intraday backfills. Chunking
# at 500 rows keeps us safely below both ceilings on every backend.
_OHLCV_INSERT_CHUNK = 500


def save_ohlcv(symbol: str, timeframe: str, rows: Iterable[dict]) -> int:
    """Insert OHLCV rows, skipping conflicts. Returns the actual inserted count."""
    rows = list(rows)
    if not rows:
        return 0
    payload = [
        {"symbol": symbol, "timeframe": timeframe, **r} for r in rows
    ]
    insert_fn = _upsert_dialect()
    inserted = 0
    with _engine.begin() as conn:
        for start in range(0, len(payload), _OHLCV_INSERT_CHUNK):
            chunk = payload[start:start + _OHLCV_INSERT_CHUNK]
            stmt = insert_fn(OHLCV.__table__).values(chunk)
            stmt = stmt.on_conflict_do_nothing(index_elements=["symbol", "timeframe", "open_time"])
            result = conn.execute(stmt)
            # rowcount reflects rows actually inserted (conflicts excluded). Some DBAPI
            # drivers report -1 when unknown; fall back to attempted count in that case.
            rc = getattr(result, "rowcount", None)
            inserted += rc if isinstance(rc, int) and rc >= 0 else len(chunk)
    return inserted

def delete_ohlcv(symbol: str, timeframe: str) -> int:
    """Delete all OHLCV records for a given symbol and timeframe.

    Returns the number of rows deleted.
    """
    with _engine.begin() as conn:
        stmt = (
            delete(OHLCV.__table__)
            .where(OHLCV.symbol == symbol)
            .where(OHLCV.timeframe == timeframe)
        )
        result = conn.execute(stmt)
        return result.rowcount

def load_ohlcv(
    symbol: str,
    timeframe: str,
    start: datetime | None = None,
    end: datetime | None = None,
) -> pd.DataFrame:
    stmt = select(OHLCV).where(OHLCV.symbol == symbol, OHLCV.timeframe == timeframe)
    if start is not None:
        stmt = stmt.where(OHLCV.open_time >= start)
    if end is not None:
        stmt = stmt.where(OHLCV.open_time <= end)
    stmt = stmt.order_by(OHLCV.open_time.asc())
    with session_scope() as s:
        rows = s.execute(stmt).scalars().all()
    if not rows:
        return pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(
        [{
            "open_time": r.open_time,
            "open": r.open, "high": r.high, "low": r.low,
            "close": r.close, "volume": r.volume,
        } for r in rows]
    ).set_index("open_time")
