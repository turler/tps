"""In-memory + DB-backed portfolio tracker."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from data.data_store import Trade, session_scope


@dataclass
class Position:
    symbol: str
    side: str            # BUY / SELL
    qty: float
    entry_price: float
    tp: float
    sl: float
    entry_time: datetime
    risk: float
    broker_order_id: Optional[str] = None
    tag: str = ""


@dataclass
class Portfolio:
    open_position: Optional[Position] = None
    pending_orders: dict = field(default_factory=dict)  # tag -> broker order id
    halt_until_next_signal: bool = False

    def has_position(self) -> bool:
        return self.open_position is not None

    def has_pending(self) -> bool:
        return bool(self.pending_orders)

    def record_close(self, exit_time: datetime, exit_price: float, reason: str,
                     strategy: str) -> None:
        if self.open_position is None:
            return
        pos = self.open_position
        diff = exit_price - pos.entry_price
        if pos.side == "SELL":
            diff = -diff
        pnl = diff * pos.qty
        with session_scope() as s:
            s.add(Trade(
                strategy=strategy, symbol=pos.symbol, side=pos.side,
                entry_time=pos.entry_time, entry_price=pos.entry_price,
                exit_time=exit_time, exit_price=exit_price, qty=pos.qty,
                pnl=pnl, reason=reason,
                meta={"risk": pos.risk},
            ))
        self.open_position = None
