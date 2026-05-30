"""Common interface for all trading strategies."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Literal, Optional


Side = Literal["BUY", "SELL"]
OrderType = Literal["LIMIT", "STOP", "MARKET"]


@dataclass
class OrderIntent:
    """A logical order the strategy wants to place. Engine/executor materialize it."""
    side: Side
    order_type: OrderType
    qty: float
    limit_price: float
    trigger_price: Optional[float] = None  # required for STOP
    current_price: Optional[float] = None
    take_profit: Optional[float] = None
    trigger_take_profit: Optional[float] = None
    trigger_stop_loss: Optional[float] = None
    stop_loss: Optional[float] = None
    tag: str = ""
    meta: dict = field(default_factory=dict)


@dataclass
class StrategyDecision:
    """What the strategy wants done now."""
    place: list[OrderIntent] = field(default_factory=list)
    cancel_tags: list[str] = field(default_factory=list)
    halt_until_next_signal: bool = False


class BaseStrategy(ABC):
    """Minimal common interface."""

    name: str = "base"

    def __init__(self, params: dict):
        self.params = dict(params)
        self.validate_params()

    @abstractmethod
    def validate_params(self) -> None:
        ...

    @abstractmethod
    def required_param_names(self) -> list[str]:
        ...

    def update_params(self, params: dict) -> None:
        self.params.update(params)
        self.validate_params()
