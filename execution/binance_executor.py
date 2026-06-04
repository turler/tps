"""Order placement against Binance (paper or live).

Provides a thin wrapper that translates strategy `OrderIntent` objects into
Binance orders via python-binance. In paper mode, orders are kept in memory.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional

import pika
from loguru import logger

from config import settings
from strategies.base_strategy import OrderIntent, BaseStrategy

from binance.client import Client
from binance.enums import (
    FUTURE_ORDER_TYPE_LIMIT,
    FUTURE_ORDER_TYPE_STOP,
    FUTURE_ORDER_TYPE_TAKE_PROFIT,
    FUTURE_ORDER_TYPE_STOP_MARKET,
    SIDE_BUY,
    SIDE_SELL,
    TIME_IN_FORCE_GTC,
)
from binance.helpers import round_step_size

@dataclass
class PlacedOrder:
    intent: OrderIntent
    broker_order_id: Optional[str]
    tp_order_id: Optional[str] = None
    sl_order_id: Optional[str] = None
    status: str = "NEW"
    # STOP entries are routed to Binance's conditional/algo endpoint and must be
    # cancelled by algoId; LIMIT entries are plain orders cancelled by orderId.
    is_algo: bool = False


@dataclass
class BinanceExecutor:
    symbol: str
    paper: bool = True
    _client: Optional["Client"] = None
    _orders: dict = field(default_factory=dict)  # tag -> PlacedOrder
    _tick_size: float = 0.1
    _quantity_precision: int = 3

    def __post_init__(self):
        if not self.paper and Client is not None and settings.binance_api_key:
            self._client = Client(
                api_key=settings.binance_api_key,
                api_secret=settings.binance_api_secret,
                testnet=settings.binance_testnet,
                demo=settings.binance_is_demo
            )
            info = self._client.futures_exchange_info()
            self._tick_size = self.get_tick_size(info, self.symbol)
            try:
                self._quantity_precision = next(si['quantityPrecision'] for si in info['symbols'] if si['symbol'] == self.symbol)
            except:
                pass

    # --- Public API used by the live loop ---

    def get_tick_size(self, info, symbol: str) -> float:
        for symbol_info in info['symbols']:
            if symbol_info['symbol'] == symbol:
                for symbol_filter in symbol_info['filters']:
                    if symbol_filter['filterType'] == 'PRICE_FILTER':
                        return float(symbol_filter['tickSize'])

    def get_rounded_price(self, price: float) -> float:
        return round_step_size(price, self._tick_size)


    def limit_price(self, side, trigger_price):
        # A stop entry must be marketable the moment it triggers, so its
        # protective limit is placed *through* the trigger: a BUY-stop fills as
        # price breaks upward, so the limit sits slightly ABOVE the trigger; a
        # SELL-stop fills as price breaks down, so the limit sits slightly BELOW.
        # (The previous direction put the limit on the wrong side, so stop
        # entries would only fill on a retrace and often never filled at all.)
        buffer = 10 * self._tick_size
        if side == 'BUY':
            return self.get_rounded_price(trigger_price + buffer)
        else:
            return self.get_rounded_price(trigger_price - buffer)


    def place(self, intent: OrderIntent) -> PlacedOrder:
        """Place the *entry* order only.

        Binance USD-M Futures has no atomic OTOCO/bracket order (OCO/OTOCO exist
        only for Spot), so TP and SL cannot be attached to a pending order. We
        therefore place the bare entry here and attach the reduce-only TP *and*
        SL once the entry actually fills — see `place_take_profit` /
        `place_stop_loss`, driven from the user-data stream.
        """
        is_algo = (intent.order_type == "STOP")
        if self.paper or self._client is None:
            order_id = f"paper-{len(self._orders)+1}"
            placed = PlacedOrder(intent=intent, broker_order_id=order_id, is_algo=is_algo)
            self._orders[intent.tag] = placed
            logger.info(f"[paper] placed {intent.side} {intent.order_type} qty={intent.qty} "
                        f"limit={intent.limit_price} trigger={intent.trigger_price}")
            return placed

        side = intent.side  # 'BUY' or 'SELL'
        logger.info(intent)
        if intent.order_type == "STOP":
            # STOP (stop-limit) entry — routed to the conditional/algo endpoint,
            # returns an algoId. No TP/SL attached.
            params = dict(
                symbol=self.symbol, side=side,
                quantity=round(intent.qty, self._quantity_precision),
                timeInForce=TIME_IN_FORCE_GTC,
                price=str(self.limit_price(side, intent.trigger_price)),
                triggerPrice=str(self.get_rounded_price(intent.trigger_price)),
                type="STOP",
                workingType="CONTRACT_PRICE",
            )
            logger.info(f"Placing STOP entry: {params}")
            resp = self._client.futures_create_order(**params)
            placed = PlacedOrder(intent=intent, broker_order_id=str(resp.get("algoId")),
                                 status=resp.get("algoStatus", "NEW"), is_algo=True)
        else:  # LIMIT
            params = dict(
                symbol=self.symbol, side=side,
                quantity=round(intent.qty, self._quantity_precision),
                timeInForce=TIME_IN_FORCE_GTC,
                price=str(self.get_rounded_price(intent.limit_price)),
                type="LIMIT",
            )
            logger.info(f"Placing LIMIT entry: {params}")
            resp = self._client.futures_create_order(**params)
            placed = PlacedOrder(intent=intent, broker_order_id=str(resp.get("orderId")),
                                 status=resp.get("status", "NEW"), is_algo=False)

        self._orders[intent.tag] = placed
        logger.info(f"[live] placed {intent.order_type} entry id={placed.broker_order_id}")
        return placed

    def place_take_profit(self, position_side: str, qty: float, tp_price: float,
                          working_type: str = "CONTRACT_PRICE") -> Optional[str]:
        """Place a reduce-only TAKE_PROFIT (limit) that closes the position.

        `position_side` is the side of the *open position* (BUY/SELL); the
        protective order is submitted on the opposite side. The resting limit
        sits at exactly `tp_price`; the conditional *triggers* ~10 ticks before
        that level (`limit_price(close_side, tp_price)`) so the limit is already
        working in the book by the time price reaches the TP — a maker fill at
        the TP price rather than a market exit. Returns the algoId.
        """
        close_side = SIDE_SELL if position_side == "BUY" else SIDE_BUY
        if self.paper or self._client is None:
            tp_id = f"paper-tp-{len(self._orders)+1}"
            logger.info(f"[paper] TP {close_side} qty={qty} limit={tp_price}")
            return tp_id
        params = dict(
            symbol=self.symbol, side=close_side, type="TAKE_PROFIT",
            quantity=round(qty, self._quantity_precision), reduceOnly=True,
            price=str(self.get_rounded_price(tp_price)),
            triggerPrice=str(self.limit_price(close_side, tp_price)),
            timeInForce=TIME_IN_FORCE_GTC,
            workingType=working_type,
        )
        logger.info(f"Placing TP (limit): {params}")
        resp = self._client.futures_create_order(**params)
        return str(resp.get("algoId"))

    def place_stop_loss(self, position_side: str, qty: float, sl_price: float,
                        working_type: str = "CONTRACT_PRICE") -> Optional[str]:
        """Place a reduce-only STOP_MARKET that closes the position at the stop.

        `position_side` is the side of the *open position* (BUY/SELL). Returns
        the algoId. STOP_MARKET guarantees the position is flattened on trigger.
        """
        close_side = SIDE_SELL if position_side == "BUY" else SIDE_BUY
        if self.paper or self._client is None:
            sl_id = f"paper-sl-{len(self._orders)+1}"
            logger.info(f"[paper] SL {close_side} qty={qty} trigger={sl_price}")
            return sl_id
        params = dict(
            symbol=self.symbol, side=close_side, type="STOP_MARKET",
            quantity=round(qty, self._quantity_precision), reduceOnly=True,
            triggerPrice=str(self.get_rounded_price(sl_price)),
            workingType=working_type,
        )
        logger.info(f"Placing SL: {params}")
        resp = self._client.futures_create_order(**params)
        return str(resp.get("algoId"))

    def cancel_conditional(self, algo_id: Optional[str]) -> None:
        """Cancel a single conditional/algo order by algoId (best-effort)."""
        if not algo_id or self.paper or self._client is None:
            return
        try:
            self._client.futures_cancel_order(symbol=self.symbol, algoId=algo_id)
            logger.info(f"cancelled conditional algoId={algo_id}")
        except Exception as e:  # pragma: no cover
            logger.warning(f"cancel conditional {algo_id} failed: {e}")

    def cancel(self, tag: str) -> None:
        order = self._orders.pop(tag, None)
        if order is None:
            return
        # Publish signal to RabbitMQ for strategy followers if setting on
        if settings.send_copy_trade:
            self._push_cancel_to_rabbitmq(tag)

        if not self.paper and self._client is not None and order.broker_order_id:
            try:
                if order.is_algo:
                    self._client.futures_cancel_order(symbol=self.symbol, algoId=order.broker_order_id)
                else:
                    self._client.futures_cancel_order(symbol=self.symbol, orderId=order.broker_order_id)
            except Exception as e:  # pragma: no cover
                logger.warning(f"cancel failed: {e}")
            # TP/SL protecting an open position are tracked on the Position, not
            # here, but cancel them too if this order ever carried them.
            self.cancel_conditional(order.tp_order_id)
            self.cancel_conditional(order.sl_order_id)
        logger.info(f"cancelled order tag={tag}")

    def cancel_all(self) -> None:
        for tag in list(self._orders.keys()):
            self.cancel(tag)

    def open_orders(self) -> list[PlacedOrder]:
        return list(self._orders.values())

    def fetch_status(self, tag: str) -> Optional[str]:
        order = self._orders.get(tag)
        if order is None:
            return None
        if self.paper or self._client is None:
            return order.status
        try:
            resp = self._client.futures_get_order(symbol=self.symbol, orderId=order.broker_order_id)
            order.status = resp.get("status", order.status)
        except Exception as e:  # pragma: no cover
            logger.warning(f"fetch_status failed: {e}")
        return order.status

    def close_position_market(self, position_side: str, qty: float, tag: str) -> Optional[str]:
        """Flatten an open position by submitting a market order on the opposite side.

        Returns the broker order id (or paper id) on success, None otherwise.
        """
        if int(qty) <= 0:
            return None
        order = self._orders.get(tag)
        if order is None:
            return None

        # Publish signal to RabbitMQ for strategy followers if setting on
        if settings.send_copy_trade:
            self._push_force_close_to_rabbitmq(tag, qty)

        opp = SIDE_SELL if position_side == "BUY" else SIDE_BUY
        if self.paper or self._client is None:
            order_id = f"paper-eod-{len(self._orders)+1}"
            logger.info(f"[paper] EOD market close {opp} qty={qty}")
            return order_id
        try:
            resp = self._client.futures_create_order(
                symbol=self.symbol, side=opp, type="MARKET",
                quantity=round(qty, self._quantity_precision), reduceOnly=True,
            )
            logger.info(f"[live] EOD market close id={resp.get('orderId')}")
            self.cancel_conditional(order.tp_order_id)
            self.cancel_conditional(order.sl_order_id)
            return str(resp.get("orderId"))
        except Exception as e:  # pragma: no cover
            logger.error(f"EOD market close failed: {e}")
            return None

    def _push_to_rabbitmq(self, intent: OrderIntent) -> None:
        """Publish order intent to RabbitMQ strategy_signals exchange."""
        import re
        tag_prefix = re.split(r'[-_]', intent.tag)[0] if intent.tag else "UNKNOWN"
        strategy_name = tag_prefix.upper()
        payload = {
            "symbol": self.symbol,
            "side": intent.side,
            "order_type": intent.order_type,
            "qty": intent.qty,
            "limit_price": intent.limit_price,
            "trigger_price": intent.trigger_price,
            "take_profit": intent.take_profit,
            "stop_loss": intent.stop_loss,
            "tag": intent.tag,
            "strategy": strategy_name,
            "meta": intent.meta,
        }
        try:
            connection = pika.BlockingConnection(
                pika.URLParameters(settings.celery_broker_url)
            )
            channel = connection.channel()
            channel.exchange_declare(
                exchange="strategy_signals", exchange_type="topic", durable=True
            )
            channel.basic_publish(
                exchange="strategy_signals",
                routing_key=strategy_name,
                body=json.dumps(payload),
                properties=pika.BasicProperties(delivery_mode=2),  # persistent
            )
            connection.close()
            logger.info(f"[rabbitmq] published signal strategy={strategy_name} tag={intent.tag}")
        except Exception as e:
            logger.warning(f"[rabbitmq] failed to publish signal: {e}")

    def _push_cancel_to_rabbitmq(self, tag: str) -> None:
        """Publish order cancel signal to RabbitMQ strategy_signals exchange."""
        import re
        tag_prefix = re.split(r'[-_]', tag)[0] if tag else "UNKNOWN"
        strategy_name = tag_prefix.upper()
        payload = {
            "action": "CANCEL",
            "tag": tag,
            "strategy": strategy_name,
        }
        try:
            connection = pika.BlockingConnection(
                pika.URLParameters(settings.celery_broker_url)
            )
            channel = connection.channel()
            channel.exchange_declare(
                exchange="strategy_signals", exchange_type="topic", durable=True
            )
            channel.basic_publish(
                exchange="strategy_signals",
                routing_key=strategy_name,
                body=json.dumps(payload),
                properties=pika.BasicProperties(delivery_mode=2),  # persistent
            )
            connection.close()
            logger.info(f"[rabbitmq] published cancel signal strategy={strategy_name} tag={tag}")
        except Exception as e:
            logger.warning(f"[rabbitmq] failed to publish cancel signal: {e}")

    def _push_force_close_to_rabbitmq(self, tag: str, qty: float) -> None:
        """Publish order force close signal to RabbitMQ strategy_signals exchange."""
        import re
        tag_prefix = re.split(r'[-_]', tag)[0] if tag else "UNKNOWN"
        strategy_name = tag_prefix.upper()
        payload = {
            "action": "FORCE_CLOSE",
            "tag": tag,
            "qty": qty,
            "symbol": self.symbol,
            "strategy": strategy_name,
        }
        try:
            connection = pika.BlockingConnection(
                pika.URLParameters(settings.celery_broker_url)
            )
            channel = connection.channel()
            channel.exchange_declare(
                exchange="strategy_signals", exchange_type="topic", durable=True
            )
            channel.basic_publish(
                exchange="strategy_signals",
                routing_key=strategy_name,
                body=json.dumps(payload),
                properties=pika.BasicProperties(delivery_mode=2),  # persistent
            )
            connection.close()
            logger.info(f"[rabbitmq] published force close signal strategy={strategy_name} tag={tag} qty={qty}")
        except Exception as e:
            logger.warning(f"[rabbitmq] failed to publish force close signal: {e}")
