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
        trigger_price = self.get_rounded_price(trigger_price)
        if side == 'BUY':
            return trigger_price - self._tick_size*11
        else:
            return trigger_price + self._tick_size*11


    def place(self, intent: OrderIntent) -> PlacedOrder:
        if self.paper or self._client is None:
            order_id = f"paper-{len(self._orders)+1}"
            placed = PlacedOrder(intent=intent, broker_order_id=order_id)
            self._orders[intent.tag] = placed
            logger.info(f"[paper] placed {intent.side} {intent.order_type} qty={intent.qty} "
                        f"limit={intent.limit_price} trigger={intent.trigger_price}")
            return placed

        if intent.side == 'BUY':
            side = 'BUY'
            reverse_side = 'SELL'
        else:
            side = 'SELL'
            reverse_side = 'BUY'

        logger.info(intent)
        if intent.order_type == "STOP":
            # Both BUY-stop-above and SELL-stop-below are STOP_LOSS_LIMIT on Binance:
            # the engine triggers when price reaches stopPrice and submits a LIMIT.
            params = dict(
                symbol=self.symbol, side=side,
                quantity=round(intent.qty, self._quantity_precision),
                timeInForce=TIME_IN_FORCE_GTC,
                price=str(self.limit_price(side, intent.trigger_price)),
                triggerPrice=str(self.get_rounded_price(intent.trigger_price)),
                type="STOP",
            )
            logger.info("Params STOP order")
            logger.info(params)
            resp = self._client.futures_create_order(**params)

            if intent.take_profit:
                params = dict(
                    symbol=self.symbol, side=reverse_side,
                    reduceOnly=True,
                    price=str(self.get_rounded_price(intent.take_profit)),
                    triggerPrice=str(self.get_rounded_price(intent.trigger_price)),
                    quantity=round(intent.qty, self._quantity_precision),
                    type='TAKE_PROFIT',
                    clientAlgoId=str(resp.get("algoId"))+'TP',
                )
                logger.info("Params TP of stop order")
                logger.info(params)
                res_tp = self._client.futures_create_order(**params)
            placed = PlacedOrder(intent=intent, broker_order_id=str(resp.get("algoId")), tp_order_id=res_tp['algoId'], sl_order_id=None, status=resp.get("status", "NEW"))
        else: # LIMIT
            params = dict(
                symbol=self.symbol, side=side,
                quantity=round(intent.qty, self._quantity_precision),
                timeInForce=TIME_IN_FORCE_GTC,
                price=str(self.get_rounded_price(intent.limit_price)),
                type="LIMIT",
            )

            resp = self._client.futures_create_order(**params)

            if intent.stop_loss:
                params = dict(
                    symbol=self.symbol, side=reverse_side,
                    reduceOnly=True,
                    quantity=round(intent.qty, self._quantity_precision),
                    triggerPrice=str(self.get_rounded_price(intent.stop_loss)),
                    type='STOP_MARKET',
                    clientAlgoId=str(resp.get("orderId"))+'SL',
                )
                logger.info(params)
                res_sl = self._client.futures_create_order(**params)
                placed = PlacedOrder(intent=intent, broker_order_id=str(resp.get("orderId")), tp_order_id=None, sl_order_id=str(res_sl.get('algoId')), status=resp.get("status", "NEW"))

        self._orders[intent.tag] = placed
        logger.info(f"[live] placed order id={placed.broker_order_id}")

        return placed

    def cancel(self, tag: str) -> None:
        order = self._orders.pop(tag, None)
        if order is None:
            return
        # Publish signal to RabbitMQ for strategy followers if setting on
        if settings.send_copy_trade:
            self._push_cancel_to_rabbitmq(tag)

        if not self.paper and self._client is not None and order.broker_order_id:
            try:
                if order.intent.order_type == 'STOP':
                    self._client.futures_cancel_order(symbol=self.symbol, algoId=order.broker_order_id)
                else:
                    self._client.futures_cancel_order(symbol=self.symbol, orderId=order.broker_order_id)
                if order.tp_order_id:
                    self._client.futures_cancel_order(symbol=self.symbol, algoId=order.tp_order_id)
                if order.sl_order_id:
                    self._client.futures_cancel_order(symbol=self.symbol, algoId=order.sl_order_id)
            except Exception as e:  # pragma: no cover
                logger.warning(f"cancel failed: {e}")
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
            if order.tp_order_id:
                self.state.executor._client.futures_cancel_order(symbol=self.symbol, algoId=order.tp_order_id)
            if order.sl_order_id:
                self.state.executor._client.futures_cancel_order(symbol=self.symbol, algoId=order.sl_order_id)

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
