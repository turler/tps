"""TPS websocket entry point.

Bridges Binance candle-close / user-stream events to the multi-timeframe TPS
decision engine in ``loop.adaptive_loop``. On the *entry* timeframe's close it
evaluates a signal, publishes it to RabbitMQ, and places the order on Binance.
"""
from __future__ import annotations

from datetime import datetime, timezone

from loguru import logger

from config import settings
from data.websocket import BinanceWebSocket
from execution.portfolio import Position
from loop.adaptive_loop import append_closed_candle, evaluate_and_act, seed_history


class TPSWebSocket(BinanceWebSocket):

    # ── Lifecycle ────────────────────────────────────────────────

    async def on_connected(self):
        logger.info("🚀 TPS WebSocket connected. Seeding multi-timeframe history...")
        try:
            seed_history(self.state)
        except Exception as e:  # pragma: no cover - network
            logger.error(f"seed_history on connect failed: {e}")
            import traceback
            traceback.print_exc()

    # ── Market data ──────────────────────────────────────────────

    async def on_kline_close(self, msg: dict):
        kline = msg.get('k')
        if not kline or not kline.get('x'):
            return  # only act on a CLOSED candle ("phá" confirmation)
        timeframe = kline.get('i')
        if timeframe not in self.state.timeframes:
            return
        append_closed_candle(self.state, timeframe, kline)
        # A signal is only *emitted* when the entry timeframe closes; other TFs
        # just refresh the trend/structure context used by the decision engine.
        if timeframe == self.state.entry_timeframe:
            evaluate_and_act(self.state)

    # ── Order lifecycle ──────────────────────────────────────────

    async def on_order_filled(self, order_id, symbol, side, qty, price, pnl,
                              close_position, client_order_id):
        logger.info(f"Order filled order_id={order_id} client_order_id={client_order_id} "
                    f"close_position={close_position} pnl={pnl}")
        pos = self.state.portfolio.open_position

        # TP / SL exit on the open position.
        if close_position and pos and pos.broker_order_id == client_order_id:
            order = self.state.executor._orders.get(pos.tag)
            if pnl > 0:
                logger.info("Position hit TP")
                self.state.portfolio.record_close(datetime.now(timezone.utc), price, "TP",
                                                   self.state.strategy.name)
            else:
                logger.info("Position hit SL")
                self.state.portfolio.record_close(datetime.now(timezone.utc), price, "SL",
                                                   self.state.strategy.name)
            self.state.executor.cancel_all()
            return

        # Entry fill -> open the position and attach a protective SL.
        matched_tag = None
        for tag, broker_order_id in self.state.portfolio.pending_orders.items():
            if str(broker_order_id) == str(order_id) or str(order_id).startswith(str(broker_order_id)):
                matched_tag = tag
                break
        if matched_tag is None:
            return
        placed = self.state.executor._orders.get(matched_tag)
        if placed is None:
            return
        intent = placed.intent
        self.state.portfolio.open_position = Position(
            symbol=symbol, side=side, qty=float(qty),
            entry_price=price, tp=intent.take_profit, sl=intent.stop_loss,
            entry_time=datetime.now(timezone.utc),
            risk=float(intent.meta.get("risk", 0.0)),
            broker_order_id=placed.broker_order_id, tag=matched_tag,
        )
        if not self.state.executor.paper and self.state.executor._client is not None:
            try:
                await self.place_sl(symbol, side, qty, intent.stop_loss, matched_tag, placed)
            except Exception as e:  # pragma: no cover
                logger.error(f"place_sl failed: {e}")
        self.state.portfolio.pending_orders.pop(matched_tag, None)
        logger.info(f"Opened {side} position tag={matched_tag} @ {price}")

    async def on_order_cancel(self, order_id, client_order_id, symbol):
        found_tag = None
        for tag, broker_order_id in list(self.state.portfolio.pending_orders.items()):
            if str(broker_order_id) == str(order_id):
                found_tag = tag
                break
        if not found_tag:
            return
        logger.info(f"Detected cancellation of TPS order tag={found_tag}")
        if settings.send_copy_trade:
            self.state.executor._push_cancel_to_rabbitmq(found_tag)
        order = self.state.executor._orders.get(found_tag)
        if order and self.state.executor._client is not None:
            try:
                if order.tp_order_id:
                    self.state.executor._client.futures_cancel_order(symbol=symbol, algoId=order.tp_order_id)
                if order.sl_order_id:
                    self.state.executor._client.futures_cancel_order(symbol=symbol, algoId=order.sl_order_id)
            except Exception as e:  # pragma: no cover
                logger.warning(f"cancel TP/SL failed: {e}")
        self.state.portfolio.pending_orders.pop(found_tag, None)
        self.state.executor._orders.pop(found_tag, None)

    # ── Helpers ──────────────────────────────────────────────────

    async def place_sl(self, symbol, side, qty, price, tag, placed):
        """Attach a STOP_MARKET protective stop after a STOP-entry fills."""
        logger.info(f"🎯 Placing SL for filled entry {symbol} tag={tag}")
        sl_side = 'BUY' if side == 'SELL' else 'SELL'
        params = dict(
            symbol=symbol, side=sl_side,
            reduceOnly=True,
            quantity=qty,
            triggerPrice=str(self.state.executor.get_rounded_price(price)),
            type='STOP_MARKET',
            clientAlgoId=str(placed.broker_order_id) + 'SL',
        )
        res_sl = self.state.executor._client.futures_create_order(**params)
        placed.sl_order_id = str(res_sl.get('algoId'))
        self.state.executor._orders[tag] = placed
