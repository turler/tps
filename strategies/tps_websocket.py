"""TPS websocket entry point.

Bridges Binance candle-close / user-stream events to the multi-timeframe TPS
decision engine in ``loop.adaptive_loop``. On the *entry* timeframe's close it
evaluates a signal, publishes it to RabbitMQ, and places the order on Binance.

Order lifecycle (Binance USD-M Futures has no atomic OTOCO/bracket order, so TP
and SL cannot ride along on a pending order):

  1. ``evaluate_and_act`` places a bare STOP *entry* at the break level — no
     TP/SL attached.
  2. When the entry fills (``on_order_filled``), we open the Position and attach
     BOTH a reduce-only TAKE_PROFIT_MARKET and STOP_MARKET. Their algoIds are
     stored on the Position.
  3. When the position closes, the reduce-only fill (or the ALGO_UPDATE trigger)
     is matched back to the stored algoId — no ``clientAlgoId`` string parsing —
     telling us unambiguously whether it was TP or SL. The other protective
     order is cancelled and the close recorded. The next entry-timeframe close
     may then produce a fresh signal.
"""
from __future__ import annotations

import asyncio
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

    # ── matching helpers ─────────────────────────────────────────

    @staticmethod
    def _matches(tracked_id, *event_ids) -> bool:
        """True if `tracked_id` is one of the (non-empty) event ids."""
        return bool(tracked_id) and str(tracked_id) in {e for e in event_ids if e}

    def _find_pending_entry(self, order_id: str, strategy_id: str):
        """Return the tag of the pending entry whose broker id matches the event."""
        for tag, broker_order_id in self.state.portfolio.pending_orders.items():
            if self._matches(broker_order_id, order_id, strategy_id):
                return tag
        return None

    # ── fill / trigger handling ──────────────────────────────────

    async def on_order_filled(self, ev: dict):
        order_id = ev["order_id"]
        strategy_id = ev["strategy_id"]
        reduce_only = ev["reduce_only"]
        symbol = ev["symbol"] or self.state.symbol
        side = ev["side"]
        price = ev["price"]
        pnl = ev["pnl"]
        logger.info(
            f"Fill event type={ev['event_type']} status={ev['status']} "
            f"order_id={order_id} si={strategy_id} reduceOnly={reduce_only} "
            f"price={price} pnl={pnl}"
        )

        pos = self.state.portfolio.open_position

        # 1) EXIT — a reduce-only TP/SL of the open position fired.
        if pos is not None and reduce_only:
            if self._matches(pos.sl_order_id, order_id, strategy_id):
                await self._handle_exit(pos, price, "SL")
                return
            if self._matches(pos.tp_order_id, order_id, strategy_id):
                await self._handle_exit(pos, price, "TP")
                return
            # A reduce-only fill that closed the position but matched neither
            # tracked id (e.g. a manual close). Record by the nearest level and
            # cancel both protective orders.
            reason = "SL" if abs(price - pos.sl) <= abs(price - pos.tp) else "TP"
            logger.warning(f"Unmatched reduce-only close; recording as {reason}")
            self.state.executor.cancel_conditional(pos.tp_order_id)
            self.state.executor.cancel_conditional(pos.sl_order_id)
            self._record_and_cleanup(pos, price, reason)
            return

        # 2) ENTRY — a pending entry filled/triggered (never reduce-only).
        if pos is None and not reduce_only:
            tag = self._find_pending_entry(order_id, strategy_id)
            if tag is not None:
                await self._on_entry_filled(tag, symbol, side, price)
                return

        logger.info("Fill event did not match the open position or a pending entry; ignoring")

    async def _on_entry_filled(self, tag: str, symbol: str, side: str, fill_price: float):
        placed = self.state.executor._orders.get(tag)
        if placed is None:
            logger.warning(f"Entry filled but no placed order tracked for tag={tag}")
            return
        intent = placed.intent
        logger.info(f"Entry filled tag={tag} ({intent.order_type} {intent.side})")

        # Drop any other pendings (TPS places one entry at a time, but be safe)
        # and clear this one *before* the await below so a duplicate fill/trigger
        # event for the same entry finds no pending and is ignored.
        for other in list(self.state.portfolio.pending_orders.keys()):
            if other != tag:
                logger.info(f"Cancelling stale pending order: {other}")
                self.state.executor.cancel(other)
                self.state.portfolio.pending_orders.pop(other, None)
        self.state.portfolio.pending_orders.pop(tag, None)

        # Confirm the position is live before sending reduce-only TP/SL (a STOP
        # entry can report TRIGGERED a hair before the fill lands).
        qty, entry_price = await self._confirm_position(symbol, intent)
        if entry_price <= 0:
            entry_price = fill_price or intent.limit_price or intent.trigger_price or 0.0

        pos = Position(
            symbol=symbol, side=intent.side, qty=qty,
            entry_price=entry_price, tp=intent.take_profit, sl=intent.stop_loss,
            entry_time=datetime.now(timezone.utc),
            risk=float(intent.meta.get("risk", 0.0)),
            broker_order_id=placed.broker_order_id, tag=tag,
        )
        self.state.portfolio.open_position = pos

        # Attach BOTH protective orders now that we hold the position.
        pos.tp_order_id = self.state.executor.place_take_profit(pos.side, qty, intent.take_profit)
        pos.sl_order_id = self.state.executor.place_stop_loss(pos.side, qty, intent.stop_loss)
        placed.tp_order_id = pos.tp_order_id
        placed.sl_order_id = pos.sl_order_id
        logger.info(
            f"Opened {pos.side} qty={qty} @ {entry_price} | "
            f"TP algoId={pos.tp_order_id} (@{intent.take_profit}) "
            f"SL algoId={pos.sl_order_id} (@{intent.stop_loss})"
        )

    async def _handle_exit(self, pos: Position, exit_price: float, reason: str):
        """Record a TP/SL close and cancel the other protective order."""
        logger.info(f"Position hit {reason}")
        other_id = pos.tp_order_id if reason == "SL" else pos.sl_order_id
        self.state.executor.cancel_conditional(other_id)
        self._record_and_cleanup(pos, exit_price, reason)

    def _record_and_cleanup(self, pos: Position, exit_price: float, reason: str):
        """Record the close and clear per-position bookkeeping.

        `record_close` resets `open_position` to None, which is what makes the
        whole flow idempotent: a duplicate event for the same close (the algo
        trigger AND the reduce-only fill both arrive) is ignored on the second
        pass because there is no longer an open position to match.
        """
        price = exit_price if exit_price and exit_price > 0 else (pos.sl if reason == "SL" else pos.tp)
        self.state.portfolio.record_close(
            datetime.now(timezone.utc), price, reason, self.state.strategy.name
        )
        self.state.executor._orders.pop(pos.tag, None)
        self.state.portfolio.pending_orders.clear()

    async def _confirm_position(self, symbol: str, intent):
        """Return (qty, entry_price) of the live position.

        Retries briefly to absorb the trigger→fill race for STOP entries. In
        paper mode (no client) it trusts the intent.
        """
        client = self.state.executor._client
        if client is None:
            return intent.qty, float(intent.limit_price or intent.trigger_price or 0.0)
        for attempt in range(5):
            try:
                info = client.futures_position_information(symbol=symbol)
                if info:
                    amt = abs(float(info[0]["positionAmt"]))
                    if amt > 0:
                        return amt, float(info[0]["entryPrice"])
            except Exception as e:
                logger.warning(f"position confirm attempt {attempt} failed: {e}")
            await asyncio.sleep(0.4)
        logger.warning("Position not confirmed after retries; falling back to intent qty")
        return intent.qty, float(intent.limit_price or intent.trigger_price or 0.0)

    # ── cancellation handling ────────────────────────────────────

    async def on_order_cancel(self, ev: dict):
        order_id = ev["order_id"]
        strategy_id = ev["strategy_id"]
        tag = self._find_pending_entry(order_id, strategy_id)
        if tag is None:
            return
        logger.info(f"Detected cancellation of pending entry tag={tag} id={order_id}")
        if settings.send_copy_trade:
            self.state.executor._push_cancel_to_rabbitmq(tag)
        self.state.portfolio.pending_orders.pop(tag, None)
        self.state.executor._orders.pop(tag, None)
