# data/websocket.py
"""Binance futures websocket wrapper for the TPS bot.

Subscribes to one kline stream per configured timeframe *plus* the user stream,
all concurrently. Candle-close events drive signal evaluation; user-stream
events drive fill / TP-SL / re-entry handling. Mirrors mint's BinanceWebSocket
but multiplexes several timeframes at once.
"""
import asyncio

from binance import AsyncClient, BinanceSocketManager
from loguru import logger

from loop.adaptive_loop import LoopState


class BinanceWebSocket:

    def __init__(self, state: LoopState, api_key: str, api_secret: str,
                 demo: bool = False, timeframes=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.demo = demo
        self.client = None
        self.bm = None
        self._running = False
        self.state = state
        self.timeframes = list(timeframes) if timeframes else ["15m"]

    # ── Setup / Teardown ─────────────────────────────────────────

    async def connect(self):
        self.client = await AsyncClient.create(
            api_key=self.api_key, api_secret=self.api_secret, demo=self.demo,
        )
        self.bm = BinanceSocketManager(self.client)
        self._running = True
        logger.info("✅ Binance WebSocket connected.")
        await self.on_connected()

    async def on_connected(self):
        """Called once when the WebSocket first connects — override in subclass."""
        pass

    async def disconnect(self):
        self._running = False
        if self.client:
            await self.client.close_connection()
            logger.info("🔌 Binance WebSocket disconnected.")

    # ── Stream Handlers ──────────────────────────────────────────

    async def watch_klines(self, symbol: str, interval: str):
        """Listen to a single timeframe's kline stream."""
        async with self.bm.kline_futures_socket(symbol, interval=interval) as stream:
            while self._running:
                msg = await stream.recv()
                await self.on_kline_close(msg)

    async def watch_user_stream(self):
        """Listen to the user account stream (order fills, balance)."""
        async with self.bm.futures_user_socket() as stream:
            while self._running:
                msg = await stream.recv()
                await self.on_user_event(msg)

    # ── Event Callbacks (override these) ─────────────────────────

    async def on_kline_close(self, msg: dict):
        """Handle a kline message — override in subclass."""
        kline = msg.get('k', {})
        logger.debug(f"kline {kline.get('i')} closed={kline.get('x')} c={kline.get('c')}")

    async def on_user_event(self, msg: dict):
        event_type = msg.get('e')
        if event_type not in ('ORDER_TRADE_UPDATE', 'ALGO_UPDATE'):
            return

        order = msg['o']  # futures wraps data inside 'o'
        status = order['X']
        if event_type == 'ALGO_UPDATE':
            order_id = str(order['aid'])
            client_order_id = str(order['caid']) if order.get('caid') else ""
        else:
            order_id = str(order['i'])
            client_order_id = str(order['c']) if order.get('c') else ""

        symbol = order['s']
        side = order['S']
        qty = order.get('q', '0.0')
        price = float(order['L']) if order.get('L') else 0.0
        pnl = float(order['rp']) if order.get('rp') else 0.0
        close_position = order.get('cp', False)

        if status in ('CANCELED', 'EXPIRED'):
            await self.on_order_cancel(order_id, client_order_id, symbol)
        elif status == 'FILLED':
            await self.on_order_filled(order_id, symbol, side, qty, price, pnl,
                                       close_position, client_order_id)

    async def on_order_filled(self, order_id, symbol, side, qty, price, pnl,
                              close_position, client_order_id):
        """Called when an order is filled — override in subclass."""
        pass

    async def on_order_cancel(self, order_id, client_order_id, symbol):
        """Called when an order is cancelled — override in subclass."""
        pass

    # ── Start All Streams ─────────────────────────────────────────

    async def start(self, symbol: str):
        """Connect and run the user stream + one kline stream per timeframe."""
        await self.connect()
        try:
            tasks = [self.watch_user_stream()]
            tasks += [self.watch_klines(symbol, tf) for tf in self.timeframes]
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Streams cancelled.")
        finally:
            await self.disconnect()
