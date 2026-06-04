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
        """Normalize a futures user-stream message and dispatch.

        Two event types matter here:
          * ORDER_TRADE_UPDATE — plain orders (LIMIT/MARKET entries and the
            reduce-only child fills spawned when a conditional order triggers).
            The reduce-only child fill carries `si` = the *parent* algoId, which
            links the close back to the TP/SL we placed.
          * ALGO_UPDATE — conditional/algo orders (STOP entries, TAKE_PROFIT_MARKET
            and STOP_MARKET exits). Identified by `aid` (algoId).

        We hand the subclass a single normalized dict so it never has to know
        which wire shape produced it. See `strategies/tps_websocket.py`.
        """
        event_type = msg.get('e')
        if event_type not in ('ORDER_TRADE_UPDATE', 'ALGO_UPDATE'):
            return

        o = msg['o']  # futures wraps the payload inside 'o'
        status = o.get('X')

        if event_type == 'ALGO_UPDATE':
            ev = {
                'event_type': event_type,
                'status': status,
                'order_id': str(o['aid']) if o.get('aid') is not None else '',
                # Algo events are matched by their own algoId (order_id); no
                # separate parent linkage is needed.
                'strategy_id': '',
                'client_order_id': str(o['caid']) if o.get('caid') else '',
                'order_type': o.get('o', ''),
                # No fill price on algo events; `tp` is the trigger price, which
                # for our reduce-only exits equals the TP/SL level.
                'price': float(o['tp']) if o.get('tp') else 0.0,
                'pnl': 0.0,
            }
        else:  # ORDER_TRADE_UPDATE
            ev = {
                'event_type': event_type,
                'status': status,
                'order_id': str(o['i']) if o.get('i') is not None else '',
                # `si` is the originating algoId for a triggered conditional
                # order's child fill (0/absent for ordinary orders).
                'strategy_id': str(o['si']) if o.get('si') else '',
                'client_order_id': str(o['c']) if o.get('c') else '',
                'order_type': o.get('o', ''),
                'price': float(o['L']) if o.get('L') else 0.0,  # last filled price
                'pnl': float(o['rp']) if o.get('rp') else 0.0,  # realized PnL
            }

        ev.update({
            'symbol': o.get('s'),
            'side': o.get('S'),
            'qty': o.get('q', '0.0'),
            'reduce_only': bool(o.get('R', False)),
            'close_position': bool(o.get('cp', False)),
        })

        if status in ('CANCELED', 'EXPIRED'):
            await self.on_order_cancel(ev)
        elif status in ('FILLED', 'TRIGGERED'):
            await self.on_order_filled(ev)

    async def on_order_filled(self, ev: dict):
        """Called when an order fills/triggers — override in subclass."""
        pass

    async def on_order_cancel(self, ev: dict):
        """Called when an order is cancelled/expired — override in subclass."""
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
