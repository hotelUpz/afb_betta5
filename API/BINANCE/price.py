# ============================================================
# FILE: API/BINANCE/price.py
# ROLE: Binance USDT-M Futures HOT price stream via WS (aiohttp)
# STREAM: <symbol>@trade  (last trade price)
# NOTE: Single responsibility: ONLY hot price ticks (no funding, no mark price).
# ============================================================

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Iterable, List, Optional

import aiohttp


# @dataclass(frozen=True)
# class HotPriceTick:
#     symbol: str
#     price: float
#     qty: float
#     event_time_ms: int
#     trade_time_ms: int

@dataclass(frozen=True)
class HotPriceTick:
    symbol: str
    price: float
    event_time_ms: int

class BinanceHotPriceStream:
    """HOT price stream (trade ticks) for many symbols.

    Why chunking:
        For 200+ symbols, put symbols into chunks and open multiple WS connections.
        This avoids too-long URLs and keeps each socket manageable.

    WS base:
        wss://fstream.binance.com

    Combined streams URL:
        /stream?streams=btcusdt@trade/ethusdt@trade/...

    Callback:
        async def on_tick(tick: HotPriceTick) -> None
    """

    WS_BASE = "wss://fstream.binance.com"

    def __init__(
        self,
        symbols: Iterable[str],
        *,
        chunk_size: int = 80,
        ping_sec: float = 15.0,
        reconnect_min_sec: float = 1.0,
        reconnect_max_sec: float = 25.0,
        throttle_ms: int = 0,
    ):
        self.symbols = [s.upper().strip() for s in symbols if isinstance(s, str) and s.strip()]
        if not self.symbols:
            raise ValueError("symbols must be non-empty")

        self.chunk_size = max(1, int(chunk_size))
        self.ping_sec = float(ping_sec)
        self.reconnect_min_sec = float(reconnect_min_sec)
        self.reconnect_max_sec = float(reconnect_max_sec)
        self.throttle_ms = int(throttle_ms)

        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_emit_ms: Dict[str, int] = {}

    def stop(self) -> None:
        self._stop.set()

    async def aclose(self) -> None:
        """Stop all tasks and close session."""
        self._stop.set()
        for t in list(self._tasks):
            t.cancel()
        for t in list(self._tasks):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        self._tasks.clear()
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    @staticmethod
    def _to_float(v, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return default

    @staticmethod
    def _to_int(v, default: int = 0) -> int:
        try:
            return int(v)
        except Exception:
            return default

    def _chunks(self) -> List[List[str]]:
        out: List[List[str]] = []
        cur: List[str] = []
        for s in self.symbols:
            cur.append(s)
            if len(cur) >= self.chunk_size:
                out.append(cur)
                cur = []
        if cur:
            out.append(cur)
        return out

    @staticmethod
    def _make_url(symbols: List[str]) -> str:
        streams = "/".join([f"{s.lower()}@trade" for s in symbols])
        return f"{BinanceHotPriceStream.WS_BASE}/stream?streams={streams}"
    
    def _parse_tick(self, payload: Dict) -> Optional[HotPriceTick]:
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return None

        sym = data.get("s")
        if not sym:
            return None

        price = self._to_float(data.get("p"), 0.0)
        if price <= 0:
            return None

        return HotPriceTick(
            symbol=str(sym),
            price=price,
            event_time_ms=self._to_int(data.get("E"), int(time.time() * 1000)),
        )

    def _should_emit(self, sym: str, now_ms: int) -> bool:
        if self.throttle_ms <= 0:
            return True
        last = self._last_emit_ms.get(sym, 0)
        if now_ms - last >= self.throttle_ms:
            self._last_emit_ms[sym] = now_ms
            return True
        return False

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.ping_sec)
            if ws.closed:
                break
            try:
                await ws.ping()
            except Exception:
                break

    async def _run_chunk(self, symbols: List[str], on_tick: Callable[[HotPriceTick], Awaitable[None]]) -> None:
        backoff = self.reconnect_min_sec
        url = self._make_url(symbols)

        while not self._stop.is_set():
            ws = None
            ping_task = None
            try:
                assert self._session is not None
                ws = await self._session.ws_connect(url, autoping=False, max_msg_size=0)
                ping_task = asyncio.create_task(self._ping_loop(ws))
                backoff = self.reconnect_min_sec

                async for m in ws:
                    if self._stop.is_set():
                        break
                    if m.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(m.data)
                        except Exception:
                            continue
                        tick = self._parse_tick(payload)
                        if tick and tick.price > 0:
                            now_ms = tick.event_time_ms
                            if self._should_emit(tick.symbol, now_ms):
                                await on_tick(tick)
                    elif m.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                        break
            except asyncio.CancelledError:
                break
            except Exception:
                # reconnect with jitter
                sleep_for = min(self.reconnect_max_sec, backoff) * (0.7 + random.random() * 0.6)
                await asyncio.sleep(sleep_for)
                backoff = min(self.reconnect_max_sec, backoff * 1.7)
            finally:
                if ping_task:
                    ping_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await ping_task
                if ws is not None and not ws.closed:
                    with contextlib.suppress(Exception):
                        await ws.close()

    async def run(self, on_tick: Callable[[HotPriceTick], Awaitable[None]]) -> None:
        """Start stream tasks and block until stop() is called."""
        if self._session is not None:
            raise RuntimeError("Stream already running")

        self._session = aiohttp.ClientSession()
        try:
            for chunk in self._chunks():
                self._tasks.append(asyncio.create_task(self._run_chunk(chunk, on_tick)))

            await self._stop.wait()  # run until stop()

        finally:
            await self.aclose()


# # ----------------------------
# # SELF TEST (no CancelledError spam)
# # ----------------------------
async def _main():
    symbols = ['0GUSDT', '1000000BOBUSDT', '1000000MOGUSDT', '1000BONKUSDT', '1000CATUSDT', '1000CHEEMSUSDT', '1000FLOKIUSDT', '1000LUNCUSDT', '1000PEPEUSDT', '1000RATSUSDT', '1000SATSUSDT', '1000SHIBUSDT', '1000WHYUSDT', '1000XECUSDT', '1INCHUSDT', '1MBABYDOGEUSDT', '2ZUSDT', '4USDT', 'A2ZUSDT', 'AAVEUSDT', 'ACEUSDT', 'ACHUSDT', 'ACTUSDT', 'ACUUSDT', 'ACXUSDT', 'ADAUSDT', 'AERGOUSDT', 'AEROUSDT', 'AEVOUSDT', 'AGLDUSDT']  

    async def on_tick(t: HotPriceTick):
        print(f"{t.symbol:<8} price={t.price:<12} E={t.event_time_ms}")

    stream = BinanceHotPriceStream(symbols, chunk_size=50, throttle_ms=0)

    task = asyncio.create_task(stream.run(on_tick))
    try:
        await asyncio.sleep(100000)
    finally:
        stream.stop()
        await task

if __name__ == "__main__":
    asyncio.run(_main())
