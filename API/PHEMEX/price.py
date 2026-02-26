# ============================================================
# FILE: PHEMEX/price.py
# ROLE: Phemex USDT Perpetual HOT price stream via WS (aiohttp)
# STREAM: trade_p.subscribe (taker trades, fast)
# NOTE: Single responsibility: ONLY hot price ticks.
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


@dataclass(frozen=True)
class HotPriceTick:
    symbol: str
    price: float
    event_time_ms: int


class PhemexHotPriceStream:
    """HOT price stream using Phemex DataGW.

    Public WS endpoint MUST be exactly:
        wss://ws.phemex.com
    (No path/query is allowed.)

    Subscribe per symbol:
        {"id":1,"method":"trade_p.subscribe","params":["BTCUSDT"]}

    Trade message has:
        "symbol": "...", "trades_p": [[timestamp_ns, side, price, qty], ...]

    Design:
        - Chunk symbols to multiple WS connections if needed.
        - For each trade message, emit last trade price.
    """

    WS_URL = "wss://ws.phemex.com"

    def __init__(
        self,
        symbols: Iterable[str],
        *,
        chunk_size: int = 60,
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
        self._last_emit_ms: Dict[str, int] = {}
        self._id_counter = 1

    def stop(self) -> None:
        self._stop.set()

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

    def _next_id(self) -> int:
        self._id_counter += 1
        return self._id_counter

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

    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse, symbols: List[str]) -> None:
        # Small jitter so we don't spam GW
        for sym in symbols:
            req = {"id": self._next_id(), "method": "trade_p.subscribe", "params": [sym]}
            await ws.send_str(json.dumps(req))
            await asyncio.sleep(0.02)

    def _parse_trade_msg(self, payload: Dict) -> Optional[HotPriceTick]:
        if not isinstance(payload, dict):
            return None
        sym = payload.get("symbol")
        trades = payload.get("trades_p")
        if not sym or not isinstance(trades, list) or not trades:
            return None

        last = trades[-1]
        if not isinstance(last, (list, tuple)) or len(last) < 3:
            return None

        ts_ns = self._to_int(last[0], 0)
        price = self._to_float(last[2], 0.0)
        if price <= 0:
            return None

        # convert ns -> ms if looks like ns
        if ts_ns > 1_000_000_000_000:  # >1e12
            event_ms = ts_ns // 1_000_000
        else:
            event_ms = ts_ns

        return HotPriceTick(symbol=str(sym).upper(), price=price, event_time_ms=int(event_ms))

    async def _run_chunk(self, symbols: List[str], on_tick: Callable[[HotPriceTick], Awaitable[None]]) -> None:
        backoff = self.reconnect_min_sec

        while not self._stop.is_set():
            ws = None
            ping_task = None
            try:
                async with aiohttp.ClientSession() as session:
                    ws = await session.ws_connect(self.WS_URL, autoping=False, max_msg_size=0)
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    await self._subscribe(ws, symbols)
                    backoff = self.reconnect_min_sec

                    async for m in ws:
                        if self._stop.is_set():
                            break
                        if m.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(m.data)
                            except Exception:
                                continue

                            # ignore subscription replies {result:{status:"success"}}
                            if "result" in payload and "symbol" not in payload:
                                continue

                            tick = self._parse_trade_msg(payload)
                            if tick and self._should_emit(tick.symbol, tick.event_time_ms):
                                await on_tick(tick)

                        elif m.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                            break
            except asyncio.CancelledError:
                break
            except Exception:
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
        self._tasks = [asyncio.create_task(self._run_chunk(chunk, on_tick)) for chunk in self._chunks()]
        try:
            await self._stop.wait()
        finally:
            self.stop()
            for t in list(self._tasks):
                t.cancel()
            for t in list(self._tasks):
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t
            self._tasks.clear()


# ----------------------------
# SELF TEST (runs forever)
# ----------------------------
if __name__ == "__main__":
    async def _main():
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"]

        async def on_tick(t: HotPriceTick):
            print(f"{t.symbol:<10} price={t.price:<12} E={t.event_time_ms}")

        stream = PhemexHotPriceStream(symbols, chunk_size=50, throttle_ms=0)
        task = asyncio.create_task(stream.run(on_tick))
        try:
            await asyncio.sleep(10**9)
        finally:
            stream.stop()
            await task

    asyncio.run(_main())
