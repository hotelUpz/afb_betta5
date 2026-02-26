
# ============================================================
# FILE: API/BITGET/price.py
# ROLE: Bitget USDT-M Futures HOT-ish price stream via WS (aiohttp)
# CHANNEL: ticker
# NOTE:
#   - Single responsibility: ONLY hot price ticks (price + event_time_ms).
#   - Uses Bitget WebSocket V2 public endpoint.
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


class BitgetHotPriceStream:
    """Bitget ticker stream for many symbols (chunked).

    WS:
        wss://ws.bitget.com/v2/ws/public

    Subscribe message (per docs):
        {
          "op": "subscribe",
          "args": [{"instType":"USDT-FUTURES","channel":"ticker","instId":"BTCUSDT"}, ...]
        }

    Callback:
        async def on_tick(tick: HotPriceTick) -> None
    """

    WS_URL = "wss://ws.bitget.com/v2/ws/public"

    def __init__(
        self,
        symbols: Iterable[str],
        *,
        inst_type: str = "USDT-FUTURES",
        chunk_size: int = 100,
        throttle_ms: int = 0,
        ping_interval_sec: float = 25.0,
        reconnect_global_gap_sec: float = 0.35,
    ):
        self.inst_type = str(inst_type).upper().strip()
        self.symbols = [str(s).upper().strip() for s in symbols if str(s).strip()]
        self.chunk_size = max(1, int(chunk_size))
        self.throttle_ms = max(0, int(throttle_ms))
        self.ping_interval_sec = float(ping_interval_sec)

        # unified stop signal (canon with other streams)
        self._stop_evt = asyncio.Event()

        self._session: Optional[aiohttp.ClientSession] = None

        # per-symbol throttle
        self._last_emit_ms: Dict[str, int] = {}

        # global reconnect limiter (shared across chunks)
        self._reconnect_lock = asyncio.Lock()
        self._reconnect_next_ok_ts = 0.0  # monotonic seconds
        self._reconnect_gap_sec = max(0.05, float(reconnect_global_gap_sec))

    def stop(self) -> None:
        self._stop_evt.set()

    async def aclose(self) -> None:
        if self._session is not None:
            with contextlib.suppress(Exception):
                await self._session.close()
        self._session = None

    @staticmethod
    def _to_float(v) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    @staticmethod
    def _to_int(v) -> int:
        try:
            return int(float(v))
        except Exception:
            return 0

    @staticmethod
    def _chunks(xs: List[str], n: int) -> List[List[str]]:
        return [xs[i : i + n] for i in range(0, len(xs), n)]

    def _should_emit(self, sym: str, now_ms: int) -> bool:
        if self.throttle_ms <= 0:
            return True
        last = self._last_emit_ms.get(sym, 0)
        if now_ms - last >= self.throttle_ms:
            self._last_emit_ms[sym] = now_ms
            return True
        return False

    async def _await_reconnect_slot(self) -> None:
        """Prevent reconnect stampede across chunks."""
        if self._stop_evt.is_set():
            return
        async with self._reconnect_lock:
            now = time.monotonic()
            allowed_at = max(self._reconnect_next_ok_ts, now)
            jitter = random.random() * 0.25
            self._reconnect_next_ok_ts = allowed_at + self._reconnect_gap_sec + jitter
            wait = max(0.0, allowed_at - now)
        if wait > 0 and not self._stop_evt.is_set():
            await asyncio.sleep(wait)

    async def _ws_ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        # Bitget uses plain 'ping'/'pong' (text) on public WS.
        while not self._stop_evt.is_set():
            await asyncio.sleep(self.ping_interval_sec)
            if self._stop_evt.is_set():
                break
            with contextlib.suppress(Exception):
                await ws.send_str("ping")

    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse, symbols: List[str]) -> None:
        args = [{"instType": self.inst_type, "channel": "ticker", "instId": s} for s in symbols]
        msg = {"op": "subscribe", "args": args}
        await ws.send_str(json.dumps(msg))

    def _parse_tick(self, payload: dict) -> List[HotPriceTick]:
        # Expected push format:
        # {"data":[{"lastPr":"...","instId":"BTCUSDT","ts":"..."}, ...], "arg":{...}}
        data = payload.get("data")
        if not isinstance(data, list):
            return []

        out: List[HotPriceTick] = []
        for it in data:
            if not isinstance(it, dict):
                continue
            sym = str(it.get("instId") or it.get("symbol") or "").upper().strip()
            if not sym:
                continue
            price = self._to_float(it.get("lastPr"))
            ts = self._to_int(it.get("ts") or payload.get("ts"))
            if ts <= 0:
                ts = int(time.time() * 1000)
            out.append(HotPriceTick(symbol=sym, price=price, event_time_ms=ts))
        return out

    async def _run_chunk(self, symbols: List[str], on_tick: Callable[[HotPriceTick], Awaitable[None]]) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession()

        backoff = 0.5
        while not self._stop_evt.is_set():
            await self._await_reconnect_slot()
            if self._stop_evt.is_set():
                break
            try:
                async with self._session.ws_connect(self.WS_URL, heartbeat=None) as ws:
                    await self._subscribe(ws, symbols)

                    ping_task = asyncio.create_task(self._ws_ping_loop(ws))
                    try:
                        async for msg in ws:
                            if self._stop_evt.is_set():
                                break

                            if msg.type == aiohttp.WSMsgType.TEXT:
                                txt = msg.data
                                if txt in ("pong", "ping"):
                                    continue
                                try:
                                    j = json.loads(txt)
                                except Exception:
                                    continue

                                # event acks: {"event":"subscribe", ...}
                                if isinstance(j, dict) and j.get("event"):
                                    continue

                                if isinstance(j, dict) and "data" in j:
                                    ticks = self._parse_tick(j)
                                    now_ms = int(time.time() * 1000)
                                    for t in ticks:
                                        if self._should_emit(t.symbol, now_ms):
                                            await on_tick(t)

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                    finally:
                        ping_task.cancel()
                        with contextlib.suppress(Exception):
                            await ping_task

                backoff = 0.5
            except Exception:
                if self._stop_evt.is_set():
                    break
                await asyncio.sleep(backoff + random.random() * 0.25)
                backoff = min(backoff * 1.8, 10.0)

    async def run(self, on_tick: Callable[[HotPriceTick], Awaitable[None]]) -> None:
        chunks = self._chunks(self.symbols, self.chunk_size)
        tasks = [asyncio.create_task(self._run_chunk(ch, on_tick)) for ch in chunks]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await self.aclose()
