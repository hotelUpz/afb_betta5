
# ============================================================
# FILE: API/BITGET/stakan.py
# ROLE: Bitget USDT-M Futures order book TOP levels via WS (aiohttp)
# CHANNEL: books5  (top 5 per side)
# NOTE: Single responsibility: ONLY order book data.
# ============================================================

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

import aiohttp


PriceLevel = Tuple[float, float]  # (price, qty)


@dataclass(frozen=True)
class DepthTop:
    symbol: str
    bids: List[PriceLevel]
    asks: List[PriceLevel]
    event_time_ms: int


class BitgetStakanStream:
    """Bitget order book top-5 stream (chunked).

    WS:
        wss://ws.bitget.com/v2/ws/public

    Subscribe (per docs):
        {
          "op": "subscribe",
          "args": [{"instType":"USDT-FUTURES","channel":"books5","instId":"BTCUSDT"}, ...]
        }

    Push format:
        {
          "action":"snapshot",
          "arg": {...},
          "data":[{"asks":[["27000.5","8.760"],...],"bids":[...],"ts":"..."}, ...],
          "ts": 1695716059516
        }
    """

    WS_URL = "wss://ws.bitget.com/v2/ws/public"

    def __init__(
        self,
        symbols: Iterable[str],
        *,
        inst_type: str = "USDT-FUTURES",
        chunk_size: int = 50,
        throttle_ms: int = 0,
        ping_interval_sec: float = 25.0,
        reconnect_global_gap_sec: float = 0.35,
    ):
        self.inst_type = str(inst_type).upper().strip()
        self.symbols = [str(s).upper().strip() for s in symbols if str(s).strip()]
        self.chunk_size = max(1, int(chunk_size))
        self.throttle_ms = max(0, int(throttle_ms))
        self.ping_interval_sec = float(ping_interval_sec)

        self._stop_evt = asyncio.Event()
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_emit_ms: Dict[str, int] = {}

        self._reconnect_lock = asyncio.Lock()
        self._reconnect_next_ok_ts = 0.0
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
        while not self._stop_evt.is_set():
            await asyncio.sleep(self.ping_interval_sec)
            if self._stop_evt.is_set():
                break
            with contextlib.suppress(Exception):
                await ws.send_str("ping")

    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse, symbols: List[str]) -> None:
        args = [{"instType": self.inst_type, "channel": "books5", "instId": s} for s in symbols]
        await ws.send_str(json.dumps({"op": "subscribe", "args": args}))

    def _parse_depth(self, payload: dict) -> List[DepthTop]:
        if isinstance(payload.get("action"), str):
            # best-effort: only accept snapshot/update frames
            pass

        data = payload.get("data")
        if not isinstance(data, list):
            return []

        out: List[DepthTop] = []
        arg = payload.get("arg") if isinstance(payload.get("arg"), dict) else {}
        inst_id = str(arg.get("instId") or "").upper().strip()

        for it in data:
            if not isinstance(it, dict):
                continue
            sym = str(it.get("instId") or it.get("symbol") or inst_id or "").upper().strip()
            if not sym:
                continue

            bids_raw = it.get("bids") if isinstance(it.get("bids"), list) else []
            asks_raw = it.get("asks") if isinstance(it.get("asks"), list) else []

            bids: List[PriceLevel] = []
            asks: List[PriceLevel] = []

            for lvl in bids_raw:
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    bids.append((self._to_float(lvl[0]), self._to_float(lvl[1])))
            for lvl in asks_raw:
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    asks.append((self._to_float(lvl[0]), self._to_float(lvl[1])))

            ts = self._to_int(it.get("ts") or payload.get("ts"))
            if ts <= 0:
                ts = int(time.time() * 1000)

            out.append(DepthTop(symbol=sym, bids=bids, asks=asks, event_time_ms=ts))

        return out

    async def _run_chunk(self, symbols: List[str], on_depth: Callable[[DepthTop], Awaitable[None]]) -> None:
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

                                if isinstance(j, dict) and j.get("event"):
                                    continue

                                if isinstance(j, dict) and "data" in j:
                                    depths = self._parse_depth(j)
                                    now_ms = int(time.time() * 1000)
                                    for d in depths:
                                        if self._should_emit(d.symbol, now_ms):
                                            await on_depth(d)

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

    async def run(self, on_depth: Callable[[DepthTop], Awaitable[None]]) -> None:
        chunks = self._chunks(self.symbols, self.chunk_size)
        tasks = [asyncio.create_task(self._run_chunk(ch, on_depth)) for ch in chunks]
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await self.aclose()
