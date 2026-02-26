# ============================================================
# FILE: PHEMEX/price.py
# ROLE: Phemex USDT Perpetual HOT last prices via WS (aiohttp)
# STREAM: perp_market24h_pack_p.subscribe (all symbols, ~1s)
# NOTE: Single responsibility: ONLY hot "last" price ticks.
# ============================================================

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Set

import aiohttp


@dataclass(frozen=True)
class HotPriceTick:
    symbol: str
    price: float
    event_time_ms: int


class PhemexHotPriceStream:
    """
    Uses Phemex public DataGW websocket:
      - Public WS: wss://ws.phemex.com (no path/query)
      - Heartbeat: send {"method":"server.ping","params":[]} <30s (rec 5s)
      - Hot prices: perp_market24h_pack_p.subscribe -> lastRp for all symbols every ~1s

    Why this is "hot":
      - Updates keep coming even if your chosen symbol has rare trades,
        because this is a ticker pack stream (lastRp).
    """

    WS_URL = "wss://ws.phemex.com"

    def __init__(
        self,
        symbols: Iterable[str],
        *,
        ping_sec: float = 5.0,
        reconnect_min_sec: float = 0.5,
        reconnect_max_sec: float = 20.0,
        throttle_ms: int = 0,
        queue_max: int = 5000,
        drop_if_queue_full: bool = True,
    ):
        self.symbols: List[str] = [s.upper().strip() for s in symbols if isinstance(s, str) and s.strip()]
        if not self.symbols:
            raise ValueError("symbols must be non-empty")

        self._want: Set[str] = set(self.symbols)

        self.ping_sec = float(ping_sec)
        self.reconnect_min_sec = float(reconnect_min_sec)
        self.reconnect_max_sec = float(reconnect_max_sec)
        self.throttle_ms = int(throttle_ms)

        self.queue_max = int(queue_max)
        self.drop_if_queue_full = bool(drop_if_queue_full)

        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._last_emit_ms: Dict[str, int] = {}
        self._id_counter = 0

        self._field_idx: Optional[Dict[str, int]] = None  # built from "fields" once

    def stop(self) -> None:
        self._stop.set()

    def _next_id(self) -> int:
        self._id_counter += 1
        return self._id_counter

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

    def _should_emit(self, sym: str, now_ms: int) -> bool:
        if self.throttle_ms <= 0:
            return True
        last = self._last_emit_ms.get(sym, 0)
        if now_ms - last >= self.throttle_ms:
            self._last_emit_ms[sym] = now_ms
            return True
        return False

    async def _app_ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """
        Phemex recommends application-level heartbeat:
          {"id":0,"method":"server.ping","params":[]}
        """
        while not self._stop.is_set():
            await asyncio.sleep(self.ping_sec)
            if ws.closed:
                return
            try:
                req = {"id": 0, "method": "server.ping", "params": []}
                await ws.send_str(json.dumps(req))
            except Exception:
                return

    async def _subscribe_pack(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """
        Subscribe all perp symbols ticker pack:
          {"method":"perp_market24h_pack_p.subscribe","params":[],"id":...}
        """
        req = {"id": self._next_id(), "method": "perp_market24h_pack_p.subscribe", "params": []}
        await ws.send_str(json.dumps(req))

    def _parse_pack_update(self, payload: dict) -> List[HotPriceTick]:
        """
        Pack update format includes:
          - "fields": [..., "symbol", ..., "lastRp", ...]
          - "data": [[...], [...]]
          - "timestamp": ns
        """
        if not isinstance(payload, dict):
            return []

        # subscription ack
        if "result" in payload and "method" not in payload:
            return []

        method = payload.get("method")
        if method != "perp_market24h_pack_p.update":
            return []

        fields = payload.get("fields")
        data = payload.get("data")
        ts_ns = self._to_int(payload.get("timestamp"), 0)

        if isinstance(fields, list) and (self._field_idx is None):
            idx = {}
            for i, name in enumerate(fields):
                if isinstance(name, str):
                    idx[name] = i
            # need at least these
            if "symbol" in idx and "lastRp" in idx:
                self._field_idx = idx

        if not self._field_idx:
            return []

        if not isinstance(data, list) or not data:
            return []

        # ns -> ms
        event_ms = (ts_ns // 1_000_000) if ts_ns > 1_000_000_000_000 else ts_ns
        if event_ms <= 0:
            event_ms = int(time.time() * 1000)

        i_sym = self._field_idx.get("symbol", -1)
        i_last = self._field_idx.get("lastRp", -1)
        if i_sym < 0 or i_last < 0:
            return []

        out: List[HotPriceTick] = []
        for row in data:
            if not isinstance(row, (list, tuple)):
                continue
            if i_sym >= len(row) or i_last >= len(row):
                continue

            sym = row[i_sym]
            if not sym:
                continue
            sym_u = str(sym).upper()

            if sym_u not in self._want:
                continue

            price = self._to_float(row[i_last], 0.0)
            if price <= 0:
                continue

            if self._should_emit(sym_u, int(event_ms)):
                out.append(HotPriceTick(symbol=sym_u, price=price, event_time_ms=int(event_ms)))

        return out

    async def _consumer_loop(
        self,
        q: asyncio.Queue,
        on_tick: Callable[[HotPriceTick], Awaitable[None]],
    ) -> None:
        while not self._stop.is_set():
            try:
                tick = await q.get()
            except asyncio.CancelledError:
                return
            try:
                await on_tick(tick)
            except Exception:
                # single responsibility: do not crash stream because of callback
                pass
            finally:
                with contextlib.suppress(Exception):
                    q.task_done()

    async def run(self, on_tick: Callable[[HotPriceTick], Awaitable[None]]) -> None:
        """
        One connection is enough (single subscription).
        If you need also trades-level micro-updates, do a separate module/stream.
        """
        q: asyncio.Queue = asyncio.Queue(maxsize=self.queue_max)
        consumer = asyncio.create_task(self._consumer_loop(q, on_tick))
        self._tasks = [consumer]

        backoff = self.reconnect_min_sec

        try:
            while not self._stop.is_set():
                ws: Optional[aiohttp.ClientWebSocketResponse] = None
                ping_task: Optional[asyncio.Task] = None

                try:
                    async with aiohttp.ClientSession() as session:
                        ws = await session.ws_connect(self.WS_URL, autoping=True, max_msg_size=0)
                        ping_task = asyncio.create_task(self._app_ping_loop(ws))

                        await self._subscribe_pack(ws)
                        backoff = self.reconnect_min_sec

                        async for m in ws:
                            if self._stop.is_set():
                                break

                            if m.type != aiohttp.WSMsgType.TEXT:
                                if m.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
                                    break
                                continue

                            try:
                                payload = json.loads(m.data)
                            except Exception:
                                continue

                            ticks = self._parse_pack_update(payload)
                            if not ticks:
                                continue

                            for t in ticks:
                                if self.drop_if_queue_full:
                                    if q.full():
                                        # drop newest (or could drop oldest; here simplest)
                                        continue
                                    q.put_nowait(t)
                                else:
                                    await q.put(t)

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

        finally:
            self.stop()
            for t in list(self._tasks):
                t.cancel()
            for t in list(self._tasks):
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t
            self._tasks.clear()


# ----------------------------
# SELF TEST
# ----------------------------
if __name__ == "__main__":
    async def _main():
        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"]

        async def on_tick(t: HotPriceTick):
            print(f"{t.symbol:<10} price={t.price:<12} E={t.event_time_ms}")

        stream = PhemexHotPriceStream(symbols, throttle_ms=0)
        task = asyncio.create_task(stream.run(on_tick))
        try:
            await asyncio.sleep(60)
        finally:
            stream.stop()
            await task

    asyncio.run(_main())