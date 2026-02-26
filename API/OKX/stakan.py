# # ============================================================
# # FILE: OKX/stakan.py
# # ROLE: OKX Futures (SWAP) order book TOP levels via WS (aiohttp)
# # CHANNEL: books5  (top 5 per side)
# # NOTE: Single responsibility: ONLY order book data.
# # TODO: в разработке
# # ============================================================

# from __future__ import annotations

# import asyncio
# import contextlib
# import json
# import random
# import time
# from dataclasses import dataclass
# from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

# import aiohttp


# PriceLevel = Tuple[float, float]  # (price, size)


# @dataclass(frozen=True)
# class DepthTop:
#     symbol: str  # OKX instId
#     bids: List[PriceLevel]
#     asks: List[PriceLevel]
#     event_time_ms: int


# class OkxStakanStream:
#     """Top-of-book stream for OKX SWAP markets via `books5`.

#     WS public endpoint (OKX v5):
#         wss://ws.okx.com:8443/ws/v5/public

#     Subscribe:
#         {"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT-SWAP"}, ...]}

#     Push includes bids/asks arrays and ts.

#     Cache (optional):
#         cache[symbol] = {"bids": [...], "asks": [...], "ts": int}
#     """

#     WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

#     def __init__(
#         self,
#         symbols: Iterable[str],
#         *,
#         cache: Optional[Dict[str, dict]] = None,
#         ws_url: str = WS_URL,
#         chunk_size: int = 80,
#         ping_sec: float = 15.0,
#         reconnect_min_sec: float = 1.0,
#         reconnect_max_sec: float = 25.0,
#         throttle_ms: int = 0,
#         timeout_sec: float = 30.0,
#     ):
#         self.symbols = [str(s).upper().strip() for s in symbols if isinstance(s, str) and s.strip()]
#         if not self.symbols:
#             raise ValueError("symbols must be non-empty")

#         self.cache = cache
#         self.ws_url = ws_url
#         self.chunk_size = max(1, int(chunk_size))
#         self.ping_sec = float(ping_sec)
#         self.reconnect_min_sec = float(reconnect_min_sec)
#         self.reconnect_max_sec = float(reconnect_max_sec)
#         self.throttle_ms = int(throttle_ms)
#         self.timeout = aiohttp.ClientTimeout(total=float(timeout_sec))

#         self._stop = asyncio.Event()
#         self._tasks: List[asyncio.Task] = []
#         self._session: Optional[aiohttp.ClientSession] = None
#         self._last_emit_ms: Dict[str, int] = {}

#     def stop(self) -> None:
#         self._stop.set()

#     async def aclose(self) -> None:
#         self._stop.set()
#         for t in list(self._tasks):
#             t.cancel()
#         for t in list(self._tasks):
#             with contextlib.suppress(asyncio.CancelledError, Exception):
#                 await t
#         self._tasks.clear()
#         if self._session and not self._session.closed:
#             await self._session.close()
#         self._session = None

#     @staticmethod
#     def _to_float(v, default: float = 0.0) -> float:
#         try:
#             return float(v)
#         except Exception:
#             return default

#     @staticmethod
#     def _to_int(v, default: int = 0) -> int:
#         try:
#             return int(v)
#         except Exception:
#             return default

#     def _chunks(self) -> List[List[str]]:
#         out: List[List[str]] = []
#         cur: List[str] = []
#         for s in self.symbols:
#             cur.append(s)
#             if len(cur) >= self.chunk_size:
#                 out.append(cur)
#                 cur = []
#         if cur:
#             out.append(cur)
#         return out

#     def _should_emit(self, sym: str, now_ms: int) -> bool:
#         if self.throttle_ms <= 0:
#             return True
#         last = self._last_emit_ms.get(sym, 0)
#         if now_ms - last >= self.throttle_ms:
#             self._last_emit_ms[sym] = now_ms
#             return True
#         return False

#     def _parse_levels(self, arr) -> List[PriceLevel]:
#         out: List[PriceLevel] = []
#         if not isinstance(arr, list):
#             return out
#         for it in arr:
#             if not isinstance(it, (list, tuple)) or len(it) < 2:
#                 continue
#             px = self._to_float(it[0], 0.0)
#             sz = self._to_float(it[1], 0.0)
#             if px > 0 and sz >= 0:
#                 out.append((px, sz))
#         return out

#     def parse_and_store(self, payload: Dict, cache: Optional[Dict[str, dict]] = None) -> Optional[DepthTop]:
#         if not isinstance(payload, dict):
#             return None

#         arg = payload.get("arg")
#         if not isinstance(arg, dict) or arg.get("channel") != "books5":
#             return None

#         data = payload.get("data")
#         if not isinstance(data, list) or not data:
#             return None

#         d0 = data[0]
#         if not isinstance(d0, dict):
#             return None

#         inst_id = str(d0.get("instId") or arg.get("instId") or "").upper().strip()
#         if not inst_id:
#             return None

#         bids = self._parse_levels(d0.get("bids"))
#         asks = self._parse_levels(d0.get("asks"))
#         ts_ms = self._to_int(d0.get("ts"), int(time.time() * 1000))

#         depth = DepthTop(symbol=inst_id, bids=bids, asks=asks, event_time_ms=ts_ms)

#         c = cache if cache is not None else self.cache
#         if c is not None:
#             c[inst_id] = {"bids": bids, "asks": asks, "ts": ts_ms}

#         return depth

#     async def _send_subscribe(self, ws: aiohttp.ClientWebSocketResponse, symbols: List[str]) -> None:
#         args = [{"channel": "books5", "instId": s} for s in symbols]
#         await ws.send_str(json.dumps({"op": "subscribe", "args": args}))

#     async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
#         while not self._stop.is_set():
#             await asyncio.sleep(self.ping_sec)
#             if ws.closed:
#                 break
#             with contextlib.suppress(Exception):
#                 await ws.send_str("ping")

#     async def _run_chunk(self, symbols: List[str], on_depth: Callable[[DepthTop], Awaitable[None]]) -> None:
#         backoff = self.reconnect_min_sec

#         while not self._stop.is_set():
#             ws = None
#             ping_task = None
#             try:
#                 assert self._session is not None
#                 ws = await self._session.ws_connect(self.ws_url, autoping=False, max_msg_size=0)
#                 ping_task = asyncio.create_task(self._ping_loop(ws))

#                 await self._send_subscribe(ws, symbols)
#                 backoff = self.reconnect_min_sec

#                 async for m in ws:
#                     if self._stop.is_set():
#                         break

#                     if m.type == aiohttp.WSMsgType.TEXT:
#                         txt = m.data
#                         if txt == "pong":
#                             continue
#                         if txt == "ping":
#                             with contextlib.suppress(Exception):
#                                 await ws.send_str("pong")
#                             continue
#                         try:
#                             payload = json.loads(txt)
#                         except Exception:
#                             continue

#                         if "event" in payload:
#                             continue

#                         d = self.parse_and_store(payload)
#                         if d and (d.bids or d.asks):
#                             now_ms = d.event_time_ms
#                             if self._should_emit(d.symbol, now_ms):
#                                 await on_depth(d)

#                     elif m.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
#                         break

#             except asyncio.CancelledError:
#                 break
#             except Exception:
#                 sleep_for = min(self.reconnect_max_sec, backoff) * (0.7 + random.random() * 0.6)
#                 await asyncio.sleep(sleep_for)
#                 backoff = min(self.reconnect_max_sec, backoff * 1.7)
#             finally:
#                 if ping_task:
#                     ping_task.cancel()
#                     with contextlib.suppress(asyncio.CancelledError, Exception):
#                         await ping_task
#                 if ws is not None and not ws.closed:
#                     with contextlib.suppress(Exception):
#                         await ws.close()

#     async def run(self, on_depth: Callable[[DepthTop], Awaitable[None]]) -> None:
#         if self._session is not None:
#             raise RuntimeError("Stream already running")

#         self._session = aiohttp.ClientSession(timeout=self.timeout)
#         try:
#             for chunk in self._chunks():
#                 self._tasks.append(asyncio.create_task(self._run_chunk(chunk, on_depth)))
#             await self._stop.wait()
#         finally:
#             await self.aclose()


# # ----------------------------
# # SELF TEST (CTRL+C to stop)
# # ----------------------------
# async def _main() -> None:
#     symbols = [
#         "BTC-USDT-SWAP",
#         "ETH-USDT-SWAP",
#     ]

#     async def on_depth(d: DepthTop) -> None:
#         if d.bids and d.asks:
#             b0 = d.bids[0]
#             a0 = d.asks[0]
#             print(f"{d.symbol:<16} bid={b0[0]}@{b0[1]} | ask={a0[0]}@{a0[1]} | ts={d.event_time_ms}")

#     cache: Dict[str, dict] = {}
#     stream = OkxStakanStream(symbols, cache=cache, chunk_size=80, throttle_ms=0)

#     task = asyncio.create_task(stream.run(on_depth))
#     try:
#         await asyncio.Event().wait()
#     finally:
#         stream.stop()
#         await task


# if __name__ == "__main__":
#     try:
#         asyncio.run(_main())
#     except KeyboardInterrupt:
#         pass
