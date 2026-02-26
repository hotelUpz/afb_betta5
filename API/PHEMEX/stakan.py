# # ============================================================
# # FILE: PHEMEX/stakan.py
# # ROLE: Phemex USDT Perpetual order book TOP levels via WS (aiohttp)
# # STREAM: orderbook_p.subscribe (top N levels)
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


# PriceLevel = Tuple[float, float]  # (price, qty)


# @dataclass(frozen=True)
# class DepthTop:
#     symbol: str
#     bids: List[PriceLevel]
#     asks: List[PriceLevel]
#     event_time_ms: int


# class PhemexStakanStream:
#     """Top-of-book stream using Phemex DataGW.

#     Public WS endpoint MUST be exactly:
#         wss://ws.phemex.com

#     Subscribe:
#         {"id":1,"method":"orderbook_p.subscribe","params":["BTCUSDT", false, 5]}

#     Messages:
#         {"symbol":"BTCUSDT","orderbook_p":{"asks":[[price,qty],...],"bids":[...]},
#          "timestamp":..., "type":"snapshot|incremental"}

#     We keep ONLY latest top levels (bids desc, asks asc).
#     """

#     WS_URL = "wss://ws.phemex.com"

#     def __init__(
#         self,
#         symbols: Iterable[str],
#         *,
#         depth: int = 5,
#         chunk_size: int = 40,
#         ping_sec: float = 15.0,
#         reconnect_min_sec: float = 1.0,
#         reconnect_max_sec: float = 25.0,
#         throttle_ms: int = 0,
#     ):
#         self.symbols = [s.upper().strip() for s in symbols if isinstance(s, str) and s.strip()]
#         if not self.symbols:
#             raise ValueError("symbols must be non-empty")

#         self.depth = max(1, int(depth))
#         self.chunk_size = max(1, int(chunk_size))
#         self.ping_sec = float(ping_sec)
#         self.reconnect_min_sec = float(reconnect_min_sec)
#         self.reconnect_max_sec = float(reconnect_max_sec)
#         self.throttle_ms = int(throttle_ms)

#         self._stop = asyncio.Event()
#         self._tasks: List[asyncio.Task] = []
#         self._last_emit_ms: Dict[str, int] = {}
#         self._id_counter = 10

#         # local top-of-book state per symbol (price->qty)
#         self._bids: Dict[str, Dict[float, float]] = {}
#         self._asks: Dict[str, Dict[float, float]] = {}

#     def stop(self) -> None:
#         self._stop.set()

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

#     def _next_id(self) -> int:
#         self._id_counter += 1
#         return self._id_counter

#     def _should_emit(self, sym: str, now_ms: int) -> bool:
#         if self.throttle_ms <= 0:
#             return True
#         last = self._last_emit_ms.get(sym, 0)
#         if now_ms - last >= self.throttle_ms:
#             self._last_emit_ms[sym] = now_ms
#             return True
#         return False

#     async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
#         while not self._stop.is_set():
#             await asyncio.sleep(self.ping_sec)
#             if ws.closed:
#                 break
#             try:
#                 await ws.ping()
#             except Exception:
#                 break

#     async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse, symbols: List[str]) -> None:
#         for sym in symbols:
#             req = {"id": self._next_id(), "method": "orderbook_p.subscribe", "params": [sym, False, self.depth]}
#             await ws.send_str(json.dumps(req))
#             await asyncio.sleep(0.02)

#     def _apply_side(self, book: Dict[float, float], levels: List) -> None:
#         # levels: [[price, qty], ...]
#         for lv in levels:
#             if not isinstance(lv, (list, tuple)) or len(lv) < 2:
#                 continue
#             p = self._to_float(lv[0], 0.0)
#             q = self._to_float(lv[1], 0.0)
#             if p <= 0:
#                 continue
#             if q <= 0:
#                 book.pop(p, None)
#             else:
#                 book[p] = q

#     def _top_n(self, bids: Dict[float, float], asks: Dict[float, float]) -> Tuple[List[PriceLevel], List[PriceLevel]]:
#         b = sorted(bids.items(), key=lambda x: x[0], reverse=True)[: self.depth]
#         a = sorted(asks.items(), key=lambda x: x[0])[: self.depth]
#         return ([(p, q) for p, q in b], [(p, q) for p, q in a])

#     def _parse_book_msg(self, payload: Dict) -> Optional[DepthTop]:
#         if not isinstance(payload, dict):
#             return None
#         sym = payload.get("symbol")
#         ob = payload.get("orderbook_p")
#         if not sym or not isinstance(ob, dict):
#             return None

#         sym_u = str(sym).upper()
#         bids = self._bids.setdefault(sym_u, {})
#         asks = self._asks.setdefault(sym_u, {})

#         # snapshot should be treated as fresh state for top N
#         msg_type = str(payload.get("type") or "").lower()
#         if msg_type == "snapshot":
#             bids.clear()
#             asks.clear()

#         self._apply_side(bids, ob.get("bids") or [])
#         self._apply_side(asks, ob.get("asks") or [])

#         top_b, top_a = self._top_n(bids, asks)

#         ts = payload.get("timestamp")
#         ts_i = self._to_int(ts, int(time.time() * 1000))
#         # timestamp is in ns in samples
#         if ts_i > 1_000_000_000_000:
#             event_ms = ts_i // 1_000_000
#         else:
#             event_ms = ts_i

#         return DepthTop(symbol=sym_u, bids=top_b, asks=top_a, event_time_ms=int(event_ms))

#     async def _run_chunk(self, symbols: List[str], on_depth: Callable[[DepthTop], Awaitable[None]]) -> None:
#         backoff = self.reconnect_min_sec

#         while not self._stop.is_set():
#             ws = None
#             ping_task = None
#             try:
#                 async with aiohttp.ClientSession() as session:
#                     ws = await session.ws_connect(self.WS_URL, autoping=False, max_msg_size=0)
#                     ping_task = asyncio.create_task(self._ping_loop(ws))
#                     await self._subscribe(ws, symbols)
#                     backoff = self.reconnect_min_sec

#                     async for m in ws:
#                         if self._stop.is_set():
#                             break
#                         if m.type == aiohttp.WSMsgType.TEXT:
#                             try:
#                                 payload = json.loads(m.data)
#                             except Exception:
#                                 continue

#                             if "result" in payload and "symbol" not in payload:
#                                 continue

#                             d = self._parse_book_msg(payload)
#                             if d and (d.bids or d.asks) and self._should_emit(d.symbol, d.event_time_ms):
#                                 await on_depth(d)

#                         elif m.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE):
#                             break
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
#         self._tasks = [asyncio.create_task(self._run_chunk(chunk, on_depth)) for chunk in self._chunks()]
#         try:
#             await self._stop.wait()
#         finally:
#             self.stop()
#             for t in list(self._tasks):
#                 t.cancel()
#             for t in list(self._tasks):
#                 with contextlib.suppress(asyncio.CancelledError, Exception):
#                     await t
#             self._tasks.clear()


# # ----------------------------
# # SELF TEST (runs forever)
# # ----------------------------
# if __name__ == "__main__":
#     async def _main():
#         symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"]

#         async def on_depth(d: DepthTop):
#             if d.bids and d.asks:
#                 b0 = d.bids[0]
#                 a0 = d.asks[0]
#                 print(f"{d.symbol} bid={b0[0]}@{b0[1]} | ask={a0[0]}@{a0[1]} | E={d.event_time_ms}")

#         stream = PhemexStakanStream(symbols, depth=5, chunk_size=50, throttle_ms=0)
#         task = asyncio.create_task(stream.run(on_depth))
#         try:
#             await asyncio.sleep(10**9)
#         finally:
#             stream.stop()
#             await task

#     asyncio.run(_main())
