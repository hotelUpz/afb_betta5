# # ============================================================
# # FILE: API/BINANCE/stakan.py
# # ROLE: Binance USDT-M Futures order book TOP levels via WS (aiohttp)
# # STREAM: <symbol>@depth5@100ms
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


# class BinanceStakanStream:
#     """Top-of-book stream via depth5@100ms, chunked for 200+ symbols.

#     Combined streams URL:
#         wss://fstream.binance.com/stream?streams=btcusdt@depth5@100ms/...

#     Callback:
#         async def on_depth(d: DepthTop) -> None
#     """

#     WS_BASE = "wss://fstream.binance.com"

#     def __init__(
#         self,
#         symbols: Iterable[str],
#         *,
#         chunk_size: int = 50,
#         ping_sec: float = 15.0,
#         reconnect_min_sec: float = 1.0,
#         reconnect_max_sec: float = 25.0,
#         throttle_ms: int = 0,
#     ):
#         self.symbols = [s.upper().strip() for s in symbols if isinstance(s, str) and s.strip()]
#         if not self.symbols:
#             raise ValueError("symbols must be non-empty")

#         self.chunk_size = max(1, int(chunk_size))
#         self.ping_sec = float(ping_sec)
#         self.reconnect_min_sec = float(reconnect_min_sec)
#         self.reconnect_max_sec = float(reconnect_max_sec)
#         self.throttle_ms = int(throttle_ms)

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

#     @staticmethod
#     def _make_url(symbols: List[str]) -> str:
#         streams = "/".join([f"{s.lower()}@depth5@100ms" for s in symbols])
#         return f"{BinanceStakanStream.WS_BASE}/stream?streams={streams}"

#     def _parse_levels(self, arr) -> List[PriceLevel]:
#         out: List[PriceLevel] = []
#         if not isinstance(arr, list):
#             return out
#         for item in arr:
#             if not isinstance(item, (list, tuple)) or len(item) < 2:
#                 continue
#             out.append((self._to_float(item[0]), self._to_float(item[1])))
#         return out

#     def _parse_depth(self, payload: Dict) -> Optional[DepthTop]:
#         data = payload.get("data") if isinstance(payload, dict) else None
#         if not isinstance(data, dict):
#             return None
#         sym = data.get("s")
#         if not sym:
#             return None
#         bids = self._parse_levels(data.get("b"))
#         asks = self._parse_levels(data.get("a"))
#         return DepthTop(
#             symbol=str(sym),
#             bids=bids,
#             asks=asks,
#             event_time_ms=self._to_int(data.get("E"), int(time.time() * 1000)),
#         )

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

#     async def _run_chunk(self, symbols: List[str], on_depth: Callable[[DepthTop], Awaitable[None]]) -> None:
#         backoff = self.reconnect_min_sec
#         url = self._make_url(symbols)

#         while not self._stop.is_set():
#             ws = None
#             ping_task = None
#             try:
#                 assert self._session is not None
#                 ws = await self._session.ws_connect(url, autoping=False, max_msg_size=0)
#                 ping_task = asyncio.create_task(self._ping_loop(ws))
#                 backoff = self.reconnect_min_sec

#                 async for m in ws:
#                     if self._stop.is_set():
#                         break
#                     if m.type == aiohttp.WSMsgType.TEXT:
#                         try:
#                             payload = json.loads(m.data)
#                         except Exception:
#                             continue
#                         d = self._parse_depth(payload)
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
#         self._session = aiohttp.ClientSession()
#         try:
#             for chunk in self._chunks():
#                 self._tasks.append(asyncio.create_task(self._run_chunk(chunk, on_depth)))
#             await self._stop.wait()
#         finally:
#             await self.aclose()


# # # ----------------------------
# # # SELF TEST (prints 10 sec, exits clean)
# # # ----------------------------
# async def _main():
#     symbols = ['0GUSDT', '1000000BOBUSDT', '1000000MOGUSDT', '1000BONKUSDT', '1000CATUSDT', '1000CHEEMSUSDT', '1000FLOKIUSDT', '1000LUNCUSDT', '1000PEPEUSDT', '1000RATSUSDT', '1000SATSUSDT', '1000SHIBUSDT', '1000WHYUSDT', '1000XECUSDT', '1INCHUSDT', '1MBABYDOGEUSDT', '2ZUSDT', '4USDT', 'A2ZUSDT', 'AAVEUSDT', 'ACEUSDT', 'ACHUSDT', 'ACTUSDT', 'ACUUSDT', 'ACXUSDT', 'ADAUSDT', 'AERGOUSDT', 'AEROUSDT', 'AEVOUSDT', 'AGLDUSDT']  

#     async def on_depth(d: DepthTop):
#         if d.bids and d.asks:
#             b0 = d.bids[0]
#             a0 = d.asks[0]
#             print(f"{d.symbol} bid={b0[0]}@{b0[1]} | ask={a0[0]}@{a0[1]} | E={d.event_time_ms}")

#     stream = BinanceStakanStream(symbols, chunk_size=50, throttle_ms=0)
#     task = asyncio.create_task(stream.run(on_depth))
#     try:
#         await asyncio.sleep(1000000)
#     finally:
#         stream.stop()
#         await task

# if __name__ == "__main__":
#     asyncio.run(_main())
