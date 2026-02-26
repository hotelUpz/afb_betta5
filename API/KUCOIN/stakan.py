# # ============================================================
# # FILE: API/KUCOIN/stakan.py
# # ROLE: KuCoin Futures order book TOP levels via WS (aiohttp)
# # STREAM: /contractMarket/level2Depth5:{symbol}
# # NOTE: Single responsibility: ONLY order book data.
# # TODO: в разработке
# # ============================================================

# from __future__ import annotations

# import asyncio
# import contextlib
# import json
# import random
# import string
# import time
# from dataclasses import dataclass
# from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Tuple

# import aiohttp


# PriceLevel = Tuple[float, float]  # (price, qty)


# def _now_ms() -> int:
#     return int(time.time() * 1000)


# def _rand_id(n: int = 10) -> str:
#     return "".join(random.choice(string.ascii_letters + string.digits) for _ in range(n))


# @dataclass(frozen=True)
# class DepthTop:
#     symbol: str
#     bids: List[PriceLevel]
#     asks: List[PriceLevel]
#     event_time_ms: int


# class KucoinStakanStream:
#     """Top-of-book stream via level2Depth5, chunked for 200+ symbols.

#     KuCoin Futures WS:
#       - POST /api/v1/bullet-public
#       - connect to ws-api-futures.kucoin.com with token
#       - subscribe to "/contractMarket/level2Depth5:{symbol}"

#     Callback:
#         async def on_depth(d: DepthTop) -> None
#     """

#     REST_BASE = "https://api-futures.kucoin.com"

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

#     @staticmethod
#     def _chunks(symbols: List[str], chunk_size: int) -> List[List[str]]:
#         out: List[List[str]] = []
#         cur: List[str] = []
#         for s in symbols:
#             cur.append(s)
#             if len(cur) >= chunk_size:
#                 out.append(cur)
#                 cur = []
#         if cur:
#             out.append(cur)
#         return out

#     async def _get_public_token(self) -> Tuple[str, str, int]:
#         assert self._session is not None
#         url = f"{self.REST_BASE}/api/v1/bullet-public"
#         async with self._session.post(url) as resp:
#             text = await resp.text()
#             if resp.status != 200:
#                 raise RuntimeError(f"HTTP {resp.status}: {text}")
#             js = await resp.json()

#         data = js.get("data") or {}
#         if not isinstance(data, dict):
#             raise RuntimeError(f"Unexpected bullet response: {js}")

#         token = str(data.get("token") or "")
#         servers = data.get("instanceServers") or []
#         if not token or not isinstance(servers, list) or not servers:
#             raise RuntimeError(f"Bad bullet response: {js}")

#         s0 = servers[0] if isinstance(servers[0], dict) else {}
#         endpoint = str(s0.get("endpoint") or "")
#         ping_interval = int(s0.get("pingInterval") or 18000)

#         if not endpoint:
#             raise RuntimeError(f"Bad server endpoint: {js}")

#         return endpoint, token, ping_interval

#     @staticmethod
#     def _ws_url(endpoint: str, token: str) -> str:
#         ep = endpoint
#         if not ep.endswith("/"):
#             ep += "/"
#         cid = _rand_id(12)
#         return f"{ep}?token={token}&connectId={cid}"

#     @staticmethod
#     def _topic(sym: str) -> str:
#         return f"/contractMarket/level2Depth5:{sym}"

#     async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse, symbols: List[str]) -> None:
#         for sym in symbols:
#             msg = {
#                 "id": str(_now_ms()),
#                 "type": "subscribe",
#                 "topic": self._topic(sym),
#                 "response": True,
#             }
#             await ws.send_str(json.dumps(msg))

#     async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse, ping_sec: float) -> None:
#         while not self._stop.is_set():
#             await asyncio.sleep(ping_sec)
#             if ws.closed:
#                 break
#             try:
#                 await ws.send_str(json.dumps({"id": str(_now_ms()), "type": "ping"}))
#             except Exception:
#                 break

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
#         # Example:
#         # {"type":"message","topic":"/contractMarket/level2Depth5:XBTUSDTM","data":{"bids":[["89720.9",513],...],"asks":[...],"ts":1731680019100}}
#         if not isinstance(payload, dict):
#             return None
#         if payload.get("type") != "message":
#             return None
#         data = payload.get("data")
#         if not isinstance(data, dict):
#             return None

#         topic = payload.get("topic") or ""
#         sym = None
#         if isinstance(topic, str) and ":" in topic:
#             sym = topic.split(":")[-1]
#         if not sym:
#             # fallback if present
#             sym = data.get("symbol")
#         if not sym:
#             return None
#         sym = str(sym).upper().strip()

#         bids = self._parse_levels(data.get("bids"))
#         asks = self._parse_levels(data.get("asks"))

#         ts = data.get("ts") or data.get("timestamp")
#         t_int = self._to_int(ts, 0)
#         event_ms = t_int if t_int else _now_ms()

#         return DepthTop(symbol=sym, bids=bids, asks=asks, event_time_ms=int(event_ms))

#     def _should_emit(self, sym: str, now_ms: int) -> bool:
#         if self.throttle_ms <= 0:
#             return True
#         last = self._last_emit_ms.get(sym, 0)
#         if now_ms - last >= self.throttle_ms:
#             self._last_emit_ms[sym] = now_ms
#             return True
#         return False

#     async def _run_chunk(self, symbols: List[str], on_depth: Callable[[DepthTop], Awaitable[None]]) -> None:
#         backoff = self.reconnect_min_sec

#         while not self._stop.is_set():
#             ws = None
#             ping_task = None
#             try:
#                 assert self._session is not None
#                 endpoint, token, ping_interval_ms = await self._get_public_token()

#                 ws_url = self._ws_url(endpoint, token)
#                 ws = await self._session.ws_connect(ws_url, autoping=False, max_msg_size=0)

#                 await self._subscribe(ws, symbols)

#                 ping_sec = min(self.ping_sec, max(3.0, (ping_interval_ms / 1000.0) * 0.8))
#                 ping_task = asyncio.create_task(self._ping_loop(ws, ping_sec))

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
#             for chunk in self._chunks(self.symbols, self.chunk_size):
#                 self._tasks.append(asyncio.create_task(self._run_chunk(chunk, on_depth)))

#             await self._stop.wait()

#         finally:
#             await self.aclose()


# # ----------------------------
# # SELF TEST
# # ----------------------------
# async def _main():
#     symbols = ["XBTUSDTM"]

#     async def on_depth(d: DepthTop):
#         if d.bids and d.asks:
#             b0 = d.bids[0]
#             a0 = d.asks[0]
#             print(f"{d.symbol} bid={b0[0]}@{b0[1]} | ask={a0[0]}@{a0[1]} | t={d.event_time_ms}")

#     stream = KucoinStakanStream(symbols, chunk_size=50, throttle_ms=0)
#     task = asyncio.create_task(stream.run(on_depth))
#     try:
#         await asyncio.sleep(1_000_000)
#     finally:
#         stream.stop()
#         await task

# if __name__ == "__main__":
#     asyncio.run(_main())
