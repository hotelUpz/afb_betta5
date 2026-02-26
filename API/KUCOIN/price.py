# ============================================================
# FILE: API/KUCOIN/price.py
# ROLE: KuCoin Futures HOT-ish price stream via WS (aiohttp)
# STREAM: /contractMarket/tickerV2:{symbol}
# NOTE:
#   - Single responsibility: ONLY hot price ticks (price + event_time_ms).
#   - KuCoin Futures doesn't provide Binance-like super-frequent trade ticks
#     for illiquid symbols. tickerV2 pushes on orderbook changes (BBO),
#     so it's the closest "hot" stream for most symbols.
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


@dataclass(frozen=True)
class HotPriceTick:
    symbol: str          # canonical-ish symbol you asked for (upper)
    price: float         # "hot" price (mid from BBO)
    event_time_ms: int   # KuCoin 'ts' (ms) or local fallback


class KucoinWsPublic:
    """
    Minimal WS public token apply for KuCoin Futures.

    Docs:
      - WS base: wss://ws-api-futures.kucoin.com
      - Apply token (public, no auth):
          POST https://api-futures.kucoin.com/api/v1/bullet-public
    """
    REST_BULLET = "https://api-futures.kucoin.com/api/v1/bullet-public"

    def __init__(self, session: aiohttp.ClientSession, *, timeout_sec: float = 15.0):
        self._session = session
        self._timeout = aiohttp.ClientTimeout(total=float(timeout_sec))

    async def get_ws_endpoint(self) -> Tuple[str, str]:
        async with self._session.post(self.REST_BULLET, timeout=self._timeout) as resp:
            txt = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"KUCOIN bullet-public HTTP {resp.status}: {txt}")

            data = await resp.json()
            if not isinstance(data, dict) or data.get("code") != "200000":
                raise RuntimeError(f"KUCOIN bullet-public bad response: {data}")

            d = data.get("data") or {}
            token = d.get("token")
            servers = d.get("instanceServers") or []
            if not token or not servers:
                raise RuntimeError(f"KUCOIN bullet-public missing token/servers: {data}")

            endpoint = servers[0].get("endpoint")
            if not endpoint:
                raise RuntimeError(f"KUCOIN bullet-public missing endpoint: {data}")

            return str(endpoint), str(token)


class KucoinHotPriceStream:
    """
    HOT price stream for many KuCoin Futures symbols via tickerV2.

    Topic:
        /contractMarket/tickerV2:{symbol}#

    We map it to HotPriceTick(price=mid(BBO), event_time_ms=ts).

    Chunking:
        KuCoin recommends not putting too many subs into one socket.
        We'll chunk subscriptions per WS connection.

    Callback:
        async def on_tick(tick: HotPriceTick) -> None
    """

    WS_TOPIC_PREFIX = "/contractMarket/tickerV2:"
    WS_PRIVATE = False

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
        raw = [s.upper().strip() for s in symbols if isinstance(s, str) and s.strip()]
        if not raw:
            raise ValueError("symbols must be non-empty")

        # Build internal mapping:
        # asked_symbol -> kucoin_symbol_for_ws
        self._sym_map: Dict[str, str] = {}
        for s in raw:
            ks = self._to_kucoin_symbol(s)
            self._sym_map[s] = ks

        self.symbols = list(self._sym_map.keys())

        self.chunk_size = max(1, int(chunk_size))
        self.ping_sec = float(ping_sec)
        self.reconnect_min_sec = float(reconnect_min_sec)
        self.reconnect_max_sec = float(reconnect_max_sec)
        self.throttle_ms = int(throttle_ms)

        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_emit_ms: Dict[str, int] = {}

    # --------------- public control ---------------
    def stop(self) -> None:
        self._stop.set()

    async def aclose(self) -> None:
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

    # --------------- helpers ---------------
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

    @staticmethod
    def _to_kucoin_symbol(sym: str) -> str:
        """
        Minimal normalization to reduce "no data" cases:
          - If already ends with 'M' (like XBTUSDTM) -> keep
          - If ends with 'USDT' -> make 'USDTM'
          - BTC -> XBT (KuCoin uses XBT for BTC futures contracts)
        """
        s = sym.upper().strip()
        if s.endswith("M"):
            return s
        # BTCUSDT -> XBTUSDTM
        if s.startswith("BTC") and s.endswith("USDT"):
            s = "XBT" + s[3:]
        if s.endswith("USDT"):
            return s + "M"   # ...USDTM
        return s

    def _chunks(self) -> List[List[str]]:
        out: List[List[str]] = []
        cur: List[str] = []
        for asked_sym in self.symbols:
            cur.append(asked_sym)
            if len(cur) >= self.chunk_size:
                out.append(cur)
                cur = []
        if cur:
            out.append(cur)
        return out

    def _should_emit(self, asked_sym: str, now_ms: int) -> bool:
        if self.throttle_ms <= 0:
            return True
        last = self._last_emit_ms.get(asked_sym, 0)
        if now_ms - last >= self.throttle_ms:
            self._last_emit_ms[asked_sym] = now_ms
            return True
        return False

    async def _ws_ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        # KuCoin supports text ping ("ping") but also WS ping frames.
        # We'll use WS ping frame with autoping disabled.
        while not self._stop.is_set():
            await asyncio.sleep(self.ping_sec)
            if ws.closed:
                break
            try:
                await ws.ping()
            except Exception:
                break

    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse, asked_syms: List[str]) -> None:
        # send subscribe per symbol (KuCoin expects one topic per message)
        for asked_sym in asked_syms:
            ku_sym = self._sym_map[asked_sym]
            msg = {
                "id": int(time.time() * 1000),
                "type": "subscribe",
                "topic": f"{self.WS_TOPIC_PREFIX}{ku_sym}",
                "response": True,
                "privateChannel": self.WS_PRIVATE,
            }
            await ws.send_str(json.dumps(msg))

    def _parse_tick(self, asked_sym_by_ku: Dict[str, str], payload: Dict) -> Optional[HotPriceTick]:
        """
        KuCoin tickerV2 data example fields (docs):
          data: {
            symbol, bestBidPrice, bestAskPrice, bestBidSize, bestAskSize, ts, sequence, ...
          }
        """
        if not isinstance(payload, dict):
            return None
        if payload.get("type") != "message":
            return None

        topic = payload.get("topic") or ""
        data = payload.get("data")
        if not isinstance(data, dict):
            return None

        ku_sym = str(data.get("symbol") or "").upper().strip()
        if not ku_sym:
            # sometimes symbol can be parsed from topic
            # topic: /contractMarket/tickerV2:XBTUSDTM
            if ":" in topic:
                ku_sym = topic.split(":")[-1].split("#")[0].upper().strip()

        if not ku_sym:
            return None

        asked_sym = asked_sym_by_ku.get(ku_sym)
        if not asked_sym:
            return None

        bid = self._to_float(data.get("bestBidPrice"), 0.0)
        ask = self._to_float(data.get("bestAskPrice"), 0.0)

        # "hot price" = mid of BBO
        if bid > 0 and ask > 0:
            price = (bid + ask) / 2.0
        elif bid > 0:
            price = bid
        elif ask > 0:
            price = ask
        else:
            return None

        ts = self._to_int(data.get("ts"), int(time.time() * 1000))
        return HotPriceTick(symbol=asked_sym, price=price, event_time_ms=ts)

    async def _run_chunk(self, asked_syms: List[str], on_tick: Callable[[HotPriceTick], Awaitable[None]]) -> None:
        backoff = self.reconnect_min_sec

        # reverse map: kucoin_symbol -> asked_symbol
        asked_sym_by_ku = {self._sym_map[a]: a for a in asked_syms}

        while not self._stop.is_set():
            ws = None
            ping_task = None
            try:
                assert self._session is not None

                ws_sig = KucoinWsPublic(self._session)
                endpoint, token = await ws_sig.get_ws_endpoint()
                url = f"{endpoint}?token={token}"

                ws = await self._session.ws_connect(url, autoping=False, max_msg_size=0)

                await self._subscribe(ws, asked_syms)
                ping_task = asyncio.create_task(self._ws_ping_loop(ws))
                backoff = self.reconnect_min_sec

                async for m in ws:
                    if self._stop.is_set():
                        break

                    if m.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(m.data)
                        except Exception:
                            continue

                        tick = self._parse_tick(asked_sym_by_ku, payload)
                        if tick:
                            if self._should_emit(tick.symbol, tick.event_time_ms):
                                await on_tick(tick)

                    elif m.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
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
        if self._session is not None:
            raise RuntimeError("Stream already running")

        self._session = aiohttp.ClientSession()
        try:
            for chunk in self._chunks():
                self._tasks.append(asyncio.create_task(self._run_chunk(chunk, on_tick)))
            await self._stop.wait()
        finally:
            await self.aclose()


# ----------------------------
# SELF TEST (runs "forever")
# ----------------------------
async def _main():
    symbols = [
        '0GUSDT', '1000000BOBUSDT', '1000000MOGUSDT', '1000BONKUSDT', '1000CATUSDT',
        '1000CHEEMSUSDT', '1000FLOKIUSDT', '1000LUNCUSDT', '1000PEPEUSDT', '1000RATSUSDT',
        '1000SATSUSDT', '1000SHIBUSDT', '1000WHYUSDT', '1000XECUSDT', '1INCHUSDT',
        '1MBABYDOGEUSDT', '2ZUSDT', '4USDT', 'A2ZUSDT', 'AAVEUSDT', 'ACEUSDT', 'ACHUSDT',
        'ACTUSDT', 'ACUUSDT', 'ACXUSDT', 'ADAUSDT', 'AERGOUSDT', 'AEROUSDT', 'AEVOUSDT', 'AGLDUSDT'
    ]

    async def on_tick(t: HotPriceTick):
        print(f"{t.symbol:<14} price={t.price:<14} E={t.event_time_ms}")

    stream = KucoinHotPriceStream(symbols, chunk_size=50, throttle_ms=0)
    await stream.run(on_tick)

if __name__ == "__main__":
    asyncio.run(_main())
