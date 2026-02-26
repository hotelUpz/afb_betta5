# ============================================================
# FILE: OKX/price.py
# ROLE: OKX Futures (SWAP) HOT price stream via WS (aiohttp)
# CHANNEL: tickers  (best-effort hot price from bid/ask/last)
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
    symbol: str  # OKX instId, e.g. BTC-USDT-SWAP
    price: float
    event_time_ms: int


class OkxHotPriceStream:
    """HOT price stream for OKX SWAP markets.

    We use WS `tickers` channel, and compute a "hot" price as:
        mid(bid, ask) if both available else last

    WS public endpoint (OKX v5):
        wss://ws.okx.com:8443/ws/v5/public

    Subscribe format:
        {"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT-SWAP"}, ...]}

    Push format includes bidPx/askPx/last/ts fields.

    Callback:
        async def on_tick(tick: HotPriceTick) -> None

    Cache (optional):
        cache[symbol] = {"price": float, "ts": int}
    """

    WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

    def __init__(
        self,
        symbols: Iterable[str],
        *,
        cache: Optional[Dict[str, dict]] = None,
        ws_url: str = WS_URL,
        chunk_size: int = 150,
        ping_sec: float = 15.0,
        reconnect_min_sec: float = 1.0,
        reconnect_max_sec: float = 25.0,
        throttle_ms: int = 0,
        timeout_sec: float = 30.0,
    ):
        self.symbols = [str(s).upper().strip() for s in symbols if isinstance(s, str) and s.strip()]
        if not self.symbols:
            raise ValueError("symbols must be non-empty")

        self.cache = cache
        self.ws_url = ws_url
        self.chunk_size = max(1, int(chunk_size))
        self.ping_sec = float(ping_sec)
        self.reconnect_min_sec = float(reconnect_min_sec)
        self.reconnect_max_sec = float(reconnect_max_sec)
        self.throttle_ms = int(throttle_ms)
        self.timeout = aiohttp.ClientTimeout(total=float(timeout_sec))

        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_emit_ms: Dict[str, int] = {}

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

    def _should_emit(self, sym: str, now_ms: int) -> bool:
        if self.throttle_ms <= 0:
            return True
        last = self._last_emit_ms.get(sym, 0)
        if now_ms - last >= self.throttle_ms:
            self._last_emit_ms[sym] = now_ms
            return True
        return False

    @staticmethod
    def _pick_price(bid_px: float, ask_px: float, last_px: float) -> float:
        if bid_px > 0 and ask_px > 0:
            return (bid_px + ask_px) / 2.0
        if last_px > 0:
            return last_px
        if bid_px > 0:
            return bid_px
        if ask_px > 0:
            return ask_px
        return 0.0

    def parse_and_store(self, payload: Dict, cache: Optional[Dict[str, dict]] = None) -> Optional[HotPriceTick]:
        """Parse one OKX WS message. If it's a ticker push, update cache and return tick."""
        if not isinstance(payload, dict):
            return None

        arg = payload.get("arg")
        if not isinstance(arg, dict) or arg.get("channel") != "tickers":
            return None

        data = payload.get("data")
        if not isinstance(data, list) or not data:
            return None

        d0 = data[0]
        if not isinstance(d0, dict):
            return None

        inst_id = str(d0.get("instId") or arg.get("instId") or "").upper().strip()
        if not inst_id:
            return None

        bid_px = self._to_float(d0.get("bidPx"), 0.0)
        ask_px = self._to_float(d0.get("askPx"), 0.0)
        last_px = self._to_float(d0.get("last"), 0.0)
        price = self._pick_price(bid_px, ask_px, last_px)
        if price <= 0:
            return None

        ts_ms = self._to_int(d0.get("ts"), int(time.time() * 1000))
        tick = HotPriceTick(symbol=inst_id, price=price, event_time_ms=ts_ms)

        c = cache if cache is not None else self.cache
        if c is not None:
            c[inst_id] = {"price": price, "ts": ts_ms}

        return tick

    async def _send_subscribe(self, ws: aiohttp.ClientWebSocketResponse, symbols: List[str]) -> None:
        args = [{"channel": "tickers", "instId": s} for s in symbols]
        await ws.send_str(json.dumps({"op": "subscribe", "args": args}))

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        # OKX expects text ping/pong
        while not self._stop.is_set():
            await asyncio.sleep(self.ping_sec)
            if ws.closed:
                break
            with contextlib.suppress(Exception):
                await ws.send_str("ping")

    async def _run_chunk(self, symbols: List[str], on_tick: Callable[[HotPriceTick], Awaitable[None]]) -> None:
        backoff = self.reconnect_min_sec

        while not self._stop.is_set():
            ws = None
            ping_task = None
            try:
                assert self._session is not None
                ws = await self._session.ws_connect(self.ws_url, autoping=False, max_msg_size=0)
                ping_task = asyncio.create_task(self._ping_loop(ws))

                await self._send_subscribe(ws, symbols)
                backoff = self.reconnect_min_sec

                async for m in ws:
                    if self._stop.is_set():
                        break

                    if m.type == aiohttp.WSMsgType.TEXT:
                        txt = m.data
                        if txt == "pong":
                            continue
                        if txt == "ping":
                            with contextlib.suppress(Exception):
                                await ws.send_str("pong")
                            continue
                        try:
                            payload = json.loads(txt)
                        except Exception:
                            continue

                        # ignore subscribe events / errors
                        if "event" in payload:
                            continue

                        tick = self.parse_and_store(payload)
                        if tick:
                            now_ms = tick.event_time_ms
                            if self._should_emit(tick.symbol, now_ms):
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

        self._session = aiohttp.ClientSession(timeout=self.timeout)
        try:
            for chunk in self._chunks():
                self._tasks.append(asyncio.create_task(self._run_chunk(chunk, on_tick)))
            await self._stop.wait()
        finally:
            await self.aclose()


# ----------------------------
# SELF TEST (CTRL+C to stop)
# ----------------------------
async def _main() -> None:
    symbols = [
        "BTC-USDT-SWAP",
        "ETH-USDT-SWAP",
    ]

    async def on_tick(t: HotPriceTick) -> None:
        print(f"{t.symbol:<16} price={t.price:<14} ts={t.event_time_ms}")

    cache: Dict[str, dict] = {}
    stream = OkxHotPriceStream(symbols, cache=cache, chunk_size=150, throttle_ms=0)

    task = asyncio.create_task(stream.run(on_tick))
    try:
        # run forever
        await asyncio.Event().wait()
    finally:
        stream.stop()
        await task


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
