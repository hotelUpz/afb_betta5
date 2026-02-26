# ============================================================
# FILE: API/OKX/funding.py
# ROLE: OKX SWAP funding rate via WS (aiohttp) (ALL symbols by subscription)
# CHANNEL: funding-rate
# NOTE: Single responsibility: ONLY funding rate.
# DOCS: /ws/v5/public funding-rate channel (push every ~30-90s)
# ============================================================

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import aiohttp


@dataclass(frozen=True)
class FundingInfo:
    symbol: str  # OKX instId, e.g. BTC-USDT-SWAP
    funding_rate: float
    funding_time_ms: int
    next_funding_time_ms: int


class OkxFundingStream:
    """Funding rate stream for OKX SWAP instruments.

    Why WS:
        OKX REST funding endpoint is per-instId. There is no single REST call
        that returns funding for ALL instruments. WS channel `funding-rate`
        can push updates for subscribed instIds.

    WS URL:
        wss://ws.okx.com:8443/ws/v5/public

    Subscribe message:
        {"op":"subscribe","args":[{"channel":"funding-rate","instId":"BTC-USDT-SWAP"}, ...]}

    Cache (optional):
        cache[instId] = {"funding_rate": float, "funding_time_ms": int, "next_funding_time_ms": int}

    Notes:
        - OKX suggests sending string "ping" when no data for <30s and expecting "pong".
        - Funding data is pushed roughly every 30-90 seconds.
    """

    WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

    def __init__(
        self,
        inst_ids: Iterable[str],
        *,
        cache: Optional[Dict[str, dict]] = None,
        chunk_size: int = 100,
        idle_ping_sec: float = 25.0,
        reconnect_min_sec: float = 1.0,
        reconnect_max_sec: float = 25.0,
        max_msg_size: int = 0,
    ):
        self.inst_ids = [str(s).upper().strip() for s in inst_ids if isinstance(s, str) and s.strip()]
        if not self.inst_ids:
            raise ValueError("inst_ids must be non-empty")

        self.cache = cache
        self.chunk_size = max(1, int(chunk_size))
        self.idle_ping_sec = float(idle_ping_sec)
        self.reconnect_min_sec = float(reconnect_min_sec)
        self.reconnect_max_sec = float(reconnect_max_sec)
        self.max_msg_size = int(max_msg_size)

        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._session: Optional[aiohttp.ClientSession] = None

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

    def _chunks(self) -> List[List[str]]:
        out: List[List[str]] = []
        cur: List[str] = []
        for s in self.inst_ids:
            cur.append(s)
            if len(cur) >= self.chunk_size:
                out.append(cur)
                cur = []
        if cur:
            out.append(cur)
        return out

    @staticmethod
    def _to_float(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return default

    @staticmethod
    def _to_int(v: Any, default: int = 0) -> int:
        try:
            return int(v)
        except Exception:
            return default

    def parse_and_store(self, payload: Dict[str, Any], cache: Optional[Dict[str, dict]] = None) -> Optional[FundingInfo]:
        """Parse OKX push payload and optionally store into cache."""
        if not isinstance(payload, dict):
            return None

        arr = payload.get("data")
        if not isinstance(arr, list) or not arr:
            return None

        d0 = arr[0]
        if not isinstance(d0, dict):
            return None

        inst_id = str(d0.get("instId") or "").upper().strip()
        if not inst_id:
            return None

        fr = self._to_float(d0.get("fundingRate"), 0.0)
        ft = self._to_int(d0.get("fundingTime"), 0)
        nft = self._to_int(d0.get("nextFundingTime"), 0)

        out = FundingInfo(symbol=inst_id, funding_rate=fr, funding_time_ms=ft, next_funding_time_ms=nft)

        dst = cache if cache is not None else self.cache
        if dst is not None:
            now_ms = int(time.time() * 1000)
            dst[inst_id] = {
                "funding_rate": fr,
                "funding_time_ms": ft,
                "next_funding_time_ms": nft,
                "updated_at_ms": now_ms,
                "source": "ws",
            }

        return out

    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse, inst_ids: List[str]) -> None:
        msg = {
            "op": "subscribe",
            "args": [{"channel": "funding-rate", "instId": s} for s in inst_ids],
        }
        await ws.send_str(json.dumps(msg))

    async def _run_chunk(self, inst_ids: List[str]) -> None:
        backoff = self.reconnect_min_sec

        while not self._stop.is_set():
            ws: Optional[aiohttp.ClientWebSocketResponse] = None
            try:
                assert self._session is not None
                ws = await self._session.ws_connect(
                    self.WS_URL,
                    autoping=False,  # OKX wants manual ping/pong
                    max_msg_size=self.max_msg_size,
                )

                await self._subscribe(ws, inst_ids)
                backoff = self.reconnect_min_sec

                # OKX: if no message within <30 sec, send 'ping' and expect 'pong'
                while not self._stop.is_set():
                    try:
                        msg = await ws.receive(timeout=self.idle_ping_sec)
                    except asyncio.TimeoutError:
                        # keepalive
                        with contextlib.suppress(Exception):
                            await ws.send_str("ping")
                        continue

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        if msg.data == "pong":
                            continue
                        try:
                            payload = json.loads(msg.data)
                        except Exception:
                            continue

                        # ignore subscribe acks / errors
                        if payload.get("event") in ("subscribe", "unsubscribe", "error"):
                            continue

                        self.parse_and_store(payload)

                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break

            except asyncio.CancelledError:
                break
            except Exception:
                sleep_for = min(self.reconnect_max_sec, backoff) * (0.7 + random.random() * 0.6)
                await asyncio.sleep(sleep_for)
                backoff = min(self.reconnect_max_sec, backoff * 1.7)
            finally:
                if ws is not None and not ws.closed:
                    with contextlib.suppress(Exception):
                        await ws.close()

    async def run(self) -> None:
        """Start WS connections (chunked) and block until stop()."""
        if self._session is not None:
            raise RuntimeError("Stream already running")

        self._session = aiohttp.ClientSession()
        try:
            for chunk in self._chunks():
                self._tasks.append(asyncio.create_task(self._run_chunk(chunk)))

            await self._stop.wait()
        finally:
            await self.aclose()


# ----------------------------
# SELF TEST (runs forever)
# ----------------------------
async def _main() -> None:
    # a small test set; replace with your full instrument list
    insts = [
        "BTC-USDT-SWAP",
        "ETH-USDT-SWAP",
        "SOL-USDT-SWAP",
    ]

    cache: Dict[str, dict] = {}
    stream = OkxFundingStream(insts, cache=cache, chunk_size=100, idle_ping_sec=25)

    async def printer() -> None:
        last = {}
        while True:
            await asyncio.sleep(3)
            # print only when changed
            for k, v in list(cache.items()):
                sig = (v.get("funding_rate"), v.get("next_funding_time_ms"))
                if last.get(k) != sig:
                    last[k] = sig
                    print(f"{k:<14} rate={v.get('funding_rate'):+.8f} next={v.get('next_funding_time_ms')}")

    t1 = asyncio.create_task(stream.run())
    t2 = asyncio.create_task(printer())

    try:
        await asyncio.gather(t1, t2)
    finally:
        stream.stop()
        t2.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t2
        await t1


if __name__ == "__main__":
    asyncio.run(_main())
