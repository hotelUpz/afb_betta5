
# ============================================================
# FILE: API/KUCOIN/funding.py
# ROLE: KuCoin Futures funding rates via REST (aiohttp)
# NOTE: Single responsibility: ONLY funding rate data.
# ============================================================

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import aiohttp


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass(frozen=True)
class FundingInfo:
    symbol: str
    funding_rate: float
    next_funding_time_ms: int
    updated_at_ms: int
    interval_hours: int | None = None


def _extract_interval_hours(item: Dict[str, Any]) -> int | None:
    """Best-effort parser for KuCoin funding interval metadata from contracts/active payload."""
    for key in (
        "fundingInterval",
        "fundingIntervalHour",
        "fundingIntervalHours",
        "fundingFeeInterval",
        "fundingRateInterval",
        "fundingRateGranularity",
        "fundingPeriod",
    ):
        raw = item.get(key)
        if raw is None:
            continue
        try:
            v = float(raw)
        except Exception:
            continue
        if v <= 0:
            continue
        if v in (1, 4, 8):
            return int(v)
        if v in (3600, 14400, 28800):
            return int(v // 3600)
        if v in (3_600_000, 14_400_000, 28_800_000):
            return int(v // 3_600_000)
    return None


class KucoinFunding:
    """KuCoin Futures funding rates.

    Base:
        https://api-futures.kucoin.com

    Endpoints used:
        GET /api/v1/contracts/active                (bulk; includes funding fields)
        GET /api/v1/funding-rate/{symbol}/current   (single)

    Uses a shared aiohttp.ClientSession.
    """

    def __init__(
        self,
        base_url: str = "https://api-futures.kucoin.com",
        timeout_sec: float = 20.0,
        retries: int = 3,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = aiohttp.ClientTimeout(total=float(timeout_sec))
        self.retries = int(retries)
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                return self._session
            connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, enable_cleanup_closed=True)
            self._session = aiohttp.ClientSession(timeout=self.timeout, connector=connector)
            return self._session

    async def aclose(self) -> None:
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None

    async def _get_json(self, path: str) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        last_err: Optional[Exception] = None

        for attempt in range(1, self.retries + 1):
            try:
                session = await self._get_session()
                async with session.get(url) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}: {text}")
                    data = await resp.json()
                    if not isinstance(data, dict):
                        raise RuntimeError(f"Bad JSON root: {type(data)}")
                    return data
            except Exception as e:
                last_err = e
                s = (str(e) or "").lower()
                if "session is closed" in s or "connector is closed" in s or "clientconnectorerror" in s:
                    self._session = None
                if attempt < self.retries:
                    await asyncio.sleep(0.35 * attempt)
                else:
                    break

        raise RuntimeError(f"KuCoin REST failed: {path} err={last_err}")

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
    def _match_quote(item: Dict[str, Any], quote: str) -> bool:
        q = quote.upper().strip()
        for k in ("quoteCurrency", "rootSymbol", "settleCurrency"):
            v = item.get(k)
            if v and str(v).upper().strip() == q:
                return True
        sym = str(item.get("symbol") or "").upper()
        if q and sym.endswith(q + "M"):
            return True
        if q and sym.endswith(q):
            return True
        return False

    async def get_one(self, symbol: str) -> FundingInfo:
        sym = str(symbol).upper().strip()
        js = await self._get_json(f"/api/v1/funding-rate/{sym}/current")
        data = js.get("data") or {}
        if not isinstance(data, dict):
            data = {}

        rate = self._to_float(data.get("value"), 0.0)
        nxt = self._to_int(data.get("fundingTime"), 0)
        return FundingInfo(symbol=sym, funding_rate=rate, next_funding_time_ms=nxt, updated_at_ms=_now_ms())

    async def get_all(self, quote: str = "USDT", limit: Optional[int] = None) -> Dict[str, FundingInfo]:
        js = await self._get_json("/api/v1/contracts/active")
        data = js.get("data")

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = [data]

        now_ms = _now_ms()
        out: Dict[str, FundingInfo] = {}

        for it in items:
            if not isinstance(it, dict):
                continue
            if not self._match_quote(it, quote):
                continue

            sym = it.get("symbol")
            if not sym:
                continue
            sym = str(sym).upper().strip()

            rate = self._to_float(it.get("fundingFeeRate"), 0.0)
            nxt = self._to_int(it.get("nextFundingRateDateTime") or it.get("nextFundingRateTime"), 0)

            out[sym] = FundingInfo(
                symbol=sym,
                funding_rate=rate,
                next_funding_time_ms=nxt,
                updated_at_ms=now_ms,
                interval_hours=_extract_interval_hours(it),
            )

            if limit and len(out) >= int(limit):
                break

        return out


# ----------------------------
# SELF TEST
# ----------------------------
async def _main():
    api = KucoinFunding()
    data = await api.get_all("USDT", limit=10)
    for k in sorted(data.keys()):
        f = data[k]
        print(f"{f.symbol:<12} funding={f.funding_rate:+.6f} next={f.next_funding_time_ms}")
    await api.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
