
# ============================================================
# FILE: API/BITGET/funding.py
# ROLE: Bitget USDT-M Futures funding via REST (aiohttp)
# ENDPOINT: GET /api/v2/mix/market/current-fund-rate
# NOTE: Single responsibility: ONLY funding rate data.
# ============================================================

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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


class BitgetFunding:
    """Public funding REST client.

    Docs:
        GET https://api.bitget.com/api/v2/mix/market/current-fund-rate

    Notes:
        - 'symbol' is optional in docs; if omitted, API may return list for many symbols.
        - fundingRate is usually decimal fraction (e.g. 0.000068).
        - nextUpdate is next funding timestamp in ms.
        - fundingRateInterval is hours (string/int).

    Uses a shared aiohttp.ClientSession (connection pooling).
    """

    BASE_URL = "https://api.bitget.com"

    def __init__(self, timeout_sec: float = 20.0, retries: int = 3):
        self._timeout = aiohttp.ClientTimeout(total=float(timeout_sec))
        self._retries = int(retries)
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                return self._session
            connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300, enable_cleanup_closed=True)
            self._session = aiohttp.ClientSession(timeout=self._timeout, connector=connector)
            return self._session

    async def aclose(self) -> None:
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None

    async def _get_json(self, path: str, params: Optional[dict] = None) -> Any:
        url = f"{self.BASE_URL}{path}"
        last_err: Optional[Exception] = None

        for attempt in range(1, self._retries + 1):
            try:
                session = await self._get_session()
                async with session.get(url, params=params) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(min(1.0 * attempt, 3.0))
                        continue
                    if resp.status >= 400:
                        txt = await resp.text()
                        raise RuntimeError(f"HTTP {resp.status} {txt}")
                    return await resp.json(content_type=None)
            except Exception as e:
                last_err = e
                s = (str(e) or "").lower()
                if "session is closed" in s or "connector is closed" in s or "clientconnectorerror" in s:
                    self._session = None
                await asyncio.sleep(0.15 * attempt)

        raise RuntimeError(f"BitgetFunding request failed: {last_err}")

    @staticmethod
    def _to_float(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    @staticmethod
    def _to_int(v: Any) -> int:
        try:
            return int(float(v))
        except Exception:
            return 0

    async def get_one(self, symbol: str, *, product_type: str = "usdt-futures") -> FundingInfo:
        sym = str(symbol).upper().strip()
        params: Dict[str, Any] = {"symbol": sym, "productType": product_type}
        j = await self._get_json("/api/v2/mix/market/current-fund-rate", params=params)
        now_ms = _now_ms()

        if isinstance(j, dict) and str(j.get("code")) not in ("00000", "0", "200"):
            raise RuntimeError(f"BitgetFunding bad response: {j}")

        data = (j or {}).get("data") if isinstance(j, dict) else None
        if not isinstance(data, list) or not data:
            return FundingInfo(symbol=sym, funding_rate=0.0, next_funding_time_ms=0, updated_at_ms=now_ms, interval_hours=None)

        it = data[0] if isinstance(data[0], dict) else {}
        return FundingInfo(
            symbol=str(it.get("symbol") or sym).upper().strip(),
            funding_rate=self._to_float(it.get("fundingRate")),
            next_funding_time_ms=self._to_int(it.get("nextUpdate")),
            updated_at_ms=now_ms,
            interval_hours=self._to_int(it.get("fundingRateInterval")) or None,
        )

    async def get_all(self, *, product_type: str = "usdt-futures") -> List[FundingInfo]:
        """Best-effort bulk funding request."""
        params: Dict[str, Any] = {"productType": product_type}
        j = await self._get_json("/api/v2/mix/market/current-fund-rate", params=params)
        now_ms = _now_ms()

        if isinstance(j, dict) and str(j.get("code")) not in ("00000", "0", "200"):
            raise RuntimeError(f"BitgetFunding bad response: {j}")

        data = (j or {}).get("data") if isinstance(j, dict) else None
        if not isinstance(data, list):
            return []

        out: List[FundingInfo] = []
        for it in data:
            if not isinstance(it, dict):
                continue
            sym = str(it.get("symbol") or "").upper().strip()
            if not sym:
                continue
            out.append(
                FundingInfo(
                    symbol=sym,
                    funding_rate=self._to_float(it.get("fundingRate")),
                    next_funding_time_ms=self._to_int(it.get("nextUpdate")),
                    updated_at_ms=now_ms,
                    interval_hours=self._to_int(it.get("fundingRateInterval")) or None,
                )
            )
        return out
