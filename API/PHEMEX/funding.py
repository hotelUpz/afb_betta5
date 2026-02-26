
# ============================================================
# FILE: API/PHEMEX/funding.py
# ROLE: Phemex USDT-M Perpetual funding via REST (public)
# ENDPOINT: GET https://api.phemex.com/contract-biz/public/real-funding-rates
# NOTES:
#   Phemex paginates this endpoint. To fetch ALL symbols you must iterate pages.
# NOTE: Single responsibility: ONLY funding.
# ============================================================

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp


@dataclass(frozen=True)
class FundingInfo:
    symbol: str
    funding_rate: float
    next_funding_time_ms: int


class PhemexFunding:
    """Public funding client for Phemex contracts.

    Uses a shared aiohttp.ClientSession.
    """

    BASE_URL = "https://api.phemex.com"

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
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}: {text}")
                    return await resp.json()
            except Exception as e:
                last_err = e
                s = (str(e) or "").lower()
                if "session is closed" in s or "connector is closed" in s or "clientconnectorerror" in s:
                    self._session = None
                if attempt < self._retries:
                    await asyncio.sleep(0.4 * attempt)
                else:
                    break

        raise RuntimeError(f"Phemex funding failed: {path} params={params} err={last_err}")

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

    def _parse_one(self, obj: Dict[str, Any]) -> Optional[FundingInfo]:
        sym = obj.get("symbol")
        if not sym:
            return None
        return FundingInfo(
            symbol=str(sym),
            funding_rate=self._to_float(obj.get("fundingRate"), 0.0),
            next_funding_time_ms=self._to_int(obj.get("nextFundingTime") or obj.get("nextfundingTime"), 0),
        )

    @staticmethod
    def _extract_rows(payload: Any) -> List[Dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]

        if not isinstance(payload, dict):
            return []

        data = payload.get("data")
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            for k in ("rows", "result", "list"):
                v = data.get(k)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]

        for k in ("rows", "result", "list"):
            v = payload.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]

        return []

    async def get_all(self) -> List[FundingInfo]:
        out: List[FundingInfo] = []
        page_num = 1
        page_size = 200

        while True:
            payload = await self._get_json(
                "/contract-biz/public/real-funding-rates",
                params={"symbol": "ALL", "pageNum": page_num, "pageSize": page_size},
            )
            rows = self._extract_rows(payload)
            if not rows:
                break

            for obj in rows:
                fi = self._parse_one(obj)
                if fi:
                    out.append(fi)

            if len(rows) < page_size:
                break
            page_num += 1

        return out


# ----------------------------
# SELF TEST
# ----------------------------
async def _main():
    api = PhemexFunding()
    rows = await api.get_all()
    print(f"Funding rows: {len(rows)}")
    for r in rows[:15]:
        print(r)
    await api.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
