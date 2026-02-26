
# ============================================================
# FILE: API/BINANCE/funding.py
# ROLE: Binance USDT-M Futures funding via REST (ONLY funding here).
# ENDPOINT: GET https://fapi.binance.com/fapi/v1/premiumIndex
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


class BinanceFunding:
    """Public funding REST client.

    Notes:
        - symbol=None => Binance returns LIST for ALL symbols
        - symbol=str  => returns single object

    This implementation uses a shared aiohttp.ClientSession (connection pooling).
    """

    BASE_URL = "https://fapi.binance.com"

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

        raise RuntimeError(f"Binance funding failed: {path} params={params} err={last_err}")

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
        # premiumIndex uses lastFundingRate + nextFundingTime
        return FundingInfo(
            symbol=str(sym),
            funding_rate=self._to_float(obj.get("lastFundingRate"), 0.0),
            next_funding_time_ms=self._to_int(obj.get("nextFundingTime"), 0),
        )

    async def get_all(self) -> List[FundingInfo]:
        data = await self._get_json("/fapi/v1/premiumIndex", params=None)
        out: List[FundingInfo] = []

        if isinstance(data, list):
            for obj in data:
                if isinstance(obj, dict):
                    fi = self._parse_one(obj)
                    if fi:
                        out.append(fi)
        elif isinstance(data, dict):
            fi = self._parse_one(data)
            if fi:
                out.append(fi)

        return out

    async def get_one(self, symbol: str) -> Optional[FundingInfo]:
        sym = (symbol or "").upper().strip()
        if not sym:
            return None
        data = await self._get_json("/fapi/v1/premiumIndex", params={"symbol": sym})
        if isinstance(data, dict):
            return self._parse_one(data)
        return None


# ----------------------------
# SELF TEST
# ----------------------------
async def _main():
    api = BinanceFunding()
    rows = await api.get_all()
    print(f"Funding rows: {len(rows)}")
    top = sorted(rows, key=lambda r: abs(r.funding_rate), reverse=True)[:20]
    for i, r in enumerate(top, 1):
        print(f"{i:02d}. {r.symbol:<12} rate={r.funding_rate:+.6f} next={r.next_funding_time_ms}")
    print("BTCUSDT:", await api.get_one("BTCUSDT"))
    await api.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
