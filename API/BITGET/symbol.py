
# ============================================================
# FILE: API/BITGET/symbol.py
# ROLE: Bitget Futures symbols utilities (public REST)
# PURPOSE: list / validate / normalize symbols for Bitget USDT-M perpetual futures
# DOCS: GET /api/v2/mix/market/contracts
# ============================================================

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp


@dataclass(frozen=True)
class BitgetContract:
    symbol: str
    base_coin: str
    quote_coin: str
    symbol_type: str
    symbol_status: str
    fund_interval_hours: int | None = None


class BitgetSymbols:
    """Public REST client for Bitget Futures contracts.

    Endpoint:
        GET https://api.bitget.com/api/v2/mix/market/contracts?productType=usdt-futures

    Notes:
        - productType in REST is typically lower-case with dash in examples ("usdt-futures").
        - We keep only perpetual contracts and, by default, only those in normal trading status.

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

        raise RuntimeError(f"BitgetSymbols request failed: {last_err}")

    @staticmethod
    def _to_int(v: Any) -> int:
        try:
            return int(float(v))
        except Exception:
            return 0

    async def get_contracts(self, *, product_type: str = "usdt-futures", symbol: str | None = None) -> List[BitgetContract]:
        params: Dict[str, Any] = {"productType": product_type}
        if symbol:
            params["symbol"] = str(symbol).upper().strip()

        j = await self._get_json("/api/v2/mix/market/contracts", params=params)

        # Typical payload:
        # {"code":"00000","msg":"success","requestTime":...,"data":[{...}]}
        if isinstance(j, dict) and str(j.get("code")) not in ("00000", "0", "200"):
            raise RuntimeError(f"BitgetSymbols bad response: {j}")

        data = (j or {}).get("data") if isinstance(j, dict) else None
        if not isinstance(data, list):
            return []

        out: List[BitgetContract] = []
        for it in data:
            if not isinstance(it, dict):
                continue
            out.append(
                BitgetContract(
                    symbol=str(it.get("symbol") or "").upper().strip(),
                    base_coin=str(it.get("baseCoin") or "").upper().strip(),
                    quote_coin=str(it.get("quoteCoin") or "").upper().strip(),
                    symbol_type=str(it.get("symbolType") or "").lower().strip(),
                    symbol_status=str(it.get("symbolStatus") or "").lower().strip(),
                    fund_interval_hours=self._to_int(it.get("fundInterval")) or None,
                )
            )
        return out

    async def get_perp_symbols(self, quote: str = "USDT", *, product_type: str = "usdt-futures", only_live: bool = True) -> List[str]:
        """Return list of Bitget perpetual futures symbols for a given quote (default USDT)."""
        q = (quote or "USDT").upper().strip()
        contracts = await self.get_contracts(product_type=product_type)
        out: List[str] = []
        for c in contracts:
            if c.quote_coin != q:
                continue
            if c.symbol_type and c.symbol_type != "perpetual":
                continue
            if only_live and c.symbol_status and c.symbol_status != "normal":
                continue
            if c.symbol:
                out.append(c.symbol)
        return out
