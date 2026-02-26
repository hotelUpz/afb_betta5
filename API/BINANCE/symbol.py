
# ============================================================
# FILE: API/BINANCE/symbol.py
# ROLE: Load Binance USDT-M Futures PERPETUAL symbols via REST.
# ENDPOINT: GET https://fapi.binance.com/fapi/v1/exchangeInfo
# ============================================================

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import aiohttp


class BinanceSymbols:
    """Public symbols loader for Binance USDT-M Futures.

    Filters:
        - contractType == "PERPETUAL"
        - status == "TRADING"
        - quoteAsset == quote (default: "USDT")

    Returns:
        list[str] of raw Binance symbols, e.g. "BTCUSDT"

    Uses a shared aiohttp.ClientSession (connection pooling).
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

    async def _get_json(self, path: str, params: Optional[dict] = None) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{path}"
        last_err: Optional[Exception] = None

        for attempt in range(1, self._retries + 1):
            try:
                session = await self._get_session()
                async with session.get(url, params=params) as resp:
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
                if attempt < self._retries:
                    await asyncio.sleep(0.4 * attempt)
                else:
                    break

        raise RuntimeError(f"Binance symbols failed: {path} params={params} err={last_err}")

    async def get_perp_symbols(self, quote: str = "USDT", limit: Optional[int] = None) -> List[str]:
        data = await self._get_json("/fapi/v1/exchangeInfo")
        quote_u = (quote or "USDT").upper()
        out: List[str] = []

        for s in data.get("symbols", []) or []:
            if not isinstance(s, dict):
                continue
            if s.get("contractType") != "PERPETUAL":
                continue
            if s.get("status") != "TRADING":
                continue
            if (s.get("quoteAsset") or "").upper() != quote_u:
                continue
            sym = s.get("symbol")
            if sym:
                out.append(str(sym))

        out.sort()
        if limit is not None:
            return out[: int(limit)]
        return out


# ----------------------------
# SELF TEST
# ----------------------------
async def _main():
    api = BinanceSymbols()
    syms = await api.get_perp_symbols("USDT")
    print(f"BINANCE PERP USDT symbols: {len(syms)}")
    print("first 30:", syms[:30])
    await api.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
