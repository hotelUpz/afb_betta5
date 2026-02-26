
# ============================================================
# FILE: API/PHEMEX/symbol.py
# ROLE: Phemex USDT Perpetual (Futures) symbols via REST.
# NOTE: Standalone module (no package imports).
# ============================================================

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp


@dataclass(frozen=True)
class SymbolInfo:
    symbol: str
    status: str
    quote: str


class PhemexSymbols:
    """Public symbols REST.

    Uses:
        GET https://api.phemex.com/public/products

    Important:
        Phemex "spot" symbols often have prefix 's' (e.g. sBTCUSDT).
        Here we return ONLY USDT perpetual contract symbols like BTCUSDT.

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

    async def _get_json(self, path: str) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{path}"
        last_err: Optional[Exception] = None
        for attempt in range(1, self._retries + 1):
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
                if attempt < self._retries:
                    await asyncio.sleep(0.4 * attempt)
                else:
                    break
        raise RuntimeError(f"Phemex symbols failed: {path} err={last_err}")

    @staticmethod
    def _norm_quote(v: Any) -> str:
        return (str(v) if v is not None else "").upper().strip()

    def _parse_perp(self, obj: Dict[str, Any]) -> Optional[SymbolInfo]:
        sym = obj.get("symbol")
        if not sym:
            return None

        q = self._norm_quote(obj.get("quoteCurrency") or obj.get("settleCurrency") or "")
        if q != "USDT":
            return None

        sym_s = str(sym).strip()
        if sym_s.startswith("s"):
            return None

        status = str(obj.get("status") or obj.get("state") or obj.get("symbolStatus") or "Listed")
        return SymbolInfo(symbol=sym_s.upper(), status=status, quote="USDT")

    async def get_all(self) -> List[SymbolInfo]:
        data = await self._get_json("/public/products")

        root = data.get("data") if isinstance(data, dict) else None
        if not isinstance(root, dict):
            return []

        arr = root.get("perpProductsV2") or root.get("perpProducts") or []
        out: List[SymbolInfo] = []
        if isinstance(arr, list):
            for it in arr:
                if isinstance(it, dict):
                    si = self._parse_perp(it)
                    if si:
                        out.append(si)

        if not out:
            for _, v in root.items():
                if isinstance(v, list):
                    for it in v:
                        if isinstance(it, dict):
                            si = self._parse_perp(it)
                            if si:
                                out.append(si)

        seen = set()
        uniq: List[SymbolInfo] = []
        for s in out:
            if s.symbol not in seen:
                seen.add(s.symbol)
                uniq.append(s)
        return uniq


# ----------------------------
# SELF TEST
# ----------------------------
if __name__ == "__main__":
    async def _main():
        api = PhemexSymbols()
        rows = await api.get_all()
        print(f"Symbols: {len(rows)}")
        for r in rows[:20]:
            print(r)
        await api.aclose()

    asyncio.run(_main())
