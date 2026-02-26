
# ============================================================
# FILE: API/KUCOIN/symbol.py
# ROLE: KuCoin Futures symbols (USDT-M by default) via REST (aiohttp)
# NOTE: Single responsibility: ONLY fetch + filter futures symbols.
# ============================================================

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, Set

import aiohttp


class KucoinSymbols:
    """KuCoin Futures symbols (active contracts) via REST.

    Base:
        https://api-futures.kucoin.com

    Endpoint:
        GET /api/v1/contracts/active

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
    def _is_open(status: Any) -> bool:
        if status is None:
            return True
        s = str(status).strip().lower()
        return s in ("open", "trading", "1", "true", "active")

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

    async def get_perp_symbols(self, quote: str = "USDT", limit: Optional[int] = None) -> Set[str]:
        js = await self._get_json("/api/v1/contracts/active")
        data = js.get("data")

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = [data]

        out: Set[str] = set()
        for it in items:
            if not isinstance(it, dict):
                continue
            if not self._is_open(it.get("status")):
                continue
            if not self._match_quote(it, quote):
                continue
            sym = it.get("symbol")
            if not sym:
                continue
            out.add(str(sym).upper().strip())
            if limit and len(out) >= int(limit):
                break

        return out


# ----------------------------
# SELF TEST
# ----------------------------
async def _main():
    api = KucoinSymbols()
    syms = await api.get_perp_symbols("USDT", limit=50)
    print(f"KUCOIN symbols: {len(syms)}")
    print(sorted(list(syms))[:20])
    await api.aclose()


if __name__ == "__main__":
    asyncio.run(_main())
