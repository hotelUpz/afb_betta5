
# ============================================================
# FILE: OKX/symbol.py
# ROLE: OKX Futures (SWAP) symbols utilities (public REST)
# PURPOSE: list / validate / normalize symbols for OKX SWAP markets
# ============================================================

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

import aiohttp


@dataclass(frozen=True)
class OkxInstrument:
    inst_id: str
    inst_type: str
    state: str


class OkxSymbolsRest:
    """Public REST client for OKX instruments (SWAP).

    Docs:
        GET /api/v5/public/instruments?instType=SWAP

    Uses a shared aiohttp.ClientSession.
    """

    def __init__(
        self,
        base_url: str = "https://www.okx.com",
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

    async def _get_json(self, path: str, params: Optional[dict] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        last_err: Optional[Exception] = None

        for attempt in range(1, self.retries + 1):
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
                if attempt < self.retries:
                    await asyncio.sleep(0.35 * attempt)
                else:
                    break

        raise RuntimeError(f"OKX REST failed: {path} params={params} err={last_err}")

    async def instruments(self, inst_type: str = "SWAP") -> List[OkxInstrument]:
        payload = await self._get_json("/api/v5/public/instruments", params={"instType": inst_type})
        arr = payload.get("data")
        out: List[OkxInstrument] = []
        if isinstance(arr, list):
            for it in arr:
                if not isinstance(it, dict):
                    continue
                inst_id = str(it.get("instId") or "").strip()
                if not inst_id:
                    continue
                out.append(
                    OkxInstrument(
                        inst_id=inst_id,
                        inst_type=str(it.get("instType") or inst_type),
                        state=str(it.get("state") or ""),
                    )
                )
        return out

    async def symbols(self, quote: str = "USDT", only_live: bool = True) -> Set[str]:
        quote = (quote or "USDT").upper().strip()
        instruments = await self.instruments("SWAP")
        out: Set[str] = set()
        for ins in instruments:
            if only_live and ins.state.lower() != "live":
                continue
            if f"-{quote}-SWAP" in ins.inst_id.upper():
                out.add(ins.inst_id.upper())
        return out


class OkxSymbol:
    """Symbol normalization helpers for OKX SWAP."""

    @staticmethod
    def normalize(raw: str, quote: str = "USDT") -> Optional[str]:
        if not raw or not isinstance(raw, str):
            return None

        s = raw.strip().upper()
        if not s:
            return None

        if "-" in s:
            return s

        q = (quote or "USDT").upper().strip()
        if not q or not s.endswith(q):
            return None
        base = s[: -len(q)]
        if not base:
            return None
        return f"{base}-{q}-SWAP"
