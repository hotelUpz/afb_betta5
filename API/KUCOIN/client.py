# ============================================================
# FILE: API/KUCOIN/client.py
# ROLE: Thin exchange client wrapper to provide unified interface for CORE.
# ============================================================

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Optional

from c_log import UnifiedLogger
from CORE.symbols import SymbolNormalizer

from .symbol import KucoinSymbols
from .funding import KucoinFunding, FundingInfo as KucoinFundingInfo


@dataclass(frozen=True)
class FundingPoint:
    symbol: str
    funding_rate: float  # fraction
    next_funding_time_ms: int
    updated_at_ms: int
    source: str = "rest"
    interval_hours: int | None = None

    @property
    def funding_rate_pct(self) -> float:
        return float(self.funding_rate) * 100.0


class KucoinSymbolsAdapter:
    def __init__(self, logger: UnifiedLogger):
        self.logger = logger
        self.api = KucoinSymbols()

    async def get_symbol_map(self, quote: str = "USDT") -> Dict[str, str]:
        raw_syms = await self.api.get_perp_symbols(quote=quote)
        q = (quote or "USDT").upper().strip()
        out: Dict[str, str] = {}
        for raw in raw_syms:
            parsed = SymbolNormalizer.parse_kucoin_symbol(raw, quote=q)
            if not parsed:
                continue
            canon = SymbolNormalizer.canonical_pair(parsed[0], parsed[1])
            out[canon] = str(raw).upper().strip()
        return out


class KucoinFundingAdapter:
    DEFAULT_INTERVAL_HOURS = 8

    def __init__(self, logger: UnifiedLogger):
        self.logger = logger
        self.api = KucoinFunding()
        self.cache: Dict[str, FundingPoint] = {}
        self.updated_at_ms: int = 0

    @staticmethod
    def _normalize_rate(v: float) -> float:
        if v is None:
            return 0.0
        x = float(v)
        return x / 100.0 if abs(x) > 1.0 else x

    async def refresh(self, quote: str = "USDT") -> None:
        data = await self.api.get_all(quote=quote)
        now_ms = int(time.time() * 1000)
        out: Dict[str, FundingPoint] = {}
        for sym, r in data.items():
            if not isinstance(r, KucoinFundingInfo):
                continue
            out[sym] = FundingPoint(
                symbol=sym,
                funding_rate=self._normalize_rate(r.funding_rate),
                next_funding_time_ms=int(r.next_funding_time_ms or 0),
                updated_at_ms=int(r.updated_at_ms or now_ms),
                source="rest",
                interval_hours=(int(r.interval_hours) if getattr(r, "interval_hours", None) else None),
            )
        self.cache = out
        self.updated_at_ms = now_ms

    def get(self, symbol: str) -> Optional[FundingPoint]:
        return self.cache.get(str(symbol).upper().strip())

    def interval_hours(self, symbol: str) -> int:
        pt = self.get(symbol)
        if pt is not None:
            try:
                h = int(getattr(pt, "interval_hours", 0) or 0)
                if h > 0:
                    return h
            except Exception:
                pass
        return int(self.DEFAULT_INTERVAL_HOURS)


class KucoinClient:
    name = "KUCOIN"

    def __init__(self, *, logger: UnifiedLogger):
        self.logger = logger
        self.symbols = KucoinSymbolsAdapter(logger)
        self.funding = KucoinFundingAdapter(logger)

        self.price = None
        self.stakan = None

    async def bootstrap(self) -> None:
        # optional preload; UniverseBuilder will refresh later anyway
        try:
            await self.funding.refresh(quote="USDT")
        except Exception as e:
            self.logger.warning(f"KUCOIN funding preload failed: {e}")

    async def shutdown(self) -> None:
        """Best-effort cleanup for long-lived aiohttp sessions/streams."""
        # close REST sessions (symbols/funding)
        for mod_name in ("symbols", "funding"):
            mod = getattr(self, mod_name, None)
            api = getattr(mod, "api", None) if mod is not None else None
            if api is not None and hasattr(api, "aclose"):
                try:
                    await api.aclose()
                except Exception:
                    pass

        # stop OKX funding WS stream if present
        f = getattr(self, "funding", None)
        if f is not None and hasattr(f, "stop_stream"):
            try:
                await f.stop_stream()
            except Exception:
                pass
