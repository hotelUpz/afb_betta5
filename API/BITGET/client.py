# ============================================================
# FILE: API/BITGET/client.py
# ROLE: Thin exchange client wrapper to provide unified interface for CORE.
# ============================================================

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Optional

from c_log import UnifiedLogger
from CORE.symbols import SymbolNormalizer

from .symbol import BitgetSymbols
from .funding import BitgetFunding, FundingInfo as BitgetFundingInfo


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


class BitgetSymbolsAdapter:
    def __init__(self, logger: UnifiedLogger):
        self.logger = logger
        self.api = BitgetSymbols()

    async def get_symbol_map(self, quote: str = "USDT") -> Dict[str, str]:
        raw_syms = await self.api.get_perp_symbols(quote=quote, product_type="usdt-futures", only_live=True)
        q = (quote or "USDT").upper().strip()
        out: Dict[str, str] = {}
        for raw in raw_syms:
            # Bitget USDT futures symbols are like BTCUSDT -> compatible with Binance parser.
            parsed = SymbolNormalizer.parse_binance_symbol(str(raw).upper().strip(), quote=q)
            if not parsed:
                continue
            canon = SymbolNormalizer.canonical_pair(parsed[0], parsed[1])
            out[canon] = str(raw).upper().strip()
        return out


class BitgetFundingAdapter:
    """Bulk funding fetcher (REST).

    Bitget funding endpoint supports symbol optional in docs (best-effort bulk).
    If bulk returns empty (rare), you can fallback to per-symbol calls outside.
    """

    DEFAULT_INTERVAL_HOURS = 8

    def __init__(self, logger: UnifiedLogger):
        self.logger = logger
        self.api = BitgetFunding()
        self.cache: Dict[str, FundingPoint] = {}
        self.updated_at_ms: int = 0

    @staticmethod
    def _normalize_rate(v: float) -> float:
        if v is None:
            return 0.0
        x = float(v)
        return x / 100.0 if abs(x) > 1.0 else x

    async def refresh(self) -> None:
        rows = await self.api.get_all(product_type="usdt-futures")
        now_ms = int(time.time() * 1000)
        out: Dict[str, FundingPoint] = {}
        for r in rows:
            if not isinstance(r, BitgetFundingInfo):
                continue
            sym = str(r.symbol).upper().strip()
            out[sym] = FundingPoint(
                symbol=sym,
                funding_rate=self._normalize_rate(r.funding_rate),
                next_funding_time_ms=int(r.next_funding_time_ms or 0),
                updated_at_ms=now_ms,
                source="rest",
                interval_hours=r.interval_hours,
            )
        self.cache = out
        self.updated_at_ms = now_ms

    def get(self, symbol: str) -> Optional[FundingPoint]:
        return self.cache.get(str(symbol).upper().strip())

    def interval_hours(self, symbol: str) -> int:
        p = self.get(symbol)
        if p and p.interval_hours:
            return int(p.interval_hours)
        return int(self.DEFAULT_INTERVAL_HOURS)


class BitgetClient:
    name = "BITGET"

    def __init__(self, *, logger: UnifiedLogger):
        self.logger = logger
        self.symbols = BitgetSymbolsAdapter(logger)
        self.funding = BitgetFundingAdapter(logger)

        # placeholders (streams are in separate modules)
        self.price = None
        self.stakan = None

    async def bootstrap(self) -> None:
        try:
            await self.funding.refresh()
        except Exception as e:
            self.logger.warning(f"BITGET funding preload failed: {e}")

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
