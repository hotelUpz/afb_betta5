# ============================================================
# FILE: API/BINANCE/client.py
# ROLE: Thin exchange client wrapper to provide unified interface for CORE.
# ============================================================

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Optional

from c_log import UnifiedLogger
from CORE.symbols import SymbolNormalizer

from .symbol import BinanceSymbols
from .funding import BinanceFunding, FundingInfo as BinanceFundingInfo


@dataclass(frozen=True)
class FundingPoint:
    symbol: str
    funding_rate: float  # fraction, e.g. 0.0001
    next_funding_time_ms: int
    updated_at_ms: int = 0
    source: str = "rest"

    @property
    def funding_rate_pct(self) -> float:
        return float(self.funding_rate) * 100.0


class BinanceSymbolsAdapter:
    def __init__(self, logger: UnifiedLogger):
        self.logger = logger
        self.api = BinanceSymbols()

    async def get_symbol_map(self, quote: str = "USDT") -> Dict[str, str]:
        raw_syms = await self.api.get_perp_symbols(quote=quote)
        q = (quote or "USDT").upper().strip()
        out: Dict[str, str] = {}
        for raw in raw_syms:
            parsed = SymbolNormalizer.parse_binance_symbol(raw, quote=q)
            if not parsed:
                continue
            canon = SymbolNormalizer.canonical_pair(parsed[0], parsed[1])
            out[canon] = str(raw).upper().strip()
        return out


class BinanceFundingAdapter:
    """Bulk funding fetcher.

    Binance premiumIndex returns funding rates for all symbols in one call.
    Funding interval for USDT-M perpetual is effectively 8h (used as default).
    """

    DEFAULT_INTERVAL_HOURS = 8

    def __init__(self, logger: UnifiedLogger):
        self.logger = logger
        self.api = BinanceFunding()
        self.cache: Dict[str, FundingPoint] = {}
        self.updated_at_ms: int = 0

    @staticmethod
    def _normalize_rate(v: float) -> float:
        # If some API returns percents (unlikely here), normalize to fraction.
        if v is None:
            return 0.0
        x = float(v)
        return x / 100.0 if abs(x) > 1.0 else x

    async def refresh(self) -> None:
        rows = await self.api.get_all()
        now_ms = int(time.time() * 1000)
        out: Dict[str, FundingPoint] = {}
        for r in rows:
            if not isinstance(r, BinanceFundingInfo):
                continue
            sym = str(r.symbol).upper().strip()
            out[sym] = FundingPoint(
                symbol=sym,
                funding_rate=self._normalize_rate(r.funding_rate),
                next_funding_time_ms=int(r.next_funding_time_ms or 0),
                updated_at_ms=now_ms,
                source="rest",
            )
        self.cache = out
        self.updated_at_ms = now_ms

    def get(self, symbol: str) -> Optional[FundingPoint]:
        return self.cache.get(str(symbol).upper().strip())

    def interval_hours(self, symbol: str) -> int:
        # Binance does not provide interval explicitly in premiumIndex.
        return int(self.DEFAULT_INTERVAL_HOURS)


class BinanceClient:
    name = "BINANCE"

    def __init__(self, *, logger: UnifiedLogger):
        self.logger = logger
        self.symbols = BinanceSymbolsAdapter(logger)
        self.funding = BinanceFundingAdapter(logger)

        # placeholders for future stages
        self.price = None
        self.stakan = None

    async def bootstrap(self) -> None:
        # Preload funding cache (optional, but convenient).
        try:
            await self.funding.refresh()
        except Exception as e:
            self.logger.warning(f"BINANCE funding preload failed: {e}")

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
