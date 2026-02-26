# ============================================================
# FILE: CORE/symbols.py
# ROLE: Symbol normalization + symbol universe (intersection) builder
# ============================================================

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

from c_log import UnifiedLogger


class SymbolNormalizer:
    """Нормализация символов в единый canonical формат: BASE_QUOTE (uppercase).

    Примеры:
        BINANCE: BTCUSDT        -> BTC_USDT
        OKX:     BTC-USDT-SWAP  -> BTC_USDT
        PHEMEX:  BTCUSDT        -> BTC_USDT
        KUCOIN:  XBTUSDTM       -> BTC_USDT  (XBT alias)
    """

    BASE_ALIASES: Dict[str, str] = {
        "XBT": "BTC",  # KuCoin uses XBT for BTC
        "BCC": "BCH",  # historical edge case
    }

    @classmethod
    def normalize_ccy(cls, ccy: str) -> str:
        c = (ccy or "").upper().strip()
        return cls.BASE_ALIASES.get(c, c)

    @classmethod
    def canonical_pair(cls, base: str, quote: str) -> str:
        b = cls.normalize_ccy(base)
        q = cls.normalize_ccy(quote)
        if not b or not q:
            raise ValueError(f"Bad pair base={base!r} quote={quote!r}")
        return f"{b}_{q}"

    # -------------------------
    # Parsers (raw -> base/quote)
    # -------------------------
    @classmethod
    def parse_okx_inst_id(cls, inst_id: str) -> Optional[Tuple[str, str]]:
        # Typical: BTC-USDT-SWAP
        s = (inst_id or "").upper().strip()
        parts = s.split("-")
        if len(parts) < 2:
            return None
        base, quote = parts[0], parts[1]
        return cls.normalize_ccy(base), cls.normalize_ccy(quote)

    @classmethod
    def parse_binance_symbol(cls, sym: str, quote: str = "USDT") -> Optional[Tuple[str, str]]:
        s = (sym or "").upper().strip()
        q = (quote or "").upper().strip()
        if not s.endswith(q):
            return None
        base = s[: -len(q)]
        if not base:
            return None
        return cls.normalize_ccy(base), cls.normalize_ccy(q)

    @classmethod
    def parse_phemex_symbol(cls, sym: str, quote: str = "USDT") -> Optional[Tuple[str, str]]:
        s = (sym or "").upper().strip()
        q = (quote or "").upper().strip()
        if s.endswith(q):
            base = s[: -len(q)]
            if base:
                return cls.normalize_ccy(base), cls.normalize_ccy(q)
        return None

    @classmethod
    def parse_kucoin_symbol(cls, sym: str, quote: str = "USDT") -> Optional[Tuple[str, str]]:
        # KuCoin futures: XBTUSDTM (trailing 'M')
        s = (sym or "").upper().strip()
        q = (quote or "").upper().strip()
        if s.endswith(q + "M"):
            base = s[: -(len(q) + 1)]
            if base:
                return cls.normalize_ccy(base), cls.normalize_ccy(q)
        return None


@dataclass(frozen=True)
class SymbolsSnapshot:
    exchange: str
    quote: str
    symbol_map: Dict[str, str]  # canonical -> raw

    @property
    def canonical_set(self) -> Set[str]:
        return set(self.symbol_map.keys())


class SymbolsCoordinator:
    """Сбор символов с бирж + пересечение множеств.

    Контракт для exchange client:
        - .name: str
        - .symbols: объект с async get_symbol_map(quote=...) -> dict[canonical, raw]
    """

    def __init__(self, logger: UnifiedLogger):
        self.logger = logger

    async def fetch_symbol_snapshots(self, exchanges: Iterable[object], quote: str = "USDT") -> List[SymbolsSnapshot]:
        async def _one(ex) -> SymbolsSnapshot:
            name = getattr(ex, "name", ex.__class__.__name__)
            symbols_mod = getattr(ex, "symbols", None)
            if symbols_mod is None:
                self.logger.warning(f"{name}: no symbols module attached")
                return SymbolsSnapshot(exchange=name, quote=quote, symbol_map={})

            try:
                m = await symbols_mod.get_symbol_map(quote=quote)
                if not isinstance(m, dict):
                    raise TypeError(f"{name}: get_symbol_map returned {type(m)}")
                return SymbolsSnapshot(exchange=name, quote=quote, symbol_map=m)
            except Exception as e:
                self.logger.exception(f"{name}: failed to fetch symbols: {e}", exc=e)
                return SymbolsSnapshot(exchange=name, quote=quote, symbol_map={})

        tasks = [_one(ex) for ex in exchanges]
        return await asyncio.gather(*tasks)

    async def compute_common_symbols(
        self,
        exchanges: Iterable[object],
        quote: str = "USDT",
    ) -> Tuple[Set[str], Dict[str, Set[str]], Dict[str, Dict[str, str]]]:
        snaps = await self.fetch_symbol_snapshots(exchanges, quote=quote)

        per_sets: Dict[str, Set[str]] = {s.exchange: s.canonical_set for s in snaps}
        per_maps: Dict[str, Dict[str, str]] = {s.exchange: s.symbol_map for s in snaps}

        common: Optional[Set[str]] = None
        for _, s in per_sets.items():
            common = set(s) if common is None else (common & s)

        return common or set(), per_sets, per_maps
