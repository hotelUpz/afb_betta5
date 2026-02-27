# ============================================================
# FILE: CORE/universe.py
# ROLE: Build and maintain the symbol universe + grouping by funding interval.
# ============================================================

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional, Set, Tuple

from c_log import UnifiedLogger
from CORE.symbols import SymbolsCoordinator
from const import OKX_WARMUP_TTL_SEC


@dataclass
class SymbolUniverse:
    quote: str

    # canonical symbols
    canonical_union: Set[str] = field(default_factory=set)
    canonical_common: Set[str] = field(default_factory=set)
    canonical_eligible: Set[str] = field(default_factory=set)  # listed on >=2 enabled exchanges

    # per-exchange canonical<->raw mapping
    per_exchange_maps: Dict[str, Dict[str, str]] = field(default_factory=dict)  # ex -> {canon: raw}
    per_exchange_sets: Dict[str, Set[str]] = field(default_factory=dict)  # ex -> {canon}

    # grouped by funding interval hours
    # ex -> interval_hours -> {canon: raw}
    by_exchange_interval: Dict[str, Dict[int, Dict[str, str]]] = field(default_factory=dict)

    # interval_hours -> union canonical set
    union_by_interval: Dict[int, Set[str]] = field(default_factory=dict)
    pairwise_common_counts: Dict[str, Dict[str, int]] = field(default_factory=dict)


class UniverseBuilder:
    """Fetch symbols from exchanges and build grouping by funding intervals."""

    def __init__(self, logger: UnifiedLogger):
        self.logger = logger
        self._symbols = SymbolsCoordinator(logger=logger)

    @staticmethod
    def _union(sets: Iterable[Set[str]]) -> Set[str]:
        out: Set[str] = set()
        for s in sets:
            out |= set(s)
        return out

    @staticmethod
    def _eligible_union(per_sets: Dict[str, Set[str]]) -> Set[str]:
        counts: Dict[str, int] = {}
        for s in per_sets.values():
            for canon in s:
                counts[canon] = counts.get(canon, 0) + 1
        return {canon for canon, n in counts.items() if n >= 2}

    @staticmethod
    def _pairwise_common_counts(per_sets: Dict[str, Set[str]]) -> Dict[str, Dict[str, int]]:
        names = sorted(per_sets.keys())
        out: Dict[str, Dict[str, int]] = {a: {} for a in names}
        for i, a in enumerate(names):
            sa = per_sets.get(a, set())
            for b in names[i + 1:]:
                sb = per_sets.get(b, set())
                c = len(sa & sb)
                out[a][b] = c
                out.setdefault(b, {})[a] = c
        return out

    async def fetch_symbols(self, exchanges: Iterable[object], quote: str) -> SymbolUniverse:
        common, per_sets, per_maps = await self._symbols.compute_common_symbols(exchanges, quote=quote)

        canon_union = self._union(per_sets.values())
        u = SymbolUniverse(
            quote=quote,
            canonical_union=canon_union,
            canonical_common=common,
            canonical_eligible=self._eligible_union(per_sets),
            per_exchange_maps=per_maps,
            per_exchange_sets=per_sets,
            pairwise_common_counts=self._pairwise_common_counts(per_sets),
        )
        return u

    async def _refresh_funding_bulk(self, exchanges: Iterable[object], quote: str) -> None:
        tasks = []
        for ex in exchanges:
            f = getattr(ex, "funding", None)
            if f is None:
                continue
            # OKX funding is WS; others usually have refresh()
            if hasattr(f, "refresh"):
                # Kucoin expects quote
                try:
                    if ex.name == "KUCOIN":
                        tasks.append(f.refresh(quote=quote))
                    else:
                        tasks.append(f.refresh())
                except TypeError:
                    tasks.append(f.refresh())

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _ensure_okx_stream(self, exchanges: Iterable[object], universe: SymbolUniverse, cap: int = 250) -> None:
        okx = next((x for x in exchanges if getattr(x, "name", "") == "OKX"), None)
        if okx is None:
            return

        funding = getattr(okx, "funding", None)
        if funding is None or not hasattr(funding, "start_stream"):
            return

        okx_map = universe.per_exchange_maps.get("OKX", {})
        inst_ids = list(okx_map.values())
        if cap and len(inst_ids) > cap:
            inst_ids = inst_ids[: max(1, int(cap))]

        try:
            changed = await funding.start_stream(inst_ids, chunk_size=100)
            if changed:
                ready, missing = await funding.wait_ready(inst_ids, timeout_sec=float(OKX_WARMUP_TTL_SEC))
                if missing:
                    sample = ",".join(list(sorted(missing))[:10])
                    self.logger.warning(
                        f"OKX funding warmup: ready={len(ready)}/{len(inst_ids)} missing={len(missing)} ttl={float(OKX_WARMUP_TTL_SEC):.0f}s"
                        + (f" sample={sample}" if sample else "")
                    )
                else:
                    self.logger.info(
                        f"OKX funding warmup: ready={len(ready)}/{len(inst_ids)} ttl={float(OKX_WARMUP_TTL_SEC):.0f}s"
                    )
                self.logger.info(f"OKX funding stream started/restarted for {len(inst_ids)} instIds")
        except Exception as e:
            self.logger.warning(f"OKX funding stream start failed: {e}")

    def _build_interval_groups(self, exchanges: Iterable[object], universe: SymbolUniverse) -> None:
        by_ex: Dict[str, Dict[int, Dict[str, str]]] = {}
        union_by_int: Dict[int, Set[str]] = {}

        for ex in exchanges:
            ex_name = getattr(ex, "name", ex.__class__.__name__)
            cmap = universe.per_exchange_maps.get(ex_name, {})
            f = getattr(ex, "funding", None)

            dst_ex: Dict[int, Dict[str, str]] = {}
            for canon, raw in cmap.items():
                interval_h = 8
                if f is not None and hasattr(f, "interval_hours"):
                    try:
                        interval_h = int(f.interval_hours(raw))
                    except Exception:
                        interval_h = 8

                # This interval map is DIAGNOSTIC / labeling metadata only.
                # Real scan eligibility is decided later by actual nextFundingTime/TTF,
                # not by this declared adapter interval.
                # Keep only supported display buckets.
                if interval_h not in {1, 4, 8}:
                    continue

                dst_ex.setdefault(interval_h, {})[canon] = raw
                if canon in universe.canonical_eligible:
                    union_by_int.setdefault(interval_h, set()).add(canon)

            by_ex[ex_name] = dst_ex

        universe.by_exchange_interval = by_ex
        universe.union_by_interval = union_by_int

    async def build(self, exchanges: Iterable[object], *, quote: str, okx_cap: int = 250) -> SymbolUniverse:
        quote_u = (quote or "USDT").upper().strip()

        # 1) symbols
        universe = await self.fetch_symbols(exchanges, quote_u)

        # 2) funding (bulk) for REST-based exchanges
        await self._refresh_funding_bulk(exchanges, quote_u)

        # 3) OKX funding stream (optional) — improves interval grouping
        await self._ensure_okx_stream(exchanges, universe, cap=okx_cap)

        # 4) grouping
        self._build_interval_groups(exchanges, universe)

        return universe
