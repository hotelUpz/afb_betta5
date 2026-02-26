# ============================================================
# FILE: CORE/funding_engine.py
# ROLE: Funding spread calculations (core signal pre-filter).
# ============================================================

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class FundingSnapshot:
    """Normalized funding data for one (exchange, symbol).

    rate_pct: already normalized to percentage points (e.g. 0.01 == 0.01%)
    next_funding_time_ms: UTC epoch milliseconds
    """

    ex: str
    rate_pct: float
    next_funding_time_ms: int
    time_to_funding_sec: float
    updated_at_ms: int = 0
    source: str = "unknown"


@dataclass(frozen=True)
class Opportunity:
    """Concrete LONG/SHORT pair opportunity for a symbol within one funding-time cluster."""

    ts_ms: int
    canon: str

    long_ex: str
    short_ex: str

    long_rate_pct: float
    short_rate_pct: float

    # PRE_TOTALY_FUNDING_SPREAD = short_rate_pct - long_rate_pct (in %)
    pre_total_funding_spread_pct: float

    next_funding_time_ms: int
    time_to_funding_sec: float

    # transparency / debugging
    cluster_exchanges: Tuple[str, ...]
    cluster_next_min_ms: int
    cluster_next_max_ms: int


class FundingEngine:
    """Funding spread pre-filter.

    What it guarantees:
      - compares only points with synchronized next funding times (within tolerance)
      - uses normalized funding in unified coordinates (%), so delta is a simple subtraction
      - returns *all* valid LONG/SHORT pairs in each cluster (not only one "best")
    """

    def __init__(
        self,
        *,
        min_pre_spread_pct: float,
        next_funding_sync_sec: float,
        max_funding_age_sec: Optional[float] = None,
        funding_source_mode: str = "hybrid",
    ):
        self.min_pre_spread_pct = float(min_pre_spread_pct)
        self.next_funding_sync_ms = int(float(next_funding_sync_sec) * 1000)
        self.max_funding_age_sec = None if max_funding_age_sec is None else float(max_funding_age_sec)
        mode = str(funding_source_mode or "hybrid").strip().lower()
        self.funding_source_mode = mode if mode in {"ws", "rest", "hybrid"} else "hybrid"

    # ---------------------------------------------------------
    # Normalization helpers
    # ---------------------------------------------------------
    @staticmethod
    def _normalize_epoch_ms(v: object) -> Optional[int]:
        if not isinstance(v, (int, float)):
            return None
        x = int(v)
        if x <= 0:
            return None
        # If it looks like seconds (10 digits), convert to ms.
        if x < 10_000_000_000:
            x *= 1000
        return x

    @staticmethod
    def _rate_pct_from_point(p: object) -> Optional[float]:
        if p is None:
            return None

        # preferred: already in percent
        v = getattr(p, "funding_rate_pct", None)
        if isinstance(v, (int, float)):
            return float(v)

        # fallback: fraction -> percent
        v = getattr(p, "funding_rate", None)
        if isinstance(v, (int, float)):
            return float(v) * 100.0

        # generic field, try to infer scale
        v = getattr(p, "rate", None)
        if isinstance(v, (int, float)):
            x = float(v)
            # heuristic: |x| <= 1 likely fraction (e.g. 0.0001), convert to %.
            return (x * 100.0) if abs(x) <= 1.0 else x

        return None

    def _next_funding_time_ms(self, p: object) -> Optional[int]:
        if p is None:
            return None
        for attr in ("next_funding_time_ms", "nextFundingTime"):
            v = getattr(p, attr, None)
            out = self._normalize_epoch_ms(v)
            if out:
                return out
        return None


    @staticmethod
    def _updated_at_ms_from_point(p: object) -> Optional[int]:
        if p is None:
            return None
        for attr in ("updated_at_ms", "ts_ms", "event_time_ms"):
            v = getattr(p, attr, None)
            if isinstance(v, (int, float)) and int(v) > 0:
                return int(v)
        return None

    @staticmethod
    def _source_from_point(p: object) -> str:
        if p is None:
            return "unknown"
        src = getattr(p, "source", None)
        if isinstance(src, str) and src.strip():
            return src.strip().lower()
        return "unknown"

    def _is_source_allowed(self, source: str) -> bool:
        mode = self.funding_source_mode
        src = str(source or "unknown").strip().lower()
        if mode == "hybrid":
            return True
        return src == mode

    def _is_fresh_enough(self, now_ms: int, updated_at_ms: int) -> bool:
        if self.max_funding_age_sec is None:
            return True
        if self.max_funding_age_sec <= 0:
            return True
        if not updated_at_ms or updated_at_ms <= 0:
            return False
        age_sec = max(0.0, (now_ms - int(updated_at_ms)) / 1000.0)
        return age_sec <= float(self.max_funding_age_sec)

    def _pair_ttf_sync_ok(self, a: FundingSnapshot, b: FundingSnapshot) -> bool:
        delta_ms = abs(int(round(a.time_to_funding_sec * 1000.0)) - int(round(b.time_to_funding_sec * 1000.0)))
        return delta_ms <= self.next_funding_sync_ms

    # ---------------------------------------------------------
    # Clustering
    # ---------------------------------------------------------
    def _cluster_by_next_funding(self, snaps: Sequence[FundingSnapshot]) -> List[List[FundingSnapshot]]:
        if not snaps:
            return []

        s = sorted(snaps, key=lambda x: x.next_funding_time_ms)
        clusters: List[List[FundingSnapshot]] = []

        cur: List[FundingSnapshot] = []
        cur_min = 0

        for x in s:
            if not cur:
                cur = [x]
                cur_min = x.next_funding_time_ms
                continue

            # strict cluster width bound (prevents chain effect)
            if x.next_funding_time_ms - cur_min <= self.next_funding_sync_ms:
                cur.append(x)
            else:
                clusters.append(cur)
                cur = [x]
                cur_min = x.next_funding_time_ms

        if cur:
            clusters.append(cur)
        return clusters

    # ---------------------------------------------------------
    # Opportunity generation
    # ---------------------------------------------------------
    @staticmethod
    def _cluster_repr_next_ms(cluster: Sequence[FundingSnapshot]) -> int:
        times = sorted(x.next_funding_time_ms for x in cluster)
        return int(times[len(times) // 2])

    @staticmethod
    def _cluster_ttf_sec_for_pair(cluster: Sequence[FundingSnapshot], long_ex: str, short_ex: str) -> float:
        vals = [x.time_to_funding_sec for x in cluster if x.ex in (long_ex, short_ex)]
        if vals:
            return float(sum(vals) / len(vals))
        if cluster:
            return float(cluster[0].time_to_funding_sec)
        return 0.0

    def opportunities_for_cluster(self, canon: str, cluster: Sequence[FundingSnapshot]) -> List[Opportunity]:
        """Enumerate all valid LONG/SHORT pairs inside a synchronized cluster.

        If 4 exchanges are present in the same cluster, this can produce several pairs.
        Mirrored duplicates are avoided because we only build LONG=lower funding, SHORT=higher funding.
        """
        if len(cluster) < 2:
            return []

        # Sort by funding; lower funding -> better LONG leg, higher funding -> better SHORT leg.
        ordered = sorted(cluster, key=lambda x: (x.rate_pct, x.ex))

        next_ms = self._cluster_repr_next_ms(ordered)
        cluster_min = min(x.next_funding_time_ms for x in ordered)
        cluster_max = max(x.next_funding_time_ms for x in ordered)
        cluster_exs = tuple(sorted(x.ex for x in ordered))

        out: List[Opportunity] = []
        now_ms = int(time.time() * 1000)

        n = len(ordered)
        for i in range(n - 1):
            a = ordered[i]
            for j in range(i + 1, n):
                b = ordered[j]
                # LONG on lower funding, SHORT on higher funding
                long_leg = a
                short_leg = b
                if not self._pair_ttf_sync_ok(long_leg, short_leg):
                    continue
                pre_total = float(short_leg.rate_pct - long_leg.rate_pct)
                if pre_total < self.min_pre_spread_pct:
                    continue

                ttf = self._cluster_ttf_sec_for_pair(ordered, long_leg.ex, short_leg.ex)

                out.append(
                    Opportunity(
                        ts_ms=now_ms,
                        canon=canon,
                        long_ex=long_leg.ex,
                        short_ex=short_leg.ex,
                        long_rate_pct=float(long_leg.rate_pct),
                        short_rate_pct=float(short_leg.rate_pct),
                        pre_total_funding_spread_pct=pre_total,
                        next_funding_time_ms=int(next_ms),
                        time_to_funding_sec=float(ttf),
                        cluster_exchanges=cluster_exs,
                        cluster_next_min_ms=int(cluster_min),
                        cluster_next_max_ms=int(cluster_max),
                    )
                )

        return out

    def best_opportunity(self, canon: str, clusters: List[List[FundingSnapshot]]) -> Optional[Opportunity]:
        """Compatibility helper: returns best among all enumerated opportunities."""
        best: Optional[Opportunity] = None
        for cl in clusters:
            for opp in self.opportunities_for_cluster(canon, cl):
                if best is None:
                    best = opp
                    continue
                if opp.pre_total_funding_spread_pct > best.pre_total_funding_spread_pct:
                    best = opp
                elif opp.pre_total_funding_spread_pct == best.pre_total_funding_spread_pct and opp.time_to_funding_sec < best.time_to_funding_sec:
                    best = opp
        return best

    # ---------------------------------------------------------
    # Main
    # ---------------------------------------------------------
    def scan(
        self,
        *,
        canon_list: Iterable[str],
        exchanges: Iterable[object],
        per_exchange_maps: Dict[str, Dict[str, str]],
        ttf_min_sec: Optional[float] = None,
        ttf_max_sec: Optional[float] = None,
    ) -> List[Opportunity]:
        out: List[Opportunity] = []
        now_ms = int(time.time() * 1000)

        for canon in canon_list:
            snaps: List[FundingSnapshot] = []

            for ex in exchanges:
                ex_name = getattr(ex, "name", ex.__class__.__name__)
                raw = per_exchange_maps.get(ex_name, {}).get(canon)
                if not raw:
                    continue

                f = getattr(ex, "funding", None)
                if f is None or not hasattr(f, "get"):
                    continue

                try:
                    p = f.get(raw)
                except Exception:
                    p = None

                rate_pct = self._rate_pct_from_point(p)
                nxt_ms = self._next_funding_time_ms(p)
                if rate_pct is None or nxt_ms is None:
                    continue

                src = self._source_from_point(p)
                if not self._is_source_allowed(src):
                    continue

                updated_at_ms = int(self._updated_at_ms_from_point(p) or 0)
                if not self._is_fresh_enough(now_ms, updated_at_ms):
                    continue

                ttf_sec = (nxt_ms - now_ms) / 1000.0
                if ttf_sec <= 0:
                    continue
                if ttf_min_sec is not None and ttf_sec < float(ttf_min_sec):
                    continue
                if ttf_max_sec is not None and ttf_sec > float(ttf_max_sec):
                    continue

                snaps.append(
                    FundingSnapshot(
                        ex=ex_name,
                        rate_pct=float(rate_pct),
                        next_funding_time_ms=int(nxt_ms),
                        time_to_funding_sec=float(ttf_sec),
                        updated_at_ms=updated_at_ms,
                        source=src,
                    )
                )

            if len(snaps) < 2:
                continue

            clusters = self._cluster_by_next_funding(snaps)
            for cl in clusters:
                out.extend(self.opportunities_for_cluster(canon, cl))

        # rank for bot intake
        out.sort(key=lambda x: (x.pre_total_funding_spread_pct, -x.time_to_funding_sec), reverse=True)
        return out
