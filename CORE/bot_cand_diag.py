from __future__ import annotations

from collections import Counter
import time
from typing import Mapping, Any


def maybe_log_candidates_diag(*, logger, candidates: Mapping[str, Any], last_log_ms: int, period_ms: int = 15_000) -> int:
    now_ms = int(time.time() * 1000)
    if now_ms < int(last_log_ms):
        last_log_ms = 0
    if now_ms < int(last_log_ms) + max(1000, int(period_ms)):
        return int(last_log_ms)

    if not candidates:
        return int(now_ms)

    active_list = [c for c in candidates.values() if not bool(getattr(c, "signaled", False))]
    active = len(active_list)
    if active <= 0:
        return int(now_ms)

    state_counter = Counter(str(getattr(c, "state", "")) for c in candidates.values())

    fail_counter = Counter(str(getattr(c, "last_fail_reason", "")) for c in active_list if getattr(c, "last_fail_reason", ""))
    fail_hint = ""
    if fail_counter:
        top = "; ".join([f"{k} x{v}" for k, v in fail_counter.most_common(3)])
        fail_hint = f" | failTop: {top}"

    canon_counter = Counter(str(getattr(c, "canon", "")) for c in active_list if getattr(c, "canon", ""))
    top_canons = [canon for canon, _ in canon_counter.most_common(5)]
    coins_hint = ""
    if top_canons:
        coins_hint = " | coinsTop: " + ", ".join(f"{canon} x{canon_counter[canon]}" for canon in top_canons)

    logger.info(
        f"[CAND] total={len(candidates)} active={active} states={dict(state_counter)}{coins_hint}{fail_hint}"
    )

    def _fmt_pair(c) -> str:
        try:
            return f"{getattr(c, 'long_ex', '?')}->{getattr(c, 'short_ex', '?')}"
        except Exception:
            return ""

    prio = {"READY": 3, "TRACKING": 2, "NEW": 1}
    for canon in top_canons:
        group = [c for c in active_list if str(getattr(c, "canon", "")) == canon]
        if not group:
            continue
        group.sort(
            key=lambda c: (
                prio.get(str(getattr(c, "state", "")), 0),
                float(getattr(c, "last_total_spread_pct", 0.0) or 0.0),
                float(getattr(c, "pre_total_funding_spread_pct", 0.0) or 0.0),
                int(getattr(c, "last_seen_ms", 0) or 0),
            ),
            reverse=True,
        )
        c = group[0]
        ttf_m = 0.0
        try:
            nft_ms = int(getattr(c, "next_funding_time_ms", 0) or 0)
            if nft_ms > 0:
                ttf_m = max(0.0, (nft_ms - now_ms) / 1000.0 / 60.0)
        except Exception:
            ttf_m = 0.0
        logger.info(
            f"[CAND:{canon}] state={getattr(c, 'state', '?')} pair={_fmt_pair(c)} "
            f"TTF={ttf_m:.1f}m PRE={float(getattr(c, 'pre_total_funding_spread_pct', 0.0) or 0.0):.4f}% "
            f"TOTAL={float(getattr(c, 'last_total_spread_pct', 0.0) or 0.0):.4f}% "
            f"reason={str(getattr(c, 'last_fail_reason', '') or '')[:140]}"
        )

    return int(now_ms)
