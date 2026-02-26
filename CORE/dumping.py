# ============================================================
# FILE: CORE/dumping.py
# ROLE: Persist internal structures (symbols) for human inspection.
# ============================================================

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict

from c_log import UnifiedLogger
from CORE.universe import SymbolUniverse


def _project_root() -> Path:
    # .../funding_arb/CORE/dumping.py -> .../funding_arb
    return Path(__file__).resolve().parents[1]


def out_dir() -> Path:
    p = _project_root() / "OUT"
    p.mkdir(parents=True, exist_ok=True)
    return p


def dump_symbols_struct(*, struct: Dict[str, Dict[int, list[str]]], universe: SymbolUniverse, logger: UnifiedLogger) -> None:
    """Dump symbol structure (raw) + summary.

    Files:
      - OUT/symbols_struct.json (latest)
      - OUT/symbols_struct_YYYYmmdd_HHMMSS.json (snapshot)
      - OUT/symbols_summary.txt
    """

    od = out_dir()
    ts = time.strftime("%Y%m%d_%H%M%S")

    latest_json = od / "symbols_struct.json"
    snap_json = od / f"symbols_struct_{ts}.json"

    payload = {
        "quote": universe.quote,
        "generated_ts": ts,
        "struct": struct,
        "counts": {
            "per_exchange": {k: len(v) for k, v in universe.per_exchange_sets.items()},
            "union": len(universe.canonical_union),
            "eligible_union": len(getattr(universe, "canonical_eligible", set())),
            "common": len(universe.canonical_common),
            "pairwise_common": getattr(universe, "pairwise_common_counts", {}),
            "union_by_interval": {str(k): len(v) for k, v in universe.union_by_interval.items()},
        },
    }

    # JSON (pretty)
    txt = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    latest_json.write_text(txt, encoding="utf-8")
    snap_json.write_text(txt, encoding="utf-8")

    # summary txt
    lines = []
    lines.append(f"quote={universe.quote}")
    lines.append(f"generated={ts}")
    lines.append("")

    for ex, s in sorted(universe.per_exchange_sets.items()):
        lines.append(f"{ex}: {len(s)}")
    lines.append(f"UNION(raw all listed):  {len(universe.canonical_union)}")
    lines.append(f"ELIGIBLE(>=2 exchanges): {len(getattr(universe, 'canonical_eligible', set()))}")
    lines.append(f"COMMON(all exchanges):   {len(universe.canonical_common)}")
    lines.append("")
    if getattr(universe, "pairwise_common_counts", None):
        lines.append("Pairwise common counts:")
        for a, row in sorted(universe.pairwise_common_counts.items()):
            parts = [f"{b}={cnt}" for b, cnt in sorted(row.items())]
            if parts:
                lines.append(f"  {a}: " + ", ".join(parts))
        lines.append("")

    lines.append("Intervals (eligible union):")
    for interval_h, syms in sorted(universe.union_by_interval.items()):
        lines.append(f"  {interval_h}h: {len(syms)}")

    lines.append("")
    lines.append("Per-exchange intervals (raw counts):")
    for ex, by_int in sorted(struct.items()):
        parts = []
        for interval_h, raw_list in sorted(by_int.items()):
            parts.append(f"{interval_h}h={len(raw_list)}")
        lines.append(f"  {ex}: " + ", ".join(parts))

    (od / "symbols_summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    logger.info(f"[SYMBOLS] dumped structure -> {latest_json}")
