# ============================================================
# FILE: CORE/dedup.py
# ROLE: Signal de-duplication (avoid spam)
# ============================================================

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


@dataclass
class DedupItem:
    key: str
    expires_at_ms: Optional[int]  # None => forever


class SignalDeduper:
    """In-memory + optional persisted de-dup.

    key -> expires_at_ms (or None for infinite dedup).
    """

    def __init__(self, *, state_path: Path, logger=None):
        self.state_path = state_path
        self.logger = logger
        self._items: Dict[str, Optional[int]] = {}
        self._load_best_effort()

    def _log(self, level: str, msg: str) -> None:
        if self.logger is None:
            return
        fn = getattr(self.logger, level, None)
        if callable(fn):
            fn(msg)

    def _load_best_effort(self) -> None:
        try:
            if not self.state_path.exists():
                return
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return
            items = data.get("items")
            if not isinstance(items, dict):
                return
            now_ms = int(time.time() * 1000)
            for k, v in items.items():
                if v is None:
                    self._items[str(k)] = None
                    continue
                try:
                    exp = int(v)
                except Exception:
                    continue
                if exp > now_ms:
                    self._items[str(k)] = exp
            if self._items:
                forever = sum(1 for _, v in self._items.items() if v is None)
                self._log("info", f"[DEDUP] loaded {len(self._items)} keys (forever={forever})")
        except Exception:
            return

    def _save_best_effort(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "ts_ms": int(time.time() * 1000),
                "items": self._items,
            }
            self.state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        except Exception:
            return

    def cleanup(self) -> None:
        now_ms = int(time.time() * 1000)
        drop = [k for k, exp in self._items.items() if (exp is not None and exp <= now_ms)]
        for k in drop:
            self._items.pop(k, None)
        if drop:
            self._save_best_effort()

    def is_seen(self, key: str) -> bool:
        if key not in self._items:
            return False
        exp = self._items.get(key)
        if exp is None:
            return True
        return int(exp) > int(time.time() * 1000)

    def mark(self, key: str, *, expires_at_ms: Optional[int]) -> None:
        self._items[str(key)] = (None if expires_at_ms is None else int(expires_at_ms))
        self._save_best_effort()
