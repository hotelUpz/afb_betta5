# ============================================================
# FILE: API/OKX/client.py
# ROLE: Thin exchange client wrapper to provide unified interface for CORE.
# ============================================================

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from typing import Dict, Optional

from c_log import UnifiedLogger
from CORE.symbols import SymbolNormalizer

from .symbol import OkxSymbolsRest
from .funding import OkxFundingStream


@dataclass(frozen=True)
class FundingPoint:
    symbol: str  # instId
    funding_rate: float  # fraction
    funding_time_ms: int
    next_funding_time_ms: int
    updated_at_ms: int = 0
    source: str = "ws"

    @property
    def funding_rate_pct(self) -> float:
        return float(self.funding_rate) * 100.0


class OkxSymbolsAdapter:
    def __init__(self, logger: UnifiedLogger):
        self.logger = logger
        self.api = OkxSymbolsRest()

    async def get_symbol_map(self, quote: str = "USDT") -> Dict[str, str]:
        raw_syms = await self.api.symbols(quote=quote, only_live=True)
        out: Dict[str, str] = {}
        for raw in raw_syms:
            parsed = SymbolNormalizer.parse_okx_inst_id(raw)
            if not parsed:
                continue
            canon = SymbolNormalizer.canonical_pair(parsed[0], parsed[1])
            out[canon] = str(raw).upper().strip()
        return out


class OkxFundingAdapter:
    """OKX funding via WS cache.

    OKX does not have a convenient 'all symbols' REST funding call.
    We keep a WS stream running and read from its cache.
    """

    DEFAULT_INTERVAL_HOURS = 8

    def __init__(self, logger: UnifiedLogger):
        self.logger = logger
        self.cache: Dict[str, dict] = {}  # instId -> {funding_rate, funding_time_ms, next_funding_time_ms}
        self._stream: Optional[OkxFundingStream] = None
        self._task: Optional[asyncio.Task] = None

    @staticmethod
    def _normalize_rate(v: float) -> float:
        if v is None:
            return 0.0
        x = float(v)
        return x / 100.0 if abs(x) > 1.0 else x

    def _from_cache(self, inst_id: str) -> Optional[FundingPoint]:
        d = self.cache.get(inst_id)
        if not isinstance(d, dict):
            return None
        return FundingPoint(
            symbol=inst_id,
            funding_rate=self._normalize_rate(d.get("funding_rate", 0.0)),
            funding_time_ms=int(d.get("funding_time_ms") or 0),
            next_funding_time_ms=int(d.get("next_funding_time_ms") or 0),
            updated_at_ms=int(d.get("updated_at_ms") or 0),
            source=str(d.get("source") or "ws"),
        )

    def get(self, inst_id: str) -> Optional[FundingPoint]:
        return self._from_cache(str(inst_id).upper().strip())

    def interval_hours(self, inst_id: str) -> int:
        p = self.get(inst_id)
        if not p:
            return int(self.DEFAULT_INTERVAL_HOURS)
        if p.funding_time_ms and p.next_funding_time_ms and p.next_funding_time_ms > p.funding_time_ms:
            diff_ms = p.next_funding_time_ms - p.funding_time_ms
            hours = int(round(diff_ms / 3600_000))
            if hours > 0:
                return hours
        return int(self.DEFAULT_INTERVAL_HOURS)

    async def start_stream(self, inst_ids: list[str], *, chunk_size: int = 100) -> bool:
        """Start OKX funding WS stream if needed.

        Idempotent semantics:
          - if stream is running and already covers requested inst_ids -> no-op (returns False)
          - if stream is not running, or requested inst_ids contain new symbols -> (re)start (returns True)

        Note: we compare SETs to avoid false restarts due to dict order jitter during universe rebuilds.
        """
        inst_ids = [str(s).upper().strip() for s in inst_ids if isinstance(s, str) and s.strip()]
        if not inst_ids:
            raise ValueError("inst_ids must be non-empty")

        desired = set(inst_ids)

        # If task crashed or finished, reset first
        if self._task is not None and self._task.done():
            await self.stop_stream()

        if self._stream is not None and self._task is not None and not self._task.done():
            try:
                current = set(getattr(self._stream, "inst_ids", []) or [])
            except Exception:
                current = set()

            # If current is a superset of desired, keep running (no restart).
            if current and desired.issubset(current):
                return False

            # Otherwise restart with union (to keep previously subscribed symbols too).
            merged = sorted(current.union(desired)) if current else sorted(desired)
            await self.stop_stream()
            self._stream = OkxFundingStream(merged, cache=self.cache, chunk_size=chunk_size)
            self._task = asyncio.create_task(self._stream.run())
            return True

        # start fresh
        self._stream = OkxFundingStream(sorted(desired), cache=self.cache, chunk_size=chunk_size)
        self._task = asyncio.create_task(self._stream.run())
        return True

    async def stop_stream(self) -> None:
        if self._stream is not None:
            with contextlib.suppress(Exception):
                self._stream.stop()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
        if self._stream is not None:
            with contextlib.suppress(Exception):
                await self._stream.aclose()
        self._stream = None
        self._task = None

    async def wait_any(self, timeout_sec: float = 5.0) -> bool:
        """Wait until at least one funding update appears in cache."""
        t0 = time.time()
        while time.time() - t0 < timeout_sec:
            if self.cache:
                return True
            await asyncio.sleep(0.1)
        return bool(self.cache)

    async def wait_ready(self, inst_ids: list[str], timeout_sec: float = 30.0) -> tuple[set[str], set[str]]:
        """Wait until funding cache has entries for all requested inst_ids.

        OKX WS may deliver snapshots on subscribe; if some instruments stay silent,
        we return them as missing after timeout so CORE can skip them for this cycle.

        Returns: (ready_set, missing_set)
        """
        desired = {str(s).upper().strip() for s in (inst_ids or []) if str(s).strip()}
        if not desired:
            return set(), set()

        deadline = time.time() + max(0.1, float(timeout_sec))
        while time.time() < deadline:
            ready = {k for k in desired if isinstance(self.cache.get(k), dict) and int((self.cache.get(k) or {}).get("updated_at_ms") or 0) > 0}
            if len(ready) >= len(desired):
                return ready, set()
            await asyncio.sleep(0.15)

        ready = {k for k in desired if isinstance(self.cache.get(k), dict) and int((self.cache.get(k) or {}).get("updated_at_ms") or 0) > 0}
        missing = set(desired) - set(ready)
        return set(ready), set(missing)


class OKXClient:
    name = "OKX"

    def __init__(self, *, logger: UnifiedLogger):
        self.logger = logger
        self.symbols = OkxSymbolsAdapter(logger)
        self.funding = OkxFundingAdapter(logger)

        self.price = None
        self.stakan = None

    async def bootstrap(self) -> None:
        # No-op. Funding stream is started from CORE when symbol universe is known.
        return

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
