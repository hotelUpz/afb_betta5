# ============================================================
# FILE: CORE/bot.py
# ROLE: Main bot orchestrator (symbols -> funding scan scheduler -> candidate tracking)
# ============================================================

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from const import (
    DEFAULT_QUOTE,
    DEFAULT_POLL_SEC,
    ENABLED_EXCHANGES,
    EXCHANGES,
    ENGINE_FUNDING,
    ENGINE_PRICE,
    ENGINE_PRICE2,
    FUNDING_SPREAD,
    TOTAL_FUNDING_SPREAD_MIN,
    PRICE_SPREAD_WEIGHT,
    NEXT_FUNDING_SYNC_SEC,
    OKX_TEST_MAX_SYMBOLS,
    CANDIDATE_EXPIRE_SEC,
    MAX_PRICE_AGE_SEC,
    DEDUP_AFTER_FUNDING_SEC,
    DEDUP_STATE_FILE,
    TG_TOKEN,
    TG_CHAT_ID,
    RUN_MODE,
    TEST_ENABLE_FORCED_SCAN_SCHEDULER_BYPASS,
    TEST_FORCED_SCAN_EVERY_SEC,
    SCHED_REPEAT_COUNT,
    SCHED_REPEAT_INTERVAL_SEC,
    SCHED_REPEAT_1H_COUNT,
    SCHED_REPEAT_1H_INTERVAL_SEC,
    SCHED_REPEAT_4H_COUNT,
    SCHED_REPEAT_4H_INTERVAL_SEC,
    SCHED_REPEAT_8H_COUNT,
    SCHED_REPEAT_8H_INTERVAL_SEC,
    REST_FUNDING_REFRESH_EVERY_SEC,
    FUNDING_SOURCE_MODE,
    MAX_FUNDING_AGE_SEC,
    PHEMEX_TOP_FUNDING_ENABLE,
    PHEMEX_TOP_FUNDING_MIN_FUNDING,
    PHEMEX_TOP_FUNDING_SLICE,
    KUCOIN_TOP_FUNDING_ENABLE,
    KUCOIN_TOP_FUNDING_MIN_FUNDING,
    KUCOIN_TOP_FUNDING_SLICE,
)

from c_log import UnifiedLogger
from TG.notificator import TgNotificator
from TG.messages import build_funding_signal_message

from API.OKX.client import OKXClient
from API.PHEMEX.client import PhemexClient
from API.BINANCE.client import BinanceClient
from API.KUCOIN.client import KucoinClient
from API.BITGET.client import BitgetClient

from CORE.universe import UniverseBuilder, SymbolUniverse
from CORE.scheduler import FundingScanScheduler, NotificationScheduler
from CORE.funding_engine import FundingEngine, Opportunity
from CORE.dumping import dump_symbols_struct, out_dir
from CORE.market_streams import MarketStreams
from CORE.dedup import SignalDeduper
from CORE.arb_math import ArbMath, PairSpreadInput, BookQualityConfig
from CORE.startup_checks import StartupSelfCheck
from CORE.scheduler_diag import build_scheduler_eta_log_line
from CORE.bot_sched_utils import (
    build_gate_scan_ttf_window,
    scheduler_gate_summary_lines,
    maybe_log_scheduler_eta,
)
from CORE.bot_cand_diag import maybe_log_candidates_diag


@dataclass
class Candidate:
    """A concrete (canon, long_ex, short_ex) pair under validation."""

    key: str
    canon: str
    long_ex: str
    short_ex: str
    long_raw: str
    short_raw: str

    funding_long_pct: float
    funding_short_pct: float

    # PRE_TOTALY_FUNDING_SPREAD (pure funding, percent)
    pre_total_funding_spread_pct: float

    # best-available cluster info
    next_funding_time_ms: int
    cluster_exchanges: Tuple[str, ...]

    created_ms: int
    last_seen_ms: int

    # validation state
    pair_ok_since_ms: int = 0
    price_ok_since_ms: int = 0
    stakan_ok_since_ms: int = 0
    last_price_component_pct: float = 0.0
    last_total_spread_pct: float = 0.0
    last_price2_price_component_pct: float = 0.0
    last_price2_total_spread_pct: float = 0.0
    last_pre_total_funding_spread_live_pct: float = 0.0
    last_fail_reason: str = ""
    state: str = "NEW"  # NEW -> TRACKING -> READY -> SIGNALED / DROPPED

    signaled: bool = False
    last_signal_kind: str = "PRIMARY"
    ttl_anchor_funding_sig: str = ""

    # post-signal re-notify scheduler (scheduler.repeat)
    gate_hours: int = 0
    repeat_count: int = 0
    repeat_interval_sec: int = 0
    signal_sent_ms: int = 0
    repeat_done_count: int = 0
    repeat_next_ms: int = 0
    last_signal_message: str = ""


PRE_FUNDING_SIGNAL_QUIET_SEC = 5 * 60  # hard safety quiet zone before any funding event


class FundingArbBot:
    """Funding arbitrage *scanner* with staged filters.

    Pipeline:
      1) Build symbol universe (+ per-exchange interval groups) + dump structure to ./OUT
      2) Time-gated funding scans (1h/4h/8h)
      3) Funding spread pre-filter (PRE_TOTALY_FUNDING_SPREAD) + funding-time alignment
      4) For candidate pairs: start price + stakan streams (only for involved symbols)
      5) Validate combined condition for TTL:
            - stakan spread ok on both legs
            - TOTAL spread ok (funding + price counter-argument)
         => send signal to TG (or log if TG disabled)

    Notes:
      - We do NOT do execution here. Only detection + notifications.
      - Dedup prevents spam (hash by canon/pair/nextFunding bucket).
    """

    def __init__(self, logger: Optional[UnifiedLogger] = None):
        self.logger = logger or UnifiedLogger(name="core", context="BOT")

        self._self_check_report = StartupSelfCheck(self.logger).run()
        if not self._self_check_report.ok:
            raise RuntimeError(
                "Startup self-check failed. Fix config/runtime issues in logs and restart."
            )

        self.notifier = TgNotificator(
            token=str(TG_TOKEN),
            chat_id=str(TG_CHAT_ID),
            logger=self.logger,
        )

        # Runtime configs (new structured sections + compatibility aliases)
        self._cfg_exchanges: Dict[str, Dict[str, Any]] = {
            str(k).lower(): (dict(v) if isinstance(v, dict) else {})
            for k, v in (EXCHANGES or {}).items()
        }
        self._enabled_exchange_keys = self._resolve_enabled_exchange_keys()
        self.exchanges = self._build_exchange_clients()
        if not self.exchanges:
            raise RuntimeError("No supported exchanges enabled in ENABLED_EXCHANGES/EXCHANGES")

        self.quote = self._resolve_quote()
        self.poll_sec = max(0.2, float(DEFAULT_POLL_SEC))
        self.okx_cap = self._resolve_okx_cap()

        self._funding_cfg = dict(ENGINE_FUNDING or {})
        self._price_cfg = dict(ENGINE_PRICE or {})
        self._price2_cfg = dict(ENGINE_PRICE2 or {})
        self._stakan_cfg = {"enable": False}  # user-facing orderbook filter removed (legacy path disabled)

        self._funding_enabled = bool(self._funding_cfg.get("enabled", True))
        self._price_enabled = bool(self._price_cfg.get("enabled", self._price_cfg.get("enable", True)))
        self._price2_enabled = bool(self._price2_cfg.get("enabled", self._price2_cfg.get("enable", True)))
        self._stakan_enabled = False

        self._pre_spread_min = float(self._funding_cfg.get("MIN_SPREAD", FUNDING_SPREAD) or 0.0)
        self._price_weight = (float(self._price_cfg.get("PRICE_SPREAD_WEIGHT", PRICE_SPREAD_WEIGHT) or 0.0) if self._price_enabled else 0.0)
        self._total_spread_min = (
            float(self._price_cfg.get("TOTALY_SPREAD", TOTAL_FUNDING_SPREAD_MIN) or self._price_cfg.get("TOTAL_SPREAD", TOTAL_FUNDING_SPREAD_MIN) or 0.0)
            if self._price_enabled else self._pre_spread_min
        )
        self._price2_weight = (float(self._price2_cfg.get("PRICE_SPREAD_WEIGHT", self._price_cfg.get("PRICE_SPREAD_WEIGHT", PRICE_SPREAD_WEIGHT)) or 0.0) if self._price2_enabled else 0.0)
        self._price2_total_spread_min = float(self._price2_cfg.get("TOTAL_SPREAD", self._price2_cfg.get("TOTALY_SPREAD", 0.0)) or 0.0) if self._price2_enabled else 0.0

        self._price_ttl_sec = (float(self._price_cfg.get("PRICE_TTL_SEC_CONTROL", 0.0) or 0.0) if self._price_enabled else 0.0)
        self._stakan_ttl_sec = 0.0
        self._max_price_age_sec = float(self._price_cfg.get("MAX_PRICE_AGE_SEC", MAX_PRICE_AGE_SEC) or 0.0)
        self._max_book_age_sec = 0.0
        self._stakan_spread_thr = None

        self.universe: Optional[SymbolUniverse] = None

        # Global structure requested in TECH_DEBT (exchange -> interval -> raw list)
        self.raw_symbols_struct: Dict[str, Dict[int, list[str]]] = {}

        self._builder = UniverseBuilder(logger=self.logger)
        self._scheduler = FundingScanScheduler.default()
        self._notif_scheduler = NotificationScheduler.default()
        self._funding_source_mode = str(self._funding_cfg.get("FUNDING_SOURCE_MODE", FUNDING_SOURCE_MODE) or FUNDING_SOURCE_MODE).strip().lower()
        if self._funding_source_mode not in {"ws", "rest", "hybrid"}:
            self._funding_source_mode = "hybrid"
        self._max_funding_age_sec = float(self._funding_cfg.get("MAX_FUNDING_AGE_SEC", MAX_FUNDING_AGE_SEC) or 0.0)

        self._engine = FundingEngine(
            min_pre_spread_pct=float(self._pre_spread_min),
            next_funding_sync_sec=float(self._funding_cfg.get("NEXT_FUNDING_SYNC_SEC", NEXT_FUNDING_SYNC_SEC) or NEXT_FUNDING_SYNC_SEC),
            max_funding_age_sec=(self._max_funding_age_sec if self._max_funding_age_sec > 0 else None),
            funding_source_mode=self._funding_source_mode,
        )

        # Market streams for candidate validation
        self.market = MarketStreams(
            logger=self.logger,
            stakan_spread_pct_threshold=self._stakan_spread_thr,
            stakan_ttl_sec=self._stakan_ttl_sec,
        )

        # Signal de-dup state
        self._dedup = SignalDeduper(state_path=Path(DEDUP_STATE_FILE), logger=self.logger)

        # explicit book-quality config (anti-"дырявый стакан")
        # Orderbook quality filter is disabled in the current product concept.
        # Keep a neutral config object only to satisfy shared message/formatter paths.
        self._book_quality_cfg = BookQualityConfig(
            max_spread_pct=None,
            target_pos_usd=None,
            min_top_coverage_x=None,
            min_top_notional=None,
            min_side_balance_ratio=None,
        )

        self._test_force_scan_mode = bool(TEST_ENABLE_FORCED_SCAN_SCHEDULER_BYPASS)
        self._test_force_scan_every_sec = max(1, int(TEST_FORCED_SCAN_EVERY_SEC))
        self._next_test_scan_ms = 0

        # scheduler.repeat compatibility values (used for scheduler/test cadence; NOT TG signal re-send)
        self._signal_repeat_count = max(0, int(SCHED_REPEAT_COUNT))
        self._signal_repeat_interval_sec = max(0, int(SCHED_REPEAT_INTERVAL_SEC))
        self._repeat_by_gate: Dict[int, Tuple[int, int]] = {
            1: (max(0, int(SCHED_REPEAT_1H_COUNT)), max(0, int(SCHED_REPEAT_1H_INTERVAL_SEC))),
            4: (max(0, int(SCHED_REPEAT_4H_COUNT)), max(0, int(SCHED_REPEAT_4H_INTERVAL_SEC))),
            8: (max(0, int(SCHED_REPEAT_8H_COUNT)), max(0, int(SCHED_REPEAT_8H_INTERVAL_SEC))),
        }

        self._rest_funding_refresh_every_sec = max(0, int(self._funding_cfg.get("REST_REFRESH_EVERY_SEC", REST_FUNDING_REFRESH_EVERY_SEC) or 0))
        self._next_rest_funding_refresh_ms = 0

        # active candidates
        self._candidates: Dict[str, Candidate] = {}
        self._cand_diag_next_log_ms: int = 0

    def _resolve_enabled_exchange_keys(self) -> list[str]:
        enabled_order = [str(x).strip().lower() for x in (ENABLED_EXCHANGES or []) if str(x).strip()]
        if not enabled_order:
            enabled_order = [k for k, v in self._cfg_exchanges.items() if bool((v or {}).get("enable", False))]
        seen = set()
        out: list[str] = []
        for name in enabled_order:
            if name in seen:
                continue
            seen.add(name)
            ex_cfg = self._cfg_exchanges.get(name, {})
            if ex_cfg and not bool(ex_cfg.get("enable", False)):
                continue
            out.append(name)
        return out

    def _build_exchange_clients(self) -> list[Any]:
        mapping = {
            "okx": OKXClient,
            "phemex": PhemexClient,
            "binance": BinanceClient,
            "kucoin": KucoinClient,
            "bitget": BitgetClient,
        }
        out: list[Any] = []
        for key in self._enabled_exchange_keys:
            cls = mapping.get(key)
            if cls is None:
                self.logger.warning(f"[CFG] exchange '{key}' requested but adapter is not implemented in this build — skipped")
                continue
            try:
                out.append(cls(logger=self.logger))
            except Exception as e:
                self.logger.warning(f"[CFG] failed to init exchange '{key}': {e}")
        return out

    def _resolve_quote(self) -> str:
        base = (DEFAULT_QUOTE or "USDT").upper().strip() or "USDT"
        quotes = set()
        for key in self._enabled_exchange_keys:
            q = str((self._cfg_exchanges.get(key, {}) or {}).get("quote") or "").upper().strip()
            if q:
                quotes.add(q)
        if len(quotes) > 1:
            self.logger.warning(f"[CFG] multiple exchange quotes configured {sorted(quotes)}; using DEFAULT_QUOTE={base}")
            return base
        if len(quotes) == 1:
            only = next(iter(quotes))
            if only != base:
                self.logger.info(f"[CFG] quote from EXCHANGES applied: {only} (DEFAULT_QUOTE={base})")
            return only
        return base

    def _resolve_okx_cap(self) -> int:
        try:
            v = int((self._cfg_exchanges.get("okx", {}) or {}).get("max_symbols", 0) or 0)
        except Exception:
            v = 0
        if v > 0:
            return v
        return int(OKX_TEST_MAX_SYMBOLS)

    # ---------------------------------------------------------
    # SYMBOL UNIVERSE
    # ---------------------------------------------------------
    async def refresh_universe(self) -> None:
        u = await self._builder.build(self.exchanges, quote=self.quote, okx_cap=self.okx_cap)
        self.universe = u

        # Build TECH_DEBT structure: EXCHANGE -> interval_h -> list[raw_symbol]
        struct: Dict[str, Dict[int, list[str]]] = {}
        for ex_name, by_int in u.by_exchange_interval.items():
            struct[ex_name] = {}
            for interval_h, canon_to_raw in by_int.items():
                struct[ex_name][interval_h] = sorted(set(canon_to_raw.values()))
        self.raw_symbols_struct = struct

        # Persist for human inspection (dev/test only).
        # In prod this creates noisy churn in OUT/*.json on every scheduled refresh.
        # We keep the builder data in-memory, but skip disk dumps in RUN_MODE=prod.
        if str(RUN_MODE).strip().lower() != "prod":
            dump_symbols_struct(struct=self.raw_symbols_struct, universe=u, logger=self.logger)
        else:
            self.logger.debug("[SYMBOLS] skip OUT JSON dump in prod mode")

        # logging summary
        self.logger.info(f"[SYMBOLS] quote={u.quote}")
        for ex_name, s in sorted(u.per_exchange_sets.items()):
            self.logger.info(f"  {ex_name}: {len(s)}")
        self.logger.info(f"  UNION(all listed):  {len(u.canonical_union)}")
        self.logger.info(f"  ELIGIBLE(>=2 ex): {len(getattr(u, 'canonical_eligible', set()))}")
        self.logger.info(f"  COMMON(all ex):   {len(u.canonical_common)}")
        for a, row in sorted(getattr(u, 'pairwise_common_counts', {}).items()):
            parts = ", ".join(f"{b}={cnt}" for b, cnt in sorted(row.items()))
            if parts:
                self.logger.info(f"  links {a}: {parts}")

        for interval_h, s in sorted(u.union_by_interval.items()):
            self.logger.info(f"  interval {interval_h}h: {len(s)} symbols (eligible union)")

    # ---------------------------------------------------------
    # CANDIDATES
    # ---------------------------------------------------------
    @staticmethod
    def _cand_key(canon: str, long_ex: str, short_ex: str) -> str:
        return f"{canon}|{long_ex}|{short_ex}"

    def _add_candidates(self, opps: list[Opportunity], gate_h: int) -> None:
        if not self.universe:
            return

        now_ms = int(time.time() * 1000)

        # keep only a reasonable top slice per scan
        for o in opps:
            long_raw = self.universe.per_exchange_maps.get(o.long_ex, {}).get(o.canon)
            short_raw = self.universe.per_exchange_maps.get(o.short_ex, {}).get(o.canon)
            if not long_raw or not short_raw:
                continue

            key = self._cand_key(o.canon, o.long_ex, o.short_ex)
            prev = self._candidates.get(key)
            if prev:
                prev.last_seen_ms = now_ms
                prev.funding_long_pct = o.long_rate_pct
                prev.funding_short_pct = o.short_rate_pct
                prev.pre_total_funding_spread_pct = o.pre_total_funding_spread_pct
                prev.next_funding_time_ms = o.next_funding_time_ms
                prev.cluster_exchanges = o.cluster_exchanges
                prev.gate_hours = int(gate_h)
                prev.repeat_count, prev.repeat_interval_sec = self._repeat_by_gate.get(int(gate_h), (self._signal_repeat_count, self._signal_repeat_interval_sec))
            else:
                self._candidates[key] = Candidate(
                    key=key,
                    canon=o.canon,
                    long_ex=o.long_ex,
                    short_ex=o.short_ex,
                    long_raw=long_raw,
                    short_raw=short_raw,
                    funding_long_pct=o.long_rate_pct,
                    funding_short_pct=o.short_rate_pct,
                    pre_total_funding_spread_pct=o.pre_total_funding_spread_pct,
                    next_funding_time_ms=o.next_funding_time_ms,
                    cluster_exchanges=o.cluster_exchanges,
                    created_ms=now_ms,
                    last_seen_ms=now_ms,
                    gate_hours=int(gate_h),
                    repeat_count=int(self._repeat_by_gate.get(int(gate_h), (self._signal_repeat_count, self._signal_repeat_interval_sec))[0]),
                    repeat_interval_sec=int(self._repeat_by_gate.get(int(gate_h), (self._signal_repeat_count, self._signal_repeat_interval_sec))[1]),
                )

    def _cleanup_candidates(self) -> None:
        now_ms = int(time.time() * 1000)
        expire_ms = int(CANDIDATE_EXPIRE_SEC) * 1000

        drop = []
        for k, c in self._candidates.items():
            # basic staleness
            if now_ms - c.last_seen_ms > expire_ms:
                drop.append(k)
                continue

            # drop signaled after funding + grace (or keep until generic stale timeout when dedup is forever)
            if c.signaled and c.next_funding_time_ms:
                if isinstance(DEDUP_AFTER_FUNDING_SEC, (int, float)) and float(DEDUP_AFTER_FUNDING_SEC) >= 0:
                    if now_ms > int(c.next_funding_time_ms) + int(float(DEDUP_AFTER_FUNDING_SEC)) * 1000:
                        drop.append(k)

        for k in drop:
            self._candidates.pop(k, None)

        self._dedup.cleanup()

    # ---------------------------------------------------------
    # SCANS
    # ---------------------------------------------------------
    @staticmethod
    def _fmt_dt_utc(ms: int) -> str:
        if not ms:
            return "?"
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    @staticmethod
    def _price_component_pct(p_short: float, p_long: float) -> float:
        """Dynamic price spread aligned with determinants (legacy helper name)."""
        p_short = float(p_short)
        p_long = float(p_long)
        if p_short <= 0 or p_long <= 0:
            return 0.0
        return (p_short - p_long) / min(p_short, p_long) * 100.0

    @staticmethod
    def _ttf_range_for_gate(interval_h: int) -> Tuple[Optional[float], Optional[float]]:
        """Compatibility shim kept for older internal call sites.

        Real scan window source is scheduler gate config (val_sec/offset/repeats),
        implemented in _ttf_scan_window_for_gate(). Returning (None, None) here avoids
        any hidden duplicate TTF bucket logic in the config layer.
        """
        return None, None

    def _ttf_scan_window_for_gate(self, gate_h: int) -> Tuple[Optional[float], Optional[float]]:
        """TTF window used by scanner for a scheduler gate.

        Delegated to a helper module to keep bot.py thinner and keep scheduler/scan
        coupling logic in one place.
        """
        return build_gate_scan_ttf_window(
            scheduler=self._scheduler,
            gate_h=int(gate_h),
            poll_sec=float(getattr(self, "poll_sec", 1.0) or 1.0),
        )

    def _format_opp(self, o: Opportunity) -> str:
        return (
            f"{o.canon}: LONG {o.long_ex}({o.long_rate_pct:+.4f}%) / "
            f"SHORT {o.short_ex}({o.short_rate_pct:+.4f}%) | "
            f"PRE={o.pre_total_funding_spread_pct:.4f}% | "
            f"TTF={o.time_to_funding_sec/60.0:.1f}m"
        )

    async def _scan_gate(self, gate_h: int) -> None:
        if not self._funding_enabled:
            self.logger.warning("[SCAN] funding engine disabled in ENGINE_FUNDING.enabled")
            return
        if not self.universe:
            return

        # We scan the whole union, but filter by time-to-funding range.
        symbols = sorted(getattr(self.universe, "canonical_eligible", None) or self.universe.canonical_union)
        if not symbols:
            self.logger.info(f"[SCAN] gate={gate_h}h: no symbols")
            return

        ttf_min, ttf_max = self._ttf_scan_window_for_gate(gate_h)

        # Refresh REST-based funding caches right before scanning (OKX is stream-hot)
        await self._maybe_refresh_rest_funding(force=True)

        opps = self._engine.scan(
            canon_list=symbols,
            exchanges=self.exchanges,
            per_exchange_maps=self.universe.per_exchange_maps,
            ttf_min_sec=ttf_min,
            ttf_max_sec=ttf_max,
        )
        opps_raw_count = len(opps)
        opps = self._filter_opps_by_min_pair_interval(opps, gate_h=int(gate_h))

        top = opps[:10]
        uniq_canon = len({o.canon for o in opps})
        _sched_kind = None
        try:
            _g = self._scheduler.gates.get(int(gate_h)) if self._scheduler else None
            _sched_kind = getattr(_g, "last_fire_kind", None) if _g else None
        except Exception:
            _sched_kind = None
        self.logger.info(
            f"[SCAN] gate={gate_h}h({(_sched_kind or 'n/a')}): opps={len(opps)} pairs / {uniq_canon} symbols"
            f" (raw={opps_raw_count}, minIntervalFiltered={max(0, opps_raw_count - len(opps))}) | "
            f"PRE>= {float(FUNDING_SPREAD):.4f}% | next-sync<= {int(NEXT_FUNDING_SYNC_SEC)}s | "
            f"fundingSource={self._funding_source_mode} | maxFundingAge={self._max_funding_age_sec:.1f}s | "
            f"ttf=[{ttf_min},{ttf_max}]"
        )
        for o in top:
            self.logger.info("  " + self._format_opp(o))

        self._add_candidates(opps, gate_h=gate_h)

    # ---------------------------------------------------------
    # VALIDATION (price) + SIGNAL
    # ---------------------------------------------------------
    def _dedup_key(self, c: Candidate, signal_kind: str = "PRIMARY") -> str:
        # Dedup by concrete pair + funding bucket + per-leg interval signature.
        # This keeps duplicate repeats quiet, while allowing multiple valid exchange bundles
        # for the same coin in the same bucket (user-visible complaint: some pairs were suppressed).
        bucket = int(c.next_funding_time_ms // 60_000) if c.next_funding_time_ms else 0
        sig = self._pair_interval_signature(c)
        pair_id = f"{str(c.long_ex).upper()}>{str(c.short_ex).upper()}"
        return f"SIG|{str(signal_kind).upper()}|{c.canon}|{pair_id}|{bucket}|{sig}"

    def _get_exchange(self, ex_name: str):
        target = str(ex_name or "").strip().upper()
        for ex in self.exchanges:
            name = str(getattr(ex, "name", "") or "").strip().upper()
            if name == target:
                return ex
        return None

    def _get_live_funding_point(self, ex_name: str, raw_symbol: str):
        ex = self._get_exchange(ex_name)
        if not ex:
            return None
        f = getattr(ex, "funding", None)
        if f is None or not hasattr(f, "get"):
            return None
        try:
            return f.get(raw_symbol)
        except Exception:
            return None

    def _extract_live_funding_meta(self, p) -> Dict[str, Any]:
        if p is None:
            return {"rate_pct": None, "next_ms": None, "updated_at_ms": None, "source": "unknown"}
        return {
            "rate_pct": self._engine._rate_pct_from_point(p),
            "next_ms": self._engine._next_funding_time_ms(p),
            "updated_at_ms": self._engine._updated_at_ms_from_point(p),
            "source": self._engine._source_from_point(p),
        }

    def _funding_age_sec(self, updated_at_ms: Optional[int], now_ms: int) -> Optional[float]:
        if not isinstance(updated_at_ms, (int, float)) or int(updated_at_ms) <= 0:
            return None
        return max(0.0, (int(now_ms) - int(updated_at_ms)) / 1000.0)

    def _funding_source_allowed(self, source: str) -> bool:
        return self._engine._is_source_allowed(source)

    def _funding_ttf_desync_ms(self, nxt_long: int, nxt_short: int, *, now_ms: int) -> int:
        ttf_l_ms = int(nxt_long) - int(now_ms)
        ttf_s_ms = int(nxt_short) - int(now_ms)
        return abs(ttf_l_ms - ttf_s_ms)

    @staticmethod
    def _funding_sig(rate_long: float, rate_short: float, nxt_long: int, nxt_short: int) -> str:
        return f"{float(rate_long):+.8f}|{float(rate_short):+.8f}|{int(nxt_long)}|{int(nxt_short)}"

    def _pair_leg_intervals_from_opp(self, o: Opportunity) -> list[int]:
        vals: list[int] = []
        for ex_name in (str(o.long_ex), str(o.short_ex)):
            raw_symbol = None
            try:
                raw_symbol = (self.universe.per_exchange_maps.get(str(ex_name), {}) if self.universe else {}).get(str(o.canon))
            except Exception:
                raw_symbol = None
            h = self._resolve_leg_interval_hours(str(ex_name), str(raw_symbol or ""), str(o.canon))
            if not (isinstance(h, int) and h > 0):
                h = self._resolve_exchange_interval_h(ex_name, str(o.canon))
            if isinstance(h, int) and h > 0:
                vals.append(int(h))
        return sorted(set(vals))

    def _pair_min_interval_h_from_opp(self, o: Opportunity) -> Optional[int]:
        vals = self._pair_leg_intervals_from_opp(o)
        return min(vals) if vals else None

    def _filter_opps_by_min_pair_interval(self, opps: list[Opportunity], gate_h: int) -> list[Opportunity]:
        """Gate routing filter for opportunities.

        Previous behavior allowed a pair only on the gate equal to the *minimum* leg interval.
        That suppresses valid mixed-interval pairs (e.g. 4h+8h) on the 8h gate, which users can
        legitimately want to see when the shared next funding event is in the 8h scan window.

        New behavior:
          - if no interval metadata is available -> keep the pair (don't guess/drop)
          - otherwise keep the pair when the current gate matches *any* leg interval
        De-dup logic still protects from duplicate notifications for the same funding bucket.
        """
        out: list[Opportunity] = []
        gh = int(gate_h)
        for o in opps:
            legs = self._pair_leg_intervals_from_opp(o)
            if (not legs) or (gh in legs):
                out.append(o)
        return out

    def _pair_interval_signature(self, c: Candidate) -> str:
        lh = self._resolve_leg_interval_hours(c.long_ex, c.long_raw, c.canon)
        sh = self._resolve_leg_interval_hours(c.short_ex, c.short_raw, c.canon)
        ltxt = f"{int(lh)}h" if isinstance(lh, int) and lh > 0 else "?"
        stxt = f"{int(sh)}h" if isinstance(sh, int) and sh > 0 else "?"
        return "+".join(sorted((ltxt, stxt)))

    async def _maybe_refresh_rest_funding(self, force: bool = False) -> None:
        """Refresh REST-based funding caches periodically so TTL validation sees fresh funding."""
        if not self._funding_enabled:
            return
        if self._funding_source_mode == "ws":
            return
        now_ms = int(time.time() * 1000)
        if not force:
            if self._rest_funding_refresh_every_sec <= 0:
                return
            if now_ms < self._next_rest_funding_refresh_ms:
                return
        self._next_rest_funding_refresh_ms = now_ms + max(1, self._rest_funding_refresh_every_sec) * 1000

        for ex in self.exchanges:
            # OKX funding is already hot-streamed; skip unless adapter has no stream support.
            if getattr(ex, "name", "") == "OKX":
                continue
            try:
                f = getattr(ex, "funding", None)
                if f is not None and hasattr(f, "refresh"):
                    await f.refresh()
            except Exception as e:
                self.logger.warning(f"[{getattr(ex, 'name', ex.__class__.__name__)}] funding refresh failed: {e}")


    def _top_funding_dedup_key(self, prefix: str, *, bucket_min: int, gates: Optional[list[int]] = None) -> str:
        """Stable dedup key for TOP funding info alerts.

        Intentionally does NOT include live rates/signature to avoid spam on tiny cache updates
        inside the same funding bucket. One alert per bucket (optionally per gate set).
        """
        gate_part = "all"
        if gates:
            try:
                norm = sorted({int(g) for g in gates if isinstance(g, (int, float)) and int(g) in (1, 4, 8)})
                if norm:
                    gate_part = ",".join(f"{g}h" for g in norm)
            except Exception:
                gate_part = "all"
        return f"{str(prefix).upper()}|{int(bucket_min)}|{gate_part}"

    async def _notify_top_funding(
        self,
        *,
        ex_name: str,
        title: str,
        enable: bool,
        min_funding_abs_pct: Optional[float],
        slice_n: Optional[int],
        dedup_prefix: str,
        gates: Optional[list[int]] = None,
    ) -> None:
        if not bool(enable):
            return
        # If both filters are off, this notifier is effectively disabled
        if min_funding_abs_pct is None and slice_n is None:
            return
        if not self.universe:
            return

        ex = self._get_exchange(str(ex_name))
        if not ex:
            return
        f = getattr(ex, "funding", None)
        cache = getattr(f, "cache", None)
        if not isinstance(cache, dict) or not cache:
            return

        ex_key = str(getattr(ex, "name", ex_name) or ex_name).strip().upper()
        raw_to_canon = {str(raw).upper().strip(): canon for canon, raw in self.universe.per_exchange_maps.get(ex_key, {}).items()}

        rows = []
        now_ms = int(time.time() * 1000)
        for raw, pt in cache.items():
            raw_norm = str(raw).upper().strip()
            canon = raw_to_canon.get(raw_norm)
            if not canon:
                continue
            fmeta = self._extract_live_funding_meta(pt)
            rate_pct, nxt_ms = fmeta.get("rate_pct"), fmeta.get("next_ms")
            if rate_pct is None or nxt_ms is None:
                continue
            try:
                rate_pct_f = float(rate_pct)
            except Exception:
                continue
            ttf_sec = (int(nxt_ms) - now_ms) / 1000.0
            if ttf_sec <= 0:
                continue
            rows.append((abs(rate_pct_f), rate_pct_f, canon, raw_norm, int(nxt_ms), float(ttf_sec)))

        if not rows:
            return

        # filter by min_funding (absolute %)
        if min_funding_abs_pct is not None:
            try:
                thr = abs(float(min_funding_abs_pct))
            except Exception:
                thr = None
            if thr is not None and thr > 0:
                rows = [r for r in rows if float(r[0]) >= thr]
                if not rows:
                    return

        rows_all = list(rows)

        if gates:
            # Align the info alert with the actual scan gate window(s) to avoid confusion
            ranges: list[tuple[Optional[float], Optional[float]]] = []
            for g in gates:
                try:
                    rng = self._ttf_scan_window_for_gate(int(g))
                except Exception:
                    rng = (None, None)
                ranges.append(rng)

            def _in_any_gate_window(ttf_sec: float) -> bool:
                if not ranges:
                    return True
                for lo, hi in ranges:
                    if lo is not None and float(ttf_sec) < float(lo):
                        continue
                    if hi is not None and float(ttf_sec) > float(hi):
                        continue
                    return True
                return False

            rows = [r for r in rows if _in_any_gate_window(float(r[5]))]
            if not rows and rows_all:
                # fallback to global list if gate filter removed everything
                rows = rows_all

        if not rows:
            return

        rows.sort(reverse=True)

        if slice_n is None:
            top = rows
        else:
            try:
                top_n = max(1, int(slice_n))
            except Exception:
                top_n = 10
            top = rows[:top_n]

        if not top:
            return

        # Dedup info message strictly by funding bucket (+gate set), not by live rate jitter
        bucket = int(min(r[4] for r in top) // 60_000)
        dkey = self._top_funding_dedup_key(dedup_prefix, bucket_min=bucket, gates=gates)
        if self._dedup.is_seen(dkey):
            return
        _exp_ms = None
        if isinstance(DEDUP_AFTER_FUNDING_SEC, (int, float)) and float(DEDUP_AFTER_FUNDING_SEC) >= 0:
            _exp_ms = int(min(r[4] for r in top)) + int(float(DEDUP_AFTER_FUNDING_SEC)) * 1000
        self._dedup.mark(dkey, expires_at_ms=_exp_ms)

        # lines = [f"📊 {title}:\n\n"]
        # for _, rate_pct, canon, raw, nxt_ms, ttf_sec in top:
        #     lines.append(
        #         f"• {canon} | funding={rate_pct:+.4f}% | next={self._fmt_dt_utc(nxt_ms)} | TTF={ttf_sec/60.0:.1f}m"
        #     )
        # await self.notifier.send_text("\n".join(lines))
        # self.logger.info(f"[{dedup_prefix}] sent {len(top)} rows; bucket={bucket}; gates={gates or 'all'}")

        # ---- pretty header with filters ----
        filters = []
        if min_funding_abs_pct is not None:
            try:
                filters.append(f">= {abs(float(min_funding_abs_pct)):.4f}%")
            except Exception:
                pass
        if slice_n is not None:
            try:
                filters.append(f"top {max(1, int(slice_n))}")
            except Exception:
                filters.append("top ?")
        # if gates:
        #     try:
        #         gtxt = ",".join(f"{int(g)}h" for g in sorted({int(x) for x in gates if int(x) in (1, 4, 8)}))
        #         if gtxt:
        #             filters.append(f"gate {gtxt}")
        #     except Exception:
        #         pass

        hdr = f"📊 {title}"
        if filters:
            hdr += f" ({' | '.join(filters)})"

        lines = [hdr]
        for _, rate_pct, canon, raw, nxt_ms, ttf_sec in top:
            # only TTF, no timestamp
            lines.append(f"• {canon} | funding={rate_pct:+.4f}% | TTF={ttf_sec/60.0:.1f}m")

        await self.notifier.send_text("\n".join(lines))
        self.logger.info(f"[{dedup_prefix}] sent {len(top)} rows; bucket={bucket}; gates={gates or 'all'}")

    async def _notify_phemex_top_funding(self, gates: Optional[list[int]] = None) -> None:
        await self._notify_top_funding(
            ex_name="PHEMEX",
            title="PHEMEX TOP funding",
            enable=bool(PHEMEX_TOP_FUNDING_ENABLE),
            min_funding_abs_pct=PHEMEX_TOP_FUNDING_MIN_FUNDING,
            slice_n=PHEMEX_TOP_FUNDING_SLICE,
            dedup_prefix="PHEMEX_TOP",
            gates=gates,
        )

    async def _notify_kucoin_top_funding(self, gates: Optional[list[int]] = None) -> None:
        await self._notify_top_funding(
            ex_name="KUCOIN",
            title="KUCOIN TOP funding",
            enable=bool(KUCOIN_TOP_FUNDING_ENABLE),
            min_funding_abs_pct=KUCOIN_TOP_FUNDING_MIN_FUNDING,
            slice_n=KUCOIN_TOP_FUNDING_SLICE,
            dedup_prefix="KUCOIN_TOP",
            gates=gates,
        )
    async def _process_signal_repeats(self) -> None:
        """No-op by design.

        scheduler.repeat now means repeated SCAN/CHECK cycles for a gate (handled in CORE.scheduler),
        not Telegram re-send of an already emitted signal.
        """
        return


    def _arm_signal_repeats(self, c: Candidate, msg: str, now_ms: int) -> None:
        # Deprecated semantics: keep method for compatibility, but do nothing.
        # Repeats are scheduler-level repeated scans, not TG message repeats.
        return


    async def _shutdown(self) -> None:
        try:
            await self.market.close()
        except Exception as e:
            self.logger.warning(f"[SHUTDOWN] market streams close failed: {e}")
        try:
            await self.notifier.close()
        except Exception as e:
            self.logger.warning(f"[SHUTDOWN] tg notifier close failed: {e}")
        for ex in self.exchanges:
            if hasattr(ex, "shutdown"):
                try:
                    await ex.shutdown()
                except Exception as e:
                    self.logger.warning(f"[SHUTDOWN] {getattr(ex, 'name', ex.__class__.__name__)} failed: {e}")

    async def _evaluate_candidates(self) -> None:
        if not self.universe or not self._candidates:
            return

        if not self._funding_enabled:
            return

        # Periodic refresh for REST-based funding adapters (during TTL tracking)
        await self._maybe_refresh_rest_funding(force=False)

        # Build needed raw symbols per exchange (only not-signaled)
        need: Dict[str, Set[str]] = {}
        for c in self._candidates.values():
            if c.signaled:
                continue
            need.setdefault(c.long_ex, set()).add(c.long_raw)
            need.setdefault(c.short_ex, set()).add(c.short_raw)

        if not need:
            return

        await self.market.ensure(need)

        loop_now_ms = int(time.time() * 1000)
        total_min = float(self._total_spread_min)
        pre_min = float(self._pre_spread_min)
        price_w = float(self._price_weight)
        price2_enabled = bool(self._price2_enabled)
        price2_total_min = float(self._price2_total_spread_min)
        price2_w = float(self._price2_weight)
        thr_spread = self._stakan_spread_thr

        for c in list(self._candidates.values()):
            if c.signaled:
                continue

            c.last_fail_reason = ""

            # --- live funding re-check (important for TTL validity) ---
            # atomic-ish local snapshot: read all legs once and evaluate against a single timestamp
            snap_now_ms = int(time.time() * 1000)
            p_f_long = self._get_live_funding_point(c.long_ex, c.long_raw)
            p_f_short = self._get_live_funding_point(c.short_ex, c.short_raw)
            f_long = self._extract_live_funding_meta(p_f_long)
            f_short = self._extract_live_funding_meta(p_f_short)
            rate_long, nxt_long = f_long.get("rate_pct"), f_long.get("next_ms")
            rate_short, nxt_short = f_short.get("rate_pct"), f_short.get("next_ms")
            if rate_long is None or rate_short is None or not nxt_long or not nxt_short:
                c.pair_ok_since_ms = 0
                c.price_ok_since_ms = 0
                c.stakan_ok_since_ms = 0
                c.state = "TRACKING"
                c.last_fail_reason = "live funding data unavailable"
                continue

            src_long = str(f_long.get("source") or "unknown")
            src_short = str(f_short.get("source") or "unknown")
            if not self._funding_source_allowed(src_long) or not self._funding_source_allowed(src_short):
                c.pair_ok_since_ms = 0
                c.price_ok_since_ms = 0
                c.stakan_ok_since_ms = 0
                c.state = "TRACKING"
                c.last_fail_reason = f"funding source mode={self._funding_source_mode}: {c.long_ex}={src_long}, {c.short_ex}={src_short}"
                continue

            age_long = self._funding_age_sec(f_long.get("updated_at_ms"), snap_now_ms)
            age_short = self._funding_age_sec(f_short.get("updated_at_ms"), snap_now_ms)
            if self._max_funding_age_sec > 0 and (
                age_long is None or age_short is None or age_long > self._max_funding_age_sec or age_short > self._max_funding_age_sec
            ):
                c.pair_ok_since_ms = 0
                c.price_ok_since_ms = 0
                c.stakan_ok_since_ms = 0
                c.state = "TRACKING"
                c.last_fail_reason = (
                    f"stale funding data: {c.long_ex}={age_long if age_long is not None else 'NA'}s, "
                    f"{c.short_ex}={age_short if age_short is not None else 'NA'}s > {self._max_funding_age_sec:.1f}s"
                )
                continue

            # time sync between legs must stay valid (TTF delta, not raw epoch only)
            next_sync_sec = float(self._funding_cfg.get("NEXT_FUNDING_SYNC_SEC", NEXT_FUNDING_SYNC_SEC) or NEXT_FUNDING_SYNC_SEC)
            ttf_desync_ms = self._funding_ttf_desync_ms(int(nxt_long), int(nxt_short), now_ms=snap_now_ms)
            if ttf_desync_ms > int(next_sync_sec) * 1000:
                c.pair_ok_since_ms = 0
                c.price_ok_since_ms = 0
                c.stakan_ok_since_ms = 0
                c.state = "TRACKING"
                c.last_fail_reason = f"TTF desync {ttf_desync_ms/1000.0:.1f}s > {float(next_sync_sec):.1f}s"
                continue

            # update candidate funding snapshot from live caches
            c.funding_long_pct = float(rate_long)
            c.funding_short_pct = float(rate_short)
            c.next_funding_time_ms = int((int(nxt_long) + int(nxt_short)) // 2)
            c.last_pre_total_funding_spread_live_pct = ArbMath.calc_pre_total_funding_spread_pct(
                long_rate_pct=float(rate_long),
                short_rate_pct=float(rate_short),
            )
            c.pre_total_funding_spread_pct = float(c.last_pre_total_funding_spread_live_pct)

            funding_sig = self._funding_sig(float(rate_long), float(rate_short), int(nxt_long), int(nxt_short))
            if c.ttl_anchor_funding_sig and c.ttl_anchor_funding_sig != funding_sig:
                # Critical integrity guard: reset TTL accumulation if funding basis changed mid-hold.
                c.pair_ok_since_ms = 0
                c.price_ok_since_ms = 0
                c.stakan_ok_since_ms = 0
            c.ttl_anchor_funding_sig = funding_sig

            if c.pre_total_funding_spread_pct < pre_min:
                c.pair_ok_since_ms = 0
                c.price_ok_since_ms = 0
                c.stakan_ok_since_ms = 0
                c.state = "TRACKING"
                c.last_fail_reason = (
                    f"PRE funding spread {c.pre_total_funding_spread_pct:.4f}% < {pre_min:.4f}%"
                )
                continue

            # Hard no-signal quiet zone right before funding (protects against late noisy alerts)
            ttf_to_funding_sec = max(0.0, (int(c.next_funding_time_ms) - int(snap_now_ms)) / 1000.0) if c.next_funding_time_ms else 0.0
            if ttf_to_funding_sec < float(PRE_FUNDING_SIGNAL_QUIET_SEC):
                c.pair_ok_since_ms = 0
                c.price_ok_since_ms = 0
                c.stakan_ok_since_ms = 0
                c.state = "TRACKING"
                c.last_fail_reason = (
                    f"quiet zone: TTF {ttf_to_funding_sec:.1f}s < {int(PRE_FUNDING_SIGNAL_QUIET_SEC)}s"
                )
                continue

            # -------- PRICE gate (optional) --------
            p_long = 0.0
            p_short = 0.0
            if self._price_enabled:
                pl = self.market.get_price_info(c.long_ex, c.long_raw, now_ms=snap_now_ms)
                ps = self.market.get_price_info(c.short_ex, c.short_raw, now_ms=snap_now_ms)
                if not pl or not ps:
                    c.pair_ok_since_ms = 0
                    c.price_ok_since_ms = 0
                    c.state = "TRACKING"
                    c.last_fail_reason = "price data unavailable"
                    continue
                if (
                    self._max_price_age_sec > 0
                    and (pl["age_sec"] > float(self._max_price_age_sec) or ps["age_sec"] > float(self._max_price_age_sec))
                ):
                    c.pair_ok_since_ms = 0
                    c.price_ok_since_ms = 0
                    c.state = "TRACKING"
                    c.last_fail_reason = "stale price data"
                    continue
                p_long = float(pl.get("price") or 0.0)
                p_short = float(ps.get("price") or 0.0)
                if p_long <= 0 or p_short <= 0:
                    c.pair_ok_since_ms = 0
                    c.price_ok_since_ms = 0
                    c.state = "TRACKING"
                    c.last_fail_reason = "non-positive price"
                    continue

                spread_out = ArbMath.calc_total_spread(
                    PairSpreadInput(
                        pre_total_funding_spread_pct=float(c.pre_total_funding_spread_pct),
                        p_long=p_long,
                        p_short=p_short,
                        price_weight=price_w,
                    )
                )
                c.last_price_component_pct = float(spread_out.price_component_pct)
                c.last_total_spread_pct = float(spread_out.total_spread_pct)
                ok_total = c.last_total_spread_pct >= total_min

                c.last_price2_price_component_pct = 0.0
                c.last_price2_total_spread_pct = 0.0
                ok_total_price2 = False
                if price2_enabled:
                    spread_out_rev = ArbMath.calc_total_spread(
                        PairSpreadInput(
                            pre_total_funding_spread_pct=float(-c.pre_total_funding_spread_pct),
                            p_long=p_short,
                            p_short=p_long,
                            price_weight=price2_w,
                        )
                    )
                    c.last_price2_price_component_pct = float(spread_out_rev.price_component_pct)
                    c.last_price2_total_spread_pct = float(spread_out_rev.total_spread_pct)
                    ok_total_price2 = c.last_price2_total_spread_pct >= price2_total_min
            else:
                # Price module disabled: TOTAL gate collapses to PRE funding spread
                c.last_price_component_pct = 0.0
                c.last_total_spread_pct = float(c.pre_total_funding_spread_pct)
                ok_total = c.pre_total_funding_spread_pct >= pre_min
                c.last_price2_price_component_pct = 0.0
                c.last_price2_total_spread_pct = float(-c.pre_total_funding_spread_pct) if price2_enabled else 0.0
                ok_total_price2 = bool(price2_enabled and (c.last_price2_total_spread_pct >= price2_total_min))
                c.price_ok_since_ms = snap_now_ms

            # -------- STAKAN gate (optional) --------
            book_long: Optional[dict] = None
            book_short: Optional[dict] = None
            ok_long_legacy = True
            ok_short_legacy = True
            q_long_ok = True
            q_short_ok = True
            q_long_reason = ""
            q_short_reason = ""

            if self._stakan_enabled:
                ok_long_legacy, book_long = self.market.is_spread_ok_now(c.long_ex, c.long_raw)
                ok_short_legacy, book_short = self.market.is_spread_ok_now(c.short_ex, c.short_raw)
                if not book_long or not book_short:
                    c.pair_ok_since_ms = 0
                    c.stakan_ok_since_ms = 0
                    c.state = "TRACKING"
                    c.last_fail_reason = "book data unavailable"
                    continue
                if (
                    self._max_book_age_sec > 0
                    and (book_long["age_sec"] > float(self._max_book_age_sec) or book_short["age_sec"] > float(self._max_book_age_sec))
                ):
                    c.pair_ok_since_ms = 0
                    c.stakan_ok_since_ms = 0
                    c.state = "TRACKING"
                    c.last_fail_reason = "stale book data"
                    continue

                if thr_spread is None or float(thr_spread) <= 0:
                    ok_long_legacy = True
                    ok_short_legacy = True

                bll = ArbMath.build_book_leg(c.long_ex, book_long)
                bls = ArbMath.build_book_leg(c.short_ex, book_short)
                if not bll or not bls:
                    c.pair_ok_since_ms = 0
                    c.stakan_ok_since_ms = 0
                    c.state = "TRACKING"
                    c.last_fail_reason = "invalid book snapshot"
                    continue

                q_long = ArbMath.check_book_quality(book=bll, cfg=self._book_quality_cfg)
                q_short = ArbMath.check_book_quality(book=bls, cfg=self._book_quality_cfg)
                q_long_ok = bool(q_long.ok)
                q_short_ok = bool(q_short.ok)
                q_long_reason = str(q_long.fail_reason or "")
                q_short_reason = str(q_short.fail_reason or "")
                ok_books = bool(ok_long_legacy and ok_short_legacy and q_long_ok and q_short_ok)
            else:
                ok_books = True
                # synthetic books for message rendering compatibility
                book_long = {"bid": p_long, "ask": p_long, "spread_pct": 0.0, "top_min_notional": 0.0, "top_balance_ratio": 0.0}
                book_short = {"bid": p_short, "ask": p_short, "spread_pct": 0.0, "top_min_notional": 0.0, "top_balance_ratio": 0.0}
                c.stakan_ok_since_ms = snap_now_ms

            # -------- Separate TTL controls --------
            if ok_total:
                if not c.price_ok_since_ms:
                    c.price_ok_since_ms = snap_now_ms
            else:
                c.price_ok_since_ms = 0

            if ok_books:
                if not c.stakan_ok_since_ms:
                    c.stakan_ok_since_ms = snap_now_ms
            else:
                c.stakan_ok_since_ms = 0

            price_held_sec = ((snap_now_ms - c.price_ok_since_ms) / 1000.0) if c.price_ok_since_ms else 0.0
            stakan_held_sec = ((snap_now_ms - c.stakan_ok_since_ms) / 1000.0) if c.stakan_ok_since_ms else 0.0

            price_ttl_ready = (not self._price_enabled) or (self._price_ttl_sec <= 0) or (c.price_ok_since_ms and price_held_sec >= self._price_ttl_sec)
            stakan_ttl_ready = (not self._stakan_enabled) or (self._stakan_ttl_sec <= 0) or (c.stakan_ok_since_ms and stakan_held_sec >= self._stakan_ttl_sec)
            primary_ready = bool(ok_total and ok_books and price_ttl_ready and stakan_ttl_ready)
            price2_ready = bool(price2_enabled and ok_total_price2 and ok_books and price_ttl_ready and stakan_ttl_ready)
            ok_now = bool(primary_ready or price2_ready)

            if ok_now:
                signal_kind = "PRIMARY" if primary_ready else "PRICE2_REVERSE"
                c.state = "READY"
                active_since = [x for x in (c.price_ok_since_ms, c.stakan_ok_since_ms) if x]
                c.pair_ok_since_ms = min(active_since) if active_since else snap_now_ms
                held_sec = (snap_now_ms - c.pair_ok_since_ms) / 1000.0

                dkey = self._dedup_key(c, signal_kind=signal_kind)
                if self._dedup.is_seen(dkey):
                    c.signaled = True
                    c.state = "SIGNALED"
                    continue

                expires_at_ms = None
                if isinstance(DEDUP_AFTER_FUNDING_SEC, (int, float)) and float(DEDUP_AFTER_FUNDING_SEC) >= 0:
                    expires_at_ms = int(c.next_funding_time_ms) + int(float(DEDUP_AFTER_FUNDING_SEC)) * 1000
                self._dedup.mark(dkey, expires_at_ms=expires_at_ms)

                if signal_kind == "PRICE2_REVERSE":
                    _msg_total_min = float(price2_total_min)
                    _msg_price_w = float(price2_w)
                    c.last_signal_kind = "PRICE2_REVERSE"
                    # build reversed-view candidate snapshot for message formatting
                    c_msg = Candidate(
                        key=c.key, canon=c.canon, long_ex=c.short_ex, short_ex=c.long_ex,
                        long_raw=c.short_raw, short_raw=c.long_raw,
                        funding_long_pct=float(c.funding_short_pct), funding_short_pct=float(c.funding_long_pct),
                        pre_total_funding_spread_pct=float(-c.pre_total_funding_spread_pct),
                        next_funding_time_ms=int(c.next_funding_time_ms), cluster_exchanges=tuple(c.cluster_exchanges or ()),
                        created_ms=int(c.created_ms), last_seen_ms=int(c.last_seen_ms),
                        pair_ok_since_ms=int(c.pair_ok_since_ms), price_ok_since_ms=int(c.price_ok_since_ms), stakan_ok_since_ms=int(c.stakan_ok_since_ms),
                        last_price_component_pct=float(c.last_price2_price_component_pct),
                        last_total_spread_pct=float(c.last_price2_total_spread_pct),
                        last_pre_total_funding_spread_live_pct=float(-c.last_pre_total_funding_spread_live_pct),
                        last_fail_reason=str(c.last_fail_reason), state=str(c.state), signaled=bool(c.signaled),
                        ttl_anchor_funding_sig=str(c.ttl_anchor_funding_sig), gate_hours=int(c.gate_hours),
                        repeat_count=int(c.repeat_count), repeat_interval_sec=int(c.repeat_interval_sec),
                        signal_sent_ms=int(c.signal_sent_ms), repeat_done_count=int(c.repeat_done_count),
                        repeat_next_ms=int(c.repeat_next_ms), last_signal_message=str(c.last_signal_message),
                    )
                    msg = self._build_signal_message(
                        c=c_msg,
                        p_long=p_short,
                        p_short=p_long,
                        book_long=book_short or {},
                        book_short=book_long or {},
                        held_sec=held_sec,
                        total_min=_msg_total_min,
                        price_w=_msg_price_w,
                        signal_kind=signal_kind,
                    )
                else:
                    c.last_signal_kind = "PRIMARY"
                    msg = self._build_signal_message(
                        c=c,
                        p_long=p_long,
                        p_short=p_short,
                        book_long=book_long or {},
                        book_short=book_short or {},
                        held_sec=held_sec,
                        total_min=total_min,
                        price_w=price_w,
                        signal_kind=signal_kind,
                    )

                try:
                    gates_hint = [int(c.gate_hours)] if int(getattr(c, "gate_hours", 0) or 0) in (1, 4, 8) else None
                    await self._notify_phemex_top_funding(gates=gates_hint)
                except Exception as e:
                    self.logger.warning(f"[PHEMEX TOP] notify failed: {e}")

                try:
                    gates_hint = [int(c.gate_hours)] if int(getattr(c, "gate_hours", 0) or 0) in (1, 4, 8) else None
                    await self._notify_kucoin_top_funding(gates=gates_hint)
                except Exception as e:
                    self.logger.warning(f"[KUCOIN TOP] notify failed: {e}")

                await self.notifier.send_text(msg)
                c.signaled = True
                c.state = "SIGNALED"
            else:
                c.pair_ok_since_ms = 0
                c.state = "TRACKING"
                reasons = []
                if self._stakan_enabled:
                    if thr_spread is not None and float(thr_spread) > 0:
                        if not ok_long_legacy:
                            reasons.append(f"{c.long_ex} legacy spread<{float(thr_spread):.4f}%")
                        if not ok_short_legacy:
                            reasons.append(f"{c.short_ex} legacy spread<{float(thr_spread):.4f}%")
                    if not q_long_ok:
                        reasons.append(f"{c.long_ex} quality: {q_long_reason}")
                    if not q_short_ok:
                        reasons.append(f"{c.short_ex} quality: {q_short_reason}")
                    if ok_books and not stakan_ttl_ready:
                        reasons.append(f"STAKAN TTL {stakan_held_sec:.1f}s < {self._stakan_ttl_sec:.1f}s")
                if self._price_enabled:
                    if not ok_total:
                        reasons.append(
                            f"TOTAL {c.last_total_spread_pct:.4f}% < {total_min:.4f}% "
                            f"(PRE {c.pre_total_funding_spread_pct:.4f}% + PRICE {c.last_price_component_pct:.4f}% * w {price_w:.2f})"
                        )
                    if price2_enabled and not ok_total_price2:
                        reasons.append(
                            f"PRICE2 TOTAL {c.last_price2_total_spread_pct:.4f}% < {price2_total_min:.4f}% "
                            f"(PRE {-c.pre_total_funding_spread_pct:.4f}% + PRICE {c.last_price2_price_component_pct:.4f}% * w {price2_w:.2f})"
                        )
                    elif (ok_total or ok_total_price2) and not price_ttl_ready:
                        reasons.append(f"PRICE TTL {price_held_sec:.1f}s < {self._price_ttl_sec:.1f}s")
                c.last_fail_reason = "; ".join(reasons[:4])

        self._maybe_log_candidates_diag()

    def _book_quality_required_top_notional(self) -> Optional[float]:
        return ArbMath.calc_required_top_notional(self._book_quality_cfg)

    def _book_quality_cfg_summary(self) -> str:
        cfg = getattr(self, "_book_quality_cfg", None)
        if not cfg:
            return "disabled"

        req_top = self._book_quality_required_top_notional()
        parts: list[str] = []
        if isinstance(getattr(cfg, "max_spread_pct", None), (int, float)):
            parts.append(f"maxSpread={float(cfg.max_spread_pct)}%")
        else:
            parts.append("maxSpread=disabled")

        if req_top is not None:
            if getattr(cfg, "target_pos_usd", None) and getattr(cfg, "min_top_coverage_x", None):
                parts.append(
                    f"top>=target({float(cfg.target_pos_usd):.4g})*x({float(cfg.min_top_coverage_x):.4g})={req_top:.4g}"
                )
            else:
                parts.append(f"top>={req_top:.4g}")
        else:
            parts.append("topFilter=disabled")

        if isinstance(getattr(cfg, "min_side_balance_ratio", None), (int, float)):
            parts.append(f"minBalance={float(cfg.min_side_balance_ratio)}")
        else:
            parts.append("minBalance=disabled")
        return ", ".join(parts)


    # def _maybe_log_candidates_diag(self) -> None:
    #     now_ms = int(time.time() * 1000)
    #     if now_ms < self._cand_diag_next_log_ms:
    #         return
    #     self._cand_diag_next_log_ms = now_ms + 15_000

    #     if not self._candidates:
    #         return

    #     state_counter = Counter(c.state for c in self._candidates.values())
    #     active = sum(1 for c in self._candidates.values() if not c.signaled)
    #     if active <= 0:
    #         return

    #     fail_counter = Counter(
    #         c.last_fail_reason for c in self._candidates.values()
    #         if (not c.signaled) and c.last_fail_reason
    #     )
    #     fail_hint = ""
    #     if fail_counter:
    #         top = "; ".join([f"{k} x{v}" for k, v in fail_counter.most_common(3)])
    #         fail_hint = f" | failTop: {top}"

    #     self.logger.info(
    #         f"[CAND] total={len(self._candidates)} active={active} "
    #         f"states={dict(state_counter)}{fail_hint}"
    #     )

    def _maybe_log_candidates_diag(self) -> None:
        self._cand_diag_next_log_ms = maybe_log_candidates_diag(
            logger=self.logger,
            candidates=self._candidates,
            last_log_ms=int(getattr(self, "_cand_diag_next_log_ms", 0) or 0),
            period_ms=15_000,
        )

    def _resolve_exchange_interval_h(self, ex_name: str, canon: str) -> Optional[int]:
        """Resolve *actual* funding interval (hours) for exchange+canonical symbol (display only).

        We rely on UniverseBuilder-produced mapping:
            universe.by_exchange_interval[EX][interval_h] -> {canon: raw_symbol}

        If interval can't be resolved confidently, return None (no guessing).
        """
        if not self.universe:
            return None

        ex_key = str(ex_name or "").upper().strip()
        by_ex = (self.universe.by_exchange_interval or {})
        by_int = by_ex.get(ex_key) or by_ex.get(str(ex_name or "").strip()) or {}
        if not isinstance(by_int, dict):
            return None

        canon_key = str(canon or "").strip()
        if not canon_key:
            return None

        for interval_h, cmap in by_int.items():
            try:
                if canon_key in (cmap or {}):
                    iv = int(interval_h)
                    if iv in {1, 4, 8}:
                        return iv
            except Exception:
                continue
        return None


    def _resolve_leg_interval_hours(self, ex_name: str, raw_symbol: str, canon: str) -> Optional[int]:
        """Resolve *actual* funding interval (hours) for a конкретной ноги (exchange+symbol).

        Important: this is for MESSAGE LABELING only. If we can't resolve confidently,
        return None rather than guessing (to avoid '8h magic').

        Sources:
          - universe.by_exchange_interval[EX][interval_h] contains {canon: raw_symbol} mappings
            produced by UniverseBuilder from adapters.
        """
        if not self.universe:
            return None

        ex_key = str(ex_name or "").upper().strip()
        by_ex = (self.universe.by_exchange_interval or {})
        by_int = by_ex.get(ex_key) or by_ex.get(str(ex_name or "").strip()) or {}
        if not isinstance(by_int, dict):
            return None

        def _iter_intervals():
            items = []
            for interval_h, cmap in (by_int or {}).items():
                try:
                    iv = int(interval_h)
                except Exception:
                    continue
                if iv not in {1, 4, 8}:
                    continue
                items.append((iv, cmap or {}))
            items.sort(key=lambda x: x[0])
            return items

        # 1) raw-symbol lookup (preferred for exact leg; avoids canon collisions across interval buckets)
        raw_key = str(raw_symbol or "").strip().upper()
        if raw_key:
            for iv, cmap in _iter_intervals():
                try:
                    vals = {str(v).strip().upper() for v in (cmap or {}).values() if v is not None}
                    if raw_key in vals:
                        return iv
                except Exception:
                    continue

        # 2) canonical fallback (best effort only)
        canon_key = str(canon or "").strip()
        if canon_key:
            for iv, cmap in _iter_intervals():
                try:
                    if canon_key in (cmap or {}):
                        return iv
                except Exception:
                    continue

        return None


    def _build_signal_message(
        self,
        *,
        c: Candidate,
        p_long: float,
        p_short: float,
        book_long: dict,
        book_short: dict,
        held_sec: float,
        total_min: float,
        price_w: float,
        signal_kind: str = "PRIMARY",
    ) -> str:
        ttf_sec = max(0.0, (c.next_funding_time_ms - int(time.time() * 1000)) / 1000.0) if c.next_funding_time_ms else 0.0
        next_sync_sec = float(self._funding_cfg.get("NEXT_FUNDING_SYNC_SEC", NEXT_FUNDING_SYNC_SEC) or NEXT_FUNDING_SYNC_SEC)

        msg_kwargs = dict(
            canon=c.canon,
            next_funding_time_ms=int(c.next_funding_time_ms or 0),
            fmt_dt_utc=self._fmt_dt_utc,
            ttf_min=ttf_sec / 60.0,
            cluster_exchanges=tuple(c.cluster_exchanges or ()),
            listed_exchanges=tuple(sorted([ex for ex, m in (self.universe.per_exchange_maps.items() if self.universe else []) if c.canon in m])),
            next_sync_sec=next_sync_sec,
            long_ex=c.long_ex,
            long_raw=c.long_raw,
            funding_long_pct=float(c.funding_long_pct),
            p_long=float(p_long),
            short_ex=c.short_ex,
            short_raw=c.short_raw,
            funding_short_pct=float(c.funding_short_pct),
            p_short=float(p_short),
            pre_spread_live_pct=float(c.pre_total_funding_spread_pct),
            pre_spread_min=float(self._pre_spread_min),
            price_enabled=bool(self._price_enabled),
            price_component_pct=float(c.last_price_component_pct),
            price_weight=float(price_w),
            total_spread_pct=float(c.last_total_spread_pct),
            total_spread_min=float(total_min),
            stakan_enabled=bool(self._stakan_enabled),
            book_long=book_long or {},
            book_short=book_short or {},
            price_ttl_sec=float(self._price_ttl_sec),
            price_ok_since_ms=int(c.price_ok_since_ms or 0),
            stakan_ttl_sec=float(self._stakan_ttl_sec),
            stakan_spread_thr=self._stakan_spread_thr,
            stakan_ok_since_ms=int(c.stakan_ok_since_ms or 0),
            held_sec=float(held_sec),
            book_quality_cfg_summary=self._book_quality_cfg_summary(),
            required_top_notional=self._book_quality_required_top_notional(),
            long_interval_h=self._resolve_leg_interval_hours(c.long_ex, c.long_raw, c.canon),
            short_interval_h=self._resolve_leg_interval_hours(c.short_ex, c.short_raw, c.canon),
            scan_gate_h=None,  # hide scheduler jargon in end-user signal text
        )
        # formatter may not yet support the new arg in older TG/messages.py builds
        try:
            msg = build_funding_signal_message(**msg_kwargs, signal_kind=str(signal_kind).upper())
        except TypeError:
            msg = build_funding_signal_message(**msg_kwargs)

        sk = str(signal_kind).upper()
        if sk == "PRICE2_REVERSE":
            msg = "🔁 PRICE2 reverse signal\n" + msg
        return msg

    def _scheduler_gate_summary_lines(self) -> list[str]:
        return scheduler_gate_summary_lines(scheduler=self._scheduler)

    def _log_scheduler_eta(self, *, force: bool = False) -> None:
        """Throttled scheduler ETA diagnostics.

        Default cadence is 60s to avoid production log spam. ``force=True`` logs immediately.
        """
        try:
            now_ms = int(time.time() * 1000)
            last_ms = 0 if force else int(getattr(self, "_sched_eta_last_log_ms", 0) or 0)
            self._sched_eta_last_log_ms = maybe_log_scheduler_eta(
                logger=self.logger,
                scheduler=self._scheduler,
                now_ms=now_ms,
                last_log_ms=last_ms,
                period_sec=60,
            )
        except Exception:
            pass

    # ---------------------------------------------------------
    # MAIN LOOP
    # ---------------------------------------------------------
    async def run_forever(self) -> None:
        try:
            # bootstrap funding caches (best-effort)
            for ex in self.exchanges:
                if hasattr(ex, "bootstrap"):
                    try:
                        await ex.bootstrap()
                    except Exception as e:
                        self.logger.warning(f"{getattr(ex, 'name', ex.__class__.__name__)} bootstrap failed: {e}")

            await self.refresh_universe()
            await self._maybe_refresh_rest_funding(force=True)

            try:
                enabled_ex = [getattr(ex, "name", ex.__class__.__name__) for ex in self.exchanges]
            except Exception:
                enabled_ex = []
            ex_line = ", ".join(enabled_ex) if enabled_ex else "n/a"
            mode_label = "test" if bool(self._test_force_scan_mode) else "prod"
            await self.notifier.send_text(
                f"✅ Скринер запущен\n"
                f"Quote: {self.quote} | eligible: {len(getattr(self.universe, 'canonical_eligible', set())) if self.universe else 0}\n"
                f"Режим: {mode_label}\n"
                f"Биржи: {ex_line}"
            )

            if not self._test_force_scan_mode:
                for line in self._scheduler_gate_summary_lines():
                    self.logger.info(line)
                self._log_scheduler_eta(force=True)

            if self._test_force_scan_mode:
                self.logger.warning(
                    f"[TEST MODE] scheduler bypass enabled, scan cadence={self._test_force_scan_every_sec}s (from scheduler repeat config)"
                )
                self._next_test_scan_ms = 0

            while True:
                fired = []
                now_ms = int(time.time() * 1000)
                now_dt = datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc)

                if self._test_force_scan_mode:
                    if now_ms >= self._next_test_scan_ms:
                        fired = [1, 4, 8]
                        self._next_test_scan_ms = now_ms + self._test_force_scan_every_sec * 1000
                else:
                    fired = self._scheduler.tick(now_dt)

                notif_fired: list[str] = []
                try:
                    notif_fired = self._notif_scheduler.tick(now_dt) if getattr(self, "_notif_scheduler", None) else []
                except Exception:
                    notif_fired = []

                if fired:
                    if not self._test_force_scan_mode:
                        try:
                            fired_diag = []
                            for _g in fired:
                                _gate = getattr(self._scheduler, "gates", {}).get(int(_g)) if hasattr(self, "_scheduler") else None
                                _kind = str(getattr(_gate, "last_fire_kind", "") or "?")
                                fired_diag.append(f"{int(_g)}h:{_kind}")
                            if fired_diag:
                                self.logger.info(f"[SCHED] fired: {', '.join(fired_diag)}")
                        except Exception:
                            pass

                    # Always rebuild symbol universe right before scans
                    try:
                        await self.refresh_universe()
                    except Exception as e:
                        self.logger.warning(f"[SYMBOLS] refresh failed: {e}")

                    for gate_h in fired:
                        try:
                            await self._scan_gate(gate_h)
                        except Exception as e:
                            self.logger.warning(f"[SCAN] gate={gate_h}h failed: {e}")

                # Notification schedulers (independent of scan gates)
                if notif_fired:
                    ctx_gates = [int(g) for g in fired if int(g) in (1, 4, 8)] if fired else None

                    if "phemex_top_funding" in notif_fired:
                        try:
                            await self._notify_phemex_top_funding(gates=ctx_gates)
                        except Exception as e:
                            self.logger.warning(f"[PHEMEX TOP] scheduler notify failed: {e}")

                    if "kucoin_top_funding" in notif_fired:
                        try:
                            await self._notify_kucoin_top_funding(gates=ctx_gates)
                        except Exception as e:
                            self.logger.warning(f"[KUCOIN TOP] scheduler notify failed: {e}")

                # candidates can become valid any time (TTL)
                try:
                    await self._evaluate_candidates()
                except Exception as e:
                    self.logger.warning(f"[CAND] evaluate failed: {e}")

                self._cleanup_candidates()
                
                # Periodic scheduler ETA diagnostics (throttled internally).
                # Keep this in the main loop so we can see countdowns even when no gates fire yet.
                self._log_scheduler_eta()

                await asyncio.sleep(min(self.poll_sec, 1.0))
        finally:
            await self._shutdown()
