# ============================================================
# FILE: CORE/startup_checks.py
# ROLE: Startup config/runtime self-checks (best-effort validation)
# ============================================================

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from const import (
    ENABLED_EXCHANGES,
    EXCHANGES,
    DEDUP_AFTER_FUNDING_SEC,
    TG_TOKEN,
    TG_CHAT_ID,
    DEFAULT_POLL_SEC,
    ENGINE_FUNDING,
    ENGINE_PRICE,
    ENGINE_STAKAN,
    RUN_MODE,
    SCHED_REPEAT_COUNT,
    SCHED_REPEAT_INTERVAL_SEC,
    SCHED_REPEAT_1H_COUNT,
    SCHED_REPEAT_1H_INTERVAL_SEC,
    SCHED_REPEAT_4H_COUNT,
    SCHED_REPEAT_4H_INTERVAL_SEC,
    SCHED_REPEAT_8H_COUNT,
    SCHED_REPEAT_8H_INTERVAL_SEC,
    SCHED_MARKS_1H_SEC_BEFORE,
    SCHED_MARKS_1H_OFFSET_SEC,
    SCHED_MARKS_4H_SEC_BEFORE,
    SCHED_MARKS_4H_OFFSET_SEC,
    SCHED_MARKS_8H_SEC_BEFORE,
    SCHED_MARKS_8H_OFFSET_SEC,
    CONFIG_DEFAULTS_USED,
)


SUPPORTED_EXCHANGE_ADAPTERS = {"binance", "okx", "phemex", "kucoin", "bitget"}
KNOWN_EXCHANGE_KEYS = SUPPORTED_EXCHANGE_ADAPTERS | {"bybit"}

from CORE.scheduler import scheduler_mark_window_sec


def _effective_repeat_count(before_sec: int, offset_sec: int, repeat_count: int, repeat_interval_sec: int) -> int:
    rep = max(0, int(repeat_count))
    intr = int(repeat_interval_sec)
    if rep <= 0 or intr <= 0:
        return 0
    avail_sec = max(0, int(before_sec) - int(offset_sec))
    if avail_sec <= 0:
        return 0
    return min(rep, avail_sec // intr)


@dataclass
class StartupSelfCheckReport:
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    infos: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


class StartupSelfCheck:
    """Lightweight startup checks.

    Goals:
    - catch obvious config mistakes early (with readable logs),
    - create required runtime dirs,
    - avoid hard-crash from trivial env/config issues.

    Does NOT perform network I/O.
    """

    def __init__(self, logger):
        self.logger = logger

    def run(self) -> StartupSelfCheckReport:
        r = StartupSelfCheckReport()
        self._check_runtime_dirs(r)
        self._check_applied_defaults(r)
        self._check_tg(r)
        self._check_run_mode(r)
        self._check_exchanges(r)
        self._check_timers_and_thresholds(r)
        self._check_dedup(r)
        self._emit(r)
        return r

    def _check_runtime_dirs(self, r: StartupSelfCheckReport) -> None:
        for path in (Path("OUT"), Path("logs")):
            try:
                path.mkdir(parents=True, exist_ok=True)
                test = path / ".write_test.tmp"
                test.write_text("ok", encoding="utf-8")
                test.unlink(missing_ok=True)
                r.infos.append(f"runtime dir OK: {path.as_posix()}")
            except Exception as e:
                r.errors.append(f"runtime dir not writable: {path.as_posix()} ({e})")


    def _check_applied_defaults(self, r: StartupSelfCheckReport) -> None:
        if not CONFIG_DEFAULTS_USED:
            return
        for item in CONFIG_DEFAULTS_USED:
            r.infos.append(f"config default applied: {item}")

    def _check_tg(self, r: StartupSelfCheckReport) -> None:
        has_token = bool((TG_TOKEN or "").strip())
        has_chat = bool((TG_CHAT_ID or "").strip())
        if has_token and has_chat:
            r.infos.append("telegram notifier: enabled")
            return
        if has_token ^ has_chat:
            r.warnings.append("telegram notifier: partial config (TG_TOKEN/TG_CHAT_ID). TG will be disabled")
            return
        r.infos.append("telegram notifier: disabled (empty TG_TOKEN/TG_CHAT_ID)")

    def _check_run_mode(self, r: StartupSelfCheckReport) -> None:
        mode = str(RUN_MODE or "prod").strip().lower()
        if mode not in {"prod", "test_realtime"}:
            r.warnings.append(f"RUN_MODE={mode!r} is unknown; config fallback expected")
            return
        r.infos.append(f"RUN_MODE={mode}")


    def _check_exchanges(self, r: StartupSelfCheckReport) -> None:
        enabled = [str(x).strip().lower() for x in (ENABLED_EXCHANGES or []) if str(x).strip()]
        if not enabled:
            r.warnings.append("ENABLED_EXCHANGES is empty; bot will fallback to EXCHANGES[*].enable")
            enabled = [
                str(k).strip().lower()
                for k, v in (EXCHANGES or {}).items()
                if isinstance(v, dict) and bool(v.get("enable", False))
            ]

        if not enabled:
            r.errors.append("no exchanges enabled in ENABLED_EXCHANGES/EXCHANGES")
            return

        seen = set()
        dups = set()
        for name in enabled:
            if name in seen:
                dups.add(name)
            seen.add(name)
            if name not in KNOWN_EXCHANGE_KEYS:
                r.warnings.append(f"unknown exchange key in config: {name}")
            elif name not in SUPPORTED_EXCHANGE_ADAPTERS:
                r.warnings.append(f"adapter not implemented in this build: {name} (will be skipped)")

        if dups:
            r.warnings.append(f"duplicate names in ENABLED_EXCHANGES: {sorted(dups)}")

        # quote mismatch warning (non-fatal)
        quotes = set()
        for name in enabled:
            cfg = (EXCHANGES or {}).get(name)
            if isinstance(cfg, dict):
                q = str(cfg.get("quote") or "").upper().strip()
                if q:
                    quotes.add(q)
        if len(quotes) > 1:
            r.warnings.append(f"multiple quotes in EXCHANGES: {sorted(quotes)} (bot will use DEFAULT_QUOTE)")

    def _check_timers_and_thresholds(self, r: StartupSelfCheckReport) -> None:
        def _nonneg(name: str, value) -> None:
            try:
                v = float(value)
            except Exception:
                r.errors.append(f"{name} must be numeric, got {value!r}")
                return
            if v < 0:
                r.errors.append(f"{name} must be >= 0, got {v}")

        def _nonneg_or_none(name: str, value) -> None:
            if value is None:
                return
            try:
                v = float(value)
            except Exception:
                r.errors.append(f"{name} must be None or numeric, got {value!r}")
                return
            if v < 0:
                r.errors.append(f"{name} must be >= 0 or None, got {v}")

        _nonneg("DEFAULT_POLL_SEC", DEFAULT_POLL_SEC)

        fcfg = dict(ENGINE_FUNDING or {})
        pcfg = dict(ENGINE_PRICE or {})
        scfg = dict(ENGINE_STAKAN or {})

        for name, cfg, keys in (
            ("ENGINE_FUNDING", fcfg, ["MIN_SPREAD", "NEXT_FUNDING_SYNC_SEC", "REST_REFRESH_EVERY_SEC", "MAX_FUNDING_AGE_SEC"]),
            ("ENGINE_PRICE", pcfg, ["PRICE_SPREAD_WEIGHT", "TOTALY_SPREAD", "MAX_PRICE_AGE_SEC"]),
            ("ENGINE_STAKAN", scfg, ["STAKAN_SPREAD", "MAX_BOOK_AGE_SEC"]),
        ):
            if not isinstance(cfg, dict):
                r.errors.append(f"{name} must be dict")
                continue
            for key in keys:
                if key in cfg and cfg.get(key) is not None:
                    _nonneg(f"{name}.{key}", cfg.get(key))

        # TTL controls: None is valid and means "disabled / no confirmation TTL"
        if isinstance(fcfg, dict) and "FUNDING_TTL_SEC_CONTROL" in fcfg:
            _nonneg_or_none("ENGINE_FUNDING.FUNDING_TTL_SEC_CONTROL", fcfg.get("FUNDING_TTL_SEC_CONTROL"))
        if isinstance(pcfg, dict) and "PRICE_TTL_SEC_CONTROL" in pcfg:
            _nonneg_or_none("ENGINE_PRICE.PRICE_TTL_SEC_CONTROL", pcfg.get("PRICE_TTL_SEC_CONTROL"))
        if isinstance(scfg, dict) and "STAKAN_TTL_SEC_CONTROL" in scfg:
            _nonneg_or_none("ENGINE_STAKAN.STAKAN_TTL_SEC_CONTROL", scfg.get("STAKAN_TTL_SEC_CONTROL"))

        # scheduler marks (normalized in const; validate non-negative resolved values)
        for nm, val in (
            ("scheduler.marks_1h.val_sec", SCHED_MARKS_1H_SEC_BEFORE),
            ("scheduler.marks_1h.offset_sec", SCHED_MARKS_1H_OFFSET_SEC),
            ("scheduler.marks_4h.val_sec", SCHED_MARKS_4H_SEC_BEFORE),
            ("scheduler.marks_4h.offset_sec", SCHED_MARKS_4H_OFFSET_SEC),
            ("scheduler.marks_8h.val_sec", SCHED_MARKS_8H_SEC_BEFORE),
            ("scheduler.marks_8h.offset_sec", SCHED_MARKS_8H_OFFSET_SEC),
        ):
            _nonneg(nm, val)

        # scheduler.repeat validation (global + per-gate normalized repeats)
        for nm, cnt, intr in (
            ("scheduler.repeat", SCHED_REPEAT_COUNT, SCHED_REPEAT_INTERVAL_SEC),
            ("scheduler.marks_1h.repeat", SCHED_REPEAT_1H_COUNT, SCHED_REPEAT_1H_INTERVAL_SEC),
            ("scheduler.marks_4h.repeat", SCHED_REPEAT_4H_COUNT, SCHED_REPEAT_4H_INTERVAL_SEC),
            ("scheduler.marks_8h.repeat", SCHED_REPEAT_8H_COUNT, SCHED_REPEAT_8H_INTERVAL_SEC),
        ):
            if int(cnt) < 0:
                r.errors.append(f"{nm}.count must be >=0, got {cnt}")
            if int(intr) < 0:
                r.errors.append(f"{nm}.interval_sec must be >=0, got {intr}")

        # prod timing sanity (UTC scheduler tick cadence vs mark window)
        try:
            poll_sec = float(DEFAULT_POLL_SEC)
            win_sec = float(scheduler_mark_window_sec())
            if poll_sec > win_sec:
                r.warnings.append(
                    f"DEFAULT_POLL_SEC={poll_sec:g}s > scheduler_window_sec={win_sec:g}s: exact mark window can be missed in prod; "
                    f"strict scheduler skips missed windows, so set poll <= window"
                )
        except Exception:
            pass

        # scheduler mark geometry checks (resolved target should remain inside the same interval)
        for label, interval_h, before_sec, offset_sec, rep_cnt, rep_int in (
            ("1h", 1, SCHED_MARKS_1H_SEC_BEFORE, SCHED_MARKS_1H_OFFSET_SEC, SCHED_REPEAT_1H_COUNT, SCHED_REPEAT_1H_INTERVAL_SEC),
            ("4h", 4, SCHED_MARKS_4H_SEC_BEFORE, SCHED_MARKS_4H_OFFSET_SEC, SCHED_REPEAT_4H_COUNT, SCHED_REPEAT_4H_INTERVAL_SEC),
            ("8h", 8, SCHED_MARKS_8H_SEC_BEFORE, SCHED_MARKS_8H_OFFSET_SEC, SCHED_REPEAT_8H_COUNT, SCHED_REPEAT_8H_INTERVAL_SEC),
        ):
            interval_sec = int(interval_h) * 3600
            target_offset_from_boundary_start = interval_sec - int(before_sec) + int(offset_sec)
            if target_offset_from_boundary_start < 0 or target_offset_from_boundary_start >= interval_sec:
                r.warnings.append(
                    f"scheduler.marks_{label}: val_sec/offset_sec move trigger outside same {label} bucket "
                    f"(target shifts into adjacent boundary). Check val_sec={before_sec}, offset_sec={offset_sec}"
                )

            # If repeats don't fit before boundary, it's allowed, but user should know some repeats won't happen.
            if int(rep_cnt) > 0 and int(rep_int) > 0:
                latest_repeat_after_initial = int(rep_cnt) * int(rep_int)
                # available time from initial mark to boundary is approximately 'before_sec - offset_sec'
                avail_sec = int(before_sec) - int(offset_sec)
                if avail_sec > 0 and latest_repeat_after_initial > avail_sec:
                    eff = _effective_repeat_count(before_sec, offset_sec, rep_cnt, rep_int)
                    r.warnings.append(
                        f"scheduler.marks_{label}.repeat requested {int(rep_cnt)}x/{int(rep_int)}s, but only {eff} repeat(s) fit before boundary "
                        f"(need ~{latest_repeat_after_initial}s, available ~{avail_sec}s). Runtime scheduler will auto-trim repeats."
                    )
                if float(DEFAULT_POLL_SEC) > 0 and int(rep_int) > 0 and float(DEFAULT_POLL_SEC) > float(rep_int):
                    r.warnings.append(
                        f"scheduler.marks_{label}.repeat interval {int(rep_int)}s < main_loop_poll_sec {float(DEFAULT_POLL_SEC):g}s; "
                        f"effective repeat cadence will be limited by main loop"
                    )

    def _check_stakan_flags(self, r: StartupSelfCheckReport) -> None:
        r.infos.append("orderbook filter: removed from user config (legacy code path disabled)")

    def _check_dedup(self, r: StartupSelfCheckReport) -> None:
        if DEDUP_AFTER_FUNDING_SEC is None:
            r.infos.append("DEDUP_AFTER_FUNDING_SEC=None (forever dedup)")
            return
        try:
            v = float(DEDUP_AFTER_FUNDING_SEC)
        except Exception:
            r.errors.append(f"DEDUP_AFTER_FUNDING_SEC must be None or number, got {DEDUP_AFTER_FUNDING_SEC!r}")
            return
        if v < 0:
            r.errors.append(f"DEDUP_AFTER_FUNDING_SEC must be >= 0 or None, got {v}")

    def _emit(self, r: StartupSelfCheckReport) -> None:
        for msg in r.infos:
            self._safe_log("info", f"[SELF-CHECK] {msg}")
        for msg in r.warnings:
            self._safe_log("warning", f"[SELF-CHECK] {msg}")
        for msg in r.errors:
            self._safe_log("error", f"[SELF-CHECK] {msg}")

        if r.ok:
            self._safe_log("info", "[SELF-CHECK] OK")
        else:
            self._safe_log("error", f"[SELF-CHECK] FAILED: {len(r.errors)} error(s)")

    def _safe_log(self, level: str, text: str) -> None:
        fn = getattr(self.logger, level, None)
        if callable(fn):
            try:
                fn(text)
                return
            except Exception:
                pass
        try:
            print(text)
        except Exception:
            pass
