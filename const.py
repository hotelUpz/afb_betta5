from __future__ import annotations

"""Unified configuration loader (single source of truth = cfg.json).

This module intentionally stays thin:
- reads cfg.json
- normalizes types / aliases
- exposes runtime constants used by the codebase

Notes:
- Backward compatibility aliases are kept only where current code still imports them.
- Hidden logic-shaping duplicates (legacy TTF buckets, scheduler mark strings, hard candidate cap,
  legacy orderbook metrics) were removed to avoid scheduler/filter desync.
"""

import json
from decimal import getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional

_CFG_PATH = Path(__file__).resolve().parent / "cfg.json"


# Exposed for startup diagnostics: which defaults were applied because cfg.json omitted a value.
CONFIG_DEFAULTS_USED: list[str] = []


def _note_default(name: str, value: Any) -> None:
    try:
        CONFIG_DEFAULTS_USED.append(f"{name}={value!r}")
    except Exception:
        pass


def _load_cfg() -> Dict[str, Any]:
    if not _CFG_PATH.exists():
        raise FileNotFoundError(f"cfg.json not found: {_CFG_PATH}")
    with _CFG_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise RuntimeError("cfg.json root must be object")
    return data


_CFG: Dict[str, Any] = _load_cfg()


def _get(path: str, default: Any = None) -> Any:
    cur: Any = _CFG
    for part in path.split('.'):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(part)
        if cur is None:
            return default
    return cur


def _first(*values: Any) -> Any:
    for v in values:
        if v is not None:
            return v
    return None


_MISSING = object()

def _get_key_allow_none(d: dict, key: str, default=_MISSING):
    """Return d[key] if key exists (even if None), else default."""
    if isinstance(d, dict) and key in d:
        return d.get(key)
    return default


def _to_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        x = v.strip().lower()
        if x in {"1", "true", "yes", "y", "on"}:
            return True
        if x in {"0", "false", "no", "n", "off"}:
            return False
    return bool(default)


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _norm_none_num(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, str) and v.strip().lower() in {"", "none", "null"}:
        return None
    try:
        return float(v)
    except Exception:
        return None



def _norm_none_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, str) and v.strip().lower() in {"", "none", "null"}:
        return None
    try:
        return int(v)
    except Exception:
        return None



def _setdefault_with_note(dct: Dict[str, Any], key: str, value: Any, note_name: str) -> None:
    if key not in dct:
        dct[key] = value
        _note_default(note_name, value)


# ============================================================
# TELEGRAM
# ============================================================
TG_TOKEN = str(_first(_get("telegram.token"), "") or "").strip()
TG_CHAT_ID = str(_first(_get("telegram.chat_id"), "") or "").strip()

# Telegram rate-limit guard (anti-429 flood wait)
# These are intentionally under telegram.* (user-facing, safe defaults).
TG_MIN_SEND_INTERVAL_SEC = _to_float(_first(_get("telegram.min_send_interval_sec"), 1.0), 1.0)
TG_RETRY_MAX_ATTEMPTS = _to_int(_first(_get("telegram.retry_max_attempts"), 3), 3)
TG_RETRY_BACKOFF_PAD_SEC = _to_float(_first(_get("telegram.retry_backoff_pad_sec"), 0.5), 0.5)


# ============================================================
# CORE RUNTIME
# ============================================================
DEBUG = _to_bool(_first(_get("runtime.debug"), True), True)
DEFAULT_QUOTE = str(_first(_get("runtime.quote"), _get("runtime.default_quote"), "USDT") or "USDT").strip().upper() or "USDT"
MAIN_LOOP_POLL_SEC = _to_float(_first(_get("runtime.main_loop_poll_sec"), 2), 2.0)
SCAN_LOOP = MAIN_LOOP_POLL_SEC
DEFAULT_POLL_SEC = MAIN_LOOP_POLL_SEC

RUNTIME_TEST_MODE = _to_bool(_first(_get("runtime.test_mode"), None), False)
_RUN_MODE_RAW = str(_first(_get("runtime.run_mode"), "test_realtime" if RUNTIME_TEST_MODE else "prod") or "prod").strip().lower()
RUN_MODE = _RUN_MODE_RAW if _RUN_MODE_RAW in {"prod", "test_realtime"} else ("test_realtime" if RUNTIME_TEST_MODE else "prod")

# scheduler.repeat supports BOTH formats:
#   - global: [count, interval_sec] or {"count": ..., "interval_sec": ...}   (compat)
#   - per-gate (preferred): scheduler.marks_{1h|4h|8h}.repeat = [count, interval_sec]
def _norm_repeat_pair(raw, *, default_count=0, default_interval=300):
    cnt_raw = None
    int_raw = None
    if isinstance(raw, (list, tuple)):
        if len(raw) >= 1:
            cnt_raw = raw[0]
        if len(raw) >= 2:
            int_raw = raw[1]
    elif isinstance(raw, dict):
        cnt_raw = raw.get("count")
        int_raw = raw.get("interval_sec")
    cnt = _to_int(_first(cnt_raw, default_count), default_count)
    intr = _to_int(_first(int_raw, default_interval), default_interval)
    if cnt <= 0 or intr <= 0:
        return 0, 0
    return cnt, intr


_sched_repeat_raw = _get("scheduler.repeat", None)
SCHED_REPEAT_COUNT, SCHED_REPEAT_INTERVAL_SEC = _norm_repeat_pair(
    _sched_repeat_raw,
    default_count=0,
    default_interval=_to_int(_first(_get("runtime.test_forced_scan_every_sec"), 300), 300),
)

TEST_ENABLE_FORCED_SCAN_SCHEDULER_BYPASS = bool(RUNTIME_TEST_MODE or RUN_MODE == "test_realtime")
TEST_FORCE_SCAN_MODE = TEST_ENABLE_FORCED_SCAN_SCHEDULER_BYPASS
# No hidden derivation from scheduler.repeat: user/runtime value remains source of truth in test mode.
_TEST_FORCE_SCAN_CFG = _get("runtime.test_forced_scan_every_sec", None)
if _TEST_FORCE_SCAN_CFG is None:
    _note_default("runtime.test_forced_scan_every_sec", 300)
TEST_FORCED_SCAN_EVERY_SEC = _to_int(_first(_TEST_FORCE_SCAN_CFG, 300), 300)
TEST_FORCE_SCAN_EVERY_SEC = TEST_FORCED_SCAN_EVERY_SEC


# ============================================================
# EXCHANGES
# ============================================================
ENABLED_EXCHANGES: List[str] = [
    str(x).strip().lower()
    for x in (_get("exchanges.enabled", []) or [])
    if str(x).strip()
]

_items_cfg = _get("exchanges.items", None)
if isinstance(_items_cfg, dict):
    EXCHANGES: Dict[str, Dict[str, Any]] = {
        str(k).upper() if str(k).isupper() else str(k).lower(): (dict(v) if isinstance(v, dict) else {})
        for k, v in _items_cfg.items()
    }
else:
    # synthesize defaults from enabled list (enabled list is the source of truth)
    EXCHANGES = {name: {"enable": True, "quote": DEFAULT_QUOTE, "max_symbols": 0} for name in ENABLED_EXCHANGES}

OKX_TEST_MAX_SYMBOLS = _to_int(_first(_get("exchanges.okx_test_max_symbols"), 0), 0)


# ============================================================
# ENGINE
# ============================================================
ENGINE_FUNDING: Dict[str, Any] = dict(_get("engine.funding", {}) or {})
ENGINE_PRICE: Dict[str, Any] = dict(_get("engine.price", {}) or {})
ENGINE_PRICE2: Dict[str, Any] = dict(_get("engine.price2", {}) or {})
# keep disabled compatibility dict (user-facing orderbook/stakan config removed)
ENGINE_STAKAN: Dict[str, Any] = dict(_get("engine.stakan", {}) or {})
ENGINE_STAKAN.setdefault("enable", False)

_funding_aliases = {
    "min_spread": "MIN_SPREAD",
    "ttl_sec_control": "FUNDING_TTL_SEC_CONTROL",
    "funding_ttl_sec_control": "FUNDING_TTL_SEC_CONTROL",
    "source_mode": "FUNDING_SOURCE_MODE",
    "funding_source_mode": "FUNDING_SOURCE_MODE",
    # legacy user-facing transport/freshness keys (compat only)
    "next_funding_sync_sec": "NEXT_FUNDING_SYNC_SEC",
    "rest_refresh_every_sec": "REST_REFRESH_EVERY_SEC",
    "max_funding_age_sec": "MAX_FUNDING_AGE_SEC",
}
for k_src, k_dst in _funding_aliases.items():
    if k_dst not in ENGINE_FUNDING and k_src in ENGINE_FUNDING:
        ENGINE_FUNDING[k_dst] = ENGINE_FUNDING[k_src]

_price_aliases = {
    "enabled": "enable",
    "price_spread_weight": "PRICE_SPREAD_WEIGHT",
    "total_spread": "TOTALY_SPREAD",
    "ttl_sec_control": "PRICE_TTL_SEC_CONTROL",
    "price_ttl_sec_control": "PRICE_TTL_SEC_CONTROL",
    "max_price_age_sec": "MAX_PRICE_AGE_SEC",
}
for k_src, k_dst in _price_aliases.items():
    if k_dst not in ENGINE_PRICE and k_src in ENGINE_PRICE:
        ENGINE_PRICE[k_dst] = ENGINE_PRICE[k_src]
for k_src, k_dst in _price_aliases.items():
    if k_dst not in ENGINE_PRICE2 and k_src in ENGINE_PRICE2:
        ENGINE_PRICE2[k_dst] = ENGINE_PRICE2[k_src]
# price2 supports TOTAL_SPREAD canonical key too
if "TOTAL_SPREAD" not in ENGINE_PRICE2 and "total_spread" in ENGINE_PRICE2:
    ENGINE_PRICE2["TOTAL_SPREAD"] = ENGINE_PRICE2["total_spread"]

_fsm = str(_first(ENGINE_FUNDING.get("FUNDING_SOURCE_MODE"), ENGINE_FUNDING.get("funding_source_mode"), "hybrid") or "hybrid").strip().lower()
if _fsm not in {"ws", "rest", "hybrid"}:
    _fsm = "hybrid"
ENGINE_FUNDING["FUNDING_SOURCE_MODE"] = _fsm

# Hidden transport/freshness values remain runtime-only (not user-facing), but we track defaults explicitly.
_setdefault_with_note(ENGINE_FUNDING, "MAX_FUNDING_AGE_SEC", 120, "engine.funding.MAX_FUNDING_AGE_SEC")
_setdefault_with_note(ENGINE_FUNDING, "NEXT_FUNDING_SYNC_SEC", 300, "engine.funding.NEXT_FUNDING_SYNC_SEC")
_setdefault_with_note(ENGINE_FUNDING, "REST_REFRESH_EVERY_SEC", 30, "engine.funding.REST_REFRESH_EVERY_SEC")
_setdefault_with_note(ENGINE_FUNDING, "OKX_WARMUP_TTL_SEC", 30, "engine.funding.OKX_WARMUP_TTL_SEC")
_setdefault_with_note(ENGINE_FUNDING, "enabled", True, "engine.funding.enabled")
_setdefault_with_note(ENGINE_PRICE, "MAX_PRICE_AGE_SEC", 2.5, "engine.price.MAX_PRICE_AGE_SEC")
_setdefault_with_note(ENGINE_PRICE, "enable", True, "engine.price.enabled")
ENGINE_PRICE.setdefault("enabled", bool(ENGINE_PRICE.get("enable", True)))
ENGINE_PRICE2.setdefault("enabled", bool(ENGINE_PRICE2.get("enable", ENGINE_PRICE2.get("enabled", True))))

# Backward-compatible flat aliases used by current code
FUNDING_SPREAD = _to_float(_first(ENGINE_FUNDING.get("MIN_SPREAD"), ENGINE_FUNDING.get("min_spread"), 0.10), 0.10)
FUNDING_SPREAD_MIN = FUNDING_SPREAD
TOTAL_FUNDING_SPREAD_MIN = _to_float(_first(ENGINE_PRICE.get("TOTALY_SPREAD"), ENGINE_PRICE.get("total_spread"), 0.05), 0.05)
PRICE_SPREAD_WEIGHT = _to_float(_first(ENGINE_PRICE.get("PRICE_SPREAD_WEIGHT"), ENGINE_PRICE.get("price_spread_weight"), 1.0), 1.0)
NEXT_FUNDING_SYNC_SEC = _to_int(_first(ENGINE_FUNDING.get("NEXT_FUNDING_SYNC_SEC"), 300), 300)
REST_FUNDING_REFRESH_EVERY_SEC = _to_int(_first(ENGINE_FUNDING.get("REST_REFRESH_EVERY_SEC"), 30), 30)
MAX_FUNDING_AGE_SEC = _to_float(_first(ENGINE_FUNDING.get("MAX_FUNDING_AGE_SEC"), 120), 120.0)
OKX_WARMUP_TTL_SEC = _to_float(_first(ENGINE_FUNDING.get("OKX_WARMUP_TTL_SEC"), 30), 30.0)
FUNDING_SOURCE_MODE = str(ENGINE_FUNDING.get("FUNDING_SOURCE_MODE", "hybrid"))
MAX_PRICE_AGE_SEC = _to_float(_first(ENGINE_PRICE.get("MAX_PRICE_AGE_SEC"), 2.5), 2.5)

# Legacy orderbook aliases are intentionally non-operational (pipeline disabled)
MAX_BOOK_AGE_SEC = 0.0
STAKAN_SPREAD = None
STAKAN_SPREAD_TTL = 0.0


# ============================================================
# DEDUP / CANDIDATES
# ============================================================
CANDIDATE_EXPIRE_SEC = _to_int(_first(_get("candidates.expire_sec"), 15 * 60), 15 * 60)
_dedup_after = _first(_get("dedup.signal_after_funding_sec"), _get("dedup.after_funding_sec"), None)
DEDUP_AFTER_FUNDING_SEC: Optional[int] = None if _dedup_after is None else _to_int(_dedup_after, 0)
DEDUP_STATE_FILE = "OUT/dedup_state.json"


# ============================================================
# SCHEDULER MARKS / REPEATS
# ============================================================
_m1 = dict(_get("scheduler.marks_1h", {}) or {})
_m4 = dict(_get("scheduler.marks_4h", {}) or {})
_m8 = dict(_get("scheduler.marks_8h", {}) or {})

# Core scheduler tolerances (UTC gates):
#   - window_sec: how wide the on-time fire window is
#   - late_grace_sec: additional tolerance after window
# NOTE: This is NOT the same as gate.repeat. These values only affect
# the *moment* when a gate can fire, not the scan cadence once fired.
SCHED_WINDOW_SEC = _to_int(_first(_get("scheduler.window_sec"), 180), 180)
SCHED_LATE_GRACE_SEC = _to_int(_first(_get("scheduler.late_grace_sec"), 180), 180)

SCHED_MARKS_1H_SEC_BEFORE = _to_int(_first(_m1.get("val_sec"), _get("scheduler.marks_1h_sec"), None), 35 * 60)
SCHED_MARKS_1H_OFFSET_SEC = _to_int(_first(_m1.get("offset_sec"), _get("scheduler.marks_1h_offset_sec"), 0), 0)
SCHED_MARKS_4H_SEC_BEFORE = _to_int(_first(_m4.get("val_sec"), _get("scheduler.marks_4h_sec"), None), 60 * 60)
SCHED_MARKS_4H_OFFSET_SEC = _to_int(_first(_m4.get("offset_sec"), _get("scheduler.marks_4h_offset_sec"), 0), 0)
SCHED_MARKS_8H_SEC_BEFORE = _to_int(_first(_m8.get("val_sec"), _get("scheduler.marks_8h_sec"), None), 90 * 60)
SCHED_MARKS_8H_OFFSET_SEC = _to_int(_first(_m8.get("offset_sec"), _get("scheduler.marks_8h_offset_sec"), 0), 0)

SCHED_REPEAT_1H_COUNT, SCHED_REPEAT_1H_INTERVAL_SEC = _norm_repeat_pair(
    _first(_m1.get("repeat"), _sched_repeat_raw, [0, 300]),
    default_count=SCHED_REPEAT_COUNT,
    default_interval=max(1, int(_first(SCHED_REPEAT_INTERVAL_SEC, 300))),
)
SCHED_REPEAT_4H_COUNT, SCHED_REPEAT_4H_INTERVAL_SEC = _norm_repeat_pair(
    _first(_m4.get("repeat"), _sched_repeat_raw, [0, 300]),
    default_count=SCHED_REPEAT_COUNT,
    default_interval=max(1, int(_first(SCHED_REPEAT_INTERVAL_SEC, 300))),
)
SCHED_REPEAT_8H_COUNT, SCHED_REPEAT_8H_INTERVAL_SEC = _norm_repeat_pair(
    _first(_m8.get("repeat"), _sched_repeat_raw, [0, 300]),
    default_count=SCHED_REPEAT_COUNT,
    default_interval=max(1, int(_first(SCHED_REPEAT_INTERVAL_SEC, 300))),
)

# ============================================================
# Scheduler marks for notifications (TOP funding info alerts)
# NOTE: separate schedulers (independent of scan gates).
# You can optionally set "interval_hours" inside scheduler.<name> section; default=1.
# ============================================================
_m_phemex_top = dict(_get("scheduler.phemex_top_funding", {}) or {})
_m_kucoin_top = dict(_get("scheduler.kucoin_top_funding", {}) or {})

SCHED_PHEMEX_TOP_INTERVAL_HOURS = _to_int(_first(_m_phemex_top.get("interval_hours"), _m_phemex_top.get("interval_h"), 1), 1)
SCHED_PHEMEX_TOP_SEC_BEFORE = _to_int(_first(_m_phemex_top.get("val_sec"), 1800), 1800)
SCHED_PHEMEX_TOP_OFFSET_SEC = _to_int(_first(_m_phemex_top.get("offset_sec"), 0), 0)
SCHED_REPEAT_PHEMEX_TOP_COUNT, SCHED_REPEAT_PHEMEX_TOP_INTERVAL_SEC = _norm_repeat_pair(
    _first(_m_phemex_top.get("repeat"), [0, 300]),
    default_count=0,
    default_interval=300,
)

SCHED_KUCOIN_TOP_INTERVAL_HOURS = _to_int(_first(_m_kucoin_top.get("interval_hours"), _m_kucoin_top.get("interval_h"), 1), 1)
SCHED_KUCOIN_TOP_SEC_BEFORE = _to_int(_first(_m_kucoin_top.get("val_sec"), 1800), 1800)
SCHED_KUCOIN_TOP_OFFSET_SEC = _to_int(_first(_m_kucoin_top.get("offset_sec"), 0), 0)
SCHED_REPEAT_KUCOIN_TOP_COUNT, SCHED_REPEAT_KUCOIN_TOP_INTERVAL_SEC = _norm_repeat_pair(
    _first(_m_kucoin_top.get("repeat"), [0, 300]),
    default_count=0,
    default_interval=300,
)


# ============================================================
# LOGGING / DISPLAY / TIME
# ============================================================
LOG_DEBUG = _to_bool(_first(_get("logging.debug"), _get("logging.LOG_DEBUG"), False), False)
LOG_INFO = _to_bool(_first(_get("logging.info"), _get("logging.LOG_INFO"), True), True)
LOG_WARNING = _to_bool(_first(_get("logging.warning"), _get("logging.LOG_WARNING"), True), True)
LOG_ERROR = _to_bool(_first(_get("logging.error"), _get("logging.LOG_ERROR"), True), True)
MAX_LOG_LINES = _to_int(_first(_get("logging.max_log_lines"), _get("logging.MAX_LOG_LINES"), 2000), 2000)
TIME_ZONE = str(_first(_get("logging.time_zone"), _get("logging.TIME_ZONE"), "UTC") or "UTC")
LOG_HTTP = _to_bool(_first(_get("logging.http"), _get("logging.LOG_HTTP"), False), False)
LOG_HTTP_BODIES = _to_bool(_first(_get("logging.http_bodies"), _get("logging.LOG_HTTP_BODIES"), False), False)

PRECISION: Dict[str, Any] = dict(_get("precision", {"funding_rate": 6, "spread_pct": 6, "default": 6}) or {})
DECIMAL_CONTEXT_PREC = _to_int(_first(_get("precision.decimal_context_prec"), 28), 28)
getcontext().prec = int(DECIMAL_CONTEXT_PREC)


# ============================================================
# Notifications (TOP funding info alerts)
# ============================================================
_phemex_top = dict(_get("notifications.phemex_top_funding", {}) or {})

PHEMEX_TOP_FUNDING_ENABLE = _to_bool(
    _first(_get_key_allow_none(_phemex_top, "enabled", _MISSING), _get("notifications.PHEMEX_TOP_FUNDING_ENABLE"), True),
    True,
)

PHEMEX_TOP_FUNDING_MIN_FUNDING = _norm_none_num(
    _first(_get_key_allow_none(_phemex_top, "min_funding", _MISSING), _get("notifications.PHEMEX_TOP_FUNDING_MIN_FUNDING"), None)
)

# IMPORTANT: if cfg has "slice": null -> keep None (no slicing)
_raw_slice = _get_key_allow_none(_phemex_top, "slice", _MISSING)
if _raw_slice is _MISSING:
    _raw_slice = _first(_get("notifications.PHEMEX_TOP_FUNDING_SLICE"), 20)
PHEMEX_TOP_FUNDING_SLICE = _norm_none_int(_raw_slice)

_kucoin_top = dict(_get("notifications.kucoin_top_funding", {}) or {})

KUCOIN_TOP_FUNDING_ENABLE = _to_bool(
    _first(_get_key_allow_none(_kucoin_top, "enabled", _MISSING), _get("notifications.KUCOIN_TOP_FUNDING_ENABLE"), True),
    True,
)

KUCOIN_TOP_FUNDING_MIN_FUNDING = _norm_none_num(
    _first(_get_key_allow_none(_kucoin_top, "min_funding", _MISSING), _get("notifications.KUCOIN_TOP_FUNDING_MIN_FUNDING"), None)
)

_raw_slice = _get_key_allow_none(_kucoin_top, "slice", _MISSING)
if _raw_slice is _MISSING:
    _raw_slice = _first(_get("notifications.KUCOIN_TOP_FUNDING_SLICE"), 20)
KUCOIN_TOP_FUNDING_SLICE = _norm_none_int(_raw_slice)
