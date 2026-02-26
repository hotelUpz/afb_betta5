from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Tuple

from CORE.scheduler_diag import build_scheduler_eta_log_line


def build_gate_scan_ttf_window(*, scheduler, gate_h: int, poll_sec: float) -> Tuple[Optional[float], Optional[float]]:
    """Compute scan TTF window for a scheduler gate.

    The scanner is triggered by scheduler events (initial/repeat/late-catchup). The window must
    be aligned with the *actual* scheduler semantics, including a tolerance for runtime lag.

    Important behavior:
    - for initial/repeat events: keep a bounded window near the configured mark
      (with tolerance for async jitter).
    """
    gate = None
    try:
        gate = getattr(getattr(scheduler, "gates", None), "get", lambda _x: None)(int(gate_h))
    except Exception:
        gate = None
    if gate is None:
        return None, None

    try:
        before_sec = float(getattr(gate, "seconds_before_boundary", 0.0) or 0.0)
        offset_sec = float(getattr(gate, "offset_sec", 0.0) or 0.0)
        repeat_count = max(0, int(getattr(gate, "repeat_count", 0) or 0))
        repeat_every = max(0.0, float(getattr(gate, "repeat_interval_sec", 0.0) or 0.0))
        window_sec = max(0.0, float(getattr(gate, "window_sec", 0.0) or 0.0))
        late_grace_sec = max(0.0, float(getattr(gate, "late_grace_sec", 0.0) or 0.0))
        last_fire_kind = str(getattr(gate, "last_fire_kind", "") or "").strip().lower()
    except Exception:
        return None, None

    base_ttf_sec = max(0.0, before_sec - offset_sec)
    repeat_span_sec = max(0.0, float(repeat_count) * repeat_every)

    try:
        ps = float(poll_sec or 1.0)
    except Exception:
        ps = 1.0

    # Wider than before to absorb small async stalls. We still keep a meaningful upper bound.
    jitter_sec = max(30.0, ps * 8.0, window_sec * 2.0)

    lo = max(0.0, base_ttf_sec - repeat_span_sec - jitter_sec)
    hi = max(0.0, base_ttf_sec + jitter_sec)

    # on-time fires can still drift around the edge of the window due to loop jitter
    lo = max(0.0, lo - min(max(window_sec, ps * 5.0), 120.0))

    if hi < lo:
        lo, hi = hi, lo
    return lo, hi


def scheduler_gate_summary_lines(*, scheduler) -> list[str]:
    lines: list[str] = []
    try:
        for gate_h in (1, 4, 8):
            g = (getattr(scheduler, "gates", None) or {}).get(gate_h)
            if not g:
                continue
            before = int(getattr(g, "seconds_before_boundary", 0) or 0)
            offset = int(getattr(g, "offset_sec", 0) or 0)
            rep_cnt = int(getattr(g, "repeat_count", 0) or 0)
            rep_int = int(getattr(g, "repeat_interval_sec", 0) or 0)
            win = int(getattr(g, "window_sec", 0) or 0)
            late = int(getattr(g, "late_grace_sec", 0) or 0)

            when_sec = before - offset
            when_txt = f"{when_sec}s before" if when_sec >= 0 else f"{abs(when_sec)}s after"
            rep_txt = (f"repeat {rep_cnt}x/{rep_int}s" if rep_cnt > 0 and rep_int > 0 else "repeat off")
            lines.append(
                f"[SCHED] {gate_h}h: {when_txt} | window={win}s | lateGrace={late}s | {rep_txt}"
            )
    except Exception:
        return lines
    return lines


def maybe_log_scheduler_eta(*, logger, scheduler, now_ms: int, last_log_ms: int, period_sec: int = 60) -> int:
    """Rate-limited scheduler ETA log helper.

    Default period is intentionally 60s to avoid log spam in production. Returns updated
    ``last_log_ms`` (or original value if not logged).
    """
    period_ms = max(5, int(period_sec)) * 1000
    if int(now_ms) < int(last_log_ms) + period_ms:
        return int(last_log_ms)
    now_utc = datetime.fromtimestamp(int(now_ms) / 1000.0, tz=timezone.utc)
    line = build_scheduler_eta_log_line(scheduler, now_utc=now_utc)
    if line:
        logger.info(line)
        return int(now_ms)
    return int(last_log_ms)
