from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass(frozen=True)
class GateEta:
    gate_h: int
    to_boundary_sec: int
    to_initial_fire_sec: int
    next_kind: str  # initial | repeat#N | overdue-now
    repeat_count_effective: int
    repeat_interval_sec: int
    seconds_before_boundary: int
    offset_sec: int


def _next_boundary_dt(now: datetime, interval_h: int) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    sec = int(now.timestamp())
    interval_sec = max(1, int(interval_h)) * 3600
    next_sec = ((sec // interval_sec) + 1) * interval_sec
    return datetime.fromtimestamp(next_sec, tz=timezone.utc)


def gate_eta(scheduler: Any, gate_h: int, now: Optional[datetime] = None) -> Optional[GateEta]:
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)

    gates = getattr(scheduler, "gates", {}) or {}
    g = gates.get(int(gate_h))
    if not g:
        return None

    boundary = _next_boundary_dt(now, int(gate_h))
    to_boundary_sec = max(0, int((boundary - now).total_seconds()))

    before = int(getattr(g, "seconds_before_boundary", 0) or 0)
    offset = int(getattr(g, "offset_sec", 0) or 0)
    rep_cnt = max(0, int(getattr(g, "repeat_count", 0) or 0))
    rep_int = max(0, int(getattr(g, "repeat_interval_sec", 0) or 0))

    initial_fire_dt = boundary - __import__('datetime').timedelta(seconds=max(0, before - offset))
    to_initial = int((initial_fire_dt - now).total_seconds())

    kind = "initial"
    next_eta = to_initial
    if to_initial <= 0 and rep_cnt > 0 and rep_int > 0:
        # Find the next repeat not yet passed.
        chosen = None
        for i in range(1, rep_cnt + 1):
            dt_i = initial_fire_dt + __import__('datetime').timedelta(seconds=i * rep_int)
            eta_i = int((dt_i - now).total_seconds())
            if eta_i > 0:
                chosen = (i, eta_i)
                break
        if chosen is not None:
            idx, eta = chosen
            kind = f"repeat#{idx}"
            next_eta = eta
        else:
            kind = "overdue-now"
            next_eta = 0

    return GateEta(
        gate_h=int(gate_h),
        to_boundary_sec=to_boundary_sec,
        to_initial_fire_sec=max(0, int(next_eta)),
        next_kind=kind,
        repeat_count_effective=rep_cnt,
        repeat_interval_sec=rep_int,
        seconds_before_boundary=before,
        offset_sec=offset,
    )


def build_scheduler_eta_log_line(scheduler: Any, now: Optional[datetime] = None) -> str:
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)

    parts = [f"utc={now.strftime('%H:%M:%S')}"]
    for h in (1, 4, 8):
        info = gate_eta(scheduler, h, now)
        if info is None:
            continue
        parts.append(
            f"{h}h:boundary={info.to_boundary_sec}s,next={info.to_initial_fire_sec}s({info.next_kind})"
        )
    return "[SCHED-ETA] " + " | ".join(parts)
