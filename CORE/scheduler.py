# ============================================================
# FILE: CORE/scheduler.py
# ROLE: Time gates for scheduled funding scans (UTC exact marks).
# ============================================================

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from const import (
    SCHED_MARKS_1H_SEC_BEFORE,
    SCHED_MARKS_1H_OFFSET_SEC,
    SCHED_MARKS_4H_SEC_BEFORE,
    SCHED_MARKS_4H_OFFSET_SEC,
    SCHED_MARKS_8H_SEC_BEFORE,
    SCHED_MARKS_8H_OFFSET_SEC,
    SCHED_REPEAT_1H_COUNT,
    SCHED_REPEAT_1H_INTERVAL_SEC,
    SCHED_REPEAT_4H_COUNT,
    SCHED_REPEAT_4H_INTERVAL_SEC,
    SCHED_REPEAT_8H_COUNT,
    SCHED_REPEAT_8H_INTERVAL_SEC,
    SCHED_WINDOW_SEC,
    SCHED_LATE_GRACE_SEC,
    # notifications (TOP funding info alerts)
    SCHED_PHEMEX_TOP_INTERVAL_HOURS,
    SCHED_PHEMEX_TOP_SEC_BEFORE,
    SCHED_PHEMEX_TOP_OFFSET_SEC,
    SCHED_REPEAT_PHEMEX_TOP_COUNT,
    SCHED_REPEAT_PHEMEX_TOP_INTERVAL_SEC,
    SCHED_KUCOIN_TOP_INTERVAL_HOURS,
    SCHED_KUCOIN_TOP_SEC_BEFORE,
    SCHED_KUCOIN_TOP_OFFSET_SEC,
    SCHED_REPEAT_KUCOIN_TOP_COUNT,
    SCHED_REPEAT_KUCOIN_TOP_INTERVAL_SEC,
    DEFAULT_POLL_SEC,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def scheduler_mark_window_sec() -> int:
    """Single scheduler window source (internal).

    Kept in scheduler layer (not config loader) to avoid another config-level source of truth.

    Practical note:
    a 10s window is too optimistic for a real async loop that may spend time in
    symbol refresh, REST funding refresh and candidate evaluation. We keep the
    window bounded, but intentionally more tolerant to avoid missing gate events
    due to short hiccups.
    """
    try:
        poll = float(DEFAULT_POLL_SEC)
    except Exception:
        poll = 1.0

    # Target roughly 20-30 scheduler ticks, but keep sane bounds.
    # Examples:
    #   poll=1s  -> 30s
    #   poll=0.5 -> 20s (floor)
    #   poll=2s  -> 60s
    return max(20, min(120, int(max(poll * 30.0, 20.0))))


def scheduler_late_grace_sec(window_sec: int) -> int:
    """How long after a missed event we still allow one catch-up fire.

    This is intentionally wider than ``window_sec`` because a single cycle can be
    delayed by network / REST refresh / universe rebuild. A catch-up is still
    safe: de-dup + per-boundary event IDs prevent duplicates.
    """
    try:
        poll = float(DEFAULT_POLL_SEC)
    except Exception:
        poll = 1.0
    base = max(int(window_sec) * 6, int(max(poll, 1.0) * 90.0), 180)
    return max(60, min(900, int(base)))


def _cap_repeat_count_for_gate(
    *,
    seconds_before_boundary: int,
    offset_sec: int,
    repeat_count: int,
    repeat_interval_sec: int,
) -> int:
    """Trim impossible repeat_count so repeats never spill beyond the boundary.

    We preserve cadence (repeat_interval_sec) and only reduce count. This makes prod
    behavior deterministic even when config is slightly optimistic.
    """
    cnt = max(0, int(repeat_count))
    intr = int(repeat_interval_sec)
    if cnt <= 0 or intr <= 0:
        return 0
    # available time from initial mark to funding boundary (supports negative offset = earlier trigger)
    available = max(0, int(seconds_before_boundary) - int(offset_sec))
    if available <= 0:
        return 0
    return min(cnt, available // intr)


def _next_boundary(now: datetime, interval_hours: int) -> datetime:
    if interval_hours <= 0:
        raise ValueError("interval_hours must be > 0")
    t = now.replace(minute=0, second=0, microsecond=0)
    step = int(interval_hours)
    h = t.hour
    if h % step == 0 and now.minute == 0 and now.second == 0:
        return t + timedelta(hours=step)
    add = (step - (h % step)) % step
    if add == 0:
        add = step
    return t + timedelta(hours=add)


@dataclass
class ScanGate:
    interval_hours: int
    seconds_before_boundary: int
    offset_sec: int = 0
    window_sec: int = 60
    # Allow small lateness for initial mark (e.g. loop jitter / short stall).
    # Semantics: we allow firing in [target, target + window + late_grace).
    late_grace_sec: int = 300

    # repeat = repeated CHECK cycles for the same funding boundary (NOT message re-send)
    repeat_count: int = 0
    repeat_interval_sec: int = 0

    # per-boundary state
    _active_boundary_key: int = 0
    _initial_sent: bool = False
    _repeat_done: int = 0
    _next_repeat_ts: float = 0.0
    _target_ts: float = 0.0
    _boundary_ts: float = 0.0

    # diagnostic (best-effort; consumed by caller for logs if needed)
    last_fire_kind: str = ""

    def _target(self, now: datetime) -> tuple[datetime, datetime]:
        nxt = _next_boundary(now, self.interval_hours)
        target = (
            nxt
            - timedelta(seconds=max(0, int(self.seconds_before_boundary)))
            + timedelta(seconds=int(self.offset_sec))
        )
        return nxt, target

    def _reset_boundary(self, key: int, *, target_ts: float = 0.0, boundary_ts: float = 0.0) -> None:
        self._active_boundary_key = int(key)
        self._initial_sent = False
        self._repeat_done = 0
        self._next_repeat_ts = 0.0
        self._target_ts = float(target_ts or 0.0)
        self._boundary_ts = float(boundary_ts or 0.0)
        self.last_fire_kind = ""

    def _seed_repeat_schedule_after_initial(self, now_ts: float) -> None:
        """Seed repeats from TARGET anchor (not from 'now') to avoid drift when initial fire is late.

        If process restarted / loop lagged and several repeat slots are already in the past,
        they are considered missed (no burst catch-up). We schedule the next *future* repeat only.
        """
        cnt = int(self.repeat_count)
        intr = int(self.repeat_interval_sec)
        if cnt <= 0 or intr <= 0:
            self._repeat_done = 0
            self._next_repeat_ts = 0.0
            return

        anchor = float(self._target_ts or 0.0)
        if anchor <= 0:
            # fallback (shouldn't happen): keep old semantics relative to actual fire time
            self._repeat_done = 0
            self._next_repeat_ts = now_ts + float(intr)
            return

        # Repeat slots are: anchor + intr, anchor + 2*intr, ... (count slots total)
        elapsed = max(0.0, float(now_ts) - anchor)
        slots_passed = int(elapsed // float(intr))
        # slots_passed includes the first repeat slot once we're >= anchor+intr
        self._repeat_done = max(0, min(cnt, slots_passed))

        if self._repeat_done >= cnt:
            self._next_repeat_ts = 0.0
            return

        next_slot_index = self._repeat_done + 1
        nxt = anchor + float(next_slot_index * intr)
        # guard against numerical edge cases
        if nxt <= now_ts:
            nxt = now_ts + float(intr)
        self._next_repeat_ts = nxt

    def should_fire(self, now: datetime) -> bool:
        self.last_fire_kind = ""
        nxt, target = self._target(now)
        key = int(nxt.timestamp())
        target_ts = float(target.timestamp())
        boundary_ts = float(nxt.timestamp())

        if key != self._active_boundary_key:
            self._reset_boundary(key, target_ts=target_ts, boundary_ts=boundary_ts)
        else:
            # Keep diagnostics/anchor in sync in case of any drift/re-computation
            self._target_ts = target_ts
            self._boundary_ts = boundary_ts

        window = timedelta(seconds=max(1, int(self.window_sec)))
        grace = timedelta(seconds=max(0, int(self.late_grace_sec)))
        now_ts = float(now.timestamp())

        # 1) initial mark fire (window + grace)
        #    Normal fire window: [target, target + window + grace).
        #
        #    IMPORTANT (2026-02-26):
        #    Do NOT allow a broad "catch-up anywhere before the boundary" fire.
        #    That behavior caused confusing retroactive scans after restarts
        #    (looked like scheduler desync / test-mode spam). If the process starts too late,
        #    it should wait for the next boundary cycle unless it is still within the
        #    explicit grace window configured here.
        if not self._initial_sent:
            if now < target:
                return False

            fire_kind = ""
            if now < (target + window + grace):
                fire_kind = "initial"
            else:
                return False

            self._initial_sent = True
            self.last_fire_kind = fire_kind
            self._seed_repeat_schedule_after_initial(now_ts)
            return True

        # 2) scheduled repeated checks for the SAME boundary (if configured)
        if self.repeat_count <= 0 or self.repeat_interval_sec <= 0:
            return False
        if self._repeat_done >= int(self.repeat_count):
            return False
        if now >= nxt:
            return False
        if now < target:
            return False
        if self._next_repeat_ts <= 0:
            return False
        if now_ts < self._next_repeat_ts:
            return False

        # Fire one repeat (no burst catch-up). Then advance to the next future slot.
        self._repeat_done += 1
        self.last_fire_kind = "repeat"

        if self._repeat_done < int(self.repeat_count):
            intr = float(self.repeat_interval_sec)
            # anchor-driven next slot to avoid drift when scans themselves are slow
            anchor = float(self._target_ts or 0.0)
            candidate = (
                anchor + float((self._repeat_done + 1) * int(self.repeat_interval_sec))
                if anchor > 0
                else (now_ts + intr)
            )
            if candidate <= now_ts:
                # loop lagged across one/many slots -> skip missed slots, schedule next future slot only
                slots_ahead = int((now_ts - (anchor if anchor > 0 else now_ts)) // intr) + 1 if intr > 0 else 1
                if anchor > 0:
                    candidate = anchor + float(slots_ahead * intr)
                else:
                    candidate = now_ts + intr
                # clamp repeat_done to avoid exceeding logical repeat count when many slots were skipped
                if anchor > 0 and intr > 0:
                    logical_done = max(
                        self._repeat_done,
                        min(int(self.repeat_count), int((now_ts - anchor) // intr)),
                    )
                    self._repeat_done = logical_done
            if self._repeat_done < int(self.repeat_count) and candidate < boundary_ts:
                self._next_repeat_ts = candidate
            else:
                self._next_repeat_ts = 0.0
        else:
            self._next_repeat_ts = 0.0
        return True


@dataclass
class FundingScanScheduler:
    gates: Dict[int, ScanGate] = field(default_factory=dict)

    @classmethod
    def default(cls) -> "FundingScanScheduler":
        # One source of truth for scheduler tolerance.
        # Kept configurable via cfg.json -> scheduler.window_sec / scheduler.late_grace_sec.
        window_sec = max(1, int(SCHED_WINDOW_SEC))
        late_grace_sec = max(0, int(SCHED_LATE_GRACE_SEC))

        r1 = _cap_repeat_count_for_gate(
            seconds_before_boundary=int(SCHED_MARKS_1H_SEC_BEFORE),
            offset_sec=int(SCHED_MARKS_1H_OFFSET_SEC),
            repeat_count=int(SCHED_REPEAT_1H_COUNT),
            repeat_interval_sec=int(SCHED_REPEAT_1H_INTERVAL_SEC),
        )
        r4 = _cap_repeat_count_for_gate(
            seconds_before_boundary=int(SCHED_MARKS_4H_SEC_BEFORE),
            offset_sec=int(SCHED_MARKS_4H_OFFSET_SEC),
            repeat_count=int(SCHED_REPEAT_4H_COUNT),
            repeat_interval_sec=int(SCHED_REPEAT_4H_INTERVAL_SEC),
        )
        r8 = _cap_repeat_count_for_gate(
            seconds_before_boundary=int(SCHED_MARKS_8H_SEC_BEFORE),
            offset_sec=int(SCHED_MARKS_8H_OFFSET_SEC),
            repeat_count=int(SCHED_REPEAT_8H_COUNT),
            repeat_interval_sec=int(SCHED_REPEAT_8H_INTERVAL_SEC),
        )

        return cls(
            gates={
                1: ScanGate(
                    interval_hours=1,
                    seconds_before_boundary=int(SCHED_MARKS_1H_SEC_BEFORE),
                    offset_sec=int(SCHED_MARKS_1H_OFFSET_SEC),
                    window_sec=window_sec,
                    late_grace_sec=late_grace_sec,
                    repeat_count=int(r1),
                    repeat_interval_sec=int(SCHED_REPEAT_1H_INTERVAL_SEC),
                ),
                4: ScanGate(
                    interval_hours=4,
                    seconds_before_boundary=int(SCHED_MARKS_4H_SEC_BEFORE),
                    offset_sec=int(SCHED_MARKS_4H_OFFSET_SEC),
                    window_sec=window_sec,
                    late_grace_sec=late_grace_sec,
                    repeat_count=int(r4),
                    repeat_interval_sec=int(SCHED_REPEAT_4H_INTERVAL_SEC),
                ),
                8: ScanGate(
                    interval_hours=8,
                    seconds_before_boundary=int(SCHED_MARKS_8H_SEC_BEFORE),
                    offset_sec=int(SCHED_MARKS_8H_OFFSET_SEC),
                    window_sec=window_sec,
                    late_grace_sec=late_grace_sec,
                    repeat_count=int(r8),
                    repeat_interval_sec=int(SCHED_REPEAT_8H_INTERVAL_SEC),
                ),
            }
        )

    def tick(self, now: datetime | None = None) -> List[int]:
        now = now or _utcnow()
        fired: List[int] = []
        for k, gate in sorted(self.gates.items()):
            if gate.should_fire(now):
                fired.append(k)
        return fired


@dataclass
class NotificationScheduler:
    """Separate schedulers for notification jobs (independent of scan gates)."""

    gates: Dict[str, ScanGate] = field(default_factory=dict)

    @classmethod
    def default(cls) -> "NotificationScheduler":
        # Keep same tolerance as FundingScanScheduler.
        window_sec = max(1, int(SCHED_WINDOW_SEC))
        late_grace_sec = max(0, int(SCHED_LATE_GRACE_SEC))

        gates: Dict[str, ScanGate] = {}

        # PHEMEX TOP funding
        try:
            ih = int(SCHED_PHEMEX_TOP_INTERVAL_HOURS)
        except Exception:
            ih = 1
        try:
            sec_before = int(SCHED_PHEMEX_TOP_SEC_BEFORE)
        except Exception:
            sec_before = 0
        try:
            offset = int(SCHED_PHEMEX_TOP_OFFSET_SEC)
        except Exception:
            offset = 0

        if ih > 0 and sec_before > 0:
            r = _cap_repeat_count_for_gate(
                seconds_before_boundary=sec_before,
                offset_sec=offset,
                repeat_count=int(SCHED_REPEAT_PHEMEX_TOP_COUNT),
                repeat_interval_sec=int(SCHED_REPEAT_PHEMEX_TOP_INTERVAL_SEC),
            )
            gates["phemex_top_funding"] = ScanGate(
                interval_hours=ih,
                seconds_before_boundary=sec_before,
                offset_sec=offset,
                window_sec=window_sec,
                late_grace_sec=late_grace_sec,
                repeat_count=int(r),
                repeat_interval_sec=int(SCHED_REPEAT_PHEMEX_TOP_INTERVAL_SEC),
            )

        # KUCOIN TOP funding
        try:
            ih = int(SCHED_KUCOIN_TOP_INTERVAL_HOURS)
        except Exception:
            ih = 1
        try:
            sec_before = int(SCHED_KUCOIN_TOP_SEC_BEFORE)
        except Exception:
            sec_before = 0
        try:
            offset = int(SCHED_KUCOIN_TOP_OFFSET_SEC)
        except Exception:
            offset = 0

        if ih > 0 and sec_before > 0:
            r = _cap_repeat_count_for_gate(
                seconds_before_boundary=sec_before,
                offset_sec=offset,
                repeat_count=int(SCHED_REPEAT_KUCOIN_TOP_COUNT),
                repeat_interval_sec=int(SCHED_REPEAT_KUCOIN_TOP_INTERVAL_SEC),
            )
            gates["kucoin_top_funding"] = ScanGate(
                interval_hours=ih,
                seconds_before_boundary=sec_before,
                offset_sec=offset,
                window_sec=window_sec,
                late_grace_sec=late_grace_sec,
                repeat_count=int(r),
                repeat_interval_sec=int(SCHED_REPEAT_KUCOIN_TOP_INTERVAL_SEC),
            )

        return cls(gates=gates)

    def tick(self, now: datetime | None = None) -> List[str]:
        now = now or _utcnow()
        fired: List[str] = []
        for k in sorted(self.gates.keys()):
            gate = self.gates[k]
            if gate.should_fire(now):
                fired.append(k)
        return fired
