# ============================================================
# FILE: TG/messages.py
# ROLE: Telegram/text message builders (formatting only)
# ============================================================

from __future__ import annotations

from typing import Callable, Optional


def build_funding_signal_message(
    *,
    canon: str,
    next_funding_time_ms: int,
    fmt_dt_utc: Callable[[int], str],
    ttf_min: float,
    cluster_exchanges: tuple[str, ...] | list[str],
    next_sync_sec: float,
    long_ex: str,
    long_raw: str,
    funding_long_pct: float,
    p_long: float,
    short_ex: str,
    short_raw: str,
    funding_short_pct: float,
    p_short: float,
    pre_spread_live_pct: float,
    pre_spread_min: float,
    price_enabled: bool,
    price_component_pct: float,
    price_weight: float,
    total_spread_pct: float,
    total_spread_min: float,
    stakan_enabled: bool,
    book_long: Optional[dict],
    book_short: Optional[dict],
    price_ttl_sec: float,
    price_ok_since_ms: int,
    stakan_ttl_sec: float,
    stakan_spread_thr: Optional[float],
    stakan_ok_since_ms: int,
    held_sec: float,
    book_quality_cfg_summary: str,
    required_top_notional: Optional[float],
    listed_exchanges: tuple[str, ...] | list[str] | None = None,
    long_interval_h: int | None = None,
    short_interval_h: int | None = None,
    scan_gate_h: int | None = None,
) -> str:
    # NOTE: Many params remain for compatibility/future extensions; message is intentionally compact.
    lines: list[str] = []
    lines.append("✅ FUNDING ARB SIGNAL")
    lines.append("")
    lines.append(f"SYMBOL: {canon}")
    lines.append("")
    lines.append(f"Next funding: {fmt_dt_utc(next_funding_time_ms)} | TTF={ttf_min:.1f}m")
    # Trader-facing output: show concrete per-leg funding timeframes only.
    # Avoid scheduler/gate jargon in the message body.
    if long_interval_h and long_interval_h > 0:
        lines.append(f"{long_ex} — {int(long_interval_h)}h")
    if short_interval_h and short_interval_h > 0 and short_ex != long_ex:
        lines.append(f"{short_ex} — {int(short_interval_h)}h")

    lines.append("")
    lines.append(f"🟢 LONG:  {long_ex} | funding={funding_long_pct:+.4f}% | price={p_long:.10g}")
    lines.append(f"🔴 SHORT: {short_ex} | funding={funding_short_pct:+.4f}% | price={p_short:.10g}")
    lines.append("")
    lines.append("СПРЕДЫ:")
    lines.append(f"• PRE funding: {pre_spread_live_pct:.4f}% (min {pre_spread_min:.4f}%)")
    if price_enabled:
        lines.append(f"• PRICE (short-long): {price_component_pct:+.4f}% × w={price_weight:.2f}")
        lines.append(f"• TOTAL: {total_spread_pct:.4f}% (min {total_spread_min:.4f}%)")
        lines.append(f"• Formula: PRE + PRICE×{price_weight:.2f}")
    else:
        lines.append("• PRICE: module disabled")
        lines.append(f"• TOTAL: {total_spread_pct:.4f}% (fallback=PRE)")
        lines.append("• Formula: PRE")

    # cluster = ", ".join([str(x) for x in cluster_exchanges if str(x).strip()])
    # if cluster:
    #     lines.append("")
    #     lines.append(f"Comparable: {cluster}")

    if listed_exchanges:
        listed = ", ".join([str(x) for x in listed_exchanges if str(x).strip()])
        if listed:
            lines.append("")
            lines.append(f"Listed on: {listed}")

    return "\n".join(lines)
