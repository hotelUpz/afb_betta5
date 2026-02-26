# ============================================================
# FILE: CORE/arb_math.py
# ROLE: Pure math + explicit entities for pair evaluation.
# ============================================================

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class FundingLeg:
    ex: str
    rate_pct: float
    next_funding_time_ms: int


@dataclass(frozen=True)
class PriceLeg:
    ex: str
    price: float
    ts_ms: int


@dataclass(frozen=True)
class BookLeg:
    ex: str
    bid: float
    ask: float
    spread_pct: float
    ts_ms: int
    bid_qty: float = 0.0
    ask_qty: float = 0.0
    bid_notional: float = 0.0
    ask_notional: float = 0.0
    top_min_notional: float = 0.0
    top_balance_ratio: float = 0.0


@dataclass(frozen=True)
class BookQualityConfig:
    max_spread_pct: Optional[float] = None
    # Preferred model: relative to intended position size (quote currency, e.g. USDT)
    target_pos_usd: Optional[float] = None
    min_top_coverage_x: Optional[float] = None
    # Legacy fallback (absolute top notional threshold)
    min_top_notional: Optional[float] = None
    min_side_balance_ratio: Optional[float] = None


@dataclass(frozen=True)
class BookQualityCheck:
    ok: bool
    fail_reason: str = ""


@dataclass(frozen=True)
class PairSpreadInput:
    pre_total_funding_spread_pct: float
    p_long: float
    p_short: float
    price_weight: float


@dataclass(frozen=True)
class PairSpreadOutput:
    # Historical field name kept for backward compatibility.
    price_component_pct: float
    total_spread_pct: float

    @property
    def price_spread_pct(self) -> float:
        return float(self.price_component_pct)


class ArbMath:
    """Pure math helpers. No IO, no cache access, no side effects."""

    @staticmethod
    def calc_pre_total_funding_spread_pct(*, long_rate_pct: float, short_rate_pct: float) -> float:
        """PRE_TOTALY_FUNDING_SPREAD in percent (already normalized coords)."""
        return float(short_rate_pct) - float(long_rate_pct)

    @staticmethod
    def calc_price_component_pct(*, p_short: float, p_long: float) -> float:
        """Dynamic price spread aligned with LONG/SHORT determinants.

        + means short leg more expensive and/or long leg cheaper (favorable)
        - means opposite (unfavorable)

        We normalize by min(price) instead of midpoint to better reflect execution impact
        when one leg is noticeably cheaper. Guarded against non-positive prices.
        """
        p_short = float(p_short)
        p_long = float(p_long)
        if p_short <= 0 or p_long <= 0:
            return 0.0
        denom = min(p_short, p_long)
        if denom <= 0:
            return 0.0
        return (p_short - p_long) / denom * 100.0

    @staticmethod
    def calc_price_spread_pct(*, p_short: float, p_long: float) -> float:
        """Alias with a clearer name (price term is dynamic, not a config constant)."""
        return ArbMath.calc_price_component_pct(p_short=p_short, p_long=p_long)

    @classmethod
    def calc_total_spread(cls, x: PairSpreadInput) -> PairSpreadOutput:
        price_component = cls.calc_price_component_pct(p_short=x.p_short, p_long=x.p_long)
        total_spread = float(x.pre_total_funding_spread_pct) + float(x.price_weight) * float(price_component)
        return PairSpreadOutput(
            price_component_pct=float(price_component),
            total_spread_pct=float(total_spread),
        )

    @staticmethod
    def calc_required_top_notional(cfg: BookQualityConfig) -> Optional[float]:
        """Return effective top-level notional threshold for one side (quote currency)."""
        tp = cfg.target_pos_usd
        mx = cfg.min_top_coverage_x
        if tp is not None and mx is not None:
            tp_f = float(tp)
            mx_f = float(mx)
            if tp_f > 0 and mx_f > 0:
                return tp_f * mx_f

        # Legacy fallback only if relative model is disabled.
        if cfg.min_top_notional is not None and float(cfg.min_top_notional) > 0:
            return float(cfg.min_top_notional)
        return None

    @staticmethod
    def check_book_quality(*, book: BookLeg, cfg: BookQualityConfig) -> BookQualityCheck:
        if cfg.max_spread_pct is not None and float(cfg.max_spread_pct) > 0:
            if float(book.spread_pct) > float(cfg.max_spread_pct):
                return BookQualityCheck(False, f"spread_pct {book.spread_pct:.4f}% > {float(cfg.max_spread_pct):.4f}%")

        req_top = ArbMath.calc_required_top_notional(cfg)
        if req_top is not None and req_top > 0:
            top_min = float(book.top_min_notional)
            if top_min < req_top:
                if cfg.target_pos_usd is not None and cfg.min_top_coverage_x is not None and float(cfg.target_pos_usd) > 0 and float(cfg.min_top_coverage_x) > 0:
                    cov = top_min / req_top if req_top > 0 else 0.0
                    return BookQualityCheck(False, f"top_coverage {cov:.3f}x ({top_min:.4f} < {req_top:.4f})")
                return BookQualityCheck(False, f"top_min_notional {top_min:.4f} < {req_top:.4f}")

        if cfg.min_side_balance_ratio is not None and float(cfg.min_side_balance_ratio) > 0:
            if float(book.top_balance_ratio) < float(cfg.min_side_balance_ratio):
                return BookQualityCheck(False, f"top_balance_ratio {book.top_balance_ratio:.4f} < {float(cfg.min_side_balance_ratio):.4f}")

        return BookQualityCheck(True, "")

    @staticmethod
    def build_book_leg(ex: str, info: dict) -> Optional[BookLeg]:
        if not isinstance(info, dict):
            return None
        try:
            bid = float(info.get("bid") or 0.0)
            ask = float(info.get("ask") or 0.0)
            spread_pct = float(info.get("spread_pct") or 0.0)
            ts_ms = int(info.get("ts_ms") or 0)
            if bid <= 0 or ask <= 0 or ts_ms <= 0:
                return None
            return BookLeg(
                ex=str(ex),
                bid=bid,
                ask=ask,
                spread_pct=spread_pct,
                ts_ms=ts_ms,
                bid_qty=float(info.get("bid_qty") or 0.0),
                ask_qty=float(info.get("ask_qty") or 0.0),
                bid_notional=float(info.get("bid_notional") or 0.0),
                ask_notional=float(info.get("ask_notional") or 0.0),
                top_min_notional=float(info.get("top_min_notional") or 0.0),
                top_balance_ratio=float(info.get("top_balance_ratio") or 0.0),
            )
        except Exception:
            return None
