# ============================================================
# FILE: c_utils.py
# ROLE: Small helper utilities (time, safe casting, formatting)
# ============================================================

from __future__ import annotations

import math
import re
import time
from datetime import datetime
from decimal import Decimal, localcontext
from typing import Any, Optional

from const import DECIMAL_CONTEXT_PREC, PRECISION
from c_log import TZ

# Разрешаем только латиницу и цифры
_SYMBOL_REGEX = re.compile(r"^[A-Z0-9]+$")


def now() -> int:
    """Return current timestamp in milliseconds."""
    return int(time.time() * 1000)


class Utils:
    @staticmethod
    def safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def safe_round(value: Any, ndigits: int = 2, default: float = 0.0) -> float:
        try:
            return round(float(value), ndigits)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def milliseconds_to_datetime(milliseconds: Any) -> str:
        if milliseconds is None:
            return "N/A"
        try:
            ms = int(milliseconds)
            if ms < 0:
                return "N/A"
        except (ValueError, TypeError):
            return "N/A"

        seconds = ms / 1000 if ms > 1e10 else ms
        dt = datetime.fromtimestamp(seconds, TZ)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def format_duration(ms: Optional[int]) -> str:
        if ms is None:
            return ""

        total_seconds = int(ms) // 1000
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        if hours > 0 and minutes > 0:
            return f"{hours}h {minutes}m"
        if minutes > 0 and seconds > 0:
            return f"{minutes}m {seconds}s"
        if minutes > 0:
            return f"{minutes}m"
        return f"{seconds}s"

    @staticmethod
    def to_human_digit(value: Any) -> Optional[str]:
        if value is None:
            return None
        try:
            with localcontext() as ctx:
                ctx.prec = int(DECIMAL_CONTEXT_PREC)
                dec_value = Decimal(str(value)).normalize()
        except Exception:
            return str(value)
        if dec_value == dec_value.to_integral():
            return format(dec_value, "f")
        return format(dec_value, "f").rstrip("0").rstrip(".")

    @staticmethod
    def fmt_num(value: Any, kind: str = "default") -> str:
        try:
            x = float(value)
        except Exception:
            return str(value)
        if not math.isfinite(x):
            return str(value)
        if kind == "price":
            # No fixed precision for price (avoid damaging tiny-price assets).
            s = format(x, ".18g")
            if "e" in s or "E" in s:
                s = format(x, ".18f").rstrip("0").rstrip(".")
            return s or "0"
        digits = int(PRECISION.get(kind, PRECISION.get("default", 6)))
        return f"{x:.{digits}f}"

    @classmethod
    def fmt_price(cls, value: Any) -> str:
        return cls.fmt_num(value, "price")

    @classmethod
    def fmt_funding_rate(cls, value: Any) -> str:
        return cls.fmt_num(value, "funding_rate")

    @classmethod
    def fmt_spread_pct(cls, value: Any) -> str:
        return cls.fmt_num(value, "spread_pct")

    @staticmethod
    def normalize_symbol(raw: str) -> Optional[str]:
        if not raw or not isinstance(raw, str):
            return None

        sym = raw.strip().upper()
        if not sym:
            return None

        for ch in sym:
            if "А" <= ch <= "Я" or "а" <= ch <= "я":
                return None

        if not _SYMBOL_REGEX.match(sym):
            return None

        return sym
