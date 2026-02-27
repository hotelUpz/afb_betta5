# ============================================================
# FILE: CORE/market_streams.py
# ROLE: Start/stop price & orderbook (stakan) streams for a dynamic symbol set.
# ============================================================

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Set, Tuple


PRICE_STREAM_SHRINK_IDLE_SEC = 180  # avoid stream churn / stale-price resets on fast candidate-set shrink

from c_log import UnifiedLogger

from API.BINANCE.price import BinanceHotPriceStream
from API.BITGET.price import BitgetHotPriceStream
# from API.BINANCE.stakan import BinanceStakanStream

from API.OKX.price import OkxHotPriceStream
# from API.OKX.stakan import OkxStakanStream

from API.KUCOIN.price import KucoinHotPriceStream
# from API.KUCOIN.stakan import KucoinStakanStream

from API.PHEMEX.price2 import PhemexHotPriceStream as PhemexPackPriceStream
# from API.PHEMEX.stakan import PhemexStakanStream


@dataclass
class _Handle:
    symbols: Set[str]
    stream: Any
    task: asyncio.Task
    changed_at_ms: int


class MarketStreams:
    """Dynamic WS streams manager.

    Keeps:
      - last prices per exchange/symbol
      - best bid/ask per exchange/symbol
      - (legacy) per-symbol stakan ok_since timer

    The bot can call ensure() with *only the currently needed* symbols.

    IMPORTANT:
        For the main algorithm we use a **candidate-level** TTL (pair TTL),
        because per-symbol TTL may not reflect that both legs were "ok" simultaneously.

        MarketStreams still exposes:
          - is_spread_ok_now()  -> instant check
          - is_stakan_ok()      -> legacy per-symbol TTL check
    """

    def __init__(
        self,
        *,
        logger: UnifiedLogger,
        stakan_spread_pct_threshold: Optional[float],
        stakan_ttl_sec: Optional[float],
    ):
        self.logger = logger
        self.stakan_spread_pct_threshold = None if stakan_spread_pct_threshold is None else float(stakan_spread_pct_threshold)
        self.stakan_ttl_sec = None if stakan_ttl_sec is None else float(stakan_ttl_sec)
        self.orderbook_enabled = bool(
            (self.stakan_spread_pct_threshold is not None and self.stakan_spread_pct_threshold > 0)
            or (self.stakan_ttl_sec is not None and self.stakan_ttl_sec > 0)
        )

        # caches
        self._prices: Dict[str, Dict[str, Dict[str, float]]] = {}  # ex -> sym -> {price, ts_ms}
        self._books: Dict[str, Dict[str, Dict[str, float]]] = {}   # ex -> sym -> {bid, ask, spread_pct, ts_ms}
        self._ok_since: Dict[str, Dict[str, int]] = {}             # ex -> sym -> ts_ms

        # running streams
        self._price_handles: Dict[str, _Handle] = {}
        self._stakan_handles: Dict[str, _Handle] = {}

        self._lock = asyncio.Lock()

    # ---------------------------------------------------------
    # API (read caches)
    # ---------------------------------------------------------
    def get_price(self, ex: str, sym: str) -> Optional[float]:
        d = self._prices.get(ex, {}).get(sym)
        if not d:
            return None
        p = d.get("price")
        return float(p) if isinstance(p, (int, float)) and p > 0 else None

    def get_price_info(self, ex: str, sym: str, now_ms: Optional[int] = None) -> Optional[dict]:
        d = self._prices.get(ex, {}).get(sym)
        if not d:
            return None
        p = d.get("price")
        ts_ms = int(d.get("ts_ms") or 0)
        if not isinstance(p, (int, float)) or float(p) <= 0 or ts_ms <= 0:
            return None
        now_ms_i = int(now_ms or (time.time() * 1000))
        age_sec = max(0.0, (now_ms_i - ts_ms) / 1000.0)
        return {"price": float(p), "ts_ms": ts_ms, "age_sec": age_sec}

    def get_book_info(self, ex: str, sym: str, now_ms: Optional[int] = None) -> Optional[dict]:
        book = self._books.get(ex, {}).get(sym)
        if not book:
            return None
        bid = float(book.get("bid") or 0.0)
        ask = float(book.get("ask") or 0.0)
        spread_pct = float(book.get("spread_pct") or 0.0)
        ts_ms = int(book.get("ts_ms") or 0)
        if bid <= 0 or ask <= 0 or ts_ms <= 0:
            return None
        now_ms_i = int(now_ms or (time.time() * 1000))
        age_sec = max(0.0, (now_ms_i - ts_ms) / 1000.0)

        out = {
            "bid": bid,
            "ask": ask,
            "spread_pct": spread_pct,
            "ts_ms": ts_ms,
            "age_sec": age_sec,
        }
        # Optional enriched metrics (best-effort; depends on WS payloads / qty units)
        for k in (
            "bid_qty",
            "ask_qty",
            "mid",
            "bid_notional",
            "ask_notional",
            "top_min_notional",
            "top_balance_ratio",
        ):
            v = book.get(k)
            if isinstance(v, (int, float)):
                out[k] = float(v)
        return out

    def is_spread_ok_now(self, ex: str, sym: str) -> Tuple[bool, Optional[dict]]:
        """Instant orderbook-spread check (ignores TTL)."""
        info = self.get_book_info(ex, sym)
        if not info:
            return False, None

        thr = self.stakan_spread_pct_threshold
        if thr is None or thr <= 0:
            return True, info

        return bool(info["spread_pct"] >= thr), info

    def is_stakan_ok(self, ex: str, sym: str) -> Tuple[bool, Optional[dict]]:
        """Legacy per-symbol TTL check.

        Returns (ok, info). Info includes held_sec.

        NOTE:
            For the main algorithm we prefer candidate-level TTL.
        """

        info = self.get_book_info(ex, sym)
        if not info:
            return False, None

        thr = self.stakan_spread_pct_threshold
        ttl = self.stakan_ttl_sec

        # disabled
        if thr is None or thr <= 0:
            info = dict(info)
            info["held_sec"] = 0.0
            return True, info

        ok_since = self._ok_since.get(ex, {}).get(sym)
        held_sec = 0.0
        if ok_since:
            held_sec = max(0.0, (time.time() * 1000 - ok_since) / 1000.0)

        # instant
        if ttl is None or ttl <= 0:
            ok = bool(info["spread_pct"] >= thr)
        else:
            ok = bool(ok_since) and held_sec >= float(ttl)

        info = dict(info)
        info["held_sec"] = held_sec
        return bool(ok), info

    # ---------------------------------------------------------
    # Streams control
    # ---------------------------------------------------------
    async def ensure(self, symbols_by_exchange: Dict[str, Set[str]]) -> None:
        """Ensure streams are running exactly for requested symbols.

        symbols_by_exchange uses raw exchange symbols (instId / contract codes etc.).
        """
        async with self._lock:
            # stop removed exchanges
            for ex in list(self._price_handles.keys()):
                if ex not in symbols_by_exchange or not symbols_by_exchange.get(ex):
                    await self._stop_handle(self._price_handles.pop(ex), label=f"{ex} price")

            for ex in list(self._stakan_handles.keys()):
                if ex not in symbols_by_exchange or not symbols_by_exchange.get(ex):
                    await self._stop_handle(self._stakan_handles.pop(ex), label=f"{ex} stakan")

            # ensure per exchange
            for ex, syms in symbols_by_exchange.items():
                want = set([str(s).upper().strip() for s in syms if isinstance(s, str) and s.strip()])
                if not want:
                    continue

                await self._ensure_price(ex, want)
                if self.orderbook_enabled:
                    await self._ensure_stakan(ex, want)
                else:
                    cur_st = self._stakan_handles.pop(ex, None)
                    if cur_st:
                        await self._stop_handle(cur_st, label=f"{ex} orderbook")

    async def close(self) -> None:
        """Stop all running price/stakan streams (best effort)."""
        async with self._lock:
            for ex, h in list(self._price_handles.items()):
                try:
                    await self._stop_handle(h, label=f"{ex} price")
                finally:
                    self._price_handles.pop(ex, None)
            for ex, h in list(self._stakan_handles.items()):
                try:
                    await self._stop_handle(h, label=f"{ex} stakan")
                finally:
                    self._stakan_handles.pop(ex, None)

    # ---------------------------------------------------------
    # Internals
    # ---------------------------------------------------------
    async def _stop_handle(self, h: _Handle, *, label: str) -> None:
        if not h:
            return
        try:
            if hasattr(h.stream, "stop"):
                h.stream.stop()
        except Exception:
            pass

        if h.task:
            h.task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await h.task

        # best-effort aclose
        try:
            if hasattr(h.stream, "aclose"):
                await h.stream.aclose()
        except Exception:
            pass

        self.logger.info(f"[WS] stopped {label} ({len(h.symbols)} symbols)")

    async def _ensure_price(self, ex: str, want: Set[str]) -> None:
        cur = self._price_handles.get(ex)
        now_ms = int(time.time() * 1000)

        if cur:
            if cur.symbols == want:
                return

            # Important production guard:
            # candidate sets often shrink one-by-one immediately after signals / failures.
            # Restarting WS streams on every shrink resets hot price caches and causes
            # artificial "stale price data" on the remaining candidates.
            #
            # So we do NOT restart on fast shrink. We keep the current superset warm and
            # only allow a shrink after a quiet idle period. Growth still applies immediately.
            if want.issubset(cur.symbols):
                idle_ms = now_ms - int(getattr(cur, "changed_at_ms", 0) or 0)
                if idle_ms < int(PRICE_STREAM_SHRINK_IDLE_SEC * 1000):
                    return
            new_symbols = set(cur.symbols) | set(want)
            if want.issubset(cur.symbols) and (now_ms - int(getattr(cur, "changed_at_ms", 0) or 0) >= int(PRICE_STREAM_SHRINK_IDLE_SEC * 1000)):
                new_symbols = set(want)
            if new_symbols == cur.symbols:
                return
            await self._stop_handle(cur, label=f"{ex} price")
            want = new_symbols

        stream = self._make_price_stream(ex, want)
        task = asyncio.create_task(stream.run(lambda t, _ex=ex: self._on_price(_ex, t)))
        self._price_handles[ex] = _Handle(symbols=want, stream=stream, task=task, changed_at_ms=now_ms)
        self.logger.info(f"[WS] started {ex} price stream for {len(want)} symbols")

    async def _ensure_stakan(self, ex: str, want: Set[str]) -> None:
        pass
        # cur = self._stakan_handles.get(ex)
        # if cur and cur.symbols == want:
        #     return
        # if cur:
        #     await self._stop_handle(cur, label=f"{ex} stakan")

        # # stream = self._make_stakan_stream(ex, want)
        # task = asyncio.create_task(stream.run(lambda d, _ex=ex: self._on_depth(_ex, d)))
        # self._stakan_handles[ex] = _Handle(symbols=want, stream=stream, task=task)
        # self.logger.info(f"[WS] started {ex} stakan stream for {len(want)} symbols")

    def _make_price_stream(self, ex: str, symbols: Set[str]) -> Any:
        if ex == "BINANCE":
            return BinanceHotPriceStream(symbols, chunk_size=80, throttle_ms=0)
        if ex == "BITGET":
            return BitgetHotPriceStream(symbols, chunk_size=80, throttle_ms=0)
        if ex == "OKX":
            return OkxHotPriceStream(symbols, chunk_size=150, throttle_ms=0)
        if ex == "KUCOIN":
            return KucoinHotPriceStream(symbols, chunk_size=80, throttle_ms=0)
        if ex == "PHEMEX":
            # ticker pack stream (single subscription)
            return PhemexPackPriceStream(symbols, throttle_ms=0)
        raise ValueError(f"Unknown exchange for price stream: {ex}")

    # def _make_stakan_stream(self, ex: str, symbols: Set[str]) -> Any:
    #     if ex == "BINANCE":
    #         return BinanceStakanStream(symbols, chunk_size=50, throttle_ms=0)
    #     if ex == "OKX":
    #         return OkxStakanStream(symbols, chunk_size=150, throttle_ms=0)
    #     if ex == "KUCOIN":
    #         return KucoinStakanStream(symbols, chunk_size=50, throttle_ms=0)
    #     if ex == "PHEMEX":
    #         return PhemexStakanStream(symbols, chunk_size=40, throttle_ms=0)
    #     raise ValueError(f"Unknown exchange for stakan stream: {ex}")

    async def _on_price(self, ex: str, tick: Any) -> None:
        try:
            sym = str(getattr(tick, "symbol", "") or "").upper().strip()
            price = float(getattr(tick, "price", 0.0) or 0.0)
            ts_ms = int(getattr(tick, "event_time_ms", 0) or 0)
            if not sym or price <= 0:
                return
            if ts_ms <= 0:
                ts_ms = int(time.time() * 1000)

            self._prices.setdefault(ex, {})[sym] = {"price": price, "ts_ms": ts_ms}
        except Exception:
            return

    async def _on_depth(self, ex: str, d: Any) -> None:
        try:
            sym = str(getattr(d, "symbol", "") or "").upper().strip()
            bids = getattr(d, "bids", None)
            asks = getattr(d, "asks", None)
            ts_ms = int(getattr(d, "event_time_ms", 0) or 0)
            if not sym:
                return

            bid = 0.0
            ask = 0.0
            bid_qty = 0.0
            ask_qty = 0.0

            if isinstance(bids, list) and bids:
                best_bid = None
                for x in bids:
                    if not isinstance(x, (list, tuple)) or len(x) < 2:
                        continue
                    try:
                        px = float(x[0])
                        qty = float(x[1])
                    except Exception:
                        continue
                    if px <= 0 or qty < 0:
                        continue
                    if best_bid is None or px > best_bid[0]:
                        best_bid = (px, qty)
                if best_bid:
                    bid, bid_qty = best_bid

            if isinstance(asks, list) and asks:
                best_ask = None
                for x in asks:
                    if not isinstance(x, (list, tuple)) or len(x) < 2:
                        continue
                    try:
                        px = float(x[0])
                        qty = float(x[1])
                    except Exception:
                        continue
                    if px <= 0 or qty < 0:
                        continue
                    if best_ask is None or px < best_ask[0]:
                        best_ask = (px, qty)
                if best_ask:
                    ask, ask_qty = best_ask

            if bid <= 0 or ask <= 0:
                return

            spread_pct = (ask - bid) / bid * 100.0
            if ts_ms <= 0:
                ts_ms = int(time.time() * 1000)

            mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0.0
            bid_notional = bid * max(bid_qty, 0.0)
            ask_notional = ask * max(ask_qty, 0.0)
            top_min_notional = min(bid_notional, ask_notional)
            top_max_notional = max(bid_notional, ask_notional)
            top_balance_ratio = (top_min_notional / top_max_notional) if top_max_notional > 0 else 0.0

            self._books.setdefault(ex, {})[sym] = {
                "bid": bid,
                "ask": ask,
                "bid_qty": bid_qty,
                "ask_qty": ask_qty,
                "mid": mid,
                "bid_notional": bid_notional,
                "ask_notional": ask_notional,
                "top_min_notional": top_min_notional,
                "top_balance_ratio": top_balance_ratio,
                "spread_pct": spread_pct,
                "ts_ms": ts_ms,
            }

            # legacy per-symbol timer
            thr = self.stakan_spread_pct_threshold
            if thr is None or thr <= 0:
                return

            if spread_pct >= thr:
                self._ok_since.setdefault(ex, {})
                if sym not in self._ok_since[ex]:
                    self._ok_since[ex][sym] = int(time.time() * 1000)
            else:
                if ex in self._ok_since and sym in self._ok_since[ex]:
                    self._ok_since[ex].pop(sym, None)

        except Exception:
            return
