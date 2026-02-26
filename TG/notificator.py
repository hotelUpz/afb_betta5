# ============================================================
# FILE: TG/notificator.py
# ROLE: Telegram notifier (sendMessage) with UnifiedLogger
# ============================================================

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import Optional

import aiohttp

from const import (
    TG_MIN_SEND_INTERVAL_SEC,
    TG_RETRY_BACKOFF_PAD_SEC,
    TG_RETRY_MAX_ATTEMPTS,
)
from c_log import UnifiedLogger


class _TgRateLimit(Exception):
    def __init__(self, retry_after: float, raw: str = ""):
        super().__init__(f"Telegram rate limit: retry_after={retry_after}")
        self.retry_after = float(retry_after)
        self.raw = str(raw or "")


class TgNotificator:
    """Tiny Telegram notifier.

    - If TG disabled -> only local logs.
    - TG failures must not crash the bot.
    - Checks Telegram JSON `ok`, not only HTTP status.
    - Splits long messages (Telegram limit ~4096 chars).
    """

    MAX_TEXT_LEN = 3900  # keep margin for safety

    def __init__(self, token: str, chat_id: str, logger: UnifiedLogger):
        self.logger = logger
        self.token = (token or "").strip()
        self.chat_id = (chat_id or "").strip()
        self.base_url = f"https://api.telegram.org/bot{self.token}" if self.token else ""
        self._session: Optional[aiohttp.ClientSession] = None

        # send queue + rate limiter (prevents HTTP 429 and avoids blocking the bot loop)
        self._queue: "asyncio.Queue[str]" = asyncio.Queue(maxsize=2000)
        self._worker: Optional[asyncio.Task] = None
        self._closed = False
        self._next_send_ts = 0.0  # monotonic
        self._min_interval_sec = max(0.0, float(TG_MIN_SEND_INTERVAL_SEC))
        self._retry_max = max(1, int(TG_RETRY_MAX_ATTEMPTS))
        self._retry_pad_sec = max(0.0, float(TG_RETRY_BACKOFF_PAD_SEC))

        if not self.token or not self.chat_id:
            self.logger.info("[TG disabled] TG_TOKEN or TG_CHAT_ID is empty — will only log locally.")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is not None and not self._session.closed:
            return self._session
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        return self._session

    @classmethod
    def _split_text(cls, text: str) -> list[str]:
        s = str(text or "")
        if len(s) <= cls.MAX_TEXT_LEN:
            return [s]
        parts: list[str] = []
        rest = s
        while rest:
            if len(rest) <= cls.MAX_TEXT_LEN:
                parts.append(rest)
                break
            cut = rest.rfind("\n", 0, cls.MAX_TEXT_LEN)
            if cut <= 0:
                cut = cls.MAX_TEXT_LEN
            parts.append(rest[:cut])
            rest = rest[cut:].lstrip("\n")
        return [p for p in parts if p]

    async def _send_once(self, text: str) -> None:
        session = await self._get_session()
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        async with session.post(url, json=payload) as resp:
            body_text = await resp.text()
            if resp.status == 429:
                retry_after = 30.0
                try:
                    data = json.loads(body_text)
                    if isinstance(data, dict):
                        p = data.get("parameters") or {}
                        if isinstance(p, dict) and p.get("retry_after") is not None:
                            retry_after = float(p.get("retry_after"))
                except Exception:
                    pass
                raise _TgRateLimit(retry_after=retry_after, raw=body_text)
            if resp.status != 200:
                raise RuntimeError(f"Telegram HTTP {resp.status}: {body_text}")

            ok = True
            desc = ""
            try:
                data = await resp.json(content_type=None)
                if isinstance(data, dict):
                    ok = bool(data.get("ok", False))
                    desc = str(data.get("description") or "")
                else:
                    ok = False
                    desc = f"unexpected JSON payload type: {type(data).__name__}"
            except Exception:
                # If JSON parse failed but HTTP 200, still suspicious.
                ok = False
                desc = f"non-JSON response: {body_text[:300]}"

            if not ok:
                raise RuntimeError(f"Telegram API error: {desc or body_text[:300]}")

    async def _ensure_worker(self) -> None:
        if self._worker is not None and not self._worker.done():
            return
        # Only start when called from an async context with a running loop.
        self._worker = asyncio.create_task(self._worker_loop())

    async def _worker_loop(self) -> None:
        while not self._closed:
            try:
                text = await self._queue.get()
            except asyncio.CancelledError:
                break
            try:
                await self._send_rate_limited(text)
            except Exception as e:
                with contextlib.suppress(Exception):
                    self.logger.warning(f"[TG] send failed: {e}")
            finally:
                with contextlib.suppress(Exception):
                    self._queue.task_done()

    async def _send_rate_limited(self, text: str) -> None:
        # single worker => no extra locks required
        for attempt in range(1, self._retry_max + 1):
            now = time.monotonic()
            wait = self._next_send_ts - now
            if wait > 0:
                await asyncio.sleep(wait)

            try:
                await self._send_once(text)
                self._next_send_ts = time.monotonic() + float(self._min_interval_sec)
                return
            except _TgRateLimit as e:
                delay = max(float(self._min_interval_sec), float(e.retry_after) + float(self._retry_pad_sec))
                self._next_send_ts = time.monotonic() + delay
                if attempt >= self._retry_max:
                    raise RuntimeError(f"Telegram HTTP 429 (retry_after={e.retry_after})")
                await asyncio.sleep(delay)
            except Exception:
                # avoid burst retries on generic errors
                self._next_send_ts = time.monotonic() + float(self._min_interval_sec)
                raise

    async def send_text(self, text: str) -> None:
        if not self.token or not self.chat_id:
            self.logger.info(f"[TG disabled] {text}")
            return
        if self._closed:
            return

        await self._ensure_worker()

        parts = self._split_text(str(text))
        for i, part in enumerate(parts, start=1):
            payload = part
            if len(parts) > 1:
                payload = f"[{i}/{len(parts)}]\n{part}"
            try:
                self._queue.put_nowait(payload)
            except asyncio.QueueFull:
                with contextlib.suppress(Exception):
                    self.logger.warning("[TG] send queue overflow: dropping message")
                break

    async def close(self) -> None:
        self._closed = True
        if self._worker is not None:
            self._worker.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._worker
        self._worker = None
        if self._session is not None and not self._session.closed:
            with contextlib.suppress(Exception):
                await self._session.close()
        self._session = None