# ============================================================
# FILE: c_log.py
# ROLE: Unified logger (RotatingFileHandler) + decorator wrappers
# ============================================================

from __future__ import annotations

import pytz
from datetime import datetime
from functools import wraps
from logging.handlers import RotatingFileHandler
from pprint import pformat
from typing import Any, Optional

from const import (
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARNING,
    LOG_ERROR,
    MAX_LOG_LINES,
    TIME_ZONE,
)

import inspect
import logging
import os
import traceback


# ============================================================
# TIME
# ============================================================

TZ = pytz.timezone(TIME_ZONE)

def log_time() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# HELPERS
# ============================================================

def estimate_average_line_length(path: str, sample: int = 200) -> int:
    if not os.path.exists(path):
        return 300
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [len(next(f)) for _ in range(sample)]
        return sum(lines) // len(lines) if lines else 300
    except Exception:
        return 300


def calc_max_bytes(avg_len: int, lines: int) -> int:
    return avg_len * lines


# ============================================================
# UNIFIED LOGGER
# ============================================================
class UnifiedLogger:
    """
    Универсальный логгер:
    - logging + RotatingFileHandler
    - decorator для методов
    - совместим с async / sync
    """

    def __init__(
        self,
        name: str,
        log_dir: str = "./logs",
        max_lines: int = MAX_LOG_LINES,
        context: Optional[dict] = None,
    ):
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, f"{name}.log")

        avg_len = estimate_average_line_length(log_path)
        max_bytes = calc_max_bytes(avg_len, max_lines)

        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False  # 🔒 важно: не всплывать в root

        # 🔑 КРИТИЧНО: handler добавляем ТОЛЬКО если его ещё нет
        if not logger.handlers:
            handler = RotatingFileHandler(
                log_path,
                maxBytes=max_bytes,
                backupCount=1,
                encoding="utf-8",
            )

            handler.setFormatter(logging.Formatter(
                "%(asctime)s | %(levelname)s | %(context)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))

            logger.addHandler(handler)

            # debug-метка (можно потом убрать)
            # print(f"[LOGGER INIT] name={name} pid={os.getpid()}")

        self._logger = logging.LoggerAdapter(
            logger,
            extra={"context": context or name},
        )

    def debug(self, msg: str, *args, **kwargs):
        if LOG_DEBUG:
            print(msg)
            self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        if LOG_INFO:
            print(msg)
            self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        if LOG_WARNING:
            print(msg)
            self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        if LOG_ERROR:
            print(msg)
            self._logger.error(msg, *args, **kwargs)

    def exception(self, msg: str, *args, exc: Exception = None, **kwargs):
        if LOG_ERROR:
            if exc:
                self._logger.exception(msg, *args, **kwargs)
            else:
                self._logger.exception(msg, *args, **kwargs)

    # ======================================================
    # DECORATOR
    # ======================================================
    def total_exception_decor(self, func, context: Optional[Any] = None):
        """
        Ловит ВСЕ исключения, логирует контекст,
        НЕ крашит приложение.
        """

        if getattr(func, "_is_wrapped", False):
            return func

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as ex:
                self._log_exception(func, ex, args, kwargs, context)
                return None

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as ex:
                self._log_exception(func, ex, args, kwargs, context)
                return None

        wrapper = (
            async_wrapper
            if inspect.iscoroutinefunction(func)
            else sync_wrapper
        )
        wrapper._is_wrapped = True
        return wrapper

    def _log_exception(self, func, ex, args, kwargs, context: Optional[Any] = None):
        extra = {}
        if context is not None:
            extra["context"] = context

        self._logger.error(
            f"[EXCEPTION] {func.__qualname__} -> {ex}\n"
            f"Args:\n{pformat({'args': args, 'kwargs': kwargs})}\n"
            f"Stack:\n{traceback.format_exc()}",
            extra=extra or None,
        )

    # ======================================================
    # MASS WRAP
    # ======================================================
    def wrap_object_methods(self, obj: Any, context: Optional[Any] = None):
        for cls in obj.__class__.mro():
            if cls is object:
                continue

            for name, attr in cls.__dict__.items():
                if name.startswith("_"):
                    continue

                if name.startswith("__"):
                    continue

                if not callable(attr):
                    continue

                try:
                    original = getattr(obj, name)
                    if getattr(original, "_is_wrapped", False):
                        continue

                    wrapped = self.total_exception_decor(original, context)
                    setattr(obj, name, wrapped)
                except Exception:
                    continue