"""Retry utilities for crypto sources."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Generator, Iterable

from .errors import CryptoSourceConnectionError


@dataclass
class RetryConfig:
    enabled: bool = True
    base_delay: float = 0.5
    max_delay: float = 30.0
    backoff_factor: float = 2.0
    max_retries: int = 3

    def __post_init__(self) -> None:
        if self.base_delay <= 0:
            raise ValueError("base_delay must be positive")
        if self.max_delay <= 0:
            raise ValueError("max_delay must be positive")
        if self.backoff_factor <= 1:
            raise ValueError("backoff_factor must be greater than 1")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")


class ExponentialBackoff:
    """Iterator yielding exponential backoff intervals."""

    def __init__(self, config: RetryConfig) -> None:
        self._config = config
        self._current = config.base_delay

    def __iter__(self) -> "ExponentialBackoff":
        return self

    def __next__(self) -> float:
        value = min(self._current, self._config.max_delay)
        self._current = min(self._current * self._config.backoff_factor, self._config.max_delay)
        return value


class RetryManager:
    """Decides whether retries should continue for a given error."""

    def __init__(self, config: RetryConfig, name: str) -> None:
        self.config = config
        self.name = name

    def should_retry(self, error: Exception, attempt: int) -> bool:
        if not self.config.enabled:
            return False
        if attempt >= self.config.max_retries:
            return False
        if isinstance(error, CryptoSourceConnectionError) and getattr(error, "retryable", True):
            return True
        return False


def retry_with_backoff(func: Callable[[], object], config: RetryConfig, name: str) -> object:
    """Execute func with retry semantics."""

    manager = RetryManager(config, name)
    attempts = 0
    backoff = ExponentialBackoff(config)

    while True:
        try:
            return func()
        except CryptoSourceConnectionError as error:
            attempts += 1
            if not manager.should_retry(error, attempts):
                raise
            time.sleep(next(backoff))
        except Exception:
            raise


__all__ = [
    "RetryConfig",
    "ExponentialBackoff",
    "RetryManager",
    "retry_with_backoff",
]
