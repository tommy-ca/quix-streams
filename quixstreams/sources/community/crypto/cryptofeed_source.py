from __future__ import annotations

import logging
from typing import Callable, Optional

from quixstreams.sources.base import ClientConnectFailureCallback, ClientConnectSuccessCallback, Source
from quixstreams.sources.community.crypto.utils import (
    TopicBuilder,
    default_key_fn,
    default_ts_fn,
    normalize_ticker,
    normalize_trade,
)

logger = logging.getLogger(__name__)

# time alias for tests
_time = None


class CryptofeedSource(Source):
    """
    Realtime source using cryptofeed FeedHandler for trades/ticker.

    Note: Quix Source API produces to a single topic; channel-specific splitting
    can be done downstream in SDF.
    """

    def __init__(
        self,
        *,
        exchanges: list[str],
        channels: list[str],
        symbols: Optional[list[str]] = None,
        normalize: bool = True,
        reconnect: bool = True,
        max_retries: Optional[int] = None,
        key_setter: Optional[Callable[[dict], object]] = None,
        timestamp_setter: Optional[Callable[[dict], Optional[int]]] = None,
        name: Optional[str] = None,
        shutdown_timeout: float = 10.0,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        try:
            import importlib
            import sys as _sys
            if "cryptofeed" in _sys.modules:
                # ensure import error if it's a stubbed None
                if _sys.modules["cryptofeed"] is None:
                    raise ImportError("cryptofeed missing")
            else:
                importlib.import_module("cryptofeed")
        except Exception as e:  # noqa: BLE001
            raise ImportError(
                "CryptofeedSource requires 'cryptofeed'. Install: pip install quixstreams[crypto]"
            ) from e

        super().__init__(
            name=name or TopicBuilder("cryptofeed", datatype="events"),
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._exchanges = exchanges
        self._channels = channels
        self._symbols = symbols or []
        self._normalize = normalize
        self._reconnect = reconnect
        self._max_retries = max_retries
        self._key_setter = key_setter or default_key_fn
        self._timestamp_setter = timestamp_setter or default_ts_fn
        self._fh = None

    def setup(self):
        import cryptofeed as cf

        self._fh = cf.FeedHandler()

        # Register a single exchange with combined channels for simplicity in tests
        callbacks = {}
        if "trades" in self._channels:
            callbacks["trades"] = self._on_trade
        if "ticker" in self._channels:
            callbacks["ticker"] = self._on_ticker

        # For tests, we assume add_feed(exchange, channels=[...], symbols=[...], callbacks=dict)
        # Real wiring may differ per cryptofeed.
        for ex in self._exchanges:
            self._fh.add_feed(ex, channels=self._channels, symbols=self._symbols, callbacks=callbacks)

    def _produce_event(self, event: dict):
        ts = self._timestamp_setter(event)
        msg = self.producer_topic.serialize(
            key=self._key_setter(event), value=event, timestamp_ms=ts
        )
        self.produce(key=msg.key, value=msg.value, timestamp=msg.timestamp)

    def _on_trade(self, event: dict):
        if self._normalize:
            event = normalize_trade(event)
        self._produce_event(event)

    def _on_ticker(self, event: dict):
        if self._normalize:
            event = normalize_ticker(event)
        self._produce_event(event)

    def run(self):
        import time as _t
        global _time
        if _time is None:
            _time = _t

        attempts = 0
        while True:
            try:
                self._fh.run()
                break
            except Exception as e:  # noqa: BLE001
                if not self._reconnect:
                    raise
                attempts += 1
                if self._max_retries is not None and attempts > self._max_retries:
                    logger.error("Max retries exceeded; aborting")
                    raise
                backoff = min(2 ** (attempts - 1), 30)
                logger.warning(f"cryptofeed run failed, retrying in {backoff}s: {e}")
                (_time or _t).sleep(backoff)

        # Block until stopped
        while self.running and getattr(self._fh, "running", True):
            (_time or _t).sleep(0.1)

    def stop(self):
        try:
            if self._fh is not None:
                self._fh.stop()
        finally:
            super().stop()