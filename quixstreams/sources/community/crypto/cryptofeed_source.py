from __future__ import annotations

import importlib
import logging
import warnings
from dataclasses import replace
from typing import Callable, Optional, Union

from quixstreams.sources.base import ClientConnectFailureCallback, ClientConnectSuccessCallback, Source
from quixstreams.sources.community.crypto.config import (
    AuthProvider,
    CryptofeedConfig,
    NoAuth,
    ValidationError,
    RetryConfig,
)
from quixstreams.sources.community.crypto.errors import (
    missing_dependency_error,
    connection_error,
)
from quixstreams.sources.community.crypto.retry import retry_with_backoff
from quixstreams.sources.community.crypto.utils import (
    TopicBuilder,
    default_key_fn,
    default_ts_fn,
    normalize_ticker,
    normalize_trade,
)

logger = logging.getLogger(__name__)

class CryptofeedSource(Source):
    """
    Realtime source using cryptofeed FeedHandler for trades/ticker.

    Note: Quix Source API produces to a single topic; channel-specific splitting
    can be done downstream in SDF.
    """

    def __init__(
        self,
        config: Optional[CryptofeedConfig] = None,
        *,
        exchanges: Optional[list[str]] = None,
        channels: Optional[list[str]] = None,
        symbols: Optional[list[str]] = None,
        auth_provider: Optional[AuthProvider] = None,
        reconnect: bool = True,
        normalize: bool = True,
        max_retries: int = 3,
        retry_config: Optional[RetryConfig] = None,
        key_setter: Optional[Callable[[dict], object]] = None,
        timestamp_setter: Optional[Callable[[dict], Optional[int]]] = None,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        if config is None:
            warnings.warn(
                "Direct CryptofeedSource kwargs are deprecated; pass a CryptofeedConfig instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            config = CryptofeedConfig(
                exchanges=exchanges or [],
                channels=channels or [],
                symbols=symbols or [],
                auth_provider=auth_provider or NoAuth(),
                reconnect=reconnect,
                normalize=normalize,
                retry_config=retry_config or RetryConfig(max_retries=max_retries),
            )
        elif retry_config is not None:
            warnings.warn(
                "retry_config argument is ignored when a CryptofeedConfig instance is provided.",
                DeprecationWarning,
                stacklevel=2,
            )
        elif max_retries != 3:
            warnings.warn(
                "max_retries argument is ignored when a CryptofeedConfig instance is provided.",
                DeprecationWarning,
                stacklevel=2,
            )

        if not config.validate():
            raise ValidationError("Invalid CryptofeedConfig provided")
        
        # Import guard
        try:
            importlib.import_module("cryptofeed")
        except ImportError as e:
            raise missing_dependency_error("cryptofeed", "CryptofeedSource", "crypto") from e

        self._config = config
        retry_config = config.retry_config
        if not isinstance(retry_config, RetryConfig):
            raise ValidationError("retry_config must be provided as a RetryConfig instance")
        if not self._config.reconnect and retry_config.enabled:
            retry_config = replace(retry_config, enabled=False, max_retries=0)
        self._retry_config = retry_config

        super().__init__(
            name="cryptofeed-source",
            shutdown_timeout=self._config.shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        
        self._key_setter = key_setter or default_key_fn
        self._timestamp_setter = timestamp_setter or default_ts_fn
        self._fh = None

    def setup(self):
        cf = importlib.import_module("cryptofeed")

        self._fh = cf.FeedHandler()

        # Register a single exchange with combined channels for simplicity in tests
        callbacks = {}
        if "trades" in self._config.channels:
            callbacks["trades"] = self._on_trade
        if "ticker" in self._config.channels:
            callbacks["ticker"] = self._on_ticker

        # For tests, we assume add_feed(exchange, channels=[...], symbols=[...], callbacks=dict)
        # Real wiring may differ per cryptofeed.
        try:
            for ex in self._config.exchanges:
                self._fh.add_feed(ex, channels=self._config.channels, symbols=self._config.symbols, callbacks=callbacks)
        except ValueError as e:
            from .errors import CryptoSourceConfigError
            error_msg = f"Invalid cryptofeed configuration: {str(e)}"
            # Include specific exchange/channel details in the error message
            if "exchanges" in str(e).lower() or any(ex in str(e) for ex in self._config.exchanges):
                error_msg += f". Invalid exchange(s): {self._config.exchanges}"
            if "channels" in str(e).lower() or any(ch in str(e) for ch in self._config.channels):
                error_msg += f". Invalid channel(s): {self._config.channels}"
            raise CryptoSourceConfigError(
                error_msg,
                source="CryptofeedSource",
                context={"exchanges": self._config.exchanges, "channels": self._config.channels, "original_error": str(e)}
            ) from e

    def _produce_event(self, event: dict):
        ts = self._timestamp_setter(event)
        msg = self.producer_topic.serialize(
            key=self._key_setter(event), value=event, timestamp_ms=ts
        )
        self.produce(key=msg.key, value=msg.value, timestamp=msg.timestamp)

    def _on_trade(self, event: dict):
        event = normalize_trade(event)
        self._produce_event(event)

    def _on_ticker(self, event: dict):
        event = normalize_ticker(event)
        self._produce_event(event)

    def run(self):
        def _start_feed() -> None:
            try:
                self._fh.run()
            except Exception as error:  # pragma: no cover - runtime-specific
                logger.error("CryptofeedSource failed: %s", error)
                raise connection_error("CryptofeedSource", error)

        retry_with_backoff(_start_feed, self._retry_config, "CryptofeedSource")

        # Block until stopped
        import time
        while self.running and getattr(self._fh, "running", True):
            time.sleep(0.1)

    def stop(self):
        try:
            if self._fh is not None:
                self._fh.stop()
        finally:
            super().stop()
