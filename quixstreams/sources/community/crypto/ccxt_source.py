from __future__ import annotations

import importlib
import logging
import warnings
from typing import Optional

from quixstreams.sources.base import ClientConnectFailureCallback, ClientConnectSuccessCallback, Source
from quixstreams.sources.community.crypto.config import (
    CCXTConfig,
    AuthProvider,
    NoAuth,
)
from quixstreams.sources.community.crypto.errors import (
    missing_dependency_error,
    connection_error,
    rate_limit_error,
)
from quixstreams.sources.community.crypto.utils import (
    TopicBuilder,
    default_key_fn,
    default_ts_fn,
    normalize_klines,
    normalize_orderbook,
    normalize_trade,
)

logger = logging.getLogger(__name__)

# alias time for tests
_time = None


class CCXTSource(Source):
    """
    CCXT-based source for klines, trades, and orderbook snapshots (REST).
    """

    def __init__(
        self,
        config: Optional[CCXTConfig] = None,
        *,
        exchange: Optional[str] = None,
        mode: Optional[str] = None,
        symbols: Optional[list[str]] = None,
        interval: Optional[str] = None,
        rate_limit: bool = True,
        normalize: bool = True,
        rest_poll_interval: float = 0.0,
        shutdown_timeout: float = 10.0,
        name: Optional[str] = None,
        auth: Optional[AuthProvider] = None,
        use_websocket: bool = False,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        if config is None:
            warnings.warn(
                "Direct CCXTSource kwargs are deprecated; pass a CCXTConfig instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if symbols is None:
                symbols = []
            config = CCXTConfig(
                exchange=exchange or "",
                mode=mode or "",
                symbols=symbols,
                interval=interval,
                rate_limit=rate_limit,
                normalize=normalize,
                rest_poll_interval=rest_poll_interval,
                shutdown_timeout=shutdown_timeout,
                name=name,
                auth_provider=auth or NoAuth(),
                use_websocket=use_websocket,
            )

        if not config.validate():
            raise ValueError("Invalid CCXTConfig provided")

        # Import guard
        try:
            importlib.import_module("ccxt")
        except ImportError as e:
            raise missing_dependency_error("ccxt", "CCXTSource", "crypto") from e

        self._config = config
        
        suffix = f"_{config.interval}" if config.mode == "klines" and config.interval else ""
        super().__init__(
            name=config.name or TopicBuilder("ccxt", datatype=f"{config.mode}{suffix}"),
            shutdown_timeout=config.shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        
        self._cursor: dict[str, int] = {}
        self._client = None

    def setup(self):
        ccxt = importlib.import_module("ccxt")
        # instantiate exchange class by name (e.g., ccxt.binance())
        try:
            ex_cls = getattr(ccxt, self._config.exchange)
            self._client = ex_cls()
        except AttributeError:
            from .errors import CryptoSourceConfigError
            raise CryptoSourceConfigError(
                f"Exchange '{self._config.exchange}' not supported by CCXT",
                source="CCXTSource",
                context={"exchange": self._config.exchange, "available_exchanges": list(ccxt.exchanges)}
            )

    def _sleep(self, seconds: float):
        import time as _t
        (_time or _t).sleep(seconds)

    def _sleep_rate_limit(self):
        if self._config.rate_limit:
            rl = getattr(self._client, "rateLimit", 0) or 0
            if rl > 0:
                self._sleep(rl / 1000.0)

    def _emit(self, event: dict):
        ts = default_ts_fn(event)
        key = default_key_fn(event)
        msg = self.producer_topic.serialize(key=key, value=event, timestamp_ms=ts)
        self.produce(key=msg.key, value=msg.value, timestamp=msg.timestamp)

    def _handle_klines(self):
        for symbol in self._config.symbols:
            since = self._cursor.get(symbol)
            candles = self._client.fetchOHLCV(symbol, timeframe=self._config.interval, since=since, limit=1000)
            if candles:
                for c in candles:
                    ev = normalize_klines(c, self._config.interval or "") if self._config.normalize else c
                    # fill required exchange/symbol if normalize_klines didn't
                    ev.setdefault("exchange", self._config.exchange)
                    ev.setdefault("symbol", symbol)
                    self._emit(ev)
                # advance cursor to last close_time or open_time
                last = candles[-1]
                close_time = last[6] if len(last) > 6 else last[0]
                self._cursor[symbol] = close_time
            self._sleep_rate_limit()

    def _handle_trades(self):
        for symbol in self._config.symbols:
            since = self._cursor.get(symbol)

            try:
                trades = self._client.fetchTrades(symbol, since=since, limit=1000)
            except Exception as e:
                logger.error(f"Failed to fetch trades for {symbol}: {e}")
                if self._config.rate_limit:
                    self._sleep_rate_limit()
                continue

            if trades:
                max_ts = since or 0
                for t in trades:
                    ev = normalize_trade(t) if self._config.normalize else t
                    ev.setdefault("exchange", self._config.exchange)
                    ev.setdefault("symbol", symbol)
                    self._emit(ev)
                    ts = default_ts_fn(ev) or 0
                    if ts > max_ts:
                        max_ts = ts
                self._cursor[symbol] = max_ts
            self._sleep_rate_limit()

    def _handle_orderbook(self):
        for symbol in self._config.symbols:
            ob = self._client.fetchOrderBook(symbol)
            ev = normalize_orderbook({"exchange": self._config.exchange, "symbol": symbol, **ob}) if self._config.normalize else {"exchange": self._config.exchange, "symbol": symbol, **ob}
            self._emit(ev)
            self._sleep_rate_limit()
            if self._config.rest_poll_interval:
                self._sleep(self._config.rest_poll_interval)

    def run(self):
        # Single pass per run() for testability
        if self._config.mode == "klines":
            self._handle_klines()
        elif self._config.mode == "trades":
            self._handle_trades()
        elif self._config.mode == "orderbook":
            self._handle_orderbook()
        self.producer.flush()
