from __future__ import annotations

import logging
from typing import Optional

from quixstreams.sources.base import ClientConnectFailureCallback, ClientConnectSuccessCallback, Source
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
        *,
        exchange: str,
        mode: str,
        interval: Optional[str] = None,
        symbols: list[str],
        use_ws: bool = False,
        rest_poll_interval: Optional[float] = None,
        rate_limit: bool = True,
        normalize: bool = True,
        name: Optional[str] = None,
        shutdown_timeout: float = 10.0,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        # Import guard
        try:
            import importlib
            importlib.import_module("ccxt")
        except Exception as e:  # noqa: BLE001
            raise ImportError("CCXTSource requires 'ccxt'. Install: pip install quixstreams[crypto]") from e

        if mode not in ("klines", "trades", "orderbook"):
            raise ValueError("mode must be one of 'klines','trades','orderbook'")

        suffix = f"_{interval}" if mode == "klines" and interval else ""
        super().__init__(
            name=name or TopicBuilder("ccxt", datatype=f"{mode}{suffix}"),
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._exchange_name = exchange
        self._mode = mode
        self._interval = interval
        self._symbols = symbols
        self._use_ws = use_ws
        self._rest_poll_interval = rest_poll_interval or 0.0
        self._rate_limit = rate_limit
        self._normalize = normalize
        self._cursor: dict[str, int] = {}
        self._client = None

    def setup(self):
        import ccxt
        # instantiate exchange class by name (e.g., ccxt.binance())
        ex_cls = getattr(ccxt, self._exchange_name)
        self._client = ex_cls()

    def _sleep(self, seconds: float):
        import time as _t
        (_time or _t).sleep(seconds)

    def _sleep_rate_limit(self):
        if self._rate_limit:
            rl = getattr(self._client, "rateLimit", 0) or 0
            if rl > 0:
                self._sleep(rl / 1000.0)

    def _emit(self, event: dict):
        ts = default_ts_fn(event)
        key = default_key_fn(event)
        msg = self.producer_topic.serialize(key=key, value=event, timestamp_ms=ts)
        self.produce(key=msg.key, value=msg.value, timestamp=msg.timestamp)

    def _handle_klines(self):
        for symbol in self._symbols:
            since = self._cursor.get(symbol)
            candles = self._client.fetchOHLCV(symbol, timeframe=self._interval, since=since, limit=1000)
            if candles:
                for c in candles:
                    ev = normalize_klines(c, self._interval or "")
                    # fill required exchange/symbol if normalize_klines didn't
                    ev.setdefault("exchange", self._exchange_name)
                    ev.setdefault("symbol", symbol)
                    self._emit(ev)
                # advance cursor to last close_time or open_time
                last = candles[-1]
                close_time = last[6] if len(last) > 6 else last[0]
                self._cursor[symbol] = close_time
            self._sleep_rate_limit()

    def _handle_trades(self):
        for symbol in self._symbols:
            since = self._cursor.get(symbol)
            try:
                trades = self._client.fetchTrades(symbol, since=since, limit=1000)
            except Exception as e:  # backoff then retry once for tests
                logger.warning(f"fetchTrades failed: {e}, backing off 1s")
                self._sleep(1.0)
                trades = self._client.fetchTrades(symbol, since=since, limit=1000)
            if trades:
                max_ts = since or 0
                for t in trades:
                    ev = normalize_trade(t) if self._normalize else t
                    ev.setdefault("exchange", self._exchange_name)
                    ev.setdefault("symbol", symbol)
                    self._emit(ev)
                    ts = default_ts_fn(ev) or 0
                    if ts > max_ts:
                        max_ts = ts
                self._cursor[symbol] = max_ts
            self._sleep_rate_limit()

    def _handle_orderbook(self):
        for symbol in self._symbols:
            ob = self._client.fetchOrderBook(symbol)
            ev = normalize_orderbook({"exchange": self._exchange_name, "symbol": symbol, **ob})
            self._emit(ev)
            self._sleep_rate_limit()
            if self._rest_poll_interval:
                self._sleep(self._rest_poll_interval)

    def run(self):
        # Single pass per run() for testability
        if self._mode == "klines":
            self._handle_klines()
        elif self._mode == "trades":
            self._handle_trades()
        elif self._mode == "orderbook":
            self._handle_orderbook()
        self.producer.flush()