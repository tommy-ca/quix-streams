"""Integration-level smoke tests for crypto sources using compatibility configs."""

import importlib
import sys
import types

import pytest

from quixstreams.sources.community.crypto import (
    CryptofeedSource,
    CCXTSource,
    BinanceS3Source,
)
from quixstreams.sources.community.crypto.config import (
    CryptofeedConfig,
    CCXTConfig,
    BinanceS3Config,
    APIKeyAuth,
    AWSAuth,
    RetryConfig,
)
from quixstreams.sources.community.crypto.errors import CryptoSourceDependencyError


class TestConfigBasedConstruction:
    def test_cryptofeed_source_uses_config(self):
        config = CryptofeedConfig(
            exchanges=["BINANCE"],
            channels=["TRADES"],
            symbols=["BTC-USDT"],
            auth_provider=APIKeyAuth(api_key="key"),
            retry_config=RetryConfig(max_retries=4),
        )

        source = CryptofeedSource(config)
        assert source._config.exchanges == ["binance"]
        assert source._config.channels == ["trades"]

    def test_ccxt_source_uses_config(self):
        config = CCXTConfig(
            exchange="binance",
            mode="trades",
            symbols=["BTC/USDT"],
        )

        source = CCXTSource(config)
        assert source._config.exchange == "binance"
        assert source._config.mode == "trades"

    def test_binance_s3_source_uses_config(self):
        config = BinanceS3Config(
            bucket="binance-public-data",
            prefix="data/spot/daily/trades/",
            unsigned=True,
        )

        source = BinanceS3Source(config)
        assert source._config.bucket == "binance-public-data"
        assert source._config.unsigned is True

    def test_kwargs_path_emits_deprecation(self):
        with pytest.warns(DeprecationWarning):
            CryptofeedSource(exchanges=["binance"], channels=["trades"])
        with pytest.warns(DeprecationWarning):
            CCXTSource(exchange="binance", mode="trades", symbols=["BTC/USDT"])
        with pytest.warns(DeprecationWarning):
            BinanceS3Source(bucket="bucket", prefix="prefix")


class TestDependencyErrors:
    def test_missing_cryptofeed_dependency(self, monkeypatch):
        original_import = importlib.import_module

        def fake_import(name, *args, **kwargs):
            if name == "cryptofeed":
                raise ImportError
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(importlib, "import_module", fake_import)
        config = CryptofeedConfig(exchanges=["binance"], channels=["trades"])

        with pytest.raises(CryptoSourceDependencyError) as excinfo:
            CryptofeedSource(config)
        assert excinfo.value.package == "cryptofeed"

    def test_missing_ccxt_dependency(self, monkeypatch):
        original_import = importlib.import_module

        def fake_import(name, *args, **kwargs):
            if name == "ccxt":
                raise ImportError
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(importlib, "import_module", fake_import)
        config = CCXTConfig(exchange="binance", mode="trades", symbols=["BTC/USDT"])

        with pytest.raises(CryptoSourceDependencyError) as excinfo:
            CCXTSource(config)
        assert excinfo.value.package == "ccxt"

    def test_missing_boto3_dependency(self, monkeypatch):
        config = BinanceS3Config(bucket="bucket", prefix="prefix")
        monkeypatch.setattr("quixstreams.sources.community.crypto.binance_s3_source._BOTO3_AVAILABLE", False)
        monkeypatch.setattr("quixstreams.sources.community.crypto.binance_s3_source.boto3", None)

        with pytest.raises(CryptoSourceDependencyError) as excinfo:
            BinanceS3Source(config)
        assert excinfo.value.package == "boto3"


class TestRuntimeBehaviour:
    def test_ccxt_source_handles_rate_limit_error(self, monkeypatch):
        class FakeExchange:
            rateLimit = 1000

            def __init__(self):
                self._called = 0

            def fetchTrades(self, *_, **__):
                self._called += 1
                if self._called == 1:
                    raise RuntimeError("rate limited")
                return []

            def fetchOHLCV(self, *_, **__):
                return []

            def fetchOrderBook(self, *_, **__):
                return {}

        mod = types.SimpleNamespace(binance=FakeExchange)
        monkeypatch.setitem(sys.modules, "ccxt", mod)

        src = CCXTSource(exchange="binance", mode="trades", symbols=["BTC/USDT"])
        src._producer = types.SimpleNamespace(produce=lambda **_: None, flush=lambda: None)
        src._producer_topic = types.SimpleNamespace(serialize=lambda **kwargs: types.SimpleNamespace(
            key=kwargs.get("key"), value=kwargs.get("value"), timestamp=kwargs.get("timestamp_ms")
        ))

        src.setup()
        src.run()

    def test_cryptofeed_source_retry_loop(self, monkeypatch):
        class FakeFeedHandler:
            def __init__(self):
                self.cb = {}
                self._runs = 0
                self.running = False

            def add_feed(self, *_, **kwargs):
                self.cb = kwargs.get("callbacks", {})

            def run(self):
                self._runs += 1
                if self._runs == 1:
                    raise RuntimeError("ws failed")
                self.running = True

            def stop(self):
                self.running = False

        monkeypatch.setitem(sys.modules, "cryptofeed", types.SimpleNamespace(FeedHandler=FakeFeedHandler))
        monkeypatch.setattr(
            "quixstreams.sources.community.crypto.retry.time.sleep",
            lambda *_: None,
        )

        config = CryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"],
            symbols=["BTC-USDT"],
            reconnect=True,
            retry_config=RetryConfig(base_delay=0.2, max_retries=2),
        )
        src = CryptofeedSource(config)
        src._producer = types.SimpleNamespace(produce=lambda **_: None, flush=lambda: None)
        src._producer_topic = types.SimpleNamespace(serialize=lambda **kwargs: types.SimpleNamespace(
            key=kwargs.get("key"), value=kwargs.get("value"), timestamp=kwargs.get("timestamp_ms")
        ))

        src.setup()
        src.run()
        src.stop()
