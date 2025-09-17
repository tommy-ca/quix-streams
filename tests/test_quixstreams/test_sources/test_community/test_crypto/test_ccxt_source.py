import types
import sys
import pytest

from quixstreams.sources.community.crypto.ccxt_source import CCXTSource


def test_import_guard_for_ccxt(monkeypatch):
    # Force import failure
    saved = sys.modules.pop("ccxt", None)
    import importlib
    monkeypatch.setattr(importlib, "import_module", lambda name: (_ for _ in ()).throw(ModuleNotFoundError()) if name == "ccxt" else __import__(name))
    try:
        with pytest.raises(ImportError):
            CCXTSource(exchange="binance", mode="klines", interval="1m", symbols=["BTC/USDT"])  # should raise
    finally:
        if saved is not None:
            sys.modules["ccxt"] = saved


class FakeProducer:
    def __init__(self):
        self.produced = []
    def produce(self, **kwargs):
        self.produced.append(kwargs)
    def flush(self, timeout=None):
        return 0


class FakeTopic:
    name = "topic"
    class Msg:
        def __init__(self, key, value, ts):
            self.key = key; self.value = value; self.timestamp = ts
    def serialize(self, *, key=None, value=None, headers=None, timestamp_ms=None):
        return self.Msg(key, value, timestamp_ms)


def _wire_fake_io(src):
    src._producer = FakeProducer()
    src._producer_topic = FakeTopic()


def test_topic_naming_convention():
    src = CCXTSource(exchange="binance", mode="klines", interval="1m", symbols=["BTC/USDT"])
    assert src.name == "crypto.source.ccxt.klines_1m"
    src2 = CCXTSource(exchange="binance", mode="trades", symbols=["BTC/USDT"]) 
    assert src2.name == "crypto.source.ccxt.trades"


def test_mode_klines_cursor_advance_and_dedup(monkeypatch):
    class FakeExchange:
        rateLimit = 0
        def __init__(self):
            self.calls = []
        def fetchOHLCV(self, symbol, timeframe=None, since=None, limit=None):
            self.calls.append((symbol, timeframe, since, limit))
            # return two candles; since ignored first call
            if since is None:
                return [
                    [1000, 10, 12, 9, 11, 100, 1060, symbol, "binance"],
                    [1060, 11, 13, 10, 12, 120, 1120, symbol, "binance"],
                ]
            # on second run, nothing new
            return []

    fake_mod = types.SimpleNamespace(binance=FakeExchange)
    monkeypatch.setitem(sys.modules, "ccxt", fake_mod)

    src = CCXTSource(exchange="binance", mode="klines", interval="1m", symbols=["BTC/USDT"]) 
    _wire_fake_io(src)
    src.setup()
    src.run()
    # produced 2 candles
    assert len(src._producer.produced) == 2

    # Run again → should fetch with since and produce none
    src.run()
    # still 2
    assert len(src._producer.produced) == 2


def test_mode_trades_cursor_by_timestamp_and_trade_id(monkeypatch):
    class FakeExchange:
        rateLimit = 0
        def __init__(self):
            self.calls = 0
        def fetchTrades(self, symbol, since=None, limit=None):
            self.calls += 1
            if self.calls == 1:
                return [
                    {"exchange":"binance","symbol":symbol,"id":"t1","price":100.0,"amount":0.1,"timestamp":1000},
                    {"exchange":"binance","symbol":symbol,"id":"t2","price":100.1,"amount":0.2,"timestamp":1100},
                ]
            return []

    fake_mod = types.SimpleNamespace(binance=FakeExchange)
    monkeypatch.setitem(sys.modules, "ccxt", fake_mod)

    src = CCXTSource(exchange="binance", mode="trades", symbols=["BTC/USDT"]) 
    _wire_fake_io(src)
    src.setup()
    src.run()
    assert len(src._producer.produced) == 2
    # second run produces none
    src.run()
    assert len(src._producer.produced) == 2


def test_mode_orderbook_snapshot_polling(monkeypatch):
    class FakeExchange:
        rateLimit = 0
        def fetchOrderBook(self, symbol):
            return {"exchange":"binance","symbol":symbol,"bids":[[100,1]],"asks":[[101,1]],"timestamp":2000}

    fake_mod = types.SimpleNamespace(binance=FakeExchange)
    monkeypatch.setitem(sys.modules, "ccxt", fake_mod)

    src = CCXTSource(exchange="binance", mode="orderbook", symbols=["BTC/USDT"], rest_poll_interval=0.0) 
    _wire_fake_io(src)
    src.setup()
    src.run()
    assert len(src._producer.produced) == 1


def test_rate_limit_respected_and_backoff_on_http_errors(monkeypatch):
    import quixstreams.sources.community.crypto.ccxt_source as mod
    sleeps = []
    monkeypatch.setattr(mod, "_time", types.SimpleNamespace(sleep=lambda s: sleeps.append(s)))

    class FakeExchange:
        rateLimit = 1000  # 1 second
        def __init__(self):
            self._called = 0
        def fetchTrades(self, symbol, since=None, limit=None):
            self._called += 1
            if self._called == 1:
                raise RuntimeError("rate limited")
            return []

    fake_mod = types.SimpleNamespace(binance=FakeExchange)
    monkeypatch.setitem(sys.modules, "ccxt", fake_mod)

    src = CCXTSource(exchange="binance", mode="trades", symbols=["BTC/USDT"], rate_limit=True) 
    _wire_fake_io(src)
    src.setup()
    src.run()

    # Expect a backoff sleep and a rate limit sleep at least
    assert any(abs(s - 1.0) < 1e-6 for s in sleeps)