import types
import sys
import pytest

from quixstreams.sources.community.crypto.cryptofeed_source import CryptofeedSource


def test_import_guard_when_missing_cryptofeed(monkeypatch):
    # Simulate cryptofeed not installed by forcing import failure
    saved = sys.modules.pop("cryptofeed", None)
    import importlib
    monkeypatch.setattr(importlib, "import_module", lambda name: (_ for _ in ()).throw(ModuleNotFoundError()) if name == "cryptofeed" else __import__(name))
    try:
        with pytest.raises(ImportError):
            CryptofeedSource(exchanges=["BINANCE"], channels=["trades"], symbols=["BTC-USDT"])  # should raise
    finally:
        if saved is not None:
            sys.modules["cryptofeed"] = saved


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


def test_trade_event_mapping(monkeypatch):
    # Fake cryptofeed module
    class FakeFeedHandler:
        def __init__(self):
            self.cb = {};
            self.running = False
        def add_feed(self, exchange, channels=None, symbols=None, callbacks=None):
            # store callbacks dict like {"trades": fn, "ticker": fn2}
            self.cb = callbacks or {}
        def run(self):
            self.running = True
        def stop(self):
            self.running = False

    fake_mod = types.SimpleNamespace(FeedHandler=FakeFeedHandler)
    monkeypatch.setitem(sys.modules, "cryptofeed", fake_mod)

    src = CryptofeedSource(exchanges=["BINANCE"], channels=["trades"], symbols=["BTC-USDT"], normalize=True)
    _wire_fake_io(src)
    src.setup()

    # trigger a fake trade event via stored callback
    event = {"exchange":"binance","symbol":"BTC-USDT","id":"t1","side":"buy","price":100.0,"amount":0.1,"timestamp":123}
    src._fh.cb["trades"](event)  # invoke callback directly

    # Verify produced message
    assert len(src._producer.produced) == 1
    p = src._producer.produced[0]
    assert p["key"] == "binance:BTC-USDT"
    assert p["value"]["price"] == 100.0
    assert p["timestamp"] == 123


def test_reconnect_backoff_and_shutdown(monkeypatch):
    # FeedHandler.run will raise once, then succeed
    class FakeFeedHandler:
        def __init__(self):
            self.cb = {}
            self._runs = 0
            self.running = False
        def add_feed(self, exchange, channels=None, symbols=None, callbacks=None):
            self.cb = callbacks or {}
        def run(self):
            self._runs += 1
            if self._runs == 1:
                raise RuntimeError("ws failed")
            self.running = True
        def stop(self):
            self.running = False

    fake_mod = types.SimpleNamespace(FeedHandler=FakeFeedHandler)
    monkeypatch.setitem(sys.modules, "cryptofeed", fake_mod)

    # Patch time alias to capture sleeps
    import quixstreams.sources.community.crypto.cryptofeed_source as mod
    sleeps = []
    monkeypatch.setattr(mod, "_time", types.SimpleNamespace(sleep=lambda s: sleeps.append(s)))

    src = CryptofeedSource(exchanges=["BINANCE"], channels=["trades","ticker"], symbols=["BTC-USDT"], reconnect=True, max_retries=2)
    _wire_fake_io(src)
    src.setup()

    # Run in a try/catch-less environment; run() should handle internal retry once
    src.run()
    # We expect a backoff sleep to have occurred once
    assert sleeps, "expected backoff sleep to occur"

    # Now test shutdown calls stop()
    fh = src._fh
    assert fh.running is True
    src.stop()
    assert fh.running is False