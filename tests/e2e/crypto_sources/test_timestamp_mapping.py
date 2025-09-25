import types

import pytest

from quixstreams.sources.community.crypto import BinanceS3Source, CCXTSource, CryptofeedSource


class DummyS3:
    def __init__(self):
        self.calls = []
    def list_objects_v2(self, **kwargs):
        self.calls.append(("list_objects_v2", kwargs))
        return {"IsTruncated": False, "Contents": []}


@pytest.fixture
def mock_boto3(monkeypatch):
    dummy = DummyS3()
    
    class Mod(types.SimpleNamespace):
        def client(self, *args, **kwargs):
            return dummy
    import quixstreams.sources.community.crypto.binance_s3_source as mod
    monkeypatch.setattr(mod, "boto3", Mod())

    class DummyBC:
        UNSIGNED = object()
    class DummyCfg:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    monkeypatch.setattr(mod, "botocore", DummyBC())
    monkeypatch.setattr(mod, "BotoConfig", DummyCfg)
    return dummy


class DummyBody:
    def __init__(self, b): self._b=b
    def read(self): return self._b


class FakeTopic:
    name = "topic"
    def serialize(self, *, key=None, value=None, headers=None, timestamp_ms=None):
        return types.SimpleNamespace(key=key, value=value, timestamp=timestamp_ms)


class FakeProducer:
    def __init__(self):
        self.produced = []
    def produce(self, **kwargs):
        self.produced.append(kwargs)
    def flush(self, timeout=None):
        return 0


@pytest.fixture
def fake_topic_and_producer():
    def _attach(src):
        src._producer_topic = FakeTopic()
        prod = FakeProducer()
        src._producer = prod
        return prod
    return _attach


class TestCrossSourceTimestampMapping:
    def test_binance_s3_trades_and_klines_timestamps(self, monkeypatch, mock_boto3, fake_topic_and_producer):
        """trades use ts_event; klines fallback to close_time when timestamp not present."""
        import json, gzip

        # Provide two files: headerless klines CSV and jsonl trades
        def list_objects_v2(**kwargs):
            pref = kwargs["Prefix"]
            return {"IsTruncated": False, "Contents": [
                {"Key": "p/spot/daily/klines/BTCUSDT/2025-01-01/klines.csv"},
                {"Key": "p/spot/daily/trades/BTCUSDT/2025-01-01/trades.jsonl.gz"},
            ]}

        # Build bodies
        # klines headerless row -> open_time,open,high,low,close,volume,close_time
        kl_csv = "1000,10,12,9,11,100,1060\n".encode()
        # trades jsonl
        lines = [
            {"exchange":"binance","symbol":"BTCUSDT","price":100,"qty":1,"ts_event":2000},
        ]
        tr_gz = gzip.compress("\n".join(json.dumps(l) for l in lines).encode())

        def get_object(**kwargs):
            key = kwargs["Key"]
            if key.endswith("klines.csv"):
                return {"Body": DummyBody(kl_csv)}
            return {"Body": DummyBody(tr_gz)}

        dummy = mock_boto3
        dummy.list_objects_v2 = list_objects_v2
        dummy.get_object = get_object

        src = BinanceS3Source(bucket="b", prefix="p/")
        src._s3 = dummy
        prod = fake_topic_and_producer(src)
        src.run()

        # Expect two records: timestamps 1060 (klines) and 2000 (trades)
        ts = sorted([call["timestamp"] for call in prod.produced])
        assert ts == [1060, 2000]

    def test_ccxt_trades_timestamp_mapping(self, monkeypatch, fake_topic_and_producer):
        class FakeExchange:
            rateLimit = 0
            def fetchTrades(self, *args, **kwargs):
                return [{"exchange":"binance","symbol":"BTC/USDT","price":1,"qty":1,"timestamp":3000}]
            def fetchOHLCV(self, *args, **kwargs):
                return []
            def fetchOrderBook(self, *args, **kwargs):
                return {}

        mod = types.SimpleNamespace(binance=FakeExchange, exchanges=["binance"])
        monkeypatch.setitem(__import__("sys").modules, "ccxt", mod)

        src = CCXTSource(exchange="binance", mode="trades", symbols=["BTC/USDT"], rate_limit=False)
        prod = fake_topic_and_producer(src)
        src.setup()
        src.run()

        assert len(prod.produced) == 1
        assert prod.produced[0]["timestamp"] == 3000

    def test_cryptofeed_trades_timestamp_mapping(self, monkeypatch, fake_topic_and_producer):
        class FakeFeedHandler:
            def __init__(self):
                self.cb = {}
            def add_feed(self, *_, **kwargs):
                self.cb = kwargs.get("callbacks", {})
            def run(self):
                # Immediately invoke trade callback with a payload that has an upstream timestamp
                trade_ev = {"exchange":"binance","symbol":"BTC-USDT","price":1,"qty":1, "timestamp": 4000}
                # cryptofeed adapter normalizes via normalize_trade which maps timestamp->ts_event
                self.cb.get("trades")(trade_ev)

        monkeypatch.setitem(__import__("sys").modules, "cryptofeed", types.SimpleNamespace(FeedHandler=FakeFeedHandler))

        from quixstreams.sources.community.crypto.config import CryptofeedConfig
        cfg = CryptofeedConfig(exchanges=["binance"], channels=["trades"], symbols=["BTC-USDT"]) 
        src = CryptofeedSource(cfg)
        prod = fake_topic_and_producer(src)
        src.setup()
        src.run()

        assert len(prod.produced) == 1
        assert prod.produced[0]["timestamp"] == 4000