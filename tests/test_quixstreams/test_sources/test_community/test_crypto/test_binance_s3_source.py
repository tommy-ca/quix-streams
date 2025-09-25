import types
import pytest

from quixstreams.sources.community.crypto.binance_s3_source import BinanceS3Source


class DummyS3:
    def __init__(self):
        self.calls = []

    def list_objects_v2(self, **kwargs):
        self.calls.append(("list_objects_v2", kwargs))
        return {"KeyCount": 0}


@pytest.fixture
def mock_boto3(monkeypatch):
    dummy = DummyS3()

    class Mod(types.SimpleNamespace):
        def client(self, *args, **kwargs):
            return dummy

    import quixstreams.sources.community.crypto.binance_s3_source as mod
    monkeypatch.setattr(mod, "boto3", Mod())
    # Provide a fake botocore with UNSIGNED and Config
    class DummyBC:
        UNSIGNED = object()

    class DummyCfg:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    monkeypatch.setattr(mod, "botocore", DummyBC())
    monkeypatch.setattr(mod, "BotoConfig", DummyCfg)
    return dummy


def test_defaults_and_topic_name(mock_boto3):
    src = BinanceS3Source(bucket="b", prefix="p/")
    assert src.name == "crypto.source.binance.s3.trades"


def test_setup_unsigned_and_signed(monkeypatch, mock_boto3):
    # unsigned True
    src = BinanceS3Source(bucket="b", prefix="p", unsigned=True)
    src.setup()  # should call list_objects_v2 once
    assert mock_boto3.calls and mock_boto3.calls[0][0] == "list_objects_v2"

    # unsigned False
    mock_boto3.calls.clear()
    src2 = BinanceS3Source(bucket="b", prefix="p", unsigned=False)
    src2.setup()
    assert mock_boto3.calls and mock_boto3.calls[0][0] == "list_objects_v2"


def _fake_topic_and_producer(monkeypatch, src):
    # Fake topic.serialize returning a simple message-like object
    class Msg:
        def __init__(self, key, value, timestamp):
            self.key = key
            self.value = value
            self.timestamp = timestamp

    class FakeTopic:
        name = "topic"
        def serialize(self, *, key=None, value=None, headers=None, timestamp_ms=None):
            # encode value as dict passthrough for tests
            return Msg(key, value, timestamp_ms)

    class FakeProducer:
        def __init__(self):
            self.produced = []
        def produce(self, **kwargs):
            self.produced.append(kwargs)
        def flush(self, timeout=None):
            return 0

    src._producer_topic = FakeTopic()
    fake = FakeProducer()
    src._producer = fake
    return fake


def test_recursive_listing_and_ordering(monkeypatch, mock_boto3):
    # Prepare mock list pagination and get_object content
    import quixstreams.sources.community.crypto.binance_s3_source as mod

    keys_page1 = ["p/spot/daily/trades/BTCUSDT/2025-01-01/a-001.jsonl.gz", "p/a-010.jsonl.gz"]
    keys_page2 = ["p/a-002.jsonl.gz"]

    state = {"page": 0}
    def list_objects_v2(**kwargs):
        # simulate two pages with ContinuationToken
        if state["page"] == 0:
            state["page"] = 1
            return {"IsTruncated": True, "NextContinuationToken": "t", "Contents": [{"Key": k} for k in keys_page1]}
        else:
            return {"IsTruncated": False, "Contents": [{"Key": k} for k in keys_page2]}

    # Simple content per key: one JSON line with increasing ts_event
    import json, gzip
    def make_body(ts):
        data = json.dumps({"exchange":"binance","symbol":"BTCUSDT","price":100,"qty":1,"ts_event":ts})+"\n"
        return gzip.compress(data.encode())

    bodies = {
            "p/spot/daily/trades/BTCUSDT/2025-01-01/a-001.jsonl.gz": make_body(1000),
            "p/a-002.jsonl.gz": make_body(2000),
            "p/a-010.jsonl.gz": make_body(1500),
        }

    class DummyBody:
        def __init__(self, b): self._b=b
        def read(self): return self._b

    def get_object(**kwargs):
        key = kwargs["Key"]
        return {"Body": DummyBody(bodies[key])}

    dummy = mock_boto3
    dummy.calls.clear()
    dummy.list_objects_v2 = list_objects_v2
    # Not needed after implementation; kept for clarity

    src = BinanceS3Source(bucket="b", prefix="p")
    src._s3 = dummy  # bypass setup
    fakeprod = _fake_topic_and_producer(monkeypatch, src)
    dummy.get_object = get_object

    # Patch sleep to no-op via exposed alias
    monkeypatch.setattr(mod, "_time", types.SimpleNamespace(sleep=lambda s: None))

    src.run()
    # Ensure we produced 3 entries and metadata attached on any 'spot' path
    emitted_keys = [call["key"] for call in fakeprod.produced]
    assert emitted_keys == ["binance:BTCUSDT", "binance:BTCUSDT", "binance:BTCUSDT"]
    assert any(
        ("meta" in call["value"]) and call["value"]["meta"].get("market") == "spot"
        for call in fakeprod.produced
    )
    emitted_ts = [call["timestamp"] for call in fakeprod.produced]
    assert sorted(emitted_ts) == [1000, 1500, 2000]


# dataloader-specific tests moved to tests/.../test_crypto/binance_s3/test_dataloader.py
    # Monthly segment with no dates, prefix has no {date} placeholder
    import json, gzip
    def list_objects_v2(**kwargs):
        pref = kwargs["Prefix"]
        assert pref == "p/spot/monthly/klines/BTCUSDT/"
        return {"IsTruncated": False, "Contents": [{"Key": f"{pref}file.jsonl.gz"}]}

    class DummyBody:
        def __init__(self, b): self._b=b
        def read(self): return self._b

    def get_object(**kwargs):
        data = json.dumps({"exchange":"binance","symbol":"BTCUSDT","price":1,"qty":1,"ts_event":1})+"\n"
        import gzip as gz
        return {"Body": DummyBody(gz.compress(data.encode()))}

    dummy = mock_boto3
    dummy.list_objects_v2 = list_objects_v2
    dummy.get_object = get_object

    src = BinanceS3Source(
        bucket="b",
        prefix="p/",
        access_mode="templated_prefixes",
        prefix_template="p/{market}/{segment}/{datatype}/{symbol}/",
        market="spot",
        segments=["monthly"],
        datatypes_list=["klines"],
        symbols=["BTCUSDT"],
    )
    src._s3 = dummy
    fakeprod = _fake_topic_and_producer(monkeypatch, src)
    src.run()
    assert len(fakeprod.produced) == 1


    # UM futures, daily klines with interval in path
    import json, gzip
    expected = "p/um_futures/daily/klines/1m/BTCUSDT/2025-01-01/"
    def list_objects_v2(**kwargs):
        assert kwargs["Prefix"] == expected
        return {"IsTruncated": False, "Contents": [{"Key": f"{expected}file.jsonl.gz"}]}

    class DummyBody:
        def __init__(self, b): self._b=b
        def read(self): return self._b

    def get_object(**kwargs):
        data = json.dumps({"exchange":"binance","symbol":"BTCUSDT","price":1,"qty":1,"ts_event":1})+"\n"
        return {"Body": DummyBody(gzip.compress(data.encode()))}

    dummy = mock_boto3
    dummy.list_objects_v2 = list_objects_v2
    dummy.get_object = get_object

    src = BinanceS3Source(
        bucket="b",
        prefix="p/",
        access_mode="templated_prefixes",
        prefix_template="p/{market}/{segment}/{datatype}/{interval}/{symbol}/{date}/",
        market="um_futures",
        segments=["daily"],
        datatypes_list=["klines"],
        symbols=["BTCUSDT"],
        date_from="2025-01-01",
        date_to="2025-01-01",
        interval="1m",
    )
    src._s3 = dummy
    fakeprod = _fake_topic_and_producer(monkeypatch, src)
    src.run()
    assert len(fakeprod.produced) == 1


def test_templated_mode_requires_prefix_template(monkeypatch, mock_boto3):
    # missing prefix_template should raise ValueError in setup
    src = BinanceS3Source(
        bucket="b",
        prefix="p/",
        access_mode="templated_prefixes",
        market="spot",
        segments=["daily"],
        datatypes_list=["trades"],
        symbols=["BTCUSDT"],
        date_from="2025-01-01",
        date_to="2025-01-02",
    )
    with pytest.raises(ValueError):
        src.setup()


    # Generate prefixes for 2 symbols x 2 datatypes x 2 dates (daily) => 8 prefixes
    import json, gzip
    expected_prefixes = set()
    symbols = ["BTCUSDT","ETHUSDT"]
    datatypes = ["trades","klines"]
    dates = ["2025-01-01","2025-01-02"]
    def list_objects_v2(**kwargs):
        pref = kwargs["Prefix"]
        assert pref in expected_prefixes, f"Unexpected prefix {pref}"
        # return one file per prefix
        return {"IsTruncated": False, "Contents": [{"Key": f"{pref}file.jsonl.gz"}]}

    def make_body():
        data = json.dumps({"exchange":"binance","symbol":"X","price":1,"qty":1,"ts_event":1})+"\n"
        return gzip.compress(data.encode())

    class DummyBody:
        def __init__(self, b): self._b=b
        def read(self): return self._b

    def get_object(**kwargs):
        return {"Body": DummyBody(make_body())}

    # build expected prefixes
    for dt in datatypes:
        for sym in symbols:
            for d in dates:
                expected_prefixes.add(f"p/spot/daily/{dt}/{sym}/{d}/")

    dummy = mock_boto3
    dummy.list_objects_v2 = list_objects_v2
    dummy.get_object = get_object

    src = BinanceS3Source(
        bucket="b",
        prefix="p/",
        access_mode="templated_prefixes",
        prefix_template="p/{market}/{segment}/{datatype}/{symbol}/{date}/",
        market="spot",
        segments=["daily"],
        datatypes_list=datatypes,
        symbols=symbols,
        date_from="2025-01-01",
        date_to="2025-01-02",
    )
    src._s3 = dummy
    fakeprod = _fake_topic_and_producer(monkeypatch, src)
    src.run()
    # 8 prefixes -> 8 files -> 8 produced records
    assert len(fakeprod.produced) == 8


def test_parsers_json_gz_line_delimited(monkeypatch, mock_boto3):
    import quixstreams.sources.community.crypto.binance_s3_source as mod
    import json, gzip, zipfile, io

    def list_one(**kwargs):
        return {"IsTruncated": False, "Contents": [{"Key": "p/data.jsonl.gz"}, {"Key": "p/zip.jsonl.zip"}, {"Key": "p/klines.csv"}]}

    lines = [
        {"exchange":"binance","symbol":"BTCUSDT","price":100,"qty":1,"ts_event":1},
        {"exchange":"binance","symbol":"BTCUSDT","price":101,"qty":2,"ts_event":2},
    ]
    buf_gz = gzip.compress("\n".join(json.dumps(l) for l in lines).encode())

    # zip with a jsonl file
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('inner.jsonl', "\n".join(json.dumps(l) for l in lines))
    buf_zip = zbuf.getvalue()

    # CSV klines row: [open_time, open, high, low, close, volume, close_time]
    csv_text = "open_time,open,high,low,close,volume,close_time\n1000,10,12,9,11,100,1060\n"
    buf_csv = csv_text.encode()

    class DummyBody: 
        def __init__(self, b): self._b=b
        def read(self): return self._b

    def get_object(**kwargs):
        key = kwargs["Key"].split('/')[-1]
        if key.endswith('gz'):
            return {"Body": DummyBody(buf_gz)}
        if key.endswith('zip'):
            return {"Body": DummyBody(buf_zip)}
        if key.endswith('csv'):
            return {"Body": DummyBody(buf_csv)}
        raise AssertionError("unexpected key")

    dummy = mock_boto3
    dummy.calls.clear()
    dummy.list_objects_v2 = list_one
    dummy.get_object = get_object

    src = BinanceS3Source(bucket="b", prefix="p")
    src._s3 = dummy
    fakeprod = _fake_topic_and_producer(monkeypatch, src)

    src.run()
    # gz two, zip two, csv one → total 5
    assert len(fakeprod.produced) == 5
    # verify at least one record uses close_time as ts (CSV klines)
    assert any(call["timestamp"] == 1060 for call in fakeprod.produced)

def test_headerless_trades_csv_parsing(monkeypatch, mock_boto3):
    import csv
    # one object with headerless trades CSV rows
    def list_one(**kwargs):
        return {"IsTruncated": False, "Contents": [{"Key": "p/spot/daily/trades/BTCUSDT/2025-01-01/trades.csv"}]}

    # rows: id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch
    rows = [
        ["1", "100.5", "0.01", "1.005", "1700000000", "1", "true"],
        ["2", "100.6", "0.02", "2.012", "1700000010", "0", "false"],
    ]
    csv_text = "\n".join([",".join(r) for r in rows])

    class DummyBody:
        def __init__(self, b): self._b=b
        def read(self): return self._b

    def get_object(**kwargs):
        return {"Body": DummyBody(csv_text.encode())}

    dummy = mock_boto3
    dummy.list_objects_v2 = list_one
    dummy.get_object = get_object

    src = BinanceS3Source(bucket="b", prefix="p/")
    src._s3 = dummy
    fakeprod = _fake_topic_and_producer(monkeypatch, src)

    src.run()
    # two rows -> two produced
    assert len(fakeprod.produced) == 2
    # symbol inferred from path
    assert all(call["value"].get("symbol") == "BTCUSDT" for call in fakeprod.produced)
    # timestamps from column index 4
    assert [call["timestamp"] for call in fakeprod.produced] == [1700000000, 1700000010]


def test_nested_gz_inside_zip_parsing(monkeypatch, mock_boto3):
    import json, gzip, zipfile, io

    def list_one(**kwargs):
        return {"IsTruncated": False, "Contents": [{"Key": "p/nested.zip"}]}

    # Build inner gzipped jsonl
    lines = [
        {"exchange":"binance","symbol":"BTCUSDT","price":100,"qty":1,"ts_event":1},
        {"exchange":"binance","symbol":"BTCUSDT","price":101,"qty":2,"ts_event":2},
    ]
    inner_jsonl = "\n".join(json.dumps(l) for l in lines).encode()
    inner_gz = gzip.compress(inner_jsonl)

    # Create zip containing a .jsonl.gz file
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('inner.jsonl.gz', inner_gz)
    buf_zip = zbuf.getvalue()

    class DummyBody:
        def __init__(self, b): self._b=b
        def read(self): return self._b

    def get_object(**kwargs):
        return {"Body": DummyBody(buf_zip)}

    dummy = mock_boto3
    dummy.list_objects_v2 = list_one
    dummy.get_object = get_object

    src = BinanceS3Source(bucket="b", prefix="p")
    src._s3 = dummy
    fakeprod = _fake_topic_and_producer(monkeypatch, src)

    src.run()
    # two jsonl lines inside nested gz->zip -> two produced
    assert len(fakeprod.produced) == 2


def test_replay_speed_real_time_and_asap(monkeypatch, mock_boto3):
    import quixstreams.sources.community.crypto.binance_s3_source as mod
    import json, gzip

    def list_one(**kwargs):
        return {"IsTruncated": False, "Contents": [{"Key": "p/data.jsonl.gz"}]}

    lines = [
        {"exchange":"binance","symbol":"BTCUSDT","price":100,"qty":1,"ts_event":1000},
        {"exchange":"binance","symbol":"BTCUSDT","price":100,"qty":1,"ts_event":2000},
    ]
    buf = gzip.compress("\n".join(json.dumps(l) for l in lines).encode())
    class DummyBody: 
        def read(self): return buf
    def get_object(**kwargs):
        return {"Body": DummyBody()}

    calls = []
    monkeypatch.setattr(mod, "_time", types.SimpleNamespace(sleep=lambda s: calls.append(s)))

    dummy = mock_boto3
    dummy.calls.clear(); dummy.list_objects_v2 = list_one; dummy.get_object = get_object

    # replay_speed=1.0 should sleep approx 1.0
    src = BinanceS3Source(bucket="b", prefix="p", unsigned=True,)
    src._s3 = dummy
    fakeprod = _fake_topic_and_producer(monkeypatch, src)
    src._replay_speed = 1.0
    src.run()
    assert calls and abs(calls[0] - 1.0) < 1e-6

    # replay_speed=0 should sleep not at all
    calls.clear()
    src2 = BinanceS3Source(bucket="b", prefix="p")
    src2._s3 = dummy
    fakeprod2 = _fake_topic_and_producer(monkeypatch, src2)
    src2._replay_speed = 0.0
    src2.run()
    assert calls == []


    # Ensure that dry_run does not call get_object and produces nothing
    calls = {"listed": 0, "fetched": 0}
    def list_objects_v2(**kwargs):
        calls["listed"] += 1
        pref = kwargs["Prefix"]
        return {"IsTruncated": False, "Contents": [{"Key": f"{pref}file1.jsonl.gz"}, {"Key": f"{pref}file2.jsonl.gz"}]}
    def get_object(**kwargs):
        calls["fetched"] += 1
        raise AssertionError("should not fetch in dry_run")

    dummy = mock_boto3
    dummy.list_objects_v2 = list_objects_v2
    dummy.get_object = get_object

    src = BinanceS3Source(
        bucket="b",
        prefix="p/",
        access_mode="templated_prefixes",
        prefix_template="p/{market}/{segment}/{datatype}/{symbol}/{date}/",
        market="spot",
        segments=["daily"],
        datatypes_list=["trades"],
        symbols=["BTCUSDT"],
        date_from="2025-01-01",
        date_to="2025-01-02",
        dry_run=True,
    )
    src._s3 = dummy
    fakeprod = _fake_topic_and_producer(monkeypatch, src)
    src.run()
    assert calls["listed"] == 2  # two dates
    assert calls["fetched"] == 0
    assert len(fakeprod.produced) == 0


def test_checksum_warn_and_strict(monkeypatch, mock_boto3):
    import hashlib
    # Prepare data and wrong checksum (valid JSONL to ensure record production in warn mode)
    data = b'{"exchange":"binance","symbol":"BTCUSDT","price":1,"qty":1,"ts_event":1}\n'
    wrong_md5 = hashlib.md5(b"different").hexdigest()

    class DummyBody:
        def __init__(self, b): self._b=b
        def read(self): return self._b

    def list_one(**kwargs):
        return {"IsTruncated": False, "Contents": [{"Key": "p/data.jsonl"}]}

    def get_object_warn(**kwargs):
        k = kwargs["Key"]
        if k.endswith(".CHECKSUM"):
            return {"Body": DummyBody((wrong_md5+"\n").encode())}
        return {"Body": DummyBody(data)}

    dummy = mock_boto3
    dummy.list_objects_v2 = list_one
    dummy.get_object = get_object_warn

    # warn mode: should not raise
    src = BinanceS3Source(bucket="b", prefix="p", checksum_mode="warn")
    src._s3 = dummy
    fakeprod = _fake_topic_and_producer(monkeypatch, src)
    src.run()
    assert len(src._producer.produced) == 1

    # strict mode: should raise
    def get_object_strict(**kwargs):
        return get_object_warn(**kwargs)

    dummy.get_object = get_object_strict
    src2 = BinanceS3Source(bucket="b", prefix="p", checksum_mode="strict")
    src2._s3 = dummy
    fakeprod2 = _fake_topic_and_producer(monkeypatch, src2)
    with pytest.raises(Exception):
        src2.run()


def test_error_handling_and_retries(monkeypatch, mock_boto3):
    import quixstreams.sources.community.crypto.binance_s3_source as mod
    import json, gzip

    # two files to prove we proceed after retry
    def list_two(**kwargs):
        return {"IsTruncated": False, "Contents": [{"Key": "p/a.jsonl.gz"},{"Key":"p/b.jsonl.gz"}]}

    buf = gzip.compress(json.dumps({"exchange":"binance","symbol":"BTCUSDT","price":1,"qty":1,"ts_event":1}).encode()+b"\n")

    class DummyBody: 
        def __init__(self, b): self._b=b
        def read(self): return self._b

    attempts = {"a":0}
    def get_object(**kwargs):
        key = kwargs["Key"].split("/")[-1]
        if key == "a.jsonl.gz" and attempts["a"] == 0:
            attempts["a"] += 1
            # simulate transient error
            class E(Exception): pass
            raise E("temporary fail")
        return {"Body": DummyBody(buf)}

    sleeps = []
    monkeypatch.setattr(mod, "_time", types.SimpleNamespace(sleep=lambda s: sleeps.append(s)))

    dummy = mock_boto3
    dummy.calls.clear(); dummy.list_objects_v2 = list_two; dummy.get_object = get_object

    src = BinanceS3Source(bucket="b", prefix="p")
    src._s3 = dummy
    fakeprod = _fake_topic_and_producer(monkeypatch, src)

    # patch backoff generator to controlled values
    monkeypatch.setattr(mod, "_backoff_sequence", [0.1, 0.2, 0.4])

    src.run()
    # We should have produced 2 records despite first get_object failing once
    assert len(fakeprod.produced) == 2
    assert sleeps and sleeps[0] == 0.1
