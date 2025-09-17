import json
import gzip

import pytest

from quixstreams.sources.community.crypto.binance_s3_source import BinanceS3Source


def test_dataloader_monthly_no_dates(monkeypatch, mock_boto3, fake_topic_and_producer):
    # Monthly segment with no dates, prefix has no {date} placeholder
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
    fakeprod = fake_topic_and_producer(src)
    src.run()
    assert len(fakeprod.produced) == 1


def test_dataloader_futures_with_interval(monkeypatch, mock_boto3, fake_topic_and_producer):
    # UM futures, daily klines with interval in path
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
    fakeprod = fake_topic_and_producer(src)
    src.run()
    assert len(fakeprod.produced) == 1


def test_dataloader_prefix_generation_and_listing(monkeypatch, mock_boto3, fake_topic_and_producer):
    # Generate prefixes for 2 symbols x 2 datatypes x 2 dates (daily) => 8 prefixes
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
    fakeprod = fake_topic_and_producer(src)
    src.run()
    # 8 prefixes -> 8 files -> 8 produced records
    assert len(fakeprod.produced) == 8


def test_dataloader_dry_run_lists_without_fetching(monkeypatch, mock_boto3, fake_topic_and_producer):
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
    fakeprod = fake_topic_and_producer(src)
    src.run()
    assert calls["listed"] == 2  # two dates
    assert calls["fetched"] == 0
    assert len(fakeprod.produced) == 0
