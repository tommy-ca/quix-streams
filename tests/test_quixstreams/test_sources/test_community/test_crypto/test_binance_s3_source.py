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