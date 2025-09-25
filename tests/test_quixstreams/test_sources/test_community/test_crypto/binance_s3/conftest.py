import types
import pytest

# Shared fixture to attach fake producer/topic to a source under test

@pytest.fixture
def fake_topic_and_producer(monkeypatch):
    def _attach(src):
        class Msg:
            def __init__(self, key, value, timestamp):
                self.key = key
                self.value = value
                self.timestamp = timestamp

        class FakeTopic:
            name = "topic"
            def serialize(self, *, key=None, value=None, headers=None, timestamp_ms=None):
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
    return _attach


# Provide a local mock_boto3 fixture for this test package, mirroring the one used in test_binance_s3_source
class DummyS3:
    def __init__(self):
        self.calls = []
    def list_objects_v2(self, **kwargs):
        self.calls.append(("list_objects_v2", kwargs))
        # default empty listing; tests override methods as needed
        return {"KeyCount": 0}


@pytest.fixture
def mock_boto3(monkeypatch):
    dummy = DummyS3()

    class Mod(types.SimpleNamespace):
        def client(self, *args, **kwargs):
            return dummy

    import quixstreams.sources.community.crypto.binance_s3_source as mod
    monkeypatch.setattr(mod, "boto3", Mod())

    # Provide a fake botocore and Config alias
    class DummyBC:
        UNSIGNED = object()

    class DummyCfg:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    monkeypatch.setattr(mod, "botocore", DummyBC())
    monkeypatch.setattr(mod, "BotoConfig", DummyCfg)
    return dummy
