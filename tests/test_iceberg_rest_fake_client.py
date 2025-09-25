import types

from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
from quixstreams.sinks.community.iceberg_rest.config import create_local_rest_config
from tests.helpers.fakes import FakeRESTCatalogClient


def test_sink_uses_fake_client_for_local_testing(monkeypatch):
    cfg = create_local_rest_config(table_name="fake_table")
    sink = IcebergRESTSink(config=cfg, batch_size=2)

    # swap real client for fake to avoid network
    fake = FakeRESTCatalogClient()
    fake.table_name = cfg.table_name
    fake.warehouse_id = cfg.warehouse_id
    fake.catalog_uri = cfg.catalog_uri
    monkeypatch.setattr(sink, "client", fake)

    # write a couple of small records; send in one call to avoid dict.items() confusion
    sink.write([
        {"a": 1},
        {"a": 2},
    ])  # should flush due to batch size

    # ensure fake received a commit descriptor
    assert fake.posts, "expected fake client to receive a post"
    posted = fake.posts[-1]["records"][0]
    assert posted["table"].endswith("." + cfg.table_name)
    assert posted["record_count"] == 2

    sink.close()
    assert fake.closed is True