"""
Minimal example demonstrating how to use a fake REST catalog client with IcebergRESTSink
for local testing without a running REST catalog service.
"""
from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
from quixstreams.sinks.community.iceberg_rest.config import create_local_rest_config

try:
    # Optional: only for type hints here; not required to run the example
    from tests.helpers.fakes import FakeRESTCatalogClient  # in-repo test helper
except Exception:
    FakeRESTCatalogClient = None  # type: ignore


def main() -> None:
    cfg = create_local_rest_config(table_name="events")
    sink = IcebergRESTSink(config=cfg, batch_size=2)

    # Swap the HTTP client with an in-memory fake
    if FakeRESTCatalogClient is not None:
        fake = FakeRESTCatalogClient()
        fake.table_name = cfg.table_name
        fake.warehouse_id = cfg.warehouse_id
        fake.catalog_uri = cfg.catalog_uri
        sink.client = fake  # type: ignore[attr-defined]

    sink.write({"event": "login", "user": "alice"})
    sink.write({"event": "logout", "user": "alice"})  # triggers flush at batch_size=2
    sink.close()


if __name__ == "__main__":
    main()