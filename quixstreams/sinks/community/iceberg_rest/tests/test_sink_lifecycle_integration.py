"""Integration tests between IcebergRESTSink and TableLifecycleManager."""

from __future__ import annotations

from typing import List

import pytest

from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_config
from quixstreams.sinks.community.iceberg_rest.config import (
    CatalogConfig,
    IcebergConfig,
    StorageConfig,
    StorageProvider,
)


class RecordingManager:
    def __init__(self, **_: object) -> None:
        self.calls: List[tuple[str, dict]] = []

    def ensure_table(self, *, table_identifier: str, schema_descriptor: dict):
        self.calls.append((table_identifier, schema_descriptor))
        return object()


class RecordingClient:
    def __init__(self, *args, **kwargs) -> None:  # noqa: D401
        self.calls: List[List[dict]] = []

    def post_records(self, records):
        self.calls.append(records)

    def close(self) -> None:
        return None


def _make_sink(monkeypatch: pytest.MonkeyPatch, manager: RecordingManager) -> IcebergRESTSink:
    monkeypatch.setattr(
        "quixstreams.sinks.community.iceberg_rest.TableLifecycleManager",
        lambda **kwargs: manager,
    )
    monkeypatch.setattr(
        "quixstreams.sinks.community.iceberg_rest.sink.TableLifecycleManager",
        lambda **kwargs: manager,
    )
    monkeypatch.setattr(
        "quixstreams.sinks.community.iceberg_rest.sink.RESTCatalogClient",
        RecordingClient,
    )
    config = create_local_config(table_name="trades")
    sink = IcebergRESTSink(config=config)
    sink.setup()
    return sink


def test_sink_ensures_table_before_flush(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = RecordingManager()
    sink = _make_sink(monkeypatch, manager)

    sink.write([{"id": 1, "price": 5.0}])
    sink.flush()

    assert manager.calls, "expected manager to be invoked"
    identifier, descriptor = manager.calls[0]
    assert identifier == "local.trades"
    field_names = {field["name"] for field in descriptor["fields"]}
    assert {"id", "price"}.issubset(field_names)


def test_sink_updates_schema_descriptor_on_new_field(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = RecordingManager()
    sink = _make_sink(monkeypatch, manager)

    sink.write([{"id": 1}])
    sink.flush()
    sink.write([{"id": 2, "price": 9.5}])
    sink.flush()

    assert len(manager.calls) >= 2
    _, descriptor_initial = manager.calls[0]
    _, descriptor_after = manager.calls[-1]

    initial_fields = {field["name"] for field in descriptor_initial["fields"]}
    evolved_fields = {field["name"] for field in descriptor_after["fields"]}

    assert "price" not in initial_fields
    assert "price" in evolved_fields


def test_sink_ensures_pyiceberg_table_when_memory_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("pyiceberg")
    from pyiceberg.table import Table  # type: ignore

    catalog = CatalogConfig(uri="memory://catalog", warehouse_id="memory")
    storage = StorageConfig(
        provider=StorageProvider.MINIO,
        region="us-east-1",
        endpoint_url="http://localhost:9000",
        access_key_id="access",
        secret_access_key="secret",
    )
    config = IcebergConfig(
        table_name="trades",
        catalog=catalog,
        storage=storage,
        schema_descriptor={
            "fields": [
                {"name": "id", "type": "long", "required": True},
                {"name": "price", "type": "double"},
                {"name": "event_date", "type": "timestamp"},
            ],
            "partition_fields": [
                {"name": "event_date"},
            ],
        },
    )

    sink = IcebergRESTSink(config=config)
    sink.setup()

    manager = sink._table_manager
    if manager is None:  # pragma: no cover - optional dependency guard
        pytest.skip("Table lifecycle manager unavailable without pyiceberg")

    descriptor = sink._build_schema_descriptor()
    table = manager.ensure_table(
        table_identifier=sink._table_identifier,
        schema_descriptor=descriptor,
    )

    assert isinstance(table, Table)
    schema_obj = table.schema if not callable(getattr(table, "schema", None)) else table.schema()
    field_names = {field.name for field in getattr(schema_obj, "fields", [])}
    assert {"id", "price"}.issubset(field_names)

    sink.close()
