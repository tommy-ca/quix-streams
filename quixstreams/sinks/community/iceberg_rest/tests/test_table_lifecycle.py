"""Table lifecycle orchestration tests (Cycle 0 - RED)."""

from __future__ import annotations

import pytest

from quixstreams.sinks.community.iceberg_rest.tests.fakes import (
    FakeCatalog,
    FakeNoSuchTableError,
    FakeSchema,
    FakeTable,
    RecordingPartitionBuilder,
    RecordingSchemaAligner,
    RecordingSchemaBuilder,
)


def _build_manager(**overrides):
    """Helper to instantiate the future TableLifecycleManager with overrides."""

    from quixstreams.sinks.community.iceberg_rest.table_lifecycle import (  # noqa: PLC0415
        TableLifecycleManager,
    )

    defaults = {
        "catalog_factory": overrides.get("catalog_factory"),
        "schema_builder": overrides.get("schema_builder"),
        "partition_builder": overrides.get("partition_builder"),
        "schema_aligner": overrides.get("schema_aligner"),
        "cache_ttl_seconds": overrides.get("cache_ttl_seconds", 30.0),
        "clock": overrides.get("clock", lambda: 0.0),
    }
    return TableLifecycleManager(**defaults)


def test_manager_creates_missing_table_and_records_partition() -> None:
    catalog = FakeCatalog(table=None)
    schema_builder = RecordingSchemaBuilder(fields=[{"name": "id", "type": "long"}])
    partition_builder = RecordingPartitionBuilder(partitions=["event_date"])
    schema_aligner = RecordingSchemaAligner()

    manager = _build_manager(
        catalog_factory=lambda: catalog,
        schema_builder=schema_builder,
        partition_builder=partition_builder,
        schema_aligner=schema_aligner,
    )

    descriptor = {"table": "crypto.trades", "namespace": "warehouse.crypto"}

    table = manager.ensure_table(table_identifier="warehouse.crypto.trades", schema_descriptor=descriptor)

    assert isinstance(table, FakeTable)
    assert catalog.create_calls[0]["identifier"] == "warehouse.crypto.trades"
    assert partition_builder.calls == [descriptor]
    assert schema_builder.calls == [descriptor]


def test_manager_caches_loaded_table_until_ttl_expires(monkeypatch: pytest.MonkeyPatch) -> None:
    schema = FakeSchema(fields=[{"name": "id", "type": "long"}])
    table = FakeTable(schema_obj=schema, partition_spec=RecordingPartitionBuilder(["event_date"])({}))
    catalog = FakeCatalog(table=table)
    schema_builder = RecordingSchemaBuilder(fields=schema.fields)
    partition_builder = RecordingPartitionBuilder(partitions=["event_date"])
    schema_aligner = RecordingSchemaAligner()

    clock_ticks = [0.0, 5.0, 15.0]
    manager = _build_manager(
        catalog_factory=lambda: catalog,
        schema_builder=schema_builder,
        partition_builder=partition_builder,
        schema_aligner=schema_aligner,
        cache_ttl_seconds=10.0,
        clock=lambda: clock_ticks.pop(0),
    )

    descriptor = {"table": "crypto.trades"}
    first = manager.ensure_table(table_identifier="warehouse.crypto.trades", schema_descriptor=descriptor)
    second = manager.ensure_table(table_identifier="warehouse.crypto.trades", schema_descriptor=descriptor)

    assert first is second
    assert catalog.load_calls == ["warehouse.crypto.trades"]


def test_manager_aligns_schema_and_refreshes_table(monkeypatch: pytest.MonkeyPatch) -> None:
    existing_schema = FakeSchema(fields=[{"name": "id", "type": "long"}])
    table = FakeTable(schema_obj=existing_schema, partition_spec=RecordingPartitionBuilder(["event_date"])({}))
    catalog = FakeCatalog(table=table)
    schema_builder = RecordingSchemaBuilder(fields=[{"name": "id", "type": "long"}, {"name": "price", "type": "double"}])
    partition_builder = RecordingPartitionBuilder(partitions=["event_date"])
    schema_aligner = RecordingSchemaAligner(additions=[{"name": "price", "type": "double"}])

    manager = _build_manager(
        catalog_factory=lambda: catalog,
        schema_builder=schema_builder,
        partition_builder=partition_builder,
        schema_aligner=schema_aligner,
        cache_ttl_seconds=0.0,
    )

    descriptor = {"table": "crypto.trades"}
    result = manager.ensure_table(table_identifier="warehouse.crypto.trades", schema_descriptor=descriptor)

    assert result is table
    assert schema_aligner.calls
    assert table.update_calls == [[{"name": "price", "type": "double"}]]
    assert table.refresh_count == 1
