"""Integration tests between IcebergRESTSink and TableLifecycleManager."""

from __future__ import annotations

from typing import List

import pytest

from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_config


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
