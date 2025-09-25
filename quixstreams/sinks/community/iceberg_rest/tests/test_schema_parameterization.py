"""Schema parameterization tests for IcebergRESTSink."""

from __future__ import annotations

import pytest

from quixstreams.sinks.community.iceberg_rest import (
    IcebergRESTSink,
    create_local_config,
)
from quixstreams.sinks.community.iceberg_rest.tests.fakes import RecordingManager


class StubCommitClient:
    def __init__(self, *args, **kwargs) -> None:
        self.calls = []

    def post_records(self, records):
        self.calls.append(records)
        return type("Response", (), {"status_code": 200, "json": lambda self=None: {}})()

    def health_check(self):
        return {"status": "healthy"}

    def close(self) -> None:
        return None


def _make_sink(monkeypatch: pytest.MonkeyPatch, manager: RecordingManager, **config_kwargs):
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
        lambda *args, **kwargs: StubCommitClient(),
    )

    config = create_local_config(table_name="schema_test", **config_kwargs)
    sink = IcebergRESTSink(config=config)
    sink.setup()
    return sink


def test_schema_fields_are_used_by_lifecycle_manager(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = RecordingManager()
    sink = _make_sink(
        monkeypatch,
        manager,
        schema_fields=[
            {"name": "exchange", "type": "string", "required": True},
            {"name": "symbol", "type": "string"},
        ],
        partition_fields=[{"name": "event_date"}],
    )

    sink.write([{"id": 1, "price": 10.0}])
    sink.flush()

    assert manager.calls, "expected lifecycle manager to be invoked"
    _, descriptor = manager.calls[0]
    field_names = {field["name"] for field in descriptor["fields"]}
    assert {"exchange", "symbol"}.issubset(field_names)
    partition_names = {field["name"] for field in descriptor["partition_fields"]}
    assert "event_date" in partition_names


def test_schema_preset_provides_domain_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = RecordingManager()
    sink = _make_sink(
        monkeypatch,
        manager,
        schema_preset="crypto_trades",
    )

    sink.write([{"id": 1, "price": 42.0}])
    sink.flush()

    _, descriptor = manager.calls[0]
    field_names = {field["name"] for field in descriptor["fields"]}
    assert {"exchange", "symbol", "price"}.issubset(field_names)
    partition_names = {field["name"] for field in descriptor["partition_fields"]}
    assert "event_date" in partition_names
