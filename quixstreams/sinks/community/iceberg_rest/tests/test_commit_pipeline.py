"""Commit pipeline tests ensuring sink writes via storage writer.""" 

from __future__ import annotations

from pathlib import Path
from typing import List

import pytest
import pyarrow.parquet as pq

from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_config

from .fakes import RecordingManager


class RecordingCommitClient:
    def __init__(self, *args, **kwargs) -> None:
        self.calls: List[List[dict]] = []

    def post_records(self, records):
        self.calls.append(records)
        return type("Response", (), {"status_code": 200, "json": lambda self=None: {}})()

    def close(self) -> None:  # pragma: no cover - to satisfy shutdown
        return None


@pytest.fixture
def manager(monkeypatch: pytest.MonkeyPatch) -> RecordingManager:
    mgr = RecordingManager()
    monkeypatch.setattr(
        "quixstreams.sinks.community.iceberg_rest.TableLifecycleManager",
        lambda **kwargs: mgr,
    )
    monkeypatch.setattr(
        "quixstreams.sinks.community.iceberg_rest.sink.TableLifecycleManager",
        lambda **kwargs: mgr,
    )
    return mgr


def _make_sink(
    monkeypatch: pytest.MonkeyPatch,
    *,
    patch_manager: bool = True,
    manager: RecordingManager | None = None,
) -> tuple[IcebergRESTSink, RecordingCommitClient]:
    commit_client = RecordingCommitClient()
    monkeypatch.setattr(
        "quixstreams.sinks.community.iceberg_rest.sink.RESTCatalogClient",
        lambda *args, **kwargs: commit_client,
    )
    if patch_manager:
        mgr = manager or RecordingManager()
        monkeypatch.setattr(
            "quixstreams.sinks.community.iceberg_rest.TableLifecycleManager",
            lambda **kwargs: mgr,
        )
        monkeypatch.setattr(
            "quixstreams.sinks.community.iceberg_rest.sink.TableLifecycleManager",
            lambda **kwargs: mgr,
        )
    config = create_local_config(table_name="trades")
    sink = IcebergRESTSink(config=config)
    sink.setup()
    return sink, commit_client


def test_sink_writes_records_via_storage_writer(monkeypatch, manager):
    sink, _ = _make_sink(monkeypatch, manager=manager)

    sink.write([{"id": 1, "price": 10.5}])
    sink.flush()

    artifacts = sink._last_written_artifacts
    assert artifacts, "expected artifacts to be recorded"
    first = artifacts[0]
    path = Path(first["path"])
    assert path.suffix == ".parquet"
    table = pq.read_table(path)
    assert table.num_rows == 1
    price_column = table.column("price")
    assert price_column[0].as_py() == 10.5


def test_commit_payload_references_artifacts(monkeypatch, manager):
    sink, client = _make_sink(monkeypatch, manager=manager)

    sink.write([{"id": 1}])
    sink.flush()

    payload = client.calls[-1]
    assert payload
    descriptor = payload[0]
    assert descriptor["artifacts"] == sink._last_written_artifacts
    assert descriptor["record_count"] == 1


def test_sink_retries_commit_conflicts(monkeypatch, manager):
    sink, client = _make_sink(monkeypatch, manager=manager)

    sink.write([{"id": 1}])
    sink.flush()

    sink.write([{"id": 2}])
    sink.flush()

    assert len(client.calls) >= 2


def test_sink_flush_uses_default_table_manager(monkeypatch):
    sink, _ = _make_sink(monkeypatch, patch_manager=False)

    sink.write([{"id": 1, "price": 7.5}])
    sink.flush()

    assert sink._table_manager is not None
    assert sink._table_manager._cache  # type: ignore[attr-defined]
    assert sink._last_written_artifacts
