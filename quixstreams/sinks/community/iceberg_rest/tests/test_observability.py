"""Observability tests for IcebergRESTSink."""

from __future__ import annotations

from typing import Dict
import logging

import pytest

from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_config
from quixstreams.sinks.community.iceberg_rest.observability import MetricsCollector


class RecordingMetrics(MetricsCollector):
    def __init__(self) -> None:
        super().__init__()


class HealthyClient:
    def __init__(self, *args, **kwargs) -> None:
        self.records = []

    def post_records(self, records):
        self.records.append(records)
        return type("Response", (), {"status_code": 200, "json": lambda self=None: {}})()

    def health_check(self):
        return {"status": "healthy"}

    def close(self) -> None:
        return None


def _make_sink(monkeypatch: pytest.MonkeyPatch, metrics: RecordingMetrics) -> IcebergRESTSink:
    monkeypatch.setattr(
        "quixstreams.sinks.community.iceberg_rest.sink.RESTCatalogClient",
        lambda *args, **kwargs: HealthyClient(),
    )
    config = create_local_config(table_name="metrics_test")
    sink = IcebergRESTSink(config=config, metrics_collector=metrics)
    sink.setup()
    return sink


def test_metrics_collector_tracks_flush(monkeypatch: pytest.MonkeyPatch) -> None:
    metrics = RecordingMetrics()
    sink = _make_sink(monkeypatch, metrics)

    sink.write([{"id": 1, "price": 1.5}, {"id": 2, "price": 2.5}])
    sink.flush()

    snapshot = metrics.snapshot()
    assert snapshot["records_total"] == 2
    assert "bytes_written" in snapshot
    assert snapshot["flush_total"] == 1


def test_prometheus_metrics_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    metrics = RecordingMetrics()
    sink = _make_sink(monkeypatch, metrics)

    sink.write([{"id": 1}])
    sink.flush()

    output = sink.render_prometheus_metrics()
    lines = {line for line in output.splitlines() if line.startswith("iceberg_rest_")}
    assert "iceberg_rest_records_total" in "\n".join(lines)
    assert "iceberg_rest_flush_total" in "\n".join(lines)
    assert "iceberg_rest_bytes_written" in "\n".join(lines)


def test_prometheus_http_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    sink = _make_sink(monkeypatch, RecordingMetrics())
    sink.write([{"id": 1}])
    sink.flush()

    body, content_type = sink.prometheus_http_payload()
    assert content_type == "text/plain; version=0.0.4"
    assert b"iceberg_rest_records_total" in body


def test_health_check_reports_artifacts(monkeypatch: pytest.MonkeyPatch) -> None:
    metrics = RecordingMetrics()
    sink = _make_sink(monkeypatch, metrics)

    sink.write([{"id": 1}])
    sink.flush()

    health = sink.health_check()
    assert health["status"] == "healthy"
    assert health["client_health"]["status"] == "healthy"
    assert health["artifacts"]
    assert "metrics" in health


def test_logging_level_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    sink = _make_sink(monkeypatch, RecordingMetrics())
    sink.set_log_level("DEBUG")
    assert logging.getLogger("quixstreams.sinks.community.iceberg_rest.sink").level == logging.DEBUG


def test_alert_thresholds_raise_health_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    sink = _make_sink(monkeypatch, RecordingMetrics())
    sink.register_metric_alert("bytes_written", max_value=10)

    sink.write([{"id": 1, "payload": "x" * 64}])
    sink.flush()

    health = sink.health_check()
    assert health["alerts"]
    assert any(alert["metric"] == "bytes_written" for alert in health["alerts"])
