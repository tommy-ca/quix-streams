"""Performance smoke harness expectations for Iceberg REST sink."""

from __future__ import annotations

def test_performance_harness_runs_smoke(iceberg_performance_harness):
    harness = iceberg_performance_harness
    result = harness.run_smoke_test(records=1_000, payload_bytes=512)
    assert "throughput" in result
