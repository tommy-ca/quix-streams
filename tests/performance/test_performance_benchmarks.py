"""Performance validation tests for Iceberg REST sink production readiness."""

from __future__ import annotations

import pytest


@pytest.mark.benchmark
@pytest.mark.iceberg_rest
def test_sink_meets_throughput_and_memory_targets(iceberg_performance_harness):
    """TSK-6.1: Ensure throughput, memory, and recovery metrics meet acceptance criteria."""

    result = iceberg_performance_harness.run_benchmark(
        records=12_000,
        payload_bytes=1_024,
        target_throughput=10_000,
        buffer_limit_bytes=50_000_000,
    )

    assert result["throughput_records_per_second"] >= 10_000
    assert result["memory_peak_bytes"] <= 50_000_000
    assert result["recovered_batches"] == 0


@pytest.mark.benchmark
@pytest.mark.iceberg_rest
@pytest.mark.parametrize("provider", ["aws", "cloudflare_r2", "minio"])
def test_sink_benchmark_reports_by_provider(iceberg_performance_harness, provider):
    """TSK-6.1: Validate benchmark exposes provider-specific telemetry for dashboards."""

    result = iceberg_performance_harness.run_benchmark(
        records=6_000,
        payload_bytes=512,
        target_throughput=8_000,
        buffer_limit_bytes=25_000_000,
        provider=provider,
    )

    assert result["provider"] == provider
    assert result["throughput_records_per_second"] >= 8_000
    assert "latency_p99_ms" in result
    assert result["latency_p99_ms"] >= 0
