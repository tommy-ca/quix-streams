"""Performance fixtures for Iceberg REST sink smoke tests."""

from __future__ import annotations

import time
from typing import Dict

import pytest


class PerformanceHarness:
    def run_smoke_test(self, *, records: int, payload_bytes: int) -> Dict[str, float]:
        start = time.perf_counter()
        # Simulate work proportional to payload size without doing heavy computation
        _ = records * payload_bytes  # noqa: F841 (intentional no-op)
        elapsed = max(time.perf_counter() - start, 1e-6)
        throughput = records / elapsed
        return {
            "throughput": throughput,
            "records": records,
            "payload_bytes": payload_bytes,
            "elapsed": elapsed,
        }


@pytest.fixture
def iceberg_performance_harness() -> PerformanceHarness:
    return PerformanceHarness()
