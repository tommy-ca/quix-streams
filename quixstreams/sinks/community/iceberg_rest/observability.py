"""Observability utilities for the Iceberg REST sink."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

from prometheus_client import CollectorRegistry, generate_latest, Gauge

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4"


@dataclass
class MetricsCollector:
    """Simple in-memory metrics collector supporting counters and gauges."""

    _metrics: Dict[str, float] = field(default_factory=dict)
    _registry: CollectorRegistry = field(default_factory=CollectorRegistry)
    _gauges: Dict[str, Gauge] = field(default_factory=dict)

    def increment(self, name: str, value: float = 1.0) -> None:
        self._metrics[name] = self._metrics.get(name, 0.0) + value
        gauge = self._get_or_create_gauge(name)
        gauge.inc(value)

    def record_gauge(self, name: str, value: float) -> None:
        self._metrics[name] = value
        gauge = self._get_or_create_gauge(name)
        gauge.set(value)

    def snapshot(self) -> Dict[str, float]:
        return dict(self._metrics)

    def render_prometheus(self) -> str:
        output = generate_latest(self._registry)
        return output.decode("utf-8")

    def _get_or_create_gauge(self, name: str) -> Gauge:
        gauge = self._gauges.get(name)
        if gauge is None:
            gauge = Gauge(
                f"iceberg_rest_{name}",
                f"Iceberg REST metric {name}",
                registry=self._registry,
            )
            self._gauges[name] = gauge
        return gauge
