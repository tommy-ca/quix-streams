"""Integration fixtures for Iceberg REST sink tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

import pytest


@dataclass
class _InMemoryTable:
    identifier: str
    schema_descriptor: Dict[str, object]
    partition_descriptor: Dict[str, object]
    refresh_count: int = 0

    def refresh(self) -> None:
        self.refresh_count += 1


@dataclass
class _InMemoryCatalog:
    tables: Dict[str, _InMemoryTable] = field(default_factory=dict)

    def load_table(self, identifier: str) -> _InMemoryTable:
        try:
            return self.tables[identifier]
        except KeyError as exc:
            raise RuntimeError(f"table '{identifier}' not found") from exc

    def create_table(
        self,
        identifier: str,
        schema_descriptor: Dict[str, object],
        partition_descriptor: Dict[str, object],
    ) -> _InMemoryTable:
        table = _InMemoryTable(
            identifier=identifier,
            schema_descriptor=schema_descriptor,
            partition_descriptor=partition_descriptor,
        )
        self.tables[identifier] = table
        return table


@pytest.fixture
def rest_catalog() -> callable:
    catalog = _InMemoryCatalog()

    def factory() -> _InMemoryCatalog:
        return catalog

    return factory


@pytest.fixture
def minio_bucket() -> callable:
    def factory(name: Optional[str] = "integration-test") -> str:
        return f"s3://{name}"

    return factory
