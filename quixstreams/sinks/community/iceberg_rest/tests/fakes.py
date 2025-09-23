"""Test doubles for Iceberg REST sink lifecycle tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional


class FakeNoSuchTableError(Exception):
    """Raised when a fake catalog cannot find a table."""


@dataclass
class FakeSchema:
    """Simple schema representation tracking field order."""

    fields: List[Dict[str, Any]]

    def field_names(self) -> List[str]:
        return [field["name"] for field in self.fields]

    def has_field(self, name: str) -> bool:
        return any(field["name"] == name for field in self.fields)

    def add_field(self, field: Dict[str, Any]) -> None:
        if not self.has_field(field["name"]):
            self.fields.append(field)


@dataclass
class FakePartitionSpec:
    """Lightweight partition spec placeholder."""

    fields: List[str]


@dataclass
class FakeTable:
    """Fake pyiceberg table object recording schema updates."""

    schema_obj: FakeSchema
    partition_spec: FakePartitionSpec
    update_calls: List[List[Dict[str, Any]]] = field(default_factory=list)
    refresh_count: int = 0

    @property
    def schema(self) -> FakeSchema:
        return self.schema_obj

    def update_schema(self, fields_to_add: Iterable[Dict[str, Any]]) -> "FakeTable":
        to_add = list(fields_to_add)
        if to_add:
            self.update_calls.append(to_add)
            for field in to_add:
                self.schema_obj.add_field(field)
        return self

    def refresh(self) -> None:
        self.refresh_count += 1


class FakeCatalog:
    """Fake REST catalog storing a single table instance."""

    def __init__(self, table: Optional[FakeTable] = None) -> None:
        self._table = table
        self.load_calls: List[str] = []
        self.create_calls: List[Dict[str, Any]] = []

    def load_table(self, identifier: str) -> FakeTable:
        self.load_calls.append(identifier)
        if self._table is None:
            raise FakeNoSuchTableError(identifier)
        return self._table

    def create_table(
        self,
        identifier: str,
        schema: FakeSchema,
        partition_spec: FakePartitionSpec,
    ) -> FakeTable:
        self.create_calls.append(
            {
                "identifier": identifier,
                "schema": schema,
                "partition_spec": partition_spec,
            }
        )
        self._table = FakeTable(schema_obj=schema, partition_spec=partition_spec)
        return self._table


class RecordingStorageWriter:
    def __init__(self) -> None:
        self.calls: List[List[Dict[str, Any]]] = []

    def write(self, records: List[Dict[str, Any]]) -> List[str]:
        self.calls.append(list(records))
        return [f"file-{len(self.calls)}.parquet"]

    def close(self) -> None:
        return None


class RecordingSchemaBuilder:
    """Callable schema builder that records requests."""

    def __init__(self, fields: List[Dict[str, Any]]) -> None:
        self.fields = fields
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, descriptor: Dict[str, Any]) -> FakeSchema:
        self.calls.append(descriptor)
        return FakeSchema(fields=list(self.fields))


class RecordingPartitionBuilder:
    """Callable partition builder recording descriptors."""

    def __init__(self, partitions: List[str]) -> None:
        self.partitions = partitions
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, descriptor: Dict[str, Any]) -> FakePartitionSpec:
        self.calls.append(descriptor)
        return FakePartitionSpec(fields=list(self.partitions))


class RecordingSchemaAligner:
    """Schema aligner that tracks alignment requests and desired additions."""

    def __init__(self, additions: Optional[List[Dict[str, Any]]] = None) -> None:
        self.additions = additions or []
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, *, table: FakeTable, target_schema: FakeSchema) -> List[Dict[str, Any]]:
        self.calls.append({"table": table, "schema": target_schema})
        return list(self.additions)


class RecordingManager:
    """Recording lifecycle manager used in integration tests."""

    def __init__(self) -> None:
        self.calls: List[tuple[str, dict]] = []

    def ensure_table(self, *, table_identifier: str, schema_descriptor: dict):
        self.calls.append((table_identifier, schema_descriptor))
        return FakeTable(
            schema_obj=FakeSchema(fields=schema_descriptor.get("fields", [])),
            partition_spec=FakePartitionSpec(fields=schema_descriptor.get("partition_fields", [])),
        )
