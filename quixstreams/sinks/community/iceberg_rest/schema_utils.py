"""Helpers for building and aligning Iceberg schemas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List


try:  # pragma: no cover - optional dependency
    from pyiceberg.schema import Schema  # type: ignore
    from pyiceberg.schema import NestedField  # type: ignore
    from pyiceberg.types import (  # type: ignore
        BooleanType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
    )
    from pyiceberg.partitioning import PartitionSpec  # type: ignore

    _HAS_PYICEBERG = True
except ImportError:  # pragma: no cover - fallback structure for tests
    _HAS_PYICEBERG = False

    @dataclass
    class NestedField:  # type: ignore
        field_id: int
        name: str
        field_type: str
        required: bool = False

    @dataclass
    class Schema:  # type: ignore
        fields: List[NestedField]

    @dataclass
    class PartitionSpec:  # type: ignore
        fields: List[str]


_TYPE_MAPPING = {
    "int": IntegerType if _HAS_PYICEBERG else "int",  # type: ignore[arg-type]
    "int64": LongType if _HAS_PYICEBERG else "long",
    "long": LongType if _HAS_PYICEBERG else "long",
    "float": FloatType if _HAS_PYICEBERG else "float",
    "double": DoubleType if _HAS_PYICEBERG else "double",
    "bool": BooleanType if _HAS_PYICEBERG else "bool",
    "boolean": BooleanType if _HAS_PYICEBERG else "bool",
    "str": StringType if _HAS_PYICEBERG else "string",
    "string": StringType if _HAS_PYICEBERG else "string",
}


def build_schema(descriptor: Dict[str, object]) -> Schema:
    """Build a Schema (or fallback) from descriptor data."""

    fields_descriptor = descriptor.get("fields", []) or []
    nested_fields: List[NestedField] = []

    for index, field in enumerate(fields_descriptor, start=1):
        name = field.get("name")
        if not name:
            continue
        raw_type = str(field.get("type", "string")).lower()
        iceberg_type = _TYPE_MAPPING.get(raw_type, _TYPE_MAPPING["string"])
        required = bool(field.get("required", False))

        if _HAS_PYICEBERG:
            nested_fields.append(NestedField(index, name, iceberg_type(), required))  # type: ignore[call-arg]
        else:
            nested_fields.append(NestedField(index, name, iceberg_type, required))

    if _HAS_PYICEBERG:
        return Schema(*nested_fields)  # type: ignore[arg-type]

    return Schema(fields=nested_fields)


def build_partition_spec(descriptor: Dict[str, object]) -> PartitionSpec:
    """Build partition specification from descriptor."""

    fields = descriptor.get("partition_fields") or []
    if _HAS_PYICEBERG:
        return PartitionSpec().from_dict(fields) if hasattr(PartitionSpec, "from_dict") else PartitionSpec(fields=fields)  # type: ignore[attr-defined]
    return PartitionSpec(fields=list(fields))


def align_schema(*, table, target_schema: Schema) -> List[Dict[str, object]]:
    """Return descriptors for fields missing on the existing table."""

    existing_names = _extract_field_names(getattr(table, "schema", None))
    additions: List[Dict[str, object]] = []

    for field in getattr(target_schema, "fields", []):
        name = getattr(field, "name", None)
        if not name or name in existing_names:
            continue
        field_type = getattr(field, "field_type", getattr(field, "type", "string"))
        additions.append({"name": name, "type": str(field_type)})

    return additions


def _extract_field_names(schema) -> List[str]:
    if schema is None:
        return []
    if hasattr(schema, "field_names") and callable(schema.field_names):
        return list(schema.field_names())
    names = []
    for field in getattr(schema, "fields", []):
        name = getattr(field, "name", None)
        if name:
            names.append(name)
    return names
