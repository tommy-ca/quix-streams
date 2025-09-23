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
        TimestampType,
        StringType,
    )
    from pyiceberg.partitioning import PartitionField, PartitionSpec  # type: ignore
    from pyiceberg.transforms import (  # type: ignore
        BucketTransform,
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )

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
    "timestamp": TimestampType if _HAS_PYICEBERG else "timestamp",
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
    if not fields:
        if _HAS_PYICEBERG:
            schema = build_schema(descriptor)
            return PartitionSpec(schema=schema, fields=tuple())  # type: ignore[arg-type]
        return PartitionSpec(fields=list(fields))

    if not _HAS_PYICEBERG:
        return PartitionSpec(fields=list(fields))

    schema = build_schema(descriptor)
    name_to_field = {getattr(field, "name", None): field for field in getattr(schema, "fields", [])}
    next_field_id = (max((getattr(field, "field_id", 1000) for field in name_to_field.values()), default=999) + 1)

    partition_fields = []
    for raw_field in fields:
        name = raw_field.get("name") if isinstance(raw_field, dict) else None
        if not name:
            continue

        source_field = name_to_field.get(name)
        if source_field is None:
            continue

        source_id = int(raw_field.get("source-id", getattr(source_field, "field_id", 0)))
        field_id = raw_field.get("field-id")
        if field_id is None:
            field_id = next_field_id
            next_field_id += 1

        transform = _resolve_partition_transform(raw_field)
        alias = raw_field.get("name_alias") or name

        partition_fields.append(
            PartitionField(
                source_id=source_id,
                field_id=int(field_id),
                transform=transform,
                name=alias,
            )
        )

    return PartitionSpec(schema=schema, fields=tuple(partition_fields))  # type: ignore[arg-type]


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


def _resolve_partition_transform(spec: Dict[str, object]):
    if not _HAS_PYICEBERG:
        return None

    raw = str(spec.get("transform", "identity")).lower()

    if raw.startswith("truncate"):
        size = spec.get("width")
        if size is None:
            size = _extract_transform_param(raw, "truncate")
        return TruncateTransform(int(size)) if size else IdentityTransform()

    if raw.startswith("bucket"):
        buckets = spec.get("buckets")
        if buckets is None:
            buckets = _extract_transform_param(raw, "bucket")
        return BucketTransform(int(buckets)) if buckets else IdentityTransform()

    if raw == "day":
        return DayTransform()
    if raw == "month":
        return MonthTransform()
    if raw == "year":
        return YearTransform()
    if raw == "hour":
        return HourTransform()

    return IdentityTransform()


def _extract_transform_param(raw: str, prefix: str) -> int | None:
    start = len(prefix) + 1
    if not raw.startswith(f"{prefix}[") or not raw.endswith("]"):
        return None
    value = raw[start:-1]
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None
