"""Table lifecycle orchestration for Iceberg REST sink."""

from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
from typing import Callable, Dict, Iterable, List


try:  # pragma: no cover - optional dependency guard
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError  # type: ignore
except ImportError:  # pragma: no cover - fallback for test doubles
    class NoSuchTableError(Exception):  # type: ignore
        """Fallback exception used in tests when pyiceberg is unavailable."""

    class NamespaceAlreadyExistsError(Exception):  # type: ignore
        """Fallback namespace error used in tests when pyiceberg is unavailable."""


class SchemaAlignmentProtocol:
    """Callable protocol for schema alignment (duck-typed)."""

    def __call__(self, *, table, target_schema) -> Iterable[dict]:  # pragma: no cover - protocol helper
        raise NotImplementedError


CatalogFactory = Callable[[], object]
SchemaBuilder = Callable[[Dict[str, object]], object]
PartitionBuilder = Callable[[Dict[str, object]], object]
Clock = Callable[[], float]


@dataclass
class CachedTable:
    table: object
    timestamp: float


class TableLifecycleManager:
    """Ensure Iceberg tables exist and match the target schema."""

    def __init__(
        self,
        *,
        catalog_factory: CatalogFactory,
        schema_builder: SchemaBuilder,
        partition_builder: PartitionBuilder,
        schema_aligner: SchemaAlignmentProtocol,
        cache_ttl_seconds: float = 30.0,
        clock: Clock = monotonic,
    ) -> None:
        self._catalog_factory = catalog_factory
        self._schema_builder = schema_builder
        self._partition_builder = partition_builder
        self._schema_aligner = schema_aligner
        self._cache_ttl = max(cache_ttl_seconds, 0.0)
        self._clock = clock
        self._cache: Dict[str, CachedTable] = {}

    def ensure_table(self, *, table_identifier: str, schema_descriptor: Dict[str, object]):
        """Return a table that satisfies the target schema, creating or evolving as needed."""

        cached = self._cache.get(table_identifier)
        now = self._clock()
        if cached and self._cache_ttl > 0.0 and now - cached.timestamp <= self._cache_ttl:
            return cached.table

        catalog = self._catalog_factory()
        schema = self._schema_builder(schema_descriptor)
        partition_spec = self._partition_builder(schema_descriptor)

        try:
            table = catalog.load_table(table_identifier)
        except Exception as exc:  # pylint: disable=broad-except
            if isinstance(exc, NoSuchTableError) or exc.__class__.__name__ == "FakeNoSuchTableError":
                self._ensure_namespace(catalog, table_identifier)
                table = catalog.create_table(
                    table_identifier,
                    schema,
                    partition_spec=partition_spec,
                )
            else:
                raise

        additions = list(self._schema_aligner(table=table, target_schema=schema))
        if additions:
            table.update_schema(additions)
            if hasattr(table, "refresh"):
                table.refresh()

        self._cache[table_identifier] = CachedTable(table=table, timestamp=now)
        return table

    def _ensure_namespace(self, catalog, identifier: str) -> None:
        """Create namespace for an identifier if the catalog supports it."""

        if not hasattr(catalog, "create_namespace"):
            return

        parts = identifier.split(".")
        if len(parts) <= 1:
            return

        namespace = tuple(parts[:-1])
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:  # type: ignore
            return
        except Exception:  # pragma: no cover - best-effort for adapters without namespace API
            return


class _ManagedSchema:
    def __init__(self, fields: List[Dict[str, object]]):
        self.fields = [dict(field) for field in fields]

    def field_names(self) -> List[str]:
        return [field["name"] for field in self.fields]


class _ManagedTable:
    def __init__(self, schema: List[Dict[str, object]], partition_fields: List[Dict[str, object]]):
        self.schema_obj = _ManagedSchema(schema)
        self.partition_fields = partition_fields
        self.refresh_count = 0

    @property
    def schema(self) -> _ManagedSchema:
        return self.schema_obj

    def update_schema(self, fields_to_add: Iterable[Dict[str, object]]):
        for field in fields_to_add:
            name = field.get("name")
            if not name or any(existing["name"] == name for existing in self.schema_obj.fields):
                continue
            self.schema_obj.fields.append(dict(field))
        return self

    def refresh(self) -> None:
        self.refresh_count += 1


class InMemoryCatalogAdapter:
    """Default catalog adapter used when no external catalog is provided."""

    def __init__(self) -> None:
        self._tables: Dict[str, _ManagedTable] = {}

    def load_table(self, identifier: str) -> _ManagedTable:
        if identifier not in self._tables:
            raise NoSuchTableError(identifier)
        return self._tables[identifier]

    def create_table(self, identifier: str, schema, partition_spec) -> _ManagedTable:
        schema_fields = _schema_to_descriptor_list(schema)
        partition_fields = _partition_to_descriptor_list(partition_spec)
        table = _ManagedTable(schema_fields, partition_fields)
        self._tables[identifier] = table
        return table


def _schema_to_descriptor_list(schema) -> List[Dict[str, object]]:
    fields: List[Dict[str, object]] = []
    for field in getattr(schema, "fields", []):
        name = getattr(field, "name", None)
        if not name:
            continue
        field_type = getattr(field, "field_type", getattr(field, "type", "string"))
        fields.append({"name": name, "type": str(field_type), "required": getattr(field, "required", False)})
    return fields


def _partition_to_descriptor_list(partition_spec) -> List[Dict[str, object]]:
    fields: List[Dict[str, object]] = []
    if partition_spec is None:
        return fields
    entries = getattr(partition_spec, "fields", [])
    for entry in entries:
        name = getattr(entry, "name", getattr(entry, "source", None))
        if name:
            fields.append({"name": name})
    return fields
