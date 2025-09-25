# pipelines/iceberg/lib/custom_iceberg_sink.py
import os
import pathlib
import shutil
import time
import uuid
from typing import Dict, Any, Iterable, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, DoubleType, LongType, TimestamptzType


def _normalize_file_path(loc: str) -> str:
    # pyiceberg often returns file:/absolute/path
    return loc[len("file:"):] if isinstance(loc, str) and loc.startswith("file:") else loc


class CustomIcebergBoundedSink:
    """
    Minimal bounded writer for Iceberg tables that works with SQL, REST, or Nessie catalogs.
    - Writes Arrow batches to Parquet in a staging dir and moves to table's data dir
    - Attempts Iceberg metadata append if available in the installed pyiceberg
    """

    def __init__(
        self,
        catalog_name: str,
        catalog_type: str,
        catalog_uri: Optional[str],
        warehouse: str,
        table_ident: str,
        staging_dir: str,
        arrow_schema: pa.Schema,
        iceberg_schema: Optional[Schema] = None,
    ) -> None:
        self.catalog_name = catalog_name
        self.catalog_type = catalog_type.lower()
        self.catalog_uri = catalog_uri
        self.warehouse = warehouse
        self.table_ident = table_ident
        self.staging_dir = staging_dir
        self.arrow_schema = arrow_schema
        self.iceberg_schema = iceberg_schema

        pathlib.Path(self.staging_dir).mkdir(parents=True, exist_ok=True)
        self.catalog = self._load_catalog()
        self.table = self._ensure_table()

    def _load_catalog(self):
        # For REST, prefer ICEBERG_REST_WAREHOUSE env override if present
        rest_warehouse = os.getenv("ICEBERG_REST_WAREHOUSE")
        warehouse_uri = f"file:{self.warehouse}"
        if self.catalog_type == "rest" and rest_warehouse:
            warehouse_uri = rest_warehouse
        params: Dict[str, Any] = {"warehouse": warehouse_uri}
        if self.catalog_type in ("rest", "nessie", "sql") and self.catalog_uri:
            params.update({"type": self.catalog_type, "uri": self.catalog_uri})
        else:
            params.update({"type": self.catalog_type})
        return load_catalog(self.catalog_name, **params)

    def _ensure_table(self):
        # Use provided schema when supplied; default to a trades-like schema
        schema = self.iceberg_schema or Schema(
            NestedField(1, "exchange", StringType(), required=True),
            NestedField(2, "market", StringType(), required=True),
            NestedField(3, "symbol", StringType(), required=True),
            NestedField(4, "ts_event", TimestamptzType(), required=True),
            NestedField(5, "price", DoubleType(), required=True),
            NestedField(6, "qty", DoubleType(), required=True),
            NestedField(7, "trade_id", LongType(), required=False),
            NestedField(8, "ingest_ts", TimestamptzType(), required=True),
        )

        ident_parts = tuple(self.table_ident.split("."))
        # Create namespaces progressively (ignore if they exist)
        if len(ident_parts) >= 2:
            ns_parts = ident_parts[:-1]
            for i in range(1, len(ns_parts) + 1):
                try:
                    self.catalog.create_namespace(tuple(ns_parts[:i]))
                except Exception:
                    pass

        try:
            return self.catalog.load_table(ident_parts)
        except Exception:
            return self.catalog.create_table(ident_parts, schema=schema)

    def write_rows(self, rows: Iterable[Dict[str, Any]]) -> Optional[str]:
        """Write a batch of rows into the Iceberg table.
        Preferred: use Table.append on a PyArrow table (commits metadata).
        Fallback: write Parquet into table data directory (no metadata registration if append unsupported).
        Returns the destination file path when Parquet was written directly; otherwise None.
        """
        rows = list(rows)
        if not rows:
            return None

        batch = pa.Table.from_pylist(rows, schema=self.arrow_schema)

        # Preferred path: use PyIceberg high-level append with a PyArrow table
        if hasattr(self.table, "append"):
            try:
                try:
                    # Newer pyiceberg supports append(table: pa.Table, file_format="parquet")
                    self.table.append(batch, file_format="parquet")
                except TypeError:
                    # Some versions accept just the table
                    self.table.append(batch)
                return None
            except Exception as e:
                print(f"WARN: Table.append failed, falling back to file-based write: {e!r}")

        # Fallback path: write a Parquet file to the table data directory
        filename = f"part-{uuid.uuid4().hex}.parquet"
        stage_path = os.path.join(self.staging_dir, filename)
        pq.write_table(batch, stage_path)

        base_fs = _normalize_file_path(self.table.location())
        data_dir_fs = os.path.join(base_fs, "data")
        pathlib.Path(data_dir_fs).mkdir(parents=True, exist_ok=True)
        dest_path_fs = os.path.join(data_dir_fs, filename)
        shutil.move(stage_path, dest_path_fs)

        # No metadata registration in fallback mode
        return dest_path_fs

    def flush_every(self, buf: list, max_rows: int, max_secs: float, force: bool) -> bool:
        """Flush buffer if conditions met; returns True when a flush occurred."""
        return len(buf) >= max_rows or force or False
