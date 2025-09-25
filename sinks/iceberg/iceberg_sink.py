# sinks/iceberg/iceberg_sink.py
import os
import uuid
import time
import pathlib
import shutil
from typing import Callable, Dict, Any, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.partitioning import PartitionSpec


class IcebergCatalogConfig:
    def __init__(self,
                 name: str = "local",
                 catalog_type: str = "sql",
                 warehouse: str = "/home/tommyk/data/iceberg/warehouse",
                 uri: Optional[str] = None):
        self.name = name
        self.catalog_type = catalog_type  # "rest" or "sql" or others supported by pyiceberg
        self.warehouse = warehouse
        # Defaults
        if uri is None:
            if catalog_type == "sql":
                uri = f"sqlite:////{os.path.expanduser('~')}/data/iceberg/catalog.db"
            elif catalog_type == "rest":
                uri = "http://localhost:8181"
        self.uri = uri

    def load(self):
        params = {"warehouse": f"file:{self.warehouse}"}
        if self.catalog_type in ("sql", "rest"):
            params.update({"type": self.catalog_type, "uri": self.uri})
        else:
            params.update({"type": self.catalog_type})
        return load_catalog(self.name, **params)


class IcebergSink:
    """
    Generic, batching Iceberg sink that can be wired to Quix Streams SDF via df.sink(sink.write_batch),
    or driven by a simple poll loop. It supports:
      - REST and SQL catalogs via PyIceberg
      - Custom table ensure() function to define schema and partitioning
      - Arrow-based Parquet file writes followed by Iceberg append commits
    """

    def __init__(self,
                 catalog_config: IcebergCatalogConfig,
                 table_id: str,
                 ensure_table_fn: Callable[[Any, str], Any],
                 arrow_schema: pa.schema,
                 staging_dir: str,
                 batch_max_rows: int = 5000,
                 batch_max_secs: float = 5.0):
        self.catalog_config = catalog_config
        self.table_id = table_id
        self.ensure_table_fn = ensure_table_fn
        self.arrow_schema = arrow_schema
        self.staging_dir = staging_dir
        self.batch_max_rows = batch_max_rows
        self.batch_max_secs = batch_max_secs

        pathlib.Path(self.staging_dir).mkdir(parents=True, exist_ok=True)

        self.catalog = None
        self.table = None
        self._buf: List[Dict[str, Any]] = []
        self._last_flush = time.time()

    def open(self):
        self.catalog = self.catalog_config.load()
        self.table = self.ensure_table_fn(self.catalog, self.table_id)

    def _flush_needed(self) -> bool:
        return (len(self._buf) >= self.batch_max_rows) or ((time.time() - self._last_flush) >= self.batch_max_secs)

    def write_batch(self, rows: List[Dict[str, Any]]):
        if self.catalog is None or self.table is None:
            self.open()
        # Append rows and flush opportunistically
        for r in rows:
            if r is None:
                continue
            self._buf.append(r)
            if self._flush_needed():
                self.flush()

    def flush(self, force: bool = False):
        if not self._buf:
            return
        if not force and not self._flush_needed():
            return
        batch = pa.Table.from_pylist(self._buf, schema=self.arrow_schema)
        filename = f"part-{uuid.uuid4().hex}.parquet"
        stage_path = os.path.join(self.staging_dir, filename)
        pq.write_table(batch, stage_path)

        data_dir = os.path.join(self.table.location(), "data")
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        dest_path = os.path.join(data_dir, filename)
        shutil.move(stage_path, dest_path)

        # Commit append
        try:
            data_file = self.table.io().new_data_file(dest_path)
            self.table.new_append().append_file(data_file).commit()
        except Exception:
            # Fallback
            self.table.new_append().append_file(dest_path).commit()

        self._buf = []
        self._last_flush = time.time()

    def close(self):
        self.flush(force=True)


# Helper: ensure table builders for klines and trades

def ensure_klines_table(catalog, table_id: str):
    # Build Schema with field IDs and PartitionSpec(symbol, days(open_time))
    schema = Schema(
        NestedField(1, "exchange", StringType(), required=True),
        NestedField(2, "market", StringType(), required=True),
        NestedField(3, "symbol", StringType(), required=True),
        NestedField(4, "interval", StringType(), required=True),
        NestedField(5, "open_time", TimestamptzType(), required=True),
        NestedField(6, "open", DoubleType(), required=True),
        NestedField(7, "high", DoubleType(), required=True),
        NestedField(8, "low", DoubleType(), required=True),
        NestedField(9, "close", DoubleType(), required=True),
        NestedField(10, "volume", DoubleType(), required=True),
        NestedField(11, "close_time", TimestamptzType(), required=True),
        NestedField(12, "ingest_ts", TimestamptzType(), required=True),
    )
    spec = PartitionSpec(schema, ("symbol",), ("open_time", "day"))
    ident = tuple(table_id.split("."))
    # Ensure namespace exists
    if len(ident) >= 3:
        ns = tuple(ident[1:-1])
        try:
            catalog.create_namespace(ns)
        except Exception:
            pass
    try:
        return catalog.load_table(ident)
    except Exception:
        return catalog.create_table(ident, schema=schema, partition_spec=spec)


def ensure_trades_table(catalog, table_id: str):
    schema = Schema(
        NestedField(1, "exchange", StringType(), required=True),
        NestedField(2, "market", StringType(), required=True),
        NestedField(3, "symbol", StringType(), required=True),
        NestedField(4, "ts_event", TimestamptzType(), required=True),
        NestedField(5, "price", DoubleType(), required=True),
        NestedField(6, "qty", DoubleType(), required=True),
        NestedField(7, "trade_id", LongType(), required=False),
        NestedField(8, "ingest_ts", TimestamptzType(), required=True),
    )
    spec = PartitionSpec(schema, ("symbol",), ("ts_event", "day"))
    ident = tuple(table_id.split("."))
    if len(ident) >= 3:
        ns = tuple(ident[1:-1])
        try:
            catalog.create_namespace(ns)
        except Exception:
            pass
    try:
        return catalog.load_table(ident)
    except Exception:
        return catalog.create_table(ident, schema=schema, partition_spec=spec)
