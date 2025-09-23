# pipelines/iceberg/bounded_sink_klines_pyiceberg.py
import os
import json
import time
import uuid
import pathlib
import shutil
from datetime import datetime, timezone
from typing import Dict, Any, List

import pyarrow as pa
import pyarrow.parquet as pq

import sys
# Ensure repo root is importable for local module imports
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from quixstreams import Application
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, DoubleType, LongType, TimestamptzType
from pipelines.iceberg.lib.custom_iceberg_sink import CustomIcebergBoundedSink

# Env/config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = os.getenv("TOPIC", "crypto.binance.spot.klines.1m")
TIMEOUT_SECS = float(os.getenv("TIMEOUT_SECS", "60"))
BATCH_MAX_ROWS = int(os.getenv("BATCH_MAX_ROWS", "5000"))
BATCH_MAX_SECS = float(os.getenv("BATCH_MAX_SECS", "5.0"))
GROUP = os.getenv("GROUP", "sink-klines-bounded")

CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "local")
CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "sql")  # "rest" or "sql"
CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", f"sqlite:////{os.path.expanduser('~')}/data/iceberg/catalog.db")
REST_URI = os.getenv("ICEBERG_REST_URI", os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181"))
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "/home/tommyk/data/iceberg/warehouse")
TABLE = os.getenv("ICEBERG_TABLE", "local.crypto.binance.klines_spot_1m")
STAGING_DIR = os.getenv("STAGING_DIR", "/home/tommyk/data/iceberg/staging/klines_1m")

pathlib.Path(STAGING_DIR).mkdir(parents=True, exist_ok=True)

# Arrow schema with required fields non-nullable to match Iceberg
ARROW_SCHEMA = pa.schema([
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("market", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("interval", pa.string(), nullable=False),
    pa.field("open_time", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("open", pa.float64(), nullable=False),
    pa.field("high", pa.float64(), nullable=False),
    pa.field("low", pa.float64(), nullable=False),
    pa.field("close", pa.float64(), nullable=False),
    pa.field("volume", pa.float64(), nullable=False),
    pa.field("close_time", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("ingest_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
])


def load_iceberg_catalog():
    params = {"warehouse": f"file:{WAREHOUSE}"}
    if CATALOG_TYPE == "rest":
        params.update({"type": "rest", "uri": REST_URI})
    elif CATALOG_TYPE == "sql":
        params.update({"type": "sql", "uri": CATALOG_URI})
    else:
        params.update({"type": CATALOG_TYPE})
    return load_catalog(CATALOG_NAME, **params)


def ensure_table(catalog):
    # Build PyIceberg Schema with NestedField IDs (stable for table evolution)
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
    spec = None  # Start small without partitions for TDD; add spec later when API confirmed

    # Create namespace if needed
    try:
        ident = TABLE.split(".")
        # Create nested namespaces iteratively: (crypto), (crypto, binance)
        if len(ident) >= 2:
            ns_parts = ident[:-1]
            for i in range(1, len(ns_parts)+1):
                sub_ns = tuple(ns_parts[:i])
                try:
                    catalog.create_namespace(sub_ns)
                except Exception:
                    pass
    except Exception:
        pass

    try:
        t = catalog.load_table(tuple(TABLE.split(".")))
        return t
    except Exception:
        # Create table if it does not exist
        if spec is not None:
            return catalog.create_table(tuple(TABLE.split(".")), schema=schema, partition_spec=spec)
        else:
            return catalog.create_table(tuple(TABLE.split(".")), schema=schema)


def parse_row(m: Dict[str, Any]) -> Dict[str, Any]:
    def to_ts_ms(v):
        return datetime.fromtimestamp(float(v)/1000.0, tz=timezone.utc) if v is not None else None
    ingest_ms = int(time.time() * 1000)
    return {
        "exchange": m.get("exchange"),
        "market": m.get("market") or "spot",
        "symbol": m.get("symbol"),
        "interval": m.get("interval") or "1m",
        "open_time": to_ts_ms(m.get("open_time")),
        "open": float(m.get("open")) if m.get("open") is not None else None,
        "high": float(m.get("high")) if m.get("high") is not None else None,
        "low": float(m.get("low")) if m.get("low") is not None else None,
        "close": float(m.get("close")) if m.get("close") is not None else None,
        "volume": float(m.get("volume")) if m.get("volume") is not None else None,
        "close_time": to_ts_ms(m.get("close_time")),
        "ingest_ts": datetime.fromtimestamp(ingest_ms/1000.0, tz=timezone.utc),
    }


def main():
    app = Application(broker_address=KAFKA_BOOTSTRAP, consumer_group=GROUP, auto_offset_reset="earliest")
    topic = app.topic(TOPIC)

    iceberg_schema = Schema(
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

    sink = CustomIcebergBoundedSink(
        catalog_name=CATALOG_NAME,
        catalog_type=CATALOG_TYPE,
        catalog_uri=REST_URI if CATALOG_TYPE in ("rest", "nessie") else CATALOG_URI,
        warehouse=WAREHOUSE,
        table_ident=TABLE,
        staging_dir=STAGING_DIR,
        arrow_schema=ARROW_SCHEMA,
        iceberg_schema=iceberg_schema,
    )

    buf: List[Dict[str, Any]] = []
    last_flush = time.time()

    def flush(force=False):
        nonlocal buf, last_flush
        if not buf:
            return
        if not force and len(buf) < BATCH_MAX_ROWS and (time.time() - last_flush) < BATCH_MAX_SECS:
            return
        sink.write_rows(buf)
        buf = []
        last_flush = time.time()

    with app.get_consumer() as consumer:
        consumer.subscribe([TOPIC])
        t0 = time.time()
        while time.time() - t0 < TIMEOUT_SECS:
            msg = consumer.poll(0.2)
            if not msg:
                flush(False)
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                row = parse_row(payload)
                required = ("exchange","market","symbol","interval","open_time","open","high","low","close","volume","close_time","ingest_ts")
                if any(row[k] is None for k in required):
                    continue
                buf.append(row)
                flush(False)
            except Exception as e:
                print(f"WARN: failed to process message: {e!r}")
        flush(True)


if __name__ == "__main__":
    main()
