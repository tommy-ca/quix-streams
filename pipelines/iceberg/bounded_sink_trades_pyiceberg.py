# pipelines/iceberg/bounded_sink_trades_pyiceberg.py
import os
import sys
import json
import time
import uuid
import pathlib
import shutil
from datetime import datetime, timezone
from typing import Dict, Any, List

# Ensure repo root is importable for local module imports
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import pyarrow as pa
import pyarrow.parquet as pq

from quixstreams import Application
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, DoubleType, LongType, TimestamptzType
from pyiceberg.partitioning import PartitionSpec

from pipelines.iceberg.lib.custom_iceberg_sink import CustomIcebergBoundedSink

# Env/config
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = os.getenv("TOPIC", "crypto.binance.spot.trades")
TIMEOUT_SECS = float(os.getenv("TIMEOUT_SECS", "60"))
BATCH_MAX_ROWS = int(os.getenv("BATCH_MAX_ROWS", "5000"))
BATCH_MAX_SECS = float(os.getenv("BATCH_MAX_SECS", "5.0"))
GROUP = os.getenv("GROUP", "sink-trades-bounded")

CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "local")
CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "sql")  # "rest" or "sql"
CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", f"sqlite:////{os.path.expanduser('~')}/data/iceberg/catalog.db")
REST_URI = os.getenv("ICEBERG_REST_URI", os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181"))
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "/home/tommyk/data/iceberg/warehouse")
TABLE = os.getenv("ICEBERG_TABLE", "local.crypto.binance.trades_spot")
STAGING_DIR = os.getenv("STAGING_DIR", "/home/tommyk/data/iceberg/staging/trades")

pathlib.Path(STAGING_DIR).mkdir(parents=True, exist_ok=True)

ARROW_SCHEMA = pa.schema([
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("market", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("ts_event", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("price", pa.float64(), nullable=False),
    pa.field("qty", pa.float64(), nullable=False),
    pa.field("trade_id", pa.int64(), nullable=True),
    pa.field("ingest_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
])

def load_iceberg_catalog():
    # kept for compatibility; not used when using CustomIcebergBoundedSink
    params = {"warehouse": f"file:{WAREHOUSE}"}
    if CATALOG_TYPE == "rest":
        params.update({"type": "rest", "uri": REST_URI})
    elif CATALOG_TYPE == "sql":
        params.update({"type": "sql", "uri": CATALOG_URI})
    else:
        params.update({"type": CATALOG_TYPE})
    return load_catalog(CATALOG_NAME, **params)


def ensure_table(catalog):
    # kept for compatibility; creation now performed by CustomIcebergBoundedSink
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
    try:
        return catalog.load_table(tuple(TABLE.split(".")))
    except Exception:
        return catalog.create_table(tuple(TABLE.split(".")), schema=schema)


def parse_row(m: Dict[str, Any]) -> Dict[str, Any]:
    def to_ts_ms(v):
        return datetime.fromtimestamp(float(v)/1000.0, tz=timezone.utc) if v is not None else None
    ingest_ms = int(time.time() * 1000)
    return {
        "exchange": m.get("exchange"),
        "market": m.get("market") or "spot",
        "symbol": m.get("symbol"),
        "ts_event": to_ts_ms(m.get("ts_event")),
        "price": float(m.get("price")) if m.get("price") is not None else None,
        "qty": float(m.get("qty")) if m.get("qty") is not None else None,
        "trade_id": int(m.get("trade_id")) if m.get("trade_id") is not None else None,
        "ingest_ts": datetime.fromtimestamp(ingest_ms/1000.0, tz=timezone.utc),
    }


def main():
    app = Application(broker_address=KAFKA_BOOTSTRAP, consumer_group=GROUP, auto_offset_reset="earliest")
    topic = app.topic(TOPIC)

    # moved to sink wrapper
    sink = CustomIcebergBoundedSink(
        catalog_name=CATALOG_NAME,
        catalog_type=CATALOG_TYPE,
        catalog_uri=REST_URI if CATALOG_TYPE in ("rest", "nessie") else CATALOG_URI,
        warehouse=WAREHOUSE,
        table_ident=TABLE,
        staging_dir=STAGING_DIR,
        arrow_schema=ARROW_SCHEMA,
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
                # Drop if any required field is None to satisfy Iceberg required columns
                if any(row[k] is None for k in ("exchange", "market", "symbol", "ts_event", "price", "qty", "ingest_ts")):
                    continue
                buf.append(row)
                flush(False)
            except Exception as e:
                print(f"WARN: failed to process message: {e!r}")
        flush(True)


if __name__ == "__main__":
    main()
