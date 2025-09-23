import os, pyarrow as pa
from datetime import datetime, timezone
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, DoubleType, TimestamptzType

REST_URI = os.getenv("REST_URI", "http://localhost:8181/catalog")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "demo")

cat = load_catalog("local", **{"type": "rest", "uri": REST_URI, "warehouse": WAREHOUSE_ID})

# ensure namespaces
for ns in [("local",), ("local","crypto"), ("local","crypto","binance")]:
    try:
        cat.create_namespace(ns)
    except Exception:
        pass

schema = Schema(
    NestedField(1, "exchange", StringType(), required=True),
    NestedField(2, "symbol", StringType(), required=True),
    NestedField(3, "price", DoubleType(), required=True),
    NestedField(4, "ts_event", TimestamptzType(), required=True),
)
ident = ("local","crypto","binance","tdd_smoke")
try:
    cat.drop_table(ident)
except Exception:
    pass

# create table
cat.create_table(ident, schema=schema)

t = cat.load_table(ident)
arrow_schema = pa.schema([
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("price", pa.float64(), nullable=False),
    pa.field("ts_event", pa.timestamp("ms", tz="UTC"), nullable=False),
])
now = datetime.now(tz=timezone.utc)
batch = pa.Table.from_pylist([
    {"exchange": "binance", "symbol": "BTCUSDT", "price": 1.23, "ts_event": now},
    {"exchange": "binance", "symbol": "ETHUSDT", "price": 2.34, "ts_event": now},
    {"exchange": "binance", "symbol": "SOLUSDT", "price": 3.45, "ts_event": now},
], schema=arrow_schema)

# append (requires S3 IO; pip install s3fs if missing)
t.append(batch)

print("append ok")
