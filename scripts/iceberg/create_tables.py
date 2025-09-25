# scripts/iceberg/create_tables.py
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, DoubleType, LongType, TimestamptzType, MapType
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.transforms import DayTransform, IdentityTransform
import os

WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "/home/tommyk/data/iceberg/warehouse")
CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "local")
CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "sql")
CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", f"sqlite:////{os.path.expanduser('~')}/data/iceberg/catalog.db")


def ensure_tables():
    # Configure supported catalog types for PyIceberg; default to SQL (SQLite)
    if CATALOG_TYPE in ("rest", "nessie", "sql"):
        catalog = load_catalog(CATALOG_NAME, **{"type": CATALOG_TYPE, "uri": CATALOG_URI, "warehouse": f"file:{WAREHOUSE}"})
    else:
        # Fallback to provided type (e.g., hive, glue) with warehouse when applicable
        catalog = load_catalog(CATALOG_NAME, **{"type": CATALOG_TYPE, "warehouse": f"file:{WAREHOUSE}"})

    ns = ("crypto", "binance")
    try:
        catalog.create_namespace(ns)
    except Exception:
        # Assume namespace exists if creation fails due to existence
        pass

    trades_schema = Schema(
        ("exchange", StringType(), False, None),
        ("market", StringType(), False, None),
        ("symbol", StringType(), False, None),
        ("ts_event", TimestamptzType(), False, None),
        ("price", DoubleType(), False, None),
        ("qty", DoubleType(), False, None),
        ("trade_id", LongType(), True, None),
        ("ingest_ts", TimestamptzType(), False, None),
    )
    trades_spec = PartitionSpec(("symbol", IdentityTransform()), ("ts_event", DayTransform()))

    klines_schema = Schema(
        ("exchange", StringType(), False, None),
        ("market", StringType(), False, None),
        ("symbol", StringType(), False, None),
        ("interval", StringType(), False, None),
        ("open_time", TimestamptzType(), False, None),
        ("open", DoubleType(), False, None),
        ("high", DoubleType(), False, None),
        ("low", DoubleType(), False, None),
        ("close", DoubleType(), False, None),
        ("volume", DoubleType(), False, None),
        ("close_time", TimestamptzType(), False, None),
        ("ingest_ts", TimestamptzType(), False, None),
    )
    klines_spec = PartitionSpec(("symbol", IdentityTransform()), ("open_time", DayTransform()))

    trades_id = ("local", "crypto", "binance", "trades_spot")
    if not catalog.table_exists(trades_id):
        catalog.create_table(trades_id, schema=trades_schema, partition_spec=trades_spec)

    klines_id = ("local", "crypto", "binance", "klines_spot_1m")
    if not catalog.table_exists(klines_id):
        catalog.create_table(klines_id, schema=klines_schema, partition_spec=klines_spec)


if __name__ == "__main__":
    ensure_tables()
    print("Iceberg tables ensured at", WAREHOUSE)
