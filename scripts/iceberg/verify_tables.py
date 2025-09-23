# scripts/iceberg/verify_tables.py
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
import os
import pathlib
import glob

try:
    import pyarrow.parquet as pq
except Exception:
    pq = None

WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "/home/tommyk/data/iceberg/warehouse")
CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "local")
CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "sql")
CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", f"sqlite:////{os.path.expanduser('~')}/data/iceberg/catalog.db")
VERIFY_TABLE = os.getenv("VERIFY_TABLE")  # "klines" or "trades" or None for both


def _cat():
    if CATALOG_TYPE == "rest":
        # For REST, the warehouse may be non-file (e.g., s3://...). Pass as-is.
        return load_catalog(CATALOG_NAME, **{"type": CATALOG_TYPE, "uri": CATALOG_URI, "warehouse": WAREHOUSE})
    if CATALOG_TYPE in ("nessie", "sql"):
        return load_catalog(CATALOG_NAME, **{"type": CATALOG_TYPE, "uri": CATALOG_URI, "warehouse": f"file:{WAREHOUSE}"})
    return load_catalog(CATALOG_NAME, **{"type": CATALOG_TYPE, "warehouse": f"file:{WAREHOUSE}"})


def _load_safe(cat, ident):
    try:
        return cat.load_table(ident)
    except NoSuchTableError:
        print(f"missing table: {'.'.join(ident)}")
        return None


def _location_path(loc: str) -> str:
    # pyiceberg returns e.g. 'file:/home/..../warehouse/crypto/binance/trades_spot'
    if loc.startswith("file:"):
        return loc[len("file:"):]
    return loc


def _count_parquet_files(table) -> int:
    base = _location_path(table.location())
    data_dir = os.path.join(base, "data")
    return len(glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True))


def _read_sample_row(table):
    if pq is None:
        return "pyarrow not available"
    base = _location_path(table.location())
    data_dir = os.path.join(base, "data")
    files = glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True)
    if not files:
        return None
    # pick the first file and return first row as dict
    try:
        pf = pq.ParquetFile(files[0])
        tbl = pf.read_row_groups([0], columns=None)
        if tbl.num_rows == 0:
            return None
        return {name: tbl.column(i)[0].as_py() for i, name in enumerate(tbl.schema.names)}
    except Exception as e:
        return f"error reading parquet: {e!r}"


def verify():
    cat = _cat()
    to_verify = []
    if VERIFY_TABLE == "trades":
        to_verify = [("local","crypto","binance","trades_spot")]
    elif VERIFY_TABLE == "klines":
        to_verify = [("local","crypto","binance","klines_spot_1m")]
    else:
        to_verify = [
            ("local","crypto","binance","trades_spot"),
            ("local","crypto","binance","klines_spot_1m"),
        ]

    tables = [(name, _load_safe(cat, name)) for name in to_verify]

    for ident, t in tables:
        name = ".".join(ident)
        if t is None:
            continue
        try:
            num_files = _count_parquet_files(t)
            print(f"{name} files:", num_files)
        except Exception as e:
            print(f"{name} files: error {e!r}")
        try:
            sample = _read_sample_row(t)
            print(f"{name} sample:", sample)
        except Exception as e:
            print(f"{name} sample: error {e!r}")


if __name__ == "__main__":
    verify()
