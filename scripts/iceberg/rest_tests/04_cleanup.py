import os
from pyiceberg.catalog import load_catalog

REST_URI = os.getenv("REST_URI", "http://localhost:8181/catalog")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "demo")

cat = load_catalog("local", **{"type": "rest", "uri": REST_URI, "warehouse": WAREHOUSE_ID})
ident = ("local","crypto","binance","tdd_smoke")
try:
    cat.drop_table(ident)
    print("dropped:", ".".join(ident))
except Exception as e:
    print("drop skipped:", e)
