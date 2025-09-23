import os, json, urllib.request

REST_URI = os.getenv("REST_URI", "http://localhost:8181/catalog")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "demo")

with urllib.request.urlopen(f"{REST_URI}/v1/config?warehouse={WAREHOUSE_ID}", timeout=5) as r:
    assert r.status == 200, f"unexpected status: {r.status}"
    cfg = json.loads(r.read().decode())
    assert "endpoints" in cfg and "defaults" in cfg, f"unexpected cfg: {cfg}"
print("config ok for warehouse:", WAREHOUSE_ID)