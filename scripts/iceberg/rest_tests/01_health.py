import os, json, urllib.request

REST_URI = os.getenv("REST_URI", "http://localhost:8181/catalog")
BASE = REST_URI.rsplit("/catalog", 1)[0] if "/catalog" in REST_URI else REST_URI

with urllib.request.urlopen(f"{BASE}/health", timeout=5) as r:
    assert r.status == 200, f"unexpected status: {r.status}"
    body = json.loads(r.read().decode())
    assert body.get("health") == "ok", f"unexpected body: {body}"
print("health ok:", body)
