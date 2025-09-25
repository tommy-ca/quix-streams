from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

class FakeResponse:
    def __init__(self, status_code: int = 200, json_body: Optional[Dict[str, Any]] = None, url: str = "http://fake/endpoint"):
        self.status_code = status_code
        self._json = json_body or {"records_written": 1}
        self.reason = "OK" if 200 <= status_code < 300 else "ERROR"
        self.text = json.dumps(self._json)
        self.url = url
    def json(self) -> Dict[str, Any]:
        return self._json

class FakeRESTCatalogClient:
    """In-memory fake REST catalog client for tests/examples without network."""
    def __init__(self, *_, **__):
        self.posts: List[Dict[str, Any]] = []
        self.closed = False
        self.catalog_uri = "http://fake"
        self.table_name = "fake"
        self.warehouse_id = "local-warehouse"
        self.catalog_token = None
    def build_endpoint_url(self) -> str:
        return f"{self.catalog_uri}/warehouses/{self.warehouse_id}/tables/{self.table_name}/data"
    def build_headers(self) -> Dict[str, str]:
        return {"Content-Type": "application/json"}
    def build_payload(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {"records": records, "format": "json", "table_name": self.table_name, "batch_size": len(records)}
    def post_records(self, records: List[Dict[str, Any]]):
        self.posts.append({"records": records})
        return FakeResponse(status_code=201, json_body={"records_written": len(records)}, url=self.build_endpoint_url())
    def health_check(self) -> Dict[str, Any]:
        return {"status": "healthy", "status_code": 200, "catalog_uri": self.catalog_uri}
    def close(self) -> None:
        self.closed = True