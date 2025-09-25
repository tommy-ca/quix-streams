# Iceberg REST Sink Specification

Status: Draft
Owners: Data Integrations
Last Updated: 2025-09-23

1. Overview
- Specifies the configuration, REST contract, behavior, and acceptance criteria for IcebergRESTSink.
- Principles: KISS, SOLID (Config vs Client vs Sink), DRY naming, no legacy compat in spec.

2. Configuration (canonical, from quixstreams.sinks.community.iceberg_rest.config)
- Main model: IcebergConfig with nested CatalogConfig and StorageConfig
- CatalogConfig
  - uri: str (required, http(s) URL)
  - warehouse_id: str (required)
  - auth_type: "none" | "bearer_token" (default: "none")
  - token: Optional[str]
- StorageConfig
  - provider: "aws" | "cloudflare_r2" | "minio" | "custom" (enum)
  - region: str (default for AWS: "us-east-1"; Cloudflare R2 coerces to "auto")
  - access_key_id: Optional[str], secret_access_key: Optional[str], session_token: Optional[str]
  - endpoint_url: Optional[str] (required for minio/custom; auto-derived for R2)
  - account_id: Optional[str] (required for R2), bucket_name: Optional[str]
- IcebergConfig
  - table_name: str (required)
  - Computed/compat properties: catalog_uri, rest_uri, warehouse_id, catalog_token (read-only proxy to CatalogConfig.token), auth (pyiceberg-style dict), location
- Validation
  - CatalogConfig validates uri and warehouse_id non-empty and well-formed
  - StorageConfig validates provider-specific requirements
  - validate_config(config) raises if table_name is empty
  - Token is provided via CatalogConfig.token (immutable property on IcebergConfig); rotate by constructing a new config
- Logging (sink)
  - "Initialized IcebergRESTSink: table=..., catalog=..., batch_size=..., timeout=...s"
  - "Closing IcebergRESTSink..." then "...closed successfully" or error log

3. REST Client Contract (quixstreams.sinks.community.iceberg_rest.client)
- Endpoint
  - Build: urljoin(catalog_uri + "/", f"warehouses/{warehouse_id}/tables/{table_name}/data")
  - Method: POST
- Headers
  - Content-Type: application/json; Accept: application/json; User-Agent: QuixStreams-IcebergREST/1.0
  - Authorization: Bearer {{ICEBERG_TOKEN}} when CatalogConfig.token provided
- Payload
  - { "records": [...], "format": "json", "table_name": str, "timestamp": epoch_ms, "batch_size": int }
  - Serialized with orjson if available, else ujson/json; optionally gzip when >1MB
- Responses
  - 2xx: success; logs count if response.json().records_written present
  - 4xx/5xx: AuthenticationError for 401/403; CatalogError for others
- Retries
  - urllib3 Retry total=max_retries, backoff_factor applied; status_forcelist=[429,500,502,503,504]
- Idempotency
  - Batching and commit are handled in sink; retries occur at HTTP layer; design for at-least-once

4. Schema mapping and serialization
- Use orjson for encoding
- Field naming consistent with normalized crypto records
- Timestamp fields encoded as numeric epoch (ms/us) or ISO-8601 (document actual behavior)

5. Behavior (sink)
- Batching
  - Accumulate up to batch_size then POST; adaptive_batching adjusts based on record size
  - flush(): send pending if any; close(): flush pending and release client
- Empty batch
  - write([] or None) is a no-op; flush/close no-op when buffer empty
- Error handling
  - Non-2xx → AuthenticationError (401/403) or CatalogError; network/timeout errors mapped accordingly
  - Client close is best-effort and does not mask earlier errors (errors logged)

6. Examples (minimal, runnable)
- Environment setup
```bash path=null start=null
uv venv && source .venv/bin/activate
uv pip install -e .[dev]
ruff check .
```
- Local development with MinIO + Lakekeeper
```python path=null start=null
from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_config

config = create_local_config(table_name="crypto_trades", warehouse_id="local-warehouse")
sink = IcebergRESTSink(config=config, batch_size=3)

sink.setup()
sink.write([
    {"symbol": "BTC/USDT", "price": 65000.0, "qty": 0.01, "ts": 1700000000000},
    {"symbol": "ETH/USDT", "price": 3300.0, "qty": 1.0, "ts": 1700000005000},
    {"symbol": "SOL/USDT", "price": 145.0, "qty": 2.0, "ts": 1700000010000},
])
sink.close()
```
- Production with token (do not inline secrets)
```python path=null start=null
import os
from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_config

ICEBERG_TOKEN = os.getenv("ICEBERG_TOKEN")  # set in env securely
config = create_config(
    table_name="analytics.events",
    catalog_uri="https://catalog.example.com/api/v1",
    warehouse_id="analytics",
    provider="aws",
    region="us-east-1",
    catalog_token=ICEBERG_TOKEN,
)
sink = IcebergRESTSink(config=config, batch_size=500)
```

7. Acceptance criteria
- Construction and validation
  - Import of IcebergRESTSink succeeds
  - Configuration validates: table_name non-empty; CatalogConfig uri and warehouse_id non-empty and well-formed; provider-specific checks
- HTTP semantics
  - Authorization header included when token provided (Bearer)
  - Non-2xx responses raise AuthenticationError (401/403) or CatalogError (other 4xx/5xx)
- Write semantics
  - write([] or None) is a no-op; batching honors batch_size or adaptive strategy; flush on close sends pending
  - Logging messages present as documented for init/close

8. Test mapping
- Unit and integration
  - tests/test_iceberg_rest.py
  - tests/sinks/community/test_iceberg_rest_config.py
  - tests/e2e/iceberg_sink/*.py
  - tests/integration/test_iceberg_rest_integration.py
- Reconciliation items
  - Token immutability: Provide token via create_config/CatalogConfig; tests should not mutate cfg.catalog_token after construction
  - Fail-fast catalog validation: Either construct invalid CatalogConfig (should raise ValueError) or have sink validate via validate_sink_config before client init; tests expecting ConfigurationError at sink construction may be updated to reflect config-layer validation

9. Gaps / TODOs
- Confirm and document timestamp field name conventions when normalizing records end-to-end
- Provide a small in-memory fake client example (non-network) for local doc demo

10. Migration Notes
- create_local_rest_config() is deprecated; use create_local_config() or create_config()
- No legacy compatibility layers documented here; follow the unified configuration model

11. Timestamp conventions (E2E)
- Record-level timestamp
  - Crypto sources normalize timestamps to ts_event within each record (see crypto spec)
  - The sink preserves record fields as-is; it does not transform ts_event
- Batch-level timestamp
  - REST client adds a batch-level payload field "timestamp" = ingestion time (epoch ms)
  - This is not a substitute for per-record event time; use ts_event for event-time processing
- Recommendation
  - Ensure upstream sources include ts_event; downstream consumers should rely on ts_event for event-time semantics
  - Use batch-level timestamp for operational observability (arrival/ingestion time)
