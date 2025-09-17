# Crypto Source: Binance S3 (Draft Spec)

Status: Ready
Owner: quix-streams community

1. Problem
- Support multiple Binance archive access patterns (segments/kinds) across markets (spot, futures) and granularities (daily/monthly), including templated path generation by symbol/date/interval.
- Backfill historical crypto data from Binance public archives stored in S3 into Kafka for downstream processing and lakehouse ingestion.

2. Goals
- Replay historical records (initially trades; future: klines, order book) from S3 to Kafka via Quix Streams Source API
- Preserve event timestamps when present; configurable replay pacing (as-fast-as-possible or approximate realtime)
- Operate against public unsigned buckets or signed (IAM) access
- Provide deterministic file traversal and robust error handling with retries

3. Non-goals (v1)
- Complex folder topologies beyond a single prefix root
- Automatic schema inference for every archive variant; start with JSONL and gzip
- Cross-exchange normalization beyond basic fields

4. Installation
- pip install quixstreams[aws]

5. Configuration (proposed)
- Access modes:
  - direct_prefix: use a single prefix (existing behaviour)
  - templated_prefixes: generate multiple prefixes using a template and symbol/date iteration (dataloader pattern)
- dataloader fields (templated_prefixes):
  - prefix_template: e.g. "{root}/{market}/{segment}/{datatype}/{symbol}/{date}/"
  - root: e.g., "p" (top-level folder)
  - market: "spot"|"um_futures"|"cm_futures"
  - segments: list of "daily"|"monthly"
  - datatypes: list[str] (e.g., ["trades","klines"])
  - symbols: list[str]
  - date_from/date_to: YYYY-MM-DD inclusive for daily; monthly derived as YYYY-MM
- interval: e.g., "1m" for klines (used in filenames; optional)
  - You can reference {interval} in prefix_template if present in the folder structure (e.g., p/{market}/{segment}/{datatype}/{interval}/{symbol}/{date}/)

The dataloader iterates: for each segment in segments; for each datatype in datatypes; for each symbol in symbols; for each date in [date_from..date_to] (if segment=daily) → produces a concrete S3 prefix from the template and loads all files under it.

Examples of patterns
- Daily Trades (Spot):
  - prefix_template: p/{market}/{segment}/{datatype}/{symbol}/{date}/
  - market=spot, segments=[daily], datatypes=[trades], symbols=[BTCUSDT,ETHUSDT], date_from=2025-01-01, date_to=2025-01-02
  - Generated prefixes (subset):
    - p/spot/daily/trades/BTCUSDT/2025-01-01/
    - p/spot/daily/trades/BTCUSDT/2025-01-02/
    - p/spot/daily/trades/ETHUSDT/2025-01-01/
- Monthly Klines (Spot):
  - prefix_template: p/{market}/{segment}/{datatype}/{symbol}/
  - market=spot, segments=[monthly], datatypes=[klines], symbols=[BTCUSDT]
  - Generated prefixes:
    - p/spot/monthly/klines/BTCUSDT/

Example usage
```python
from quixstreams import Application
from quixstreams.sources.community.crypto import BinanceS3Source

app = Application(broker_address="localhost:19092")

source = BinanceS3Source(
    bucket="data.binance.example",
    prefix="p/",
    access_mode="templated_prefixes",
    prefix_template="p/{market}/{segment}/{datatype}/{symbol}/{date}/",
    market="spot",
    segments=["daily"],
    datatypes_list=["trades","klines"],
    symbols=["BTCUSDT","ETHUSDT"],
    date_from="2025-01-01",
    date_to="2025-01-02",
    checksum_mode="warn",
)

sdf = app.dataframe(source=source)
sdf.print(metadata=True)

app.run()
```
- bucket: str (required)
- prefix: str (path prefix to files; required)
- unsigned: bool = false (use anonymous access)
- region_name, endpoint_url, aws_access_key_id, aws_secret_access_key: optional
- file_format: {"infer","jsonl","csv"} = "infer"
- compression: {"infer","gzip","zip","none"} = "infer"
- datatype: {"trades","klines","orderbook"} = "trades"
- replay_speed: float = 0.0 (0.0=as fast as possible; 1.0=approx realtime pacing)
- has_partition_folders: bool = false (aligns partitions with folder structure)
- key_setter(record) -> str; default: "{exchange}:{symbol}"
- timestamp_setter(record) -> int ms; default uses record's event timestamp if present

6. Topics and schema
- CSV klines mapping: [open_time, open, high, low, close, volume, close_time, ...] -> normalized fields; ts_event set to close_time.
- ZIP archives: first file entry is decompressed and parsed as JSONL.
- Default topic name: crypto.source.binance.s3.{datatype}
- Key default: "binance:{symbol}"
- Value default: JSON dictionary; normalized minimal shape for trades:
  - { exchange, symbol, trade_id?, side?, price, qty, ts_event }

7. Behavior
- setup(): validate bucket access (ListObjectsV2 with MaxKeys=1); fire connection callbacks
- run(): recursively list objects under prefix; filter extensions by format/compression; for each object:
  - stream download; decompress; parse records line-by-line (jsonl) or rows (csv)
  - map to Kafka key/value/timestamp using provided setters
  - apply pacing if replay_speed>0 using inter-record deltas
  - on errors: retry S3 calls with exponential backoff; skip malformed lines with warning
- stop(): close client and flush producer

8. Error handling
- Optional checksum verification: looks for a sibling ".CHECKSUM" object (md5 hex) for each data object and validates against the object's raw bytes.
  - checksum_mode: "skip" (default), "warn" (log a warning on mismatch), "strict" (raise and stop)
- Transient S3 errors (5xx, throttling) retried with exponential backoff up to N attempts
- Non-retriable errors logged and file skipped; metrics hooks planned later

9. Metadata extraction
- When enabled, extracts metadata from the S3 key path: { market, segment (daily/monthly), datatype, symbol, date, s3_key } and attaches under value.meta.
- Heuristics assume a folder structure like: {market}/{segment}/{datatype}/{symbol}/{date}/file.ext
- Always includes s3_key in metadata for provenance
- Transient S3 errors (5xx, throttling) retried with exponential backoff up to N attempts
- Non-retriable errors logged and file skipped; metrics hooks planned later

9. Testing strategy
- Unit tests with botocore Stubber for list/get; parsing tests for jsonl.gz; pacing tests using time stub
- Optional integration test gated by env vars

10. Example (sketch)
```python
from quixstreams import Application
from quixstreams.sources.community.crypto import BinanceS3Source

app = Application(broker_address="localhost:19092")
source = BinanceS3Source(bucket="data.binance.example", prefix="trades/spot/BTCUSDT/2024-01-01/", unsigned=True, replay_speed=1.0)

sdf = app.dataframe(source=source)
sdf.print(metadata=True)

app.run()
```

11. Installation and extras
- pip install quixstreams[aws]

12. Configuration summary
- bucket (str, required)
- prefix (str, required)
- unsigned (bool, default: false)
- region_name, endpoint_url, aws_access_key_id, aws_secret_access_key (optional)
- file_format ("infer"|"jsonl"|"csv", default: "infer")
- compression ("infer"|"gzip"|"zip"|"none", default: "infer")
- datatype ("trades"|"klines"|"orderbook", default: "trades")
- replay_speed (float, default: 0.0)
- has_partition_folders (bool, default: false)
- key_setter (callable)
- timestamp_setter (callable)
