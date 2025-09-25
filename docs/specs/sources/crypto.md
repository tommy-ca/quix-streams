# Crypto Sources Specification

Status: Draft
Owners: Data Integrations
Last Updated: 2025-09-23

1. Overview
- This document specifies the behavior, configuration, and contracts for community crypto sources:
  - BinanceS3Source
  - CCXTSource
  - CryptofeedSource
- Goals: KISS, SOLID separation, DRY naming, no legacy/compat layers in spec. Pydantic v2 configuration is the only first-class path.

2. Configuration (Pydantic v2)
- Canonical config classes (quixstreams.sources.community.crypto.config):
  - BinanceS3Config
  - CCXTConfig
  - CryptofeedConfig
- General rules
  - All fields lower_snake_case
  - Validation at model construction; fail fast for invalid values
  - Environment variable support via load_from_env()
  - Deprecated: passing direct kwargs to sources (documented only in Migration Notes)
- Field inventory (from code)
  - BinanceS3Config
    - Required: bucket: str, prefix: str
    - Common: datatype: str = "trades", unsigned: bool = False, file_format: str = "infer", compression: str = "infer"
    - Replay: replay_speed: float = 0.0, dry_run: bool = False, shutdown_timeout: float = 30.0
    - Layout: has_partition_folders: bool = False, extract_metadata: bool = True
    - Checksums: checksum_mode: str = "skip" | "warn" | "strict"
    - Modes: access_mode: str = "direct_prefix" | "templated_prefixes"
      - prefix_template: Optional[str]
      - root: Optional[str], market: Optional[str]
      - segments: list[str] = ["daily"], datatypes_list: list[str] (defaults to [datatype])
      - symbols: list[str] = [], date_from: Optional[str], date_to: Optional[str], interval: Optional[str]
    - Auth: auth_provider: AWSAuth (defaults to NoAuth() if unsigned=True)
    - Name: name: Optional[str]
    - Validation notes:
      - bucket and prefix cannot be empty
      - When access_mode == "templated_prefixes", prefix_template is required (unless validate_prefix_template=False for tests)
  - CCXTConfig
    - Required: exchange: str, mode: str in {"trades","ticker","orderbook","klines"}, symbols: list[str]
    - Optional: interval: Optional[str] (for klines), rate_limit: bool = True, normalize: bool = True
    - REST polling: rest_poll_interval: float = 0.0
    - Shutdown: shutdown_timeout: float = 10.0
    - Name: name: Optional[str]
    - Auth: auth_provider: AuthProvider = NoAuth(), use_websocket: bool = False
    - Normalization fills exchange/symbol if missing on events
  - CryptofeedConfig
    - Required: exchanges: list[str], channels: list[str]
    - Optional: symbols: list[str] = []
    - Auth: auth_provider: AuthProvider = NoAuth()
    - Behavior: reconnect: bool = True, normalize: bool = True
    - Name: name: Optional[str]
    - Shutdown: shutdown_timeout: float = 10.0, retry_config: RetryConfig (max_retries, enabled)
    - Supported: exchanges in {binance, kraken, coinbase, bitfinex, bybit, okx}; channels in {trades,ticker,orderbook,klines}

3. Behavior
- Source lifecycle: initialize → start/iterate → shutdown
- Normalization: ensure consistent record schema per source; case and symbol normalization rules should match tests
- Backward-compat behaviors are not part of the spec (see Migration Notes)

4. Topic naming, keys, timestamps
- Deterministic patterns (from utils.TopicBuilder and tests/test_utils.py)
  - TopicBuilder(provider, exchange?, datatype, interval?) builds:
    - Base: "crypto.source.{provider}.{datatype}"
    - If exchange is provided: "crypto.source.{provider}.{exchange}.{datatype}" (exchange lowercased)
    - If interval is provided: append "_{interval}" to datatype (e.g., "klines_1m")
  - Source defaults
    - CCXTSource: name defaults to TopicBuilder("ccxt", datatype=f"{mode}{'_' + interval if mode=='klines' and interval else ''}")
    - BinanceS3Source: name defaults to TopicBuilder("binance.s3", datatype=config.datatype)
    - CryptofeedSource: defaults to literal "cryptofeed-source"
  - Key function: default_key_fn(event) -> "{exchange}:{symbol}"
  - Timestamp function: default_ts_fn(event) -> event["ts_event"] (Binance S3 CSV klines fallback to close_time)
- This section aligns with tests in tests/test_quixstreams/test_sources/test_community/test_crypto/test_utils.py

5. Schemas (record shapes)
- Normalizers (quixstreams.sources.community.crypto.utils):
  - normalize_trade: {exchange,symbol,trade_id,side,price,qty,ts_event}
  - normalize_ticker: {exchange,symbol,bid,ask,last,ts_event}
  - normalize_klines: {exchange?,symbol?,interval,open,high,low,close,volume,open_time,close_time}
  - normalize_orderbook: {exchange,symbol,bids,asks,ts_event,type}
- Notes:
  - CCXT klines tuple support: [open_time, open, high, low, close, volume, close_time?, symbol?, exchange?]
  - Missing exchange/symbol are filled from config in sources where applicable

6. Errors, retries, backoff
- CCXT
  - Respects exchange.rateLimit when rate_limit=True (sleep between requests)
  - fetchTrades exceptions are logged and skipped; cursor advances by max ts_event
  - Orderbook polling uses rest_poll_interval when > 0
- Cryptofeed
  - retry_with_backoff around FeedHandler.run; when reconnect=False, disable retries
  - Errors in run are logged and wrapped as connection_error("CryptofeedSource", ...)
- Binance S3
  - get_object retries with backoff sequence [0.5, 1.0, 2.0] (test-injectable)
  - Checksum modes: skip (default) | warn (log warning) | strict (raise on mismatch/missing)
  - Listing paginated via list_objects_v2; skips .CHECKSUM sidecars; deterministic ordering via sorted(keys)

7. Binance S3 data loader specifics
- Access modes
  - direct_prefix: iterate exactly the given prefix
  - templated_prefixes: expand prefix_template using variables
- Prefix templates and variables
  - Template variables: {root},{market},{segment},{datatype},{symbol},{date},{interval}
  - Use expand_dataloader_prefixes() for deterministic expansion
- Partition expansion
  - segments: ["daily"|"monthly"|...] ; for daily and when date_from/date_to provided, expand each day (inclusive)
  - monthly does not expand by date; interval used for klines paths
- Replay speeds
  - replay_speed seconds derived from event ts deltas; 0.0 means no delay; >0 multiplies delay
- Dry-run mode
  - When dry_run=True, list keys only; no get_object or parsing

8. Examples (minimal, runnable)
- Environment setup
```bash path=null start=null
uv venv && source .venv/bin/activate
uv pip install -e .[dev]
ruff check .
```
- CCXT trades example
```python path=null start=null
from quixstreams.sources.community.crypto import CCXTSource
from quixstreams.sources.community.crypto.simple_config import CCXTConfig

cfg = CCXTConfig(exchange="binance", mode="trades", symbols=["BTC/USDT"])  # minimal
src = CCXTSource(config=cfg)
# Single run pass (tests expect one pass per run)
src.setup()
src.run()
```
- CCXT klines example with polling
```python path=null start=null
from quixstreams.sources.community.crypto import CCXTSource
from quixstreams.sources.community.crypto.simple_config import CCXTConfig

cfg = CCXTConfig(exchange="binance", mode="orderbook", symbols=["BTC/USDT"], rest_poll_interval=0.5)
src = CCXTSource(config=cfg)
src.setup()
src.run()
```
- Cryptofeed example
```python path=null start=null
from quixstreams.sources.community.crypto import CryptofeedSource
from quixstreams.sources.community.crypto.simple_config import CryptofeedConfig

cfg = CryptofeedConfig(exchanges=["binance"], channels=["trades"], symbols=["BTC-USDT"])  # minimal
src = CryptofeedSource(config=cfg)
# src.run() would block; rely on framework to manage lifecycle
```
- Binance S3 replay (dry-run)
```python path=null start=null
from quixstreams.sources.community.crypto import BinanceS3Source
from quixstreams.sources.community.crypto.simple_config import BinanceS3Config

cfg = BinanceS3Config(bucket="binance-data", prefix="spot/trades/BTCUSDT/", dry_run=True)
src = BinanceS3Source(config=cfg)
src.setup()
src.run()  # will list keys without fetching
```

9. Acceptance criteria
- Unified configs
  - Construction fails fast on empty required fields (e.g., exchange, mode, symbols; bucket/prefix; exchanges/channels)
  - Environment loading via load_from_env supports documented variables
- Topics/keys/timestamps
  - TopicBuilder output matches tests; message key is "exchange:symbol"; timestamp uses ts_event (or close_time for CSV klines)
- Behavior
  - CCXT respects .rateLimit sleeping; orderbook polling uses rest_poll_interval; cursor advances deterministically
  - Cryptofeed retries when reconnect=True; disables retry when reconnect=False
  - Binance S3 expands templated prefixes deterministically; checksum modes behave per mode; list ordering is stable; dry-run lists only

10. Test mapping
- Utilities and normalization
  - tests/test_quixstreams/test_sources/test_community/test_crypto/test_utils.py
- CCXT source
  - tests/test_quixstreams/test_sources/test_community/test_crypto/test_ccxt_source.py
- Cryptofeed source
  - tests/test_quixstreams/test_sources/test_community/test_crypto/test_cryptofeed_source.py
- Binance S3 source
  - tests/test_quixstreams/test_sources/test_community/test_crypto/test_binance_s3_source.py
  - tests/test_quixstreams/test_sources/test_community/test_crypto/binance_s3/test_dataloader.py
  - tests/test_quixstreams/test_sources/test_community/test_crypto/binance_s3/test_dataloader_utils.py
- Config unification
  - quixstreams/sources/community/crypto/tests/test_unified_config.py

11. Gaps / TODOs
- Document environment variable matrix for Binance S3 beyond essentials (prefix_template variables)
- Add explicit tests for CSV headerless trades and nested gzip-in-zip paths (covered loosely, could be isolated)
- Clarify symbol normalization for CCXT vs Binance CSV path-derived symbol edge cases

12. Migration Notes
- Direct kwargs to sources are deprecated; use Pydantic v2 config classes
- Ensure naming consistency across configs; prefer singular/plural forms as per source capability (single vs multi exchange)

13. Environment variables (load_from_env)
- Function: load_from_env(BinanceS3Config)
- Variables (prefix CRYPTO_BINANCE_*)
  - CRYPTO_BINANCE_BUCKET: S3 bucket name (required) – maps to bucket
  - CRYPTO_BINANCE_PREFIX: S3 prefix (required) – maps to prefix
  - CRYPTO_BINANCE_DATATYPE: datatype, default "trades"
  - CRYPTO_BINANCE_UNSIGNED: bool, default False – when True, forces NoAuth
  - CRYPTO_BINANCE_ACCESS_MODE: "direct_prefix" (default) | "templated_prefixes"
  - CRYPTO_BINANCE_PREFIX_TEMPLATE: template string when access_mode=templated_prefixes
  - CRYPTO_BINANCE_DRY_RUN: bool, default False
- Template variables (for prefix_template): {root},{market},{segment},{datatype},{symbol},{date},{interval}
- Notes
  - When access_mode=templated_prefixes, prefix_template is required (unless validate_prefix_template=False in tests)
  - datatypes_list defaults to [datatype]; segments defaults to ["daily"]
  - For Cryptofeed/CCXT, environment variables are under CRYPTO_* (exchanges, channels, symbols, exchange, mode, interval, etc.)
