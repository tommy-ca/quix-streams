# Crypto Sources Technical Specification

## Overview

This document consolidates the specifications for the community crypto sources shipped with the `feature/crypto-sources-lakehouse` branch. The goal is to provide a single reference for the three primary connectors:

- `CryptofeedSource` – realtime ingestion via the `cryptofeed` websocket client.
- `CCXTSource` – REST (and optional websocket) ingestion built on `ccxt`.
- `BinanceS3Source` – historical replay from Binance public S3 archives.

Each section captures the current behaviour, configuration surface, and testing strategy. The implementation has been brought in line with the greenfield configuration system and validated through TDD-driven regression suites.

## CryptofeedSource Spec

### Problem Statement
Ingest realtime crypto market data across multiple exchanges and channels via `cryptofeed`, publishing normalized records into Kafka.

### Goals
- Support core channels (trades, ticker) with per-exchange/topic naming.
- Provide minimal normalized schemas and optional normalization toggles.
- Handle reconnection/backoff gracefully; expose retry knobs.

### Configuration
```python
CryptofeedConfig(
    exchanges: list[str],         # e.g. ["binance", "kraken"]
    channels: list[str],          # e.g. ["trades", "ticker"]
    symbols: list[str] = [],
    auth_provider: AuthProvider = NoAuth(),
    reconnect: bool = True,
    normalize: bool = True,
    retry_config: RetryConfig = RetryConfig(),
)
```

`RetryConfig` encapsulates retry toggles (`enabled`), exponential backoff parameters (`base_delay`, `max_delay`, `backoff_factor`), and the retry budget (`max_retries`).
Deprecated kwargs constructors emit a `DeprecationWarning` and delegate to the config object.

### Behaviour
- `setup()` lazily imports `cryptofeed`, creates a `FeedHandler`, and registers callbacks per exchange/channel.
- `run()` starts the handler, catching runtime errors and retrying with exponential backoff driven by `retry_config`; produces events via Quix Streams. When `reconnect` is `False`, the retry manager is disabled after the first failure.
- `stop()` stops the handler and flushes the producer.

### Schemas
- Trade: `{ exchange, symbol, trade_id?, side?, price, qty, ts_event }`
- Ticker: `{ exchange, symbol, bid?, ask?, last?, ts_event }`

### Testing Strategy
- Unit tests monkeypatch the `FeedHandler` to simulate callbacks, reconnection, and retry paths while asserting the configured `RetryConfig` delays are applied.
- Integration smoke tests (`tests/e2e/crypto_sources/test_crypto_source_integration.py`) verify config-based construction, error handling, and retry loops.

## CCXTSource Spec

### Problem Statement
Fetch historical or near-realtime data via CCXT REST APIs and publish normalized records into Kafka, honoring exchange rate limits.

### Goals
- Support `klines`, `trades`, and `orderbook` modes with per-symbol cursors.
- Respect rate limits and provide consistent retry/backoff behaviour.
- Enable optional websocket support in future iterations.

### Configuration
```python
CCXTConfig(
    exchange: str,              # e.g. "binance"
    mode: Literal["klines", "trades", "orderbook"],
    symbols: list[str],         # CCXT notation, e.g. ["BTC/USDT"]
    interval: str | None = None,
    rate_limit: bool = True,
    normalize: bool = True,
    rest_poll_interval: float = 0.0,
    shutdown_timeout: float = 10.0,
    name: str | None = None,
    auth_provider: AuthProvider = NoAuth(),
    use_websocket: bool = False,
)
```

### Behaviour
- `setup()` imports `ccxt`, instantiates the exchange client, and validates support.
- `run()` dispatches to `_handle_klines`, `_handle_trades`, or `_handle_orderbook`; each loop updates cursors and respects rate limits.
- Transient trade fetch failures trigger logging and rate-limit sleeps rather than raising immediately.

### Schemas
- Klines: `{ exchange, symbol, interval, open, high, low, close, volume, open_time, close_time }`
- Trades: `{ exchange, symbol, trade_id?, side?, price, qty, ts_event }`
- Orderbook snapshot: `{ exchange, symbol, bids, asks, ts_event, type="snapshot" }`

### Testing Strategy
- Unit tests stub the CCXT client to validate cursor advance, rate-limit sleeps, and topic naming (`tests/test_quixstreams/.../test_ccxt_source.py`).
- E2E integration tests ensure config-based paths succeed and dependency errors are surfaced.

## BinanceS3Source Spec

### Problem Statement
Replay historical Binance market data stored in public S3 buckets into Kafka, supporting multiple folder structures and templated prefixes.

### Goals
- Deterministic traversal across daily/monthly segments, markets, datatypes, and symbols.
- Support unsigned public archives and signed IAM access.
- Provide optional checksum verification and metadata extraction.

### Configuration
```python
BinanceS3Config(
    bucket: str,
    prefix: str,
    datatype: str = "trades",
    unsigned: bool = False,
    file_format: str = "infer",
    compression: str = "infer",
    replay_speed: float = 0.0,
    access_mode: Literal["direct_prefix", "templated_prefixes"] = "direct_prefix",
    prefix_template: str | None = None,
    root: str | None = None,
    market: str | None = None,
    segments: list[str] = ["daily"],
    datatypes_list: list[str] = [],
    symbols: list[str] = [],
    date_from: str | None = None,
    date_to: str | None = None,
    interval: str | None = None,
    dry_run: bool = False,
    checksum_mode: Literal["skip", "warn", "strict"] = "skip",
    auth_provider: AuthProvider = AWSAuth("", ""),
)
```

### Behaviour
- `setup()` validates access (ListObjectsV2) and raises structured errors on credential/bucket issues.
- `_iter_prefixes()` expands templated prefixes across market/segment/datatypes/symbols/date ranges.
- `run()` lists and downloads objects, decompresses/decodes (JSONL, CSV, ZIP), attaches metadata, applies optional replay pacing, and respects checksum mode.

### Testing Strategy
- Unit tests stub `boto3` with dummy clients to cover pagination, templated prefixes, parsing formats, replay pacing, checksum handling, and error retries (`tests/test_quixstreams/.../test_binance_s3_source.py`).
- Integration smoke tests validate config-based construction and dry-run behaviour.

## Testing & Validation Summary
- Regression suites run via:
  ```bash
  pytest tests/test_packaging/test_dependencies.py \
         quixstreams/sinks/community/iceberg_rest/tests/test_rest_sink.py \
         quixstreams/sources/community/crypto/tests/test_unified_config.py \
         tests/test_quixstreams/test_sources/test_community/test_crypto/test_binance_s3_source.py \
         tests/test_quixstreams/test_sources/test_community/test_crypto/test_ccxt_source.py \
         tests/test_quixstreams/test_sources/test_community/test_crypto/test_cryptofeed_source.py \
         tests/e2e/crypto_sources/test_crypto_source_integration.py \
         tests/unit/crypto/test_unit_source_integration.py -q
  ```
- Crypto source specs align with the unified configuration module (`quixstreams.sources.community.crypto.config`).
- Iceberg sink specs remain the source of truth for REST-based lakehouse ingestion.
