# Crypto Lakehouse Integration Patterns

## Overview

This document describes proven integration patterns for connecting crypto data sources to Iceberg REST sink for lakehouse storage. All patterns are based on production-ready implementations using QuixStreams.

**Components:**
- **Crypto Sources**: Real-time and historical crypto data ingestion (2110 lines, 100% complete)
- **Iceberg REST Sink**: High-performance lakehouse storage via REST catalog (6232 lines, 95%+ complete)

## Pattern 1: Real-Time Trading Data (Cryptofeed → Iceberg)

### Use Case
Stream real-time crypto trading data from exchanges via websockets for immediate analysis and alerting.

### Architecture
```
Binance/Kraken/Coinbase → Cryptofeed → QuixStreams → Iceberg REST Sink → Lakehouse
    (WebSocket)         (normalizer)    (streaming)    (REST catalog)    (storage)
```

### Configuration Pattern
```python
from quixstreams import Application
from quixstreams.sources.community.crypto import CryptofeedSource, create_cryptofeed_config
from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_config

# Application setup
app = Application(
    broker_address="localhost:9092",
    app_id="crypto-realtime-trading"
)

# Source configuration
source_config = create_cryptofeed_config(
    exchanges=["binance", "kraken"],
    channels=["trades", "ticker"],
    symbols=["BTCUSDT", "ETHUSDT", "ADAUSDT"],
    # Optional authentication for private feeds
    # auth_provider=APIKeyAuth(api_key="...", api_secret="...")
)

# Sink configuration
sink_config = create_local_config(
    table_name="crypto.realtime_trades",
    # Storage backend (S3/MinIO/R2)
    storage_endpoint="http://localhost:9000",
    storage_bucket="lakehouse"
)

# Integration
source = CryptofeedSource(source_config)
sink = IcebergRESTSink(sink_config)

topic = app.topic("crypto.trades")
sdf = app.dataframe(topic)
sdf.sink(sink)

app.run(source)
```

### Authentication Patterns

#### Crypto Source Authentication
```python
from quixstreams.sources.community.crypto import APIKeyAuth, NoAuth

# For public data (most common)
auth = NoAuth()

# For private API access
auth = APIKeyAuth(
    api_key=os.getenv("BINANCE_API_KEY"),
    api_secret=os.getenv("BINANCE_API_SECRET")
)

source_config = create_cryptofeed_config(
    exchanges=["binance"],
    channels=["trades"],
    symbols=["BTCUSDT"],
    auth_provider=auth
)
```

#### Iceberg Sink Authentication
```python
from quixstreams.sinks.community.iceberg_rest import create_s3_config

# AWS S3 authentication
sink_config = create_s3_config(
    catalog_uri="http://iceberg-catalog:8181",
    table_name="crypto.realtime_trades",
    storage_bucket="my-lakehouse-bucket",
    aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_region="us-east-1"
)

# Cloudflare R2 authentication
sink_config = create_r2_config(
    catalog_uri="http://iceberg-catalog:8181", 
    table_name="crypto.realtime_trades",
    storage_bucket="my-r2-bucket",
    r2_access_key=os.getenv("R2_ACCESS_KEY"),
    r2_secret_key=os.getenv("R2_SECRET_KEY"),
    r2_account_id=os.getenv("R2_ACCOUNT_ID")
)
```

### Data Schema
Cryptofeed normalizes data to this schema automatically:
```python
{
    "exchange": "binance",        # str - exchange name
    "symbol": "BTCUSDT",         # str - trading pair
    "price": 43250.50,           # float - trade price
    "quantity": 0.001,           # float - trade quantity
    "side": "buy",               # str - buy/sell
    "timestamp": 1704067200,     # int - unix timestamp
    "trade_id": "12345",         # str - exchange trade ID
    "channel": "trades"          # str - data type
}
```

## Pattern 2: Historical Analysis (Binance S3 → Iceberg)

### Use Case
Replay historical crypto data from public S3 archives for backtesting, research, and model training.

### Architecture
```
Binance S3 Archives → BinanceS3Source → QuixStreams → Iceberg REST Sink → Lakehouse
  (public bucket)      (replay engine)   (streaming)    (REST catalog)    (storage)
```

### Configuration Pattern
```python
from quixstreams.sources.community.crypto import BinanceS3Source, create_binance_s3_config

# Historical data configuration
source_config = create_binance_s3_config(
    bucket="binance-public-data",
    prefix_template="data/spot/daily/trades/{symbol}/",
    data_type="trades", 
    symbols=["BTCUSDT", "ETHUSDT"],
    date_from="2024-01-01",
    date_to="2024-01-31",
    replay_speed=1.0,  # 1.0 = real-time, 0.0 = as fast as possible
    enable_checksum=True
)

# Same sink configuration as real-time pattern
sink_config = create_local_config(table_name="crypto.historical_trades")

# Integration (same pattern)
app = Application(broker_address="localhost:9092", app_id="crypto-historical")
source = BinanceS3Source(source_config)
sink = IcebergRESTSink(sink_config)

topic = app.topic("crypto.historical")
sdf = app.dataframe(topic)
sdf.sink(sink)

app.run(source)
```

### Date Range Patterns
```python
# Single day
date_from="2024-01-15"
date_to="2024-01-15"

# Month range
date_from="2024-01-01" 
date_to="2024-01-31"

# Year range
date_from="2023-01-01"
date_to="2023-12-31"

# Open-ended (from date to latest available)
date_from="2024-01-01"
date_to=None
```

## Pattern 3: Multi-Exchange Aggregation (CCXT → Iceberg)

### Use Case
Aggregate data from multiple exchanges using REST APIs for cross-exchange analysis and arbitrage detection.

### Architecture
```
Multiple Exchanges → CCXT → QuixStreams → Iceberg REST Sink → Lakehouse
 (REST/WebSocket)   (API)   (streaming)    (REST catalog)    (storage)
```

### Configuration Pattern
```python
from quixstreams.sources.community.crypto import CCXTSource, create_ccxt_config

# Multi-exchange configuration
exchanges = ["binance", "kraken", "coinbase"]
for exchange in exchanges:
    source_config = create_ccxt_config(
        exchange=exchange,
        symbols=["BTC/USDT", "ETH/USDT"],
        mode="rest",  # or "websocket" if supported
        poll_interval=5,  # seconds for REST mode
        # auth_provider=APIKeyAuth(...) if private data needed
    )
    
    source = CCXTSource(source_config)
    
    # Use exchange-specific topics
    topic = app.topic(f"crypto.{exchange}")
    sdf = app.dataframe(topic)
    sdf.sink(sink)  # Same sink for all exchanges

# Run all sources
app.run(*sources)
```

## Error Handling Across Component Boundaries

### Source Error Propagation
```python
from quixstreams.sources.community.crypto.errors import (
    CryptoSourceConnectionError,
    CryptoSourceRateLimitError,
    CryptoSourceConfigError
)

try:
    app.run(source)
except CryptoSourceConnectionError as e:
    # Network/connection issues - usually retryable
    logger.error(f"Connection failed for {e.source}: {e}")
    # Crypto sources handle retries automatically
    
except CryptoSourceRateLimitError as e:
    # Rate limit hit - check retry_after in context
    retry_after = e.context.get('retry_after', 60)
    logger.warning(f"Rate limited, retry after {retry_after}s")
    
except CryptoSourceConfigError as e:
    # Configuration error - fix config and restart
    logger.error(f"Config error: {e}")
    # Non-retryable - requires manual intervention
```

### Sink Error Handling
```python
from quixstreams.sinks.community.iceberg_rest.errors import (
    IcebergRESTError,
    ConfigurationError,
    NetworkError
)

try:
    sdf.sink(sink)
except ConfigurationError as e:
    # Sink configuration issues
    logger.error(f"Sink config error: {e}")
    
except NetworkError as e:
    # Catalog connectivity issues
    logger.error(f"Catalog connection failed: {e}")
    # Sink handles retries automatically
```

### Schema Validation Errors
```python
# Schema errors are rare due to auto-detection
# but can occur with malformed data

try:
    sdf.sink(sink)
except Exception as e:
    if "schema" in str(e).lower():
        logger.error(f"Schema validation failed: {e}")
        # Log the problematic record for debugging
        # Sink will continue with next batch
```

## Schema Compatibility and Data Flow

### Automatic Schema Detection
The Iceberg REST sink automatically detects schema from the first data batch:

1. **First Batch**: Creates table with detected schema
2. **Subsequent Batches**: Evolves schema by adding new fields
3. **Type Conflicts**: Logs warning and skips incompatible records
4. **Nested Data**: Optionally flattens complex structures

### Schema Evolution Example
```python
# Initial schema (first batch)
{
    "exchange": "binance",
    "symbol": "BTCUSDT", 
    "price": 43250.50,
    "timestamp": 1704067200
}

# Evolved schema (later batch adds fields)
{
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "price": 43250.50,
    "timestamp": 1704067200,
    "volume": 1500.0,        # New field added
    "maker_taker": "taker"   # New field added
}
```

### Data Flow Optimization
```python
# Partitioning for query performance
sink_config = create_local_config(
    table_name="crypto.trades",
    # Partition by date for time-series queries
    partition_spec=[
        ("date", "day"),      # Daily partitions
        ("exchange", "identity")  # Sub-partition by exchange
    ]
)

# Batch sizing for throughput
sink_config.batch_size = 1000      # Records per batch
sink_config.batch_timeout = 5.0    # Seconds timeout
```

## Troubleshooting Common Issues

### 1. Source Connection Failures
**Symptom**: `CryptoSourceConnectionError`
**Solutions**:
- Check exchange API status
- Verify API credentials (if using private data)
- Increase retry configuration
- Check firewall/network connectivity

### 2. Sink Catalog Connectivity
**Symptom**: `NetworkError` from Iceberg sink
**Solutions**:
- Verify catalog URI is accessible
- Check catalog service health
- Verify authentication credentials
- Test with simple curl command

### 3. Schema Conflicts
**Symptom**: Schema validation warnings in logs
**Solutions**:
- Enable data normalization in crypto sources
- Review data transformation pipeline
- Check for malformed records
- Use schema presets for known data types

### 4. Performance Issues
**Symptom**: High latency or low throughput
**Solutions**:
- Tune batch sizes in sink configuration
- Optimize Kafka topic partitioning
- Monitor memory usage and CPU
- Use async/concurrent processing

### 5. Authentication Errors
**Symptom**: 401/403 errors from exchanges or storage
**Solutions**:
- Verify API keys and secrets
- Check credential expiration
- Ensure proper scopes/permissions
- Test credentials with exchange directly

## Production Deployment Patterns

### Environment Configuration
```bash
# Crypto source environment variables
export BINANCE_API_KEY="your_api_key"
export BINANCE_API_SECRET="your_secret"

# Iceberg sink environment variables
export ICEBERG_CATALOG_URI="http://iceberg-catalog:8181"
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_REGION="us-east-1"

# Kafka configuration
export KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
```

### Docker Compose Example
```yaml
version: '3.8'
services:
  crypto-pipeline:
    image: quixstreams/crypto-lakehouse
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - ICEBERG_CATALOG_URI=http://iceberg-catalog:8181
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
    depends_on:
      - kafka
      - iceberg-catalog
      
  kafka:
    image: confluentinc/cp-kafka:latest
    # ... kafka configuration
    
  iceberg-catalog:
    image: tabulario/iceberg-rest:latest
    # ... catalog configuration
```

### Monitoring and Alerting
```python
# Application metrics
app = Application(
    broker_address="kafka:9092",
    app_id="crypto-lakehouse",
    # Enable metrics collection
    metrics_enabled=True,
    metrics_port=9090
)

# Custom metrics for monitoring
def track_metrics(row):
    # Track record count, latency, errors
    metrics.increment('records_processed')
    metrics.histogram('processing_latency', row.get('latency', 0))
    return row

sdf = app.dataframe(topic)
sdf.apply(track_metrics)
sdf.sink(sink)
```

## Summary

These integration patterns provide proven approaches for connecting crypto data sources to Iceberg lakehouse storage:

1. **Real-Time Trading**: Cryptofeed → Iceberg for immediate analysis
2. **Historical Analysis**: Binance S3 → Iceberg for backtesting
3. **Multi-Exchange**: CCXT → Iceberg for cross-exchange analysis

**Key Benefits**:
- **Type Safety**: Dataclass configurations with validation
- **Auto Schema**: Automatic schema detection and evolution
- **Error Resilience**: Comprehensive error handling and retries
- **Production Ready**: Battle-tested components with monitoring
- **Simple Integration**: Factory functions and minimal configuration