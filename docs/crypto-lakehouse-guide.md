# Crypto Lakehouse Guide

Complete guide for building crypto data lakehouses using QuixStreams with crypto sources and Iceberg REST sink integration.

## Quick Start

### 1. Local Development Setup

```bash
# Create development environment
cd examples/crypto-lakehouse
python dev_setup.py --setup-local

# Start services
cd crypto-lakehouse-dev
docker-compose up -d

# Test integration
python test_pipeline.py
```

### 2. Simple Real-Time Pipeline

```python
from quixstreams import Application
from quixstreams.sources.community.crypto import CryptofeedSource, create_cryptofeed_config
from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_config

# Application
app = Application(broker_address="localhost:9092", app_id="crypto-realtime")

# Source: Real-time Binance trades
source_config = create_cryptofeed_config(
    exchanges=["binance"],
    channels=["trades"],
    symbols=["BTCUSDT", "ETHUSDT"]
)

# Sink: Local Iceberg lakehouse
sink_config = create_local_config(table_name="crypto.realtime_trades")

# Pipeline
source = CryptofeedSource(source_config)
sink = IcebergRESTSink(sink_config)

topic = app.topic("crypto.trades")
sdf = app.dataframe(topic)
sdf.sink(sink)

app.run(source)
```

### 3. Using Templates

```bash
# Use real-time trading template
python -m quixstreams.templates.loader templates/crypto-lakehouse/real-time-trading.yaml

# Use historical analysis template  
python -m quixstreams.templates.loader templates/crypto-lakehouse/historical-analysis.yaml
```

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Crypto        │    │   QuixStreams   │    │   Iceberg       │
│   Sources       │───▶│   Streaming     │───▶│   Lakehouse     │
│                 │    │   Processing    │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
│                                                               │
├─ Cryptofeed                                     ├─ REST Catalog
├─ CCXT                                           ├─ S3 Storage
└─ Binance S3                                     └─ Schema Evolution
```

**Components:**
- **Crypto Sources**: Real-time websockets, REST APIs, historical S3 data
- **QuixStreams**: Stream processing with Kafka backend
- **Iceberg REST Sink**: Schema-agnostic lakehouse storage

## Integration Patterns

### Pattern 1: Real-Time Trading Data

**Use Case**: Live crypto trading data for immediate analysis
**Source**: Cryptofeed (WebSocket)
**Data**: Trades, tickers, orderbook
**Latency**: < 100ms

```yaml
# templates/crypto-lakehouse/real-time-trading.yaml
crypto_source:
  type: "cryptofeed"
  exchanges: ["binance", "kraken"]
  channels: ["trades", "ticker"]
  symbols: ["BTCUSDT", "ETHUSDT"]

iceberg_sink:
  catalog_uri: "http://localhost:8181"
  table_name: "crypto.realtime_trades"
```

### Pattern 2: Historical Analysis

**Use Case**: Backtesting and research with historical data
**Source**: Binance S3 (Public archives)
**Data**: Historical trades, klines, tickers
**Latency**: Configurable replay speed

```yaml
# templates/crypto-lakehouse/historical-analysis.yaml
crypto_source:
  type: "binance_s3"
  bucket: "binance-public-data"
  prefix_template: "data/spot/daily/trades/{symbol}/"
  date_from: "2024-01-01"
  date_to: "2024-01-31"
  replay_speed: 1.0  # Real-time replay
```

### Pattern 3: Multi-Exchange Aggregation

**Use Case**: Cross-exchange analysis and arbitrage
**Source**: CCXT (Multiple exchanges)
**Data**: Unified format across exchanges
**Latency**: REST polling or WebSocket

```python
# Multi-exchange configuration
exchanges = ["binance", "kraken", "coinbase"]
for exchange in exchanges:
    source_config = create_ccxt_config(
        exchange=exchange,
        symbols=["BTC/USDT", "ETH/USDT"],
        mode="websocket"  # or "rest"
    )
```

## Configuration Management

### Environment Variables

```bash
# Crypto Source
export CRYPTO_EXCHANGES="binance,kraken"
export CRYPTO_CHANNELS="trades,ticker"
export CRYPTO_SYMBOLS="BTCUSDT,ETHUSDT"

# Iceberg Sink
export ICEBERG_CATALOG_URI="http://localhost:8181"
export ICEBERG_TABLE_NAME="crypto.my_data"

# Storage (S3/MinIO/R2)
export STORAGE_ENDPOINT="http://localhost:9000"
export STORAGE_BUCKET="lakehouse"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"

# Kafka
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
```

### Configuration Helpers

```python
from examples.crypto_lakehouse.config_helpers import (
    load_crypto_source_config,
    load_iceberg_sink_config,
    create_local_dev_config
)

# Load from environment
source_config = load_crypto_source_config("cryptofeed")
sink_config = load_iceberg_sink_config()

# Create development config
dev_config = create_local_dev_config("cryptofeed")
```

## Data Schemas

### Normalized Crypto Data Schema

All crypto sources normalize to this unified schema:

```python
{
    "exchange": "binance",        # Exchange name
    "symbol": "BTCUSDT",         # Trading pair
    "price": 43250.50,           # Trade price
    "quantity": 0.001,           # Trade quantity
    "side": "buy",               # Buy/sell side
    "timestamp": 1704067200,     # Unix timestamp
    "trade_id": "12345",         # Exchange trade ID
    "channel": "trades"          # Data type
}
```

### Schema Evolution

Iceberg sink automatically handles schema changes:

1. **Initial Schema**: Created from first data batch
2. **Field Addition**: New fields added automatically
3. **Type Evolution**: Compatible type changes supported
4. **Conflict Resolution**: Incompatible changes logged and skipped

Example evolution:
```python
# Initial: Basic trade data
{"exchange": "binance", "price": 43250.50, "quantity": 0.001}

# Evolved: Added volume and maker/taker
{"exchange": "binance", "price": 43250.50, "quantity": 0.001, 
 "volume": 1500.0, "maker_taker": "taker"}
```

## Authentication

### Crypto Source Authentication

```python
from quixstreams.sources.community.crypto import APIKeyAuth, NoAuth

# Public data (most common)
auth = NoAuth()

# Private API access
auth = APIKeyAuth(
    api_key=os.getenv("BINANCE_API_KEY"),
    api_secret=os.getenv("BINANCE_API_SECRET")
)
```

### Storage Authentication

```python
# AWS S3
sink_config = create_s3_config(
    aws_access_key=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_region="us-east-1"
)

# Cloudflare R2
sink_config = create_r2_config(
    r2_access_key=os.getenv("R2_ACCESS_KEY"),
    r2_secret_key=os.getenv("R2_SECRET_KEY"),
    r2_account_id=os.getenv("R2_ACCOUNT_ID")
)
```

## Error Handling

### Automatic Error Recovery

Both components include comprehensive error handling:

**Crypto Sources**:
- Connection failures → Automatic retry with exponential backoff
- Rate limiting → Respect retry-after headers
- Invalid symbols → Validation errors with suggestions
- Missing dependencies → Clear installation guidance

**Iceberg Sink**:
- Catalog connectivity → Retry with backoff
- Storage errors → Escalation to monitoring
- Schema conflicts → Log and continue with next batch
- Configuration errors → Fail fast with clear messages

### Error Monitoring

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Monitor specific errors
try:
    app.run(source)
except CryptoSourceConnectionError as e:
    # Handle connection issues
    logger.error(f"Connection failed: {e.source} - {e}")
except CryptoSourceRateLimitError as e:
    # Handle rate limiting
    retry_after = e.context.get('retry_after', 60)
    logger.warning(f"Rate limited, retry after {retry_after}s")
```

## Performance Optimization

### Throughput Tuning

```python
# Source configuration
source_config = create_cryptofeed_config(
    exchanges=["binance"],
    channels=["trades"],
    symbols=["BTCUSDT", "ETHUSDT"],
    # Limit symbols for higher throughput
)

# Sink configuration
sink_config = create_local_config(
    table_name="crypto.trades",
    batch_size=1000,      # Records per batch
    batch_timeout=5.0,    # Seconds timeout
)
```

### Partitioning Strategy

```python
# Time-based partitioning for queries
sink_config = create_local_config(
    table_name="crypto.trades",
    partition_spec=[
        ("date", "day"),           # Daily partitions
        ("exchange", "identity")   # Sub-partition by exchange
    ]
)
```

### Memory Management

```python
# Kafka topic configuration
topic_config = {
    "partitions": 6,              # Higher partitions for throughput
    "replication_factor": 3,      # Production replication
    "retention_ms": 86400000      # 24 hour retention
}
```

## Testing and Validation

### Integration Testing

```python
# Run integration test
cd examples/crypto-lakehouse
python test_integration_example.py --source cryptofeed --duration 30

# Test specific scenarios
python test_integration_example.py --source binance_s3
python test_integration_example.py --source ccxt
```

### Validation Patterns

```python
# Schema validation
from examples.crypto_lakehouse.config_helpers import validate_pipeline_config

config = create_local_dev_config("cryptofeed")
errors = validate_pipeline_config(config)
if errors:
    print("Configuration errors:", errors)
```

### Template Validation

```bash
# Validate all templates
python templates/validate_templates.py --all

# Validate specific template
python templates/validate_templates.py real-time-trading.yaml
```

## Production Deployment

### Docker Compose

```yaml
# crypto-lakehouse-dev/docker-compose.yml
version: '3.8'
services:
  crypto-pipeline:
    image: quixstreams/crypto-lakehouse
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
      - ICEBERG_CATALOG_URI=http://iceberg-rest:8181
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
    depends_on: [kafka, iceberg-rest, minio]
    
  # ... other services
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: crypto-lakehouse
spec:
  replicas: 3
  selector:
    matchLabels:
      app: crypto-lakehouse
  template:
    spec:
      containers:
      - name: crypto-pipeline
        image: quixstreams/crypto-lakehouse
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "kafka-service:9092"
        - name: ICEBERG_CATALOG_URI
          value: "http://iceberg-service:8181"
```

### Monitoring Setup

```python
# Prometheus metrics
app = Application(
    broker_address="kafka:9092",
    app_id="crypto-lakehouse",
    metrics_enabled=True,
    metrics_port=9090
)

# Custom metrics
def track_metrics(row):
    metrics.increment('crypto_records_processed')
    metrics.histogram('processing_latency', row.get('latency', 0))
    return row

sdf = app.dataframe(topic)
sdf.apply(track_metrics)
sdf.sink(sink)
```

## Troubleshooting

### Common Issues

**1. Connection Failures**
```bash
# Check service health
curl http://localhost:8181/v1/config  # Iceberg catalog
curl http://localhost:9000/minio/health/live  # MinIO

# Check Kafka
kafka-topics.sh --list --bootstrap-server localhost:9092
```

**2. Schema Conflicts**
```python
# Enable schema logging
logging.getLogger('iceberg_sink.schema').setLevel(logging.DEBUG)

# Check for malformed data
sdf.apply(lambda row: print(f"Record: {row}"))
```

**3. Performance Issues**
```python
# Monitor throughput
sdf.apply(lambda row: metrics.increment('records_per_second'))

# Check memory usage
import psutil
print(f"Memory usage: {psutil.virtual_memory().percent}%")
```

### Debug Mode

```python
# Enable detailed logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Dry run mode
source_config = create_binance_s3_config(
    dry_run=True,  # Only list files, don't process
    symbols=["BTCUSDT"]
)
```

### Performance Monitoring

```bash
# Check pipeline metrics
curl http://localhost:9090/metrics

# Monitor Kafka lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group crypto-lakehouse
```

## Examples and Use Cases

### 1. Real-Time Trading Dashboard

```python
# Real-time crypto dashboard pipeline
app = Application(broker_address="kafka:9092", app_id="crypto-dashboard")

# Multiple exchange sources
for exchange in ["binance", "kraken", "coinbase"]:
    source_config = create_cryptofeed_config(
        exchanges=[exchange],
        channels=["trades", "ticker"],
        symbols=["BTCUSDT", "ETHUSDT"]
    )
    
    source = CryptofeedSource(source_config)
    topic = app.topic(f"crypto.{exchange}")
    sdf = app.dataframe(topic)
    
    # Add exchange metadata
    sdf = sdf.apply(lambda row: {**row, "source_exchange": exchange})
    
    # Write to unified table
    sink_config = create_local_config(table_name="crypto.unified_trades")
    sdf.sink(IcebergRESTSink(sink_config))
```

### 2. Arbitrage Detection

```python
# Cross-exchange arbitrage detection
app = Application(broker_address="kafka:9092", app_id="crypto-arbitrage")

# Price aggregation from multiple exchanges
def detect_arbitrage(row):
    # Simple arbitrage detection logic
    if 'price_diff_pct' in row and row['price_diff_pct'] > 1.0:
        return {**row, "arbitrage_opportunity": True}
    return row

sdf = app.dataframe("crypto.aggregated_prices")
sdf = sdf.apply(detect_arbitrage)
sdf = sdf.filter(lambda row: row.get("arbitrage_opportunity", False))

# Store arbitrage opportunities
sink_config = create_local_config(table_name="crypto.arbitrage_opportunities")
sdf.sink(IcebergRESTSink(sink_config))
```

### 3. Historical Backtesting

```python
# Backtest trading strategy with historical data
source_config = create_binance_s3_config(
    bucket="binance-public-data",
    prefix_template="data/spot/daily/trades/{symbol}/",
    symbols=["BTCUSDT"],
    date_from="2024-01-01",
    date_to="2024-03-31",
    replay_speed=100.0  # 100x speed for faster backtesting
)

def backtest_strategy(row):
    # Implement trading strategy logic
    signal = calculate_trading_signal(row)
    return {**row, "trading_signal": signal}

sdf = app.dataframe("crypto.historical")
sdf = sdf.apply(backtest_strategy)

# Store backtest results
sink_config = create_local_config(table_name="crypto.backtest_results")
sdf.sink(IcebergRESTSink(sink_config))
```

## Next Steps

1. **Start Development**: Use `dev_setup.py --setup-local` for local environment
2. **Choose Pattern**: Select integration pattern for your use case
3. **Configure Pipeline**: Use templates or configuration helpers
4. **Test Integration**: Run integration tests before production
5. **Deploy**: Use Docker Compose or Kubernetes for production
6. **Monitor**: Set up metrics and alerting for operations

## Resources

- **Integration Patterns**: [docs/integration/crypto-lakehouse-patterns.md](integration/crypto-lakehouse-patterns.md)
- **Templates**: [templates/crypto-lakehouse/](../templates/crypto-lakehouse/)
- **Examples**: [examples/crypto-lakehouse/](../examples/crypto-lakehouse/)
- **Configuration Helpers**: [examples/crypto-lakehouse/config_helpers.py](../examples/crypto-lakehouse/config_helpers.py)
- **Development Setup**: [examples/crypto-lakehouse/dev_setup.py](../examples/crypto-lakehouse/dev_setup.py)
- **Integration Tests**: [examples/crypto-lakehouse/test_integration_example.py](../examples/crypto-lakehouse/test_integration_example.py)

## Support

For issues and questions:

1. Check troubleshooting section above
2. Review integration patterns documentation
3. Run integration tests for debugging
4. Check component logs for detailed error messages