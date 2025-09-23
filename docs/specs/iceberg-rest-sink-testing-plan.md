# IcebergRESTSink Comprehensive Testing Plan

## Overview

This document outlines a comprehensive testing strategy for the IcebergRESTSink implementation, focusing on production-ready deployment with catalog schema support for various trading and market data tables.

**Current Status**: 9/15 TDD tests passing (60% completion)
**Target**: 100% test coverage with production validation across multiple trading data schemas

## Testing Strategy

### Phase 1: Core Functionality Validation ✅ (Completed)
- [x] Configuration validation and environment variable loading
- [x] Schema auto-detection and evolution tracking  
- [x] Schema compatibility validation with error handling
- [x] Nested data structure processing (json_serialize, flatten, struct)
- [x] Kafka metadata preservation (topic, partition, offset, headers)
- [x] Commit conflict handling with SinkBackpressureError
- [x] Error context and detailed error reporting

### Phase 2: Performance & Advanced Features ⚠️ (In Progress)
- [ ] Large batch processing (>10,000 records)
- [ ] Throughput benchmarks (>1000 records/sec)
- [ ] Memory management validation (<100MB growth)
- [ ] Connection retry logic with exponential backoff
- [ ] Advanced error context validation
- [ ] Network call mocking for reliable test execution

### Phase 3: Production Schema Testing 📋 (Planned)
- [ ] Trading data schemas (trades, ticker, funding, open_interest)
- [ ] Feature engineering pipeline outputs
- [ ] Multi-table concurrent writes
- [ ] Cross-schema compatibility
- [ ] Real-world data volume testing

## Schema Testing Catalog

### Core Trading Schemas

#### 1. Trades Table Schema
```python
trades_schema_config = {
    "table_name": "crypto.trades",
    "partition_strategy": ["symbol", "date(ts_event)"],
    "schema": {
        "exchange": "string",      # Exchange identifier
        "market": "string",        # Market type (spot, futures, options)
        "symbol": "string",        # Trading pair (BTC-USDT)
        "ts_event": "timestamp",   # Event timestamp
        "price": "double",         # Trade price
        "qty": "double",           # Trade quantity
        "side": "string",          # Buy/Sell
        "trade_id": "long",        # Exchange trade ID
        "ingest_ts": "timestamp",  # Ingestion timestamp
        # Kafka metadata
        "_kafka_topic": "string",
        "_kafka_partition": "int",
        "_kafka_offset": "long",
        "_kafka_headers": "string" # JSON serialized
    }
}
```

#### 2. Ticker/Price Data Schema
```python
ticker_schema_config = {
    "table_name": "crypto.ticker",
    "partition_strategy": ["symbol", "date(ts_event)"],
    "schema": {
        "exchange": "string",
        "symbol": "string",
        "ts_event": "timestamp",
        "bid_price": "double",
        "bid_qty": "double", 
        "ask_price": "double",
        "ask_qty": "double",
        "last_price": "double",
        "last_qty": "double",
        "high_24h": "double",
        "low_24h": "double",
        "volume_24h": "double",
        "volume_quote_24h": "double",
        "price_change_24h": "double",
        "price_change_pct_24h": "double",
        "ingest_ts": "timestamp"
    }
}
```

#### 3. Funding Rates Schema
```python
funding_schema_config = {
    "table_name": "crypto.funding",
    "partition_strategy": ["symbol", "date(funding_time)"],
    "schema": {
        "exchange": "string",
        "symbol": "string",
        "funding_rate": "double",    # Current funding rate
        "funding_time": "timestamp", # Funding timestamp
        "next_funding_time": "timestamp",
        "mark_price": "double",      # Mark price
        "index_price": "double",     # Index price
        "estimated_settle_price": "double",
        "ingest_ts": "timestamp"
    }
}
```

#### 4. Open Interest Schema
```python
open_interest_schema_config = {
    "table_name": "crypto.open_interest",
    "partition_strategy": ["symbol", "date(ts_event)"],
    "schema": {
        "exchange": "string",
        "symbol": "string", 
        "ts_event": "timestamp",
        "open_interest": "double",      # Current open interest
        "open_interest_value": "double", # Value in quote currency
        "mark_price": "double",
        "ingest_ts": "timestamp"
    }
}
```

### Feature Engineering Schemas

#### 5. Technical Indicators
```python
indicators_schema_config = {
    "table_name": "features.technical_indicators",
    "partition_strategy": ["symbol", "interval", "date(ts_event)"],
    "schema": {
        "symbol": "string",
        "interval": "string",        # 1m, 5m, 1h, 1d
        "ts_event": "timestamp",
        # Price-based features
        "sma_20": "double",         # Simple Moving Average
        "ema_20": "double",         # Exponential Moving Average
        "rsi_14": "double",         # RSI indicator
        "macd": "double",           # MACD value
        "macd_signal": "double",    # MACD signal
        "macd_hist": "double",      # MACD histogram
        "bb_upper": "double",       # Bollinger Band upper
        "bb_lower": "double",       # Bollinger Band lower
        "bb_middle": "double",      # Bollinger Band middle
        # Volume-based features
        "volume_sma_20": "double",
        "volume_ratio": "double",
        # Volatility features
        "atr_14": "double",         # Average True Range
        "volatility_1h": "double",
        "volatility_24h": "double",
        "ingest_ts": "timestamp"
    }
}
```

#### 6. Market Microstructure Features
```python
microstructure_schema_config = {
    "table_name": "features.market_microstructure", 
    "partition_strategy": ["symbol", "date(ts_event)"],
    "schema": {
        "symbol": "string",
        "ts_event": "timestamp",
        # Order book features
        "bid_ask_spread": "double",
        "bid_ask_spread_pct": "double",
        "order_book_imbalance": "double",
        "weighted_mid_price": "double",
        # Trade flow features
        "trade_intensity": "double",     # Trades per minute
        "buy_sell_ratio": "double",      # Buy vs sell volume ratio  
        "large_trade_ratio": "double",   # Large trades percentage
        "price_impact": "double",        # Price impact measure
        # Liquidity features
        "effective_spread": "double",
        "quoted_spread": "double", 
        "depth_imbalance": "double",
        "liquidity_ratio": "double",
        "ingest_ts": "timestamp"
    }
}
```

#### 7. Risk Metrics
```python
risk_metrics_schema_config = {
    "table_name": "features.risk_metrics",
    "partition_strategy": ["symbol", "date(ts_event)"],
    "schema": {
        "symbol": "string",
        "ts_event": "timestamp",
        "interval": "string",        # 1h, 4h, 1d
        # Volatility measures
        "realized_volatility": "double",
        "garch_volatility": "double", 
        "parkinson_volatility": "double",
        # Risk measures
        "var_95": "double",          # Value at Risk 95%
        "var_99": "double",          # Value at Risk 99%
        "cvar_95": "double",         # Conditional VaR 95%
        "max_drawdown": "double",    # Maximum drawdown
        "sharpe_ratio": "double",    # Sharpe ratio
        "sortino_ratio": "double",   # Sortino ratio
        # Correlation measures
        "beta_btc": "double",        # Beta vs BTC
        "correlation_btc": "double", # Correlation with BTC
        "ingest_ts": "timestamp"
    }
}
```

## Test Implementation Plan

### 1. Schema Validation Tests
```python
# tests/e2e/iceberg_sink/test_schemas.py

class TestTradingSchemas:
    """Test all trading data schemas."""
    
    def test_trades_schema_validation(self):
        """Validate trades table schema auto-detection and evolution."""
        config = create_local_config(table_name="crypto.trades")
        sink = IcebergRESTSink(config=config)
        
        # Test basic trades data
        trades_batch = create_trades_batch()
        sink.write(trades_batch)
        
        # Verify schema detection
        assert "price" in sink._inferred_schema_fields
        assert "qty" in sink._inferred_schema_fields
        
    def test_ticker_schema_validation(self):
        """Validate ticker data schema."""
        # Similar test for ticker data
        pass
        
    def test_funding_schema_validation(self):
        """Validate funding rates schema.""" 
        # Similar test for funding data
        pass
        
    def test_open_interest_schema_validation(self):
        """Validate open interest schema."""
        # Similar test for open interest data
        pass
```

### 2. Performance Tests with Real Schemas
```python
# tests/e2e/iceberg_sink/test_performance.py

class TestSchemaPerformance:
    """Test performance with realistic trading data volumes."""
    
    def test_high_frequency_trades_performance(self):
        """Test with high-frequency trading data (10k+ trades/sec)."""
        config = create_local_config(table_name="crypto.trades")
        sink = IcebergRESTSink(
            config=config,
            batch_size=1000,
            adaptive_batching=True,
            max_buffer_memory_mb=100.0
        )
        
        # Generate high-frequency trading data
        hf_data = generate_hf_trades_data(records=50000)
        
        start_time = time.time()
        for batch in chunk_data(hf_data, 1000):
            sink.write(batch)
        duration = time.time() - start_time
        
        throughput = len(hf_data) / duration
        assert throughput > 1000  # >1000 records/sec
        
    def test_mixed_schema_performance(self):
        """Test performance with mixed data types (trades + ticker + funding)."""
        # Test concurrent writes to different tables
        pass
```

### 3. Integration Tests with Lakekeeper
```python
# tests/integration/iceberg_sink/test_lakekeeper_integration.py

class TestLakekeeperIntegration:
    """Integration tests with real Lakekeeper catalog."""
    
    @pytest.fixture(scope="class")
    def lakekeeper_stack(self):
        """Start Lakekeeper + MinIO stack for testing."""
        # Docker compose up lakekeeper stack
        yield
        # Docker compose down
        
    def test_end_to_end_trading_pipeline(self, lakekeeper_stack):
        """End-to-end test with real catalog and storage."""
        # Test complete pipeline: source -> sink -> query
        pass
        
    def test_schema_evolution_across_restarts(self, lakekeeper_stack):
        """Test schema evolution persists across sink restarts."""
        # Test that evolved schemas are maintained
        pass
```

### 4. Multi-Table Testing
```python
# tests/e2e/iceberg_sink/test_multi_table.py

class TestMultiTableSinks:
    """Test multiple sinks writing to different tables concurrently."""
    
    def test_concurrent_table_writes(self):
        """Test concurrent writes to different tables."""
        # Create sinks for different tables
        trades_sink = IcebergRESTSink(
            config=create_local_config(table_name="crypto.trades")
        )
        ticker_sink = IcebergRESTSink(
            config=create_local_config(table_name="crypto.ticker")
        )
        funding_sink = IcebergRESTSink(
            config=create_local_config(table_name="crypto.funding")
        )
        
        # Test concurrent writes
        import threading
        
        def write_trades():
            trades_sink.write(generate_trades_data())
            
        def write_ticker():
            ticker_sink.write(generate_ticker_data())
            
        def write_funding():
            funding_sink.write(generate_funding_data())
            
        threads = [
            threading.Thread(target=write_trades),
            threading.Thread(target=write_ticker), 
            threading.Thread(target=write_funding)
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
            
        # Validate all writes succeeded
        assert trades_sink.get_stats()["records_written"] > 0
        assert ticker_sink.get_stats()["records_written"] > 0
        assert funding_sink.get_stats()["records_written"] > 0
```

## Test Data Generation

### 1. Realistic Trading Data Generator
```python
# tests/utils/data_generators.py

def generate_trades_data(
    symbols=["BTC-USDT", "ETH-USDT", "SOL-USDT"],
    exchanges=["binance", "okx", "bybit"],
    count=1000,
    time_range_hours=1
) -> List[Dict]:
    """Generate realistic trading data."""
    trades = []
    start_time = int(time.time() * 1000)
    
    for i in range(count):
        symbol = random.choice(symbols)
        exchange = random.choice(exchanges)
        
        # Generate realistic price movement
        base_price = get_base_price(symbol)
        price_volatility = get_volatility(symbol)
        price = base_price * (1 + random.gauss(0, price_volatility))
        
        trade = {
            "exchange": exchange,
            "market": "spot",
            "symbol": symbol,
            "ts_event": start_time + (i * (time_range_hours * 3600 * 1000 // count)),
            "price": round(price, 8),
            "qty": round(random.uniform(0.001, 10.0), 8),
            "side": random.choice(["buy", "sell"]),
            "trade_id": f"{exchange}_{i}",
            "ingest_ts": int(time.time() * 1000)
        }
        trades.append(trade)
        
    return trades

def generate_feature_data(
    symbols=["BTC-USDT", "ETH-USDT"],
    intervals=["1m", "5m", "1h"],
    count=100
) -> List[Dict]:
    """Generate realistic feature engineering data."""
    # Implementation for technical indicators, risk metrics, etc.
    pass
```

### 2. Schema Evolution Test Data
```python
def generate_evolved_schema_data(base_schema: Dict, evolution_type: str) -> Dict:
    """Generate data that evolves the schema."""
    if evolution_type == "add_fields":
        # Add new optional fields
        base_schema["new_field"] = "string_value"
        base_schema["optional_numeric"] = 42.0
    elif evolution_type == "incompatible_change":
        # Change field type (should raise SchemaIncompatibilityError)
        base_schema["price"] = "not_a_number"  # was double, now string
    
    return base_schema
```

## Environment Setup

### 1. Local Testing Stack
```yaml
# docker-compose.test.yml
version: '3.8'
services:
  lakekeeper:
    image: quay.io/lakekeeper/catalog:latest-main
    ports: ["8181:8181"]
    environment:
      - LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@db:5432/postgres
      - LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@db:5432/postgres
    depends_on: [db]
    
  db:
    image: postgres:15
    environment:
      POSTGRES_DB: postgres
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data
      
  minio:
    image: minio/minio:latest
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
      
  redpanda:
    image: vectorized/redpanda:latest
    ports: ["9092:9092", "29092:29092"]
    command: |
      redpanda start
      --smp 1
      --memory 1G
      --overprovisioned
      --node-id 0
      --kafka-addr PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092
      --advertise-kafka-addr PLAINTEXT://redpanda:29092,OUTSIDE://localhost:9092
      
volumes:
  postgres_data:
  minio_data:
```

### 2. Test Configuration
```python
# tests/conftest.py

@pytest.fixture(scope="session")
def test_stack():
    """Start the complete testing stack."""
    import subprocess
    
    # Start docker compose stack
    subprocess.run(["docker-compose", "-f", "docker-compose.test.yml", "up", "-d"])
    
    # Wait for services to be ready
    wait_for_service("http://localhost:8181/v1/config")  # Lakekeeper
    wait_for_service("http://localhost:9000/minio/health/live")  # MinIO
    wait_for_service("localhost:9092")  # Redpanda
    
    yield
    
    # Cleanup
    subprocess.run(["docker-compose", "-f", "docker-compose.test.yml", "down", "-v"])

@pytest.fixture
def sink_config():
    """Standard sink configuration for tests."""
    return create_local_config(
        catalog_uri="http://localhost:8181/api/v1",
        warehouse_id="test",
        # Override storage to use test MinIO
        storage_config={
            "endpoint_url": "http://localhost:9000",
            "access_key_id": "minioadmin",
            "secret_access_key": "minioadmin",
            "bucket": "test-lakehouse"
        }
    )
```

## Execution Plan

### Phase 1: Complete Core Tests (Week 1)
- [ ] Fix remaining 6 failing TDD tests
- [ ] Add network mocking for reliable test execution
- [ ] Performance optimization for large batches
- [ ] Memory management validation

### Phase 2: Schema Testing (Week 2)
- [ ] Implement all trading schema tests (trades, ticker, funding, open_interest)
- [ ] Feature engineering schema validation
- [ ] Schema evolution testing across different data types
- [ ] Multi-table concurrent write validation

### Phase 3: Integration Testing (Week 3)
- [ ] End-to-end pipeline testing with real Kafka sources
- [ ] Performance benchmarking with realistic data volumes
- [ ] Failure recovery and error handling validation
- [ ] Production deployment validation

### Phase 4: Production Readiness (Week 4)
- [ ] Load testing with high-frequency data
- [ ] Monitoring and observability validation
- [ ] Documentation completion
- [ ] Production deployment guidelines

## Success Criteria

### Functional Requirements
- [ ] 15/15 TDD tests passing (100% completion)
- [ ] All trading schemas (trades, ticker, funding, open_interest) supported
- [ ] Feature engineering pipeline integration validated
- [ ] Schema evolution works across restarts and data type changes
- [ ] Multi-table concurrent writes perform without conflicts

### Performance Requirements  
- [ ] >10,000 records/second throughput for high-frequency trading data
- [ ] <100MB memory growth under sustained load
- [ ] <5 second latency for batch processing
- [ ] >99.9% success rate under normal operation

### Production Requirements
- [ ] Complete error handling and recovery
- [ ] Monitoring and alerting integration
- [ ] Security validation (authentication, encryption)
- [ ] Documentation and deployment guides
- [ ] Automated testing pipeline

This comprehensive testing plan ensures the IcebergRESTSink is production-ready for real-world trading data ingestion and feature engineering pipelines.