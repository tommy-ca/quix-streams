# Architecture Decision Records: Crypto Lakehouse Integration

## ADR-001: Crypto Data Normalization Strategy

### Status: Accepted
### Date: 2024-12-18

#### Context
Multiple cryptocurrency exchanges provide data in different formats and schemas. We need a consistent approach to normalize this data for lakehouse storage while preserving exchange-specific metadata.

#### Decision
Implement a layered normalization strategy:

1. **Raw Layer**: Store original exchange format with metadata
2. **Bronze Layer**: Normalized minimal schema across exchanges  
3. **Silver Layer**: Enriched data with derived fields
4. **Gold Layer**: Analytics-ready aggregated data

```python
# Normalized Trade Schema (Bronze)
{
    "exchange": "binance",      # Source exchange
    "market": "spot",           # spot|futures|options  
    "symbol": "BTCUSDT",        # Normalized symbol
    "ts_event": 1640995200000,  # Event timestamp (ms)
    "price": 47850.12,          # Trade price
    "qty": 0.00104,             # Trade quantity
    "trade_id": "123456789",    # Exchange trade ID (optional)
    "side": "buy",              # buy|sell (optional)
    "ingest_ts": 1640995201000, # Ingestion timestamp
    "meta": {                   # Exchange-specific metadata
        "s3_key": "...",
        "checksum": "..."
    }
}

# Normalized Klines Schema (Bronze)
{
    "exchange": "binance",
    "market": "spot", 
    "symbol": "BTCUSDT",
    "interval": "1m",
    "open_time": 1640995200000,
    "open": 47850.12,
    "high": 47890.45,
    "low": 47820.88,
    "close": 47865.33,
    "volume": 12.45678,
    "close_time": 1640995259999,
    "ingest_ts": 1640995261000,
    "meta": {...}
}
```

#### Rationale
- **Consistency**: Unified schema across all exchanges
- **Traceability**: Preserve original data lineage via metadata
- **Flexibility**: Support exchange-specific extensions via meta field
- **Performance**: Optimized for time-series queries and analytics

#### Consequences
- **Positive**: Simplified downstream analytics, consistent data model
- **Negative**: Additional transformation overhead, schema evolution complexity

---

## ADR-002: Iceberg Table Partitioning Strategy  

### Status: Accepted
### Date: 2024-12-18

#### Context
Crypto data has high volume and velocity characteristics requiring efficient partitioning for query performance and data management.

#### Decision
Implement hierarchical partitioning strategy:

```python
# Partition Strategy for Trades
partition_spec = PartitionSpec([
    PartitionField(
        source_id=symbol_field_id,
        transform=IdentityTransform(),
        name="symbol"
    ),
    PartitionField(
        source_id=ts_event_field_id, 
        transform=DayTransform(),
        name="event_day"
    )
])

# Partition Strategy for Klines
partition_spec = PartitionSpec([
    PartitionField(
        source_id=symbol_field_id,
        transform=IdentityTransform(), 
        name="symbol"
    ),
    PartitionField(
        source_id=open_time_field_id,
        transform=DayTransform(),
        name="open_day"  
    ),
    PartitionField(
        source_id=interval_field_id,
        transform=IdentityTransform(),
        name="interval"
    )
])
```

#### Rationale
- **Symbol-first partitioning**: Most queries filter by trading pair
- **Day-level time partitioning**: Balance between granularity and file count
- **Interval partitioning (klines)**: Support for different timeframe queries
- **Pruning efficiency**: Enable partition pruning for common query patterns

#### Consequences
- **Positive**: Excellent query performance, manageable file counts
- **Negative**: Potential small file problem for low-volume symbols

---

## ADR-003: Streaming to Batch Transition Pattern

### Status: Accepted  
### Date: 2024-12-18

#### Context
Need to transition from high-frequency streaming crypto data to efficient batch storage in Iceberg tables.

#### Decision
Implement micro-batching with configurable windows:

```python
# Batching Configuration
class IcebergBatchConfig:
    max_records: int = 10000      # Records per batch
    max_latency_seconds: int = 60  # Maximum batch hold time
    max_batch_size_mb: int = 100   # Maximum batch size
    compression: str = "snappy"    # Parquet compression
    
# Batching Strategy
1. Stream Processing: Real-time processing in Quix Streams
2. Micro-batching: Accumulate records with size/time triggers  
3. Parquet Generation: Convert to columnar format
4. Iceberg Commit: Atomic append to table
5. Compaction: Background small file consolidation
```

#### Data Flow Pattern
```
Crypto Source → Kafka Topic → SDF Processing → Sink Batching → Iceberg Table
     ↓              ↓             ↓              ↓              ↓
  Real-time      Stream         Transform      Micro-batch    Lakehouse
  (ms latency)   (seconds)      (seconds)      (minutes)      (batch)
```

#### Rationale
- **Real-time Processing**: Enable immediate stream processing 
- **Efficient Storage**: Batch writes optimize Iceberg performance
- **Latency Control**: Configurable trade-off between latency and efficiency
- **ACID Properties**: Leverage Iceberg's transactional guarantees

#### Consequences
- **Positive**: Optimal query performance, cost-effective storage
- **Negative**: Increased complexity, eventual consistency for batch data

---

## ADR-004: Multi-Catalog Support Strategy

### Status: In Progress
### Date: 2024-12-18

#### Context  
Support multiple Iceberg catalog types for different deployment scenarios and cloud providers.

#### Decision
Implement pluggable catalog architecture:

```python
# Catalog Abstraction
@dataclass
class IcebergCatalogConfig:
    catalog_type: Literal["aws_glue", "rest", "sql", "nessie"]
    
    # AWS Glue
    aws_region: Optional[str] = None
    aws_s3_uri: Optional[str] = None
    
    # REST Catalog (Lakekeeper, Tabular, etc.)
    rest_uri: Optional[str] = None
    warehouse_id: Optional[str] = None
    auth_token: Optional[str] = None
    
    # SQL Catalog
    sql_uri: Optional[str] = None
    warehouse_path: Optional[str] = None
    
    # Nessie
    nessie_uri: Optional[str] = None
    nessie_branch: Optional[str] = "main"

# Factory Pattern
def create_catalog(config: IcebergCatalogConfig):
    if config.catalog_type == "aws_glue":
        return create_glue_catalog(config)
    elif config.catalog_type == "rest": 
        return create_rest_catalog(config)
    # ... etc
```

#### Catalog Deployment Patterns

| Catalog Type | Use Case | Pros | Cons |
|--------------|----------|------|------|
| **AWS Glue** | AWS native | Integrated, managed | Vendor lock-in |
| **REST** | Cloud agnostic | Flexible, portable | Operational overhead |
| **SQL** | Development | Simple, local | Not production ready |
| **Nessie** | Git-like versioning | Advanced features | Complexity |

#### Rationale
- **Flexibility**: Support multiple deployment scenarios
- **Portability**: Avoid vendor lock-in
- **Evolution**: Enable catalog migration paths

#### Consequences
- **Positive**: Maximum deployment flexibility
- **Negative**: Increased testing complexity, configuration overhead

---

## ADR-005: Error Handling and Retry Strategy

### Status: Accepted
### Date: 2024-12-18

#### Context
Crypto data ingestion involves multiple external systems (exchanges, S3, catalogs) requiring robust error handling.

#### Decision
Implement layered error handling with circuit breakers:

```python
# Error Classification
class ErrorType(Enum):
    RETRIABLE_TRANSIENT = "retriable_transient"    # 5xx, timeouts, rate limits
    RETRIABLE_BACKOFF = "retriable_backoff"        # 429, temporary failures  
    NON_RETRIABLE = "non_retriable"                # 4xx, auth, malformed data
    FATAL = "fatal"                                # Config errors, system failures

# Retry Configuration
@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_factor: float = 2.0
    jitter: bool = True
    
    # Circuit breaker
    failure_threshold: int = 5
    recovery_timeout: int = 60
```

#### Error Handling Patterns

```python
# Source Error Handling
def fetch_with_retry(operation, config: RetryConfig):
    circuit_breaker = CircuitBreaker(config)
    
    for attempt in range(config.max_attempts):
        try:
            if circuit_breaker.is_open():
                raise CircuitBreakerOpenError()
                
            result = operation()
            circuit_breaker.record_success()
            return result
            
        except Exception as e:
            error_type = classify_error(e)
            
            if error_type == ErrorType.NON_RETRIABLE:
                circuit_breaker.record_failure()
                raise
                
            if error_type == ErrorType.FATAL:
                circuit_breaker.open()
                raise
                
            delay = calculate_backoff(attempt, config)
            time.sleep(delay)
            circuit_breaker.record_failure()
    
    raise MaxRetriesExceededError()

# Sink Error Handling  
def write_with_conflict_resolution(batch, table):
    try:
        table.append(batch)
    except CommitFailedException:
        # Iceberg conflict - trigger backpressure
        raise SinkBackpressureError(retry_after=random.uniform(0, 5))
    except Exception as e:
        # Other errors - classify and handle
        handle_sink_error(e)
```

#### Rationale
- **Resilience**: Handle transient failures gracefully
- **Resource Protection**: Circuit breakers prevent cascading failures  
- **Observability**: Clear error classification and metrics
- **Performance**: Avoid thundering herd problems

#### Consequences
- **Positive**: Production-ready reliability
- **Negative**: Complex error handling logic

---

## ADR-006: Schema Evolution Strategy

### Status: Accepted
### Date: 2024-12-18

#### Context
Crypto data schemas may evolve as exchanges add new fields or change formats.

#### Decision
Implement forward-compatible schema evolution:

```python
# Schema Evolution Principles
1. Additive Changes: New optional fields allowed
2. Compatible Defaults: Missing fields get sensible defaults  
3. Deprecation Path: Mark fields as deprecated before removal
4. Version Tracking: Track schema versions in metadata

# Schema Evolution Handler
class SchemaEvolutionHandler:
    def evolve_schema(self, current_schema, new_data):
        # Union by name - add missing fields
        evolved_schema = union_schemas_by_name(current_schema, new_data.schema)
        
        # Validate compatibility
        if not is_compatible(current_schema, evolved_schema):
            raise SchemaIncompatibilityError()
            
        # Apply evolution
        return self.table.update_schema().union_by_name(evolved_schema)
```

#### Evolution Rules

| Change Type | Allowed | Handling |
|-------------|---------|----------|
| **Add Optional Field** | ✅ | Automatic union by name |
| **Add Required Field** | ❌ | Must backfill existing data |
| **Remove Field** | ⚠️ | Deprecate first, then remove |
| **Change Type** | ❌ | Create new field, deprecate old |
| **Rename Field** | ❌ | Add new, deprecate old |

#### Rationale
- **Backward Compatibility**: Existing queries continue to work
- **Forward Compatibility**: New data doesn't break old readers
- **Flexibility**: Support exchange schema changes
- **Safety**: Prevent breaking changes

#### Consequences
- **Positive**: Robust schema evolution, no downtime
- **Negative**: Schema bloat over time, complexity

---

## System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Crypto Lakehouse Architecture                │
└─────────────────────────────────────────────────────────────────┘

┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Binance   │    │    CCXT     │    │ Cryptofeed  │
│     S3      │    │  (REST)     │    │    (WS)     │
│   Source    │    │  Source     │    │   Source    │
└─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────┐
│            Kafka Topics (Raw Data)                  │
│  crypto.source.binance.trades                       │  
│  crypto.source.ccxt.klines_1m                       │
│  crypto.source.cryptofeed.events                    │
└─────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────┐
│         Quix Streams Processing (SDF)               │
│  • Data normalization & validation                  │
│  • Real-time transformations                        │  
│  • Enrichment & derived fields                      │
└─────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────┐
│              Iceberg Sinks                          │
│  • Micro-batching (size/time triggers)              │
│  • Parquet serialization                            │
│  • Schema evolution                                  │
│  • Conflict resolution                              │  
└─────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────┐
│            Iceberg Tables (Lakehouse)               │
│                                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│  │   Trades    │  │   Klines    │  │ Orderbook   │  │
│  │   Table     │  │   Table     │  │   Table     │  │
│  └─────────────┘  └─────────────┘  └─────────────┘  │
│                                                     │
│  Partitioned by: symbol, day                       │
│  Format: Parquet (Snappy)                          │
│  Evolution: Union by name                          │
└─────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────┐
│              Analytics Layer                        │
│  • Trino/Spark queries                             │
│  • BI tools (Grafana, Superset)                    │
│  • ML pipelines                                     │
│  • Real-time dashboards                            │
└─────────────────────────────────────────────────────┘
```

## Performance Characteristics

### Latency Profile
- **Stream Processing**: < 100ms (real-time)
- **Batch Commit**: 60s (configurable)  
- **Query Latency**: < 5s (time-range queries)
- **Schema Evolution**: < 1s (metadata operation)

### Throughput Targets  
- **Trades**: 100k records/second per exchange
- **Klines**: 10k records/second across all intervals
- **Storage**: 1TB/day compressed (3TB raw)
- **Queries**: 1000 concurrent analytical queries

### Scalability Limits
- **Symbols**: 10,000+ trading pairs
- **Exchanges**: 50+ supported exchanges  
- **Retention**: 7 years historical data
- **Partitions**: 100k+ daily partitions

## Monitoring and Observability

### Key Metrics
- **Ingestion Rate**: Records/second by source
- **Processing Latency**: End-to-end data latency
- **Storage Growth**: Daily/monthly data volume
- **Query Performance**: P95 query latency
- **Error Rates**: By error type and component
- **Schema Evolution**: Changes per table per day

### Alerting Thresholds
- **High Priority**: Data loss, service unavailability  
- **Medium Priority**: Performance degradation, schema conflicts
- **Low Priority**: Capacity warnings, optimization opportunities

This architecture provides a robust foundation for crypto data lakehouse capabilities with production-ready patterns for error handling, schema evolution, and performance optimization.