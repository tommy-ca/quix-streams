# IcebergRESTSink Technical Specification

## Overview

The extended `IcebergSink` adds REST catalog support to the existing AWS Glue implementation, providing a unified interface for multiple catalog types with S3-compatible storage support:
- **AWS Glue** catalogs with S3 storage (existing, unchanged)
- **REST catalogs** with any S3-compatible storage (new)
- **Local development** with Lakekeeper + MinIO
- **Cloud-agnostic** production deployments (R2, MinIO, S3, etc.)

## Architecture

### Component Diagram
```
┌─────────────────────────────────────────────────────────┐
│                  IcebergRESTSink                        │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │
│  │   Config    │  │   Catalog   │  │     Storage     │  │
│  │  Management │  │   Client    │  │     Client      │  │
│  └─────────────┘  └─────────────┘  └─────────────────┘  │
│         │                 │                   │         │
│         └─────────────────┼───────────────────┘         │
│                           │                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │            Batch Processing Engine              │  │
│  │  • Schema Evolution  • Error Handling          │  │
│  │  • Data Serialization • Retry Logic            │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### Class Hierarchy
```python
BatchingSink
    └── IcebergSink (extended from existing)
        ├── Supports AWS Glue catalogs (existing)
        └── Supports REST catalogs (new)
            ├── AWS S3 storage (existing)
            └── S3-compatible storage (new: R2, MinIO, etc.)
```

## API Specification

### Core Implementation

```python
from dataclasses import dataclass
from typing import Literal, Optional, Dict, Any
from quixstreams.sinks import BatchingSink, SinkBatch

@dataclass
class S3CompatibleConfig:
    """Configuration for S3-compatible storage backends."""
    endpoint_url: str           # Storage endpoint URL
    bucket: str                 # Target bucket name
    access_key_id: str         # Access key ID
    secret_access_key: str     # Secret access key
    region: str = "auto"       # Region (R2 uses "auto")
    
    # Optional performance tuning
    multipart_threshold: int = 64 * 1024 * 1024  # 64MB
    max_concurrency: int = 10
    use_ssl: bool = True
    
@dataclass
class RESTAuthConfig:
    """Authentication configuration for REST catalogs."""
    type: Literal["none", "bearer_token", "basic", "oauth2"]
    token: Optional[str] = None
    username: Optional[str] = None  
    password: Optional[str] = None
    
    # OAuth2 specific
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    token_endpoint: Optional[str] = None

@dataclass
class IcebergRESTConfig:
    """Complete configuration for REST Iceberg sink."""
    rest_uri: str              # REST catalog endpoint
    warehouse_id: str          # Warehouse identifier
    storage: S3CompatibleConfig
    auth: RESTAuthConfig
    
    # Connection settings
    timeout: int = 30
    max_retries: int = 3
    retry_backoff_factor: float = 2.0
    
    # Performance settings
    batch_size: int = 10000
    batch_timeout: float = 60.0
    compression: str = "snappy"
    
    # Schema settings
    auto_create_table: bool = True
    schema_evolution_mode: Literal["strict", "union", "ignore"] = "union"

class IcebergSink(BatchingSink):  # Extended existing implementation
    """
    Iceberg sink supporting multiple catalog and storage types.
    
    Catalog Support:
    - AWS Glue (existing, production-ready)
    - REST catalogs (new: Lakekeeper, Tabular, etc.)
    
    Storage Support:
    - AWS S3 (existing)
    - Any S3-compatible storage (new: R2, MinIO, etc.)
    """
    
    def __init__(
        self,
        table_name: str,
        config: Union[AWSIcebergConfig, RESTIcebergConfig],  # Extended union
        data_catalog_spec: Literal["aws_glue", "rest"],     # Extended options
        schema: Optional[Schema] = None,
        partition_spec: Optional[PartitionSpec] = None,
        **kwargs
    ):
        """
        Initialize the REST Iceberg sink.
        
        Args:
            table_name: Fully qualified table name (e.g. "warehouse.db.table")
            config: Complete REST configuration
            schema: Optional explicit schema (will be inferred if not provided)
            partition_spec: Optional partition specification
            **kwargs: Additional BatchingSink arguments
        """
        super().__init__(**kwargs)
        
        self._table_name = table_name
        self._config = config
        self._schema = schema
        self._partition_spec = partition_spec
        
        # Will be initialized in setup()
        self._catalog = None
        self._table = None
        self._storage_client = None
        
    def setup(self):
        """Initialize catalog and storage clients."""
        self._setup_catalog()
        self._setup_storage()
        self._setup_table()
        
    def write(self, batch: SinkBatch):
        """Write a batch of records to the Iceberg table."""
        try:
            # Convert batch to PyArrow table
            arrow_table = self._batch_to_arrow(batch)
            
            # Handle schema evolution if needed
            if self._config.schema_evolution_mode != "ignore":
                self._evolve_schema_if_needed(arrow_table.schema)
            
            # Write to Iceberg table
            self._write_arrow_table(arrow_table)
            
        except Exception as e:
            self._handle_write_error(e, batch)
            
    def _setup_catalog(self):
        """Initialize REST catalog client."""
        from pyiceberg.catalog import load_catalog
        
        catalog_config = {
            "type": "rest",
            "uri": self._config.rest_uri,
            "warehouse": self._config.warehouse_id,
            "timeout": self._config.timeout,
        }
        
        # Add authentication
        if self._config.auth.type == "bearer_token":
            catalog_config["token"] = self._config.auth.token
        elif self._config.auth.type == "basic":
            catalog_config["username"] = self._config.auth.username
            catalog_config["password"] = self._config.auth.password
            
        self._catalog = load_catalog("rest_catalog", **catalog_config)
        
    def _setup_storage(self):
        """Initialize S3-compatible storage client."""
        import boto3
        from botocore.config import Config
        
        config = Config(
            retries={"max_attempts": self._config.max_retries},
            max_pool_connections=self._config.storage.max_concurrency
        )
        
        self._storage_client = boto3.client(
            "s3",
            endpoint_url=self._config.storage.endpoint_url,
            aws_access_key_id=self._config.storage.access_key_id,
            aws_secret_access_key=self._config.storage.secret_access_key,
            region_name=self._config.storage.region,
            use_ssl=self._config.storage.use_ssl,
            config=config
        )
        
    def _setup_table(self):
        """Initialize or create Iceberg table."""
        table_parts = self._table_name.split(".")
        
        # Create namespace if it doesn't exist
        if len(table_parts) > 1:
            namespace = tuple(table_parts[:-1])
            try:
                self._catalog.create_namespace(namespace)
            except Exception:
                pass  # Namespace might already exist
                
        # Load or create table
        try:
            self._table = self._catalog.load_table(self._table_name)
        except Exception:
            if self._config.auto_create_table:
                self._table = self._create_table()
            else:
                raise
                
    def _create_table(self):
        """Create a new Iceberg table with default schema."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import StringType, TimestampType, DoubleType
        
        # Default schema for crypto data
        default_schema = Schema([
            (1, "exchange", StringType(), True),
            (2, "market", StringType(), True),  
            (3, "symbol", StringType(), True),
            (4, "ts_event", TimestampType(), True),
            (5, "price", DoubleType(), True),
            (6, "qty", DoubleType(), True),
            (7, "ingest_ts", TimestampType(), True),
        ])
        
        schema = self._schema or default_schema
        partition_spec = self._partition_spec or self._get_default_partition_spec()
        
        return self._catalog.create_table(
            identifier=self._table_name,
            schema=schema,
            partition_spec=partition_spec
        )
        
    def _batch_to_arrow(self, batch: SinkBatch) -> pa.Table:
        """Convert SinkBatch to PyArrow Table."""
        import pyarrow as pa
        
        # Extract records from batch
        records = []
        for item in batch:
            record = dict(item.value)
            record["_kafka_key"] = item.key
            record["_kafka_timestamp"] = item.timestamp
            records.append(record)
            
        # Convert to PyArrow table
        return pa.Table.from_pylist(records)
        
    def _write_arrow_table(self, arrow_table: pa.Table):
        """Write PyArrow table to Iceberg."""
        try:
            self._table.append(arrow_table)
        except Exception as e:
            # Handle commit conflicts
            if "CommitFailedException" in str(e):
                import random
                import time
                
                # Exponential backoff with jitter
                delay = random.uniform(0.1, 0.5) * (2 ** self._retry_count)
                time.sleep(min(delay, 5.0))
                
                # Reload table and retry
                self._table = self._catalog.load_table(self._table_name)
                self._table.append(arrow_table)
            else:
                raise
```

### Convenience Subclasses

```python
class CloudflareR2Config(S3CompatibleConfig):
    """Pre-configured for Cloudflare R2."""
    
    def __init__(
        self,
        account_id: str,
        r2_token_id: str,
        r2_token_secret: str,
        bucket: str,
        **kwargs
    ):
        super().__init__(
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            bucket=bucket,
            access_key_id=r2_token_id,
            secret_access_key=r2_token_secret,
            region="auto",  # R2 requirement
            **kwargs
        )

class CloudflareR2Sink(IcebergRESTSink):
    """Convenience sink for Cloudflare R2 deployments."""
    
    def __init__(
        self,
        table_name: str,
        rest_catalog_uri: str,
        warehouse_id: str,
        account_id: str,
        r2_token_id: str,
        r2_token_secret: str,
        bucket: str,
        auth_config: Optional[RESTAuthConfig] = None,
        **kwargs
    ):
        storage_config = CloudflareR2Config(
            account_id=account_id,
            r2_token_id=r2_token_id,
            r2_token_secret=r2_token_secret,
            bucket=bucket
        )
        
        config = IcebergRESTConfig(
            rest_uri=rest_catalog_uri,
            warehouse_id=warehouse_id,
            storage=storage_config,
            auth=auth_config or RESTAuthConfig(type="none")
        )
        
        super().__init__(table_name=table_name, config=config, **kwargs)

class LocalIcebergSink(IcebergRESTSink):
    """Convenience sink for local development."""
    
    def __init__(
        self,
        table_name: str,
        bucket: str = "crypto-dev",
        lakekeeper_uri: str = "http://localhost:8181",
        minio_endpoint: str = "http://localhost:9000",
        warehouse_id: str = "local",
        **kwargs
    ):
        storage_config = S3CompatibleConfig(
            endpoint_url=minio_endpoint,
            bucket=bucket,
            access_key_id="minioadmin",
            secret_access_key="minioadmin",
            region="us-east-1",
            use_ssl=False
        )
        
        config = IcebergRESTConfig(
            rest_uri=lakekeeper_uri,
            warehouse_id=warehouse_id,
            storage=storage_config,
            auth=RESTAuthConfig(type="none")
        )
        
        super().__init__(table_name=table_name, config=config, **kwargs)
```

## Configuration Examples

### Cloudflare R2 Production
```python
from quixstreams.sinks.community.iceberg_rest import CloudflareR2Sink, RESTAuthConfig

# Using convenience class
sink = CloudflareR2Sink(
    table_name="crypto.binance.trades_spot",
    rest_catalog_uri="https://catalog.yourdomain.com/api/v1",
    warehouse_id="production",
    account_id=os.environ["CF_ACCOUNT_ID"],
    r2_token_id=os.environ["CF_R2_TOKEN_ID"],
    r2_token_secret=os.environ["CF_R2_TOKEN_SECRET"],
    bucket="crypto-lakehouse",
    auth_config=RESTAuthConfig(
        type="bearer_token",
        token=os.environ["CATALOG_TOKEN"]
    )
)
```

### Local Development
```python
from quixstreams.sinks.community.iceberg_rest import LocalIcebergSink

# Using convenience class
sink = LocalIcebergSink(
    table_name="crypto.binance.trades_spot",
    bucket="crypto-dev"
)
```

### Generic REST Catalog
```python
from quixstreams.sinks.community.iceberg_rest import (
    IcebergRESTSink, IcebergRESTConfig, S3CompatibleConfig, RESTAuthConfig
)

# Full configuration control
storage_config = S3CompatibleConfig(
    endpoint_url="https://s3.amazonaws.com",
    bucket="my-lakehouse",
    access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region="us-west-2"
)

auth_config = RESTAuthConfig(
    type="basic",
    username="catalog_user",
    password=os.environ["CATALOG_PASSWORD"]
)

config = IcebergRESTConfig(
    rest_uri="https://my-catalog.example.com/api/v1",
    warehouse_id="main",
    storage=storage_config,
    auth=auth_config,
    batch_size=5000,
    batch_timeout=30.0
)

sink = IcebergRESTSink(
    table_name="crypto.trades",
    config=config
)
```

## Schema Management

### Default Crypto Schema
```python
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType, TimestampType, DoubleType, LongType, BooleanType
)

# Trades schema
trades_schema = Schema([
    (1, "exchange", StringType(), True),
    (2, "market", StringType(), True),
    (3, "symbol", StringType(), True), 
    (4, "ts_event", TimestampType(), True),
    (5, "price", DoubleType(), True),
    (6, "qty", DoubleType(), True),
    (7, "trade_id", LongType(), False),
    (8, "side", StringType(), False),
    (9, "ingest_ts", TimestampType(), True),
    (10, "meta", StringType(), False),  # JSON metadata
])

# Klines schema
klines_schema = Schema([
    (1, "exchange", StringType(), True),
    (2, "market", StringType(), True),
    (3, "symbol", StringType(), True),
    (4, "interval", StringType(), True),
    (5, "open_time", TimestampType(), True),
    (6, "open", DoubleType(), True),
    (7, "high", DoubleType(), True),
    (8, "low", DoubleType(), True),
    (9, "close", DoubleType(), True),
    (10, "volume", DoubleType(), True),
    (11, "close_time", TimestampType(), True),
    (12, "ingest_ts", TimestampType(), True),
])
```

### Partitioning Strategies
```python
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.transforms import DayTransform, IdentityTransform

# Symbol + day partitioning (recommended)
partition_spec = PartitionSpec([
    (1, "symbol", IdentityTransform()),
    (2, "event_day", DayTransform(), "ts_event")
])

# Market + symbol + day partitioning (high volume)
partition_spec = PartitionSpec([
    (1, "market", IdentityTransform()),
    (2, "symbol", IdentityTransform()),  
    (3, "event_day", DayTransform(), "ts_event")
])
```

## Error Handling

### Error Classification
```python
class IcebergRESTError(Exception):
    """Base exception for REST Iceberg sink errors."""
    pass

class CatalogConnectionError(IcebergRESTError):
    """Failed to connect to REST catalog."""
    pass

class StorageConnectionError(IcebergRESTError):
    """Failed to connect to S3-compatible storage."""
    pass

class SchemaEvolutionError(IcebergRESTError):
    """Schema evolution failed."""
    pass

class CommitConflictError(IcebergRESTError):
    """Concurrent write conflict."""
    pass
```

### Retry Logic
```python
def _handle_write_error(self, error: Exception, batch: SinkBatch):
    """Handle write errors with appropriate retry logic."""
    
    if isinstance(error, CommitConflictError):
        # Retry with exponential backoff
        self._retry_with_backoff(batch)
    elif isinstance(error, (CatalogConnectionError, StorageConnectionError)):
        # Transient network errors - retry
        if self._retry_count < self._config.max_retries:
            self._retry_count += 1
            time.sleep(self._config.retry_backoff_factor ** self._retry_count)
            self.write(batch)
        else:
            raise
    else:
        # Non-retriable error
        logger.error(f"Non-retriable error: {error}")
        raise
```

## Performance Characteristics

### Throughput Targets
- **Records/second**: 10,000+ records/second
- **Batch processing**: 5-60 second batching windows
- **Storage efficiency**: 70-90% compression with Snappy
- **Query performance**: <5s for typical time-range queries

### Resource Usage
- **Memory**: ~100MB baseline + batch size dependent
- **CPU**: Moderate during batch processing, low during streaming
- **Network**: Burst during writes, minimal during streaming
- **Storage**: Efficient columnar format with compression

## Monitoring and Observability

### Key Metrics
```python
# Throughput metrics
sink.records_per_second.observe(rate)
sink.batches_per_minute.observe(batch_rate)
sink.bytes_written.observe(bytes_written)

# Latency metrics  
sink.write_latency.observe(write_duration)
sink.batch_processing_latency.observe(batch_duration)
sink.end_to_end_latency.observe(total_duration)

# Error metrics
sink.write_errors.inc(labels={"error_type": "commit_conflict"})
sink.retry_attempts.observe(retry_count)
sink.schema_evolution_events.inc()

# Resource metrics
sink.memory_usage.observe(memory_mb)
sink.active_connections.observe(connection_count)
```

### Health Checks
```python
def health_check(self) -> Dict[str, Any]:
    """Return health status of the sink."""
    return {
        "catalog_connection": self._check_catalog_health(),
        "storage_connection": self._check_storage_health(),
        "table_accessible": self._check_table_health(),
        "last_write_time": self._last_write_timestamp,
        "error_rate": self._calculate_error_rate(),
        "throughput": self._calculate_throughput()
    }
```

## Testing Strategy

### Unit Tests
- [ ] Configuration validation
- [ ] Schema evolution logic
- [ ] Error handling scenarios
- [ ] Batch processing logic
- [ ] Authentication mechanisms

### Integration Tests
- [ ] Local stack (Lakekeeper + MinIO + Redpanda)
- [ ] Cloudflare R2 integration
- [ ] Schema evolution across restarts
- [ ] Concurrent write handling
- [ ] Performance benchmarks

### End-to-End Tests
- [ ] Complete crypto source → sink pipeline
- [ ] Multi-table concurrent writes
- [ ] Large data volume processing
- [ ] Failure recovery scenarios
- [ ] Production deployment validation

## Migration and Compatibility

### From AWS Glue IcebergSink
```python
# Old AWS Glue configuration
old_sink = IcebergSink(
    table_name="glue.crypto_trades",
    config=AWSIcebergConfig(...),
    data_catalog_spec="aws_glue"
)

# New REST configuration  
new_sink = IcebergRESTSink(
    table_name="crypto.binance.trades",
    config=IcebergRESTConfig(...)
)
```

### Configuration Migration Tool
```python
def migrate_config(aws_config: AWSIcebergConfig) -> IcebergRESTConfig:
    """Helper to migrate from AWS Glue to REST configuration."""
    # Implementation would extract relevant settings
    # and map them to REST configuration
    pass
```

## Deployment Considerations

### Production Checklist
- [ ] REST catalog endpoint configured and accessible
- [ ] Storage credentials properly secured
- [ ] Network connectivity verified (catalog + storage)
- [ ] Monitoring and alerting configured
- [ ] Backup and disaster recovery procedures
- [ ] Performance baselines established

### Security Considerations
- [ ] Credentials stored securely (env vars, secrets management)
- [ ] Network traffic encrypted (HTTPS for REST, TLS for storage)
- [ ] Access controls configured (catalog + storage permissions)
- [ ] Audit logging enabled
- [ ] Regular security reviews

This specification provides the foundation for implementing a production-ready REST catalog Iceberg sink that maintains feature parity with the existing AWS Glue implementation while enabling cloud-agnostic deployments.