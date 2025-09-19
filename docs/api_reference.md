# Apache Iceberg REST Sink - API Reference

## Table of Contents

1. [Quick Start](#quick-start)
2. [Configuration](#configuration)
3. [API Classes](#api-classes)
4. [Factory Functions](#factory-functions)
5. [Error Handling](#error-handling)
6. [Performance Tuning](#performance-tuning)
7. [Examples](#examples)

## Quick Start

### Basic Usage

```python path=null start=null
from quixstreams import Application
from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_rest_config

# Create configuration
config = create_local_rest_config(table_name="my_table")

# Initialize sink
sink = IcebergRESTSink(config=config)

# Use with QuixStreams
app = Application(broker_address="localhost:9092")
topic = app.topic("input_topic")
sdf = app.dataframe(topic)
sdf.sink(sink)
app.run()
```

### Installation Requirements

```bash path=null start=null
# Core dependencies (automatically installed with QuixStreams)
pip install requests>=2.28.0
pip install urllib3>=1.26.0

# Performance optimizations (recommended)
pip install orjson>=3.8.0  # 3-10x faster JSON serialization
pip install ujson>=5.0.0   # Fallback fast JSON library
```

## Configuration

### RESTIcebergConfig Class

The main configuration class for the Iceberg REST Sink.

```python path=/home/tommyk/projects/devops/quix-streams/quixstreams/sinks/community/iceberg_rest/config.py start=25
@dataclass
class RESTIcebergConfig:
    """
    Configuration for Apache Iceberg REST catalog with S3-compatible storage.
    
    This configuration supports various cloud providers and storage backends
    through S3-compatible APIs.
    """
    
    # Required catalog settings
    catalog_uri: str                    # REST catalog endpoint URL
    table_name: str                     # Target Iceberg table name
    warehouse_id: str                   # Catalog warehouse identifier
    
    # S3-compatible storage settings
    s3_endpoint_url: Optional[str] = None        # Custom S3 endpoint (MinIO, R2, etc.)
    s3_region: Optional[str] = None              # AWS region or equivalent
    s3_access_key_id: Optional[str] = None       # S3 access key
    s3_secret_access_key: Optional[str] = None   # S3 secret key
    s3_session_token: Optional[str] = None       # Temporary session token (STS)
    
    # REST catalog authentication
    catalog_token: Optional[str] = None          # Bearer token for catalog auth
    auth_type: Literal["none", "bearer_token"] = "none"  # Authentication method
```

### Environment Variables

The sink supports environment variable configuration for sensitive values:

```bash path=null start=null
# Catalog Configuration
ICEBERG_REST_CATALOG_URI="https://catalog.example.com/api/v1"
ICEBERG_REST_WAREHOUSE_ID="production"
ICEBERG_REST_CATALOG_TOKEN="your-bearer-token"

# S3-Compatible Storage
ICEBERG_REST_S3_ENDPOINT_URL="https://s3.amazonaws.com"
ICEBERG_REST_S3_REGION="us-east-1"
ICEBERG_REST_S3_ACCESS_KEY_ID="your-access-key"
ICEBERG_REST_S3_SECRET_ACCESS_KEY="your-secret-key"
ICEBERG_REST_S3_SESSION_TOKEN="your-session-token"

# Performance Settings
ICEBERG_REST_BATCH_SIZE="1000"
ICEBERG_REST_MAX_BUFFER_MEMORY_MB="100"
ICEBERG_REST_REQUEST_TIMEOUT="10.0"
ICEBERG_REST_MAX_RETRIES="3"
ICEBERG_REST_ADAPTIVE_BATCHING="true"
```

## API Classes

### IcebergRESTSink

The main sink class that implements the QuixStreams sink interface.

```python path=/home/tommyk/projects/devops/quix-streams/quixstreams/sinks/community/iceberg_rest/sink.py start=35
class IcebergRESTSink:
    """
    Apache Iceberg REST sink for QuixStreams with performance optimizations.
    
    This sink provides:
    - Adaptive batching based on record size and memory usage
    - Configurable memory limits with automatic flushing
    - Connection pooling and retry logic
    - Comprehensive error handling and monitoring
    """
    
    def __init__(
        self,
        config: RESTIcebergConfig,
        batch_size: int = 500,
        request_timeout: float = 5.0,
        max_retries: int = 3,
        backoff_factor: float = 0.3,
        max_buffer_memory_mb: float = 50.0,
        adaptive_batching: bool = True,
        **kwargs
    ):
        """
        Initialize the Iceberg REST sink.
        
        Args:
            config: REST catalog and storage configuration
            batch_size: Default number of records per batch (used when adaptive_batching=False)
            request_timeout: HTTP request timeout in seconds
            max_retries: Maximum number of retry attempts for failed requests
            backoff_factor: Exponential backoff multiplier for retries
            max_buffer_memory_mb: Maximum buffer memory usage in MB
            adaptive_batching: Enable dynamic batch sizing based on record size
            **kwargs: Additional configuration options
            
        Raises:
            ConfigurationError: If configuration validation fails
            ValidationError: If parameter values are invalid
        """
```

#### Key Methods

##### write(records)

```python path=null start=null
def write(self, records: Union[List[Dict], Dict, None]) -> None:
    """
    Write records to the Iceberg table with adaptive batching.
    
    Args:
        records: Single record (Dict), list of records (List[Dict]), or None
        
    Raises:
        BufferError: If memory limits are exceeded
        NetworkError: If REST catalog communication fails
        CatalogError: If table operations fail
        
    Example:
        # Single record
        sink.write({"id": 1, "name": "Alice", "timestamp": "2024-01-01T00:00:00Z"})
        
        # Multiple records
        sink.write([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ])
        
        # Handle None gracefully
        sink.write(None)  # No-op
    """
```

##### flush()

```python path=null start=null
def flush(self) -> None:
    """
    Force flush all buffered records to the Iceberg table.
    
    This method should be called:
    - At application shutdown
    - When you need to ensure data durability
    - Before critical checkpoints
    
    Raises:
        NetworkError: If REST catalog communication fails
        CatalogError: If table operations fail
        
    Example:
        sink.flush()  # Ensures all buffered data is written
    """
```

##### health_check()

```python path=null start=null
def health_check(self) -> Dict[str, Any]:
    """
    Perform comprehensive health check of the sink and catalog.
    
    Returns:
        Dict containing health status with keys:
        - status: "healthy" or "unhealthy"
        - buffer_size: Number of records in buffer
        - buffer_memory_mb: Current buffer memory usage
        - client_health: REST catalog client health status
        - last_error: Most recent error (if any)
        
    Example:
        health = sink.health_check()
        if health["status"] != "healthy":
            logger.error(f"Sink unhealthy: {health}")
    """
```

##### get_stats()

```python path=null start=null
def get_stats(self) -> Dict[str, Any]:
    """
    Get detailed runtime statistics and performance metrics.
    
    Returns:
        Dict containing performance statistics with keys:
        - buffer_size: Current number of buffered records
        - buffer_memory_mb: Current buffer memory usage in MB
        - max_buffer_memory_mb: Configured maximum buffer memory
        - adaptive_batching: Whether adaptive batching is enabled
        - total_records_written: Cumulative count of written records
        - total_batches_sent: Cumulative count of batches sent
        - avg_batch_size: Average records per batch
        - compression_ratio: Average compression ratio achieved
        
    Example:
        stats = sink.get_stats()
        logger.info(f"Written {stats['total_records_written']} records in {stats['total_batches_sent']} batches")
    """
```

### RESTCatalogClient

Low-level client for REST catalog operations.

```python path=/home/tommyk/projects/devops/quix-streams/quixstreams/sinks/community/iceberg_rest/client.py start=25
class RESTCatalogClient:
    """
    High-performance REST client for Apache Iceberg catalog operations.
    
    Features:
    - Connection pooling with configurable pool sizes
    - Automatic retry with exponential backoff
    - JSON serialization optimization (orjson/ujson support)
    - Request/response compression (gzip)
    - Comprehensive error handling with context
    """
    
    def __init__(
        self,
        config: RESTIcebergConfig,
        request_timeout: float = 5.0,
        max_retries: int = 3,
        backoff_factor: float = 0.3
    ):
        """
        Initialize the REST catalog client.
        
        Args:
            config: REST catalog configuration
            request_timeout: HTTP request timeout in seconds
            max_retries: Maximum retry attempts
            backoff_factor: Exponential backoff multiplier
        """
```

#### Key Methods

##### send_records(records, table_name)

```python path=null start=null
def send_records(self, records: List[Dict], table_name: str) -> Dict[str, Any]:
    """
    Send records to the Iceberg table via REST catalog.
    
    Args:
        records: List of dictionaries representing table records
        table_name: Target Iceberg table name
        
    Returns:
        Dict containing response metadata:
        - success: Boolean indicating operation success
        - records_written: Number of records successfully written
        - compression_ratio: Achieved compression ratio
        - request_size_bytes: Size of compressed request
        - response_time_ms: Request duration in milliseconds
        
    Raises:
        NetworkError: For HTTP communication errors
        AuthenticationError: For authentication failures
        CatalogError: For catalog operation errors
        TimeoutError: For request timeout
    """
```

##### health_check()

```python path=null start=null
def health_check(self) -> Dict[str, Any]:
    """
    Check connectivity and authentication with REST catalog.
    
    Returns:
        Dict containing health information:
        - status: "healthy" or "unhealthy"
        - catalog_uri: Catalog endpoint URI
        - response_time_ms: Health check response time
        - error: Error details (if unhealthy)
    """
```

## Factory Functions

Convenience functions for creating configurations for common deployment scenarios.

### create_local_rest_config()

```python path=/home/tommyk/projects/devops/quix-streams/quixstreams/sinks/community/iceberg_rest/config.py start=180
def create_local_rest_config(
    table_name: str,
    catalog_host: str = "localhost",
    catalog_port: int = 8181,
    warehouse_id: str = "local",
    minio_host: str = "localhost",
    minio_port: int = 9000,
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin",
    **kwargs
) -> RESTIcebergConfig:
    """
    Create configuration for local development with MinIO and Lakekeeper.
    
    Args:
        table_name: Target Iceberg table name
        catalog_host: Lakekeeper catalog host (default: localhost)
        catalog_port: Lakekeeper catalog port (default: 8181)
        warehouse_id: Catalog warehouse identifier (default: local)
        minio_host: MinIO server host (default: localhost)
        minio_port: MinIO server port (default: 9000)
        minio_access_key: MinIO access key (default: minioadmin)
        minio_secret_key: MinIO secret key (default: minioadmin)
        **kwargs: Additional configuration options
        
    Returns:
        RESTIcebergConfig configured for local development
        
    Example:
        config = create_local_rest_config(
            table_name="crypto_trades",
            catalog_port=8181,
            minio_port=9001
        )
    """
```

### create_s3_rest_config()

```python path=/home/tommyk/projects/devops/quix-streams/quixstreams/sinks/community/iceberg_rest/config.py start=220
def create_s3_rest_config(
    catalog_uri: str,
    table_name: str,
    warehouse_id: str,
    aws_region: str = "us-east-1",
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    catalog_token: Optional[str] = None,
    **kwargs
) -> RESTIcebergConfig:
    """
    Create configuration for AWS S3 with REST catalog service.
    
    Args:
        catalog_uri: REST catalog endpoint URL
        table_name: Target Iceberg table name
        warehouse_id: Catalog warehouse identifier
        aws_region: AWS region for S3 operations
        aws_access_key_id: AWS access key ID (optional if using IAM)
        aws_secret_access_key: AWS secret access key
        aws_session_token: AWS session token for temporary credentials
        catalog_token: Bearer token for catalog authentication
        **kwargs: Additional configuration options
        
    Returns:
        RESTIcebergConfig configured for AWS S3
        
    Example:
        config = create_s3_rest_config(
            catalog_uri="https://tabular.io/api/v1",
            table_name="production_events",
            warehouse_id="production",
            aws_region="us-west-2",
            catalog_token=os.getenv("TABULAR_TOKEN")
        )
    """
```

### create_r2_config()

```python path=/home/tommyk/projects/devops/quix-streams/quixstreams/sinks/community/iceberg_rest/config.py start=260
def create_r2_config(
    account_id: str,
    table_name: str,
    catalog_uri: str,
    warehouse_id: str = "default",
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    catalog_token: Optional[str] = None,
    **kwargs
) -> RESTIcebergConfig:
    """
    Create configuration for Cloudflare R2 with REST catalog.
    
    Args:
        account_id: Cloudflare account ID
        table_name: Target Iceberg table name
        catalog_uri: REST catalog endpoint URL
        warehouse_id: Catalog warehouse identifier
        access_key_id: R2 API token access key
        secret_access_key: R2 API token secret
        catalog_token: Bearer token for catalog authentication
        **kwargs: Additional configuration options
        
    Returns:
        RESTIcebergConfig configured for Cloudflare R2
        
    Example:
        config = create_r2_config(
            account_id="your-cf-account-id",
            table_name="analytics_events",
            catalog_uri="https://catalog.company.com/api/v1",
            access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY")
        )
    """
```

## Error Handling

### Exception Hierarchy

```python path=/home/tommyk/projects/devops/quix-streams/quixstreams/sinks/community/iceberg_rest/errors.py start=15
# Base exception
class IcebergRESTError(Exception):
    """Base exception for Apache Iceberg REST sink operations."""
    pass

# Configuration errors
class ConfigurationError(IcebergRESTError):
    """Configuration validation or setup errors."""
    pass

class ValidationError(ConfigurationError):
    """Parameter or data validation errors."""
    pass

# Network and communication errors
class NetworkError(IcebergRESTError):
    """Network communication errors with REST catalog."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text

class TimeoutError(NetworkError):
    """Request timeout errors."""
    pass

class AuthenticationError(NetworkError):
    """Authentication and authorization errors."""
    pass

# Catalog operation errors
class CatalogError(IcebergRESTError):
    """REST catalog operation errors."""
    pass

class SchemaError(CatalogError):
    """Table schema mismatch or evolution errors."""
    pass

# Buffer and memory errors
class BufferError(IcebergRESTError):
    """Buffer management and memory limit errors."""
    
    def __init__(self, message: str, current_memory_mb: float, max_memory_mb: float):
        super().__init__(message)
        self.current_memory_mb = current_memory_mb
        self.max_memory_mb = max_memory_mb
```

### Error Context Examples

```python path=null start=null
try:
    sink.write(records)
except NetworkError as e:
    logger.error(f"Network error: {e}")
    logger.error(f"Status code: {e.status_code}")
    logger.error(f"Response: {e.response_text}")
except BufferError as e:
    logger.error(f"Buffer error: {e}")
    logger.error(f"Memory usage: {e.current_memory_mb}MB / {e.max_memory_mb}MB")
except ValidationError as e:
    logger.error(f"Validation error: {e}")
```

## Performance Tuning

### Adaptive Batching

The sink automatically adjusts batch sizes based on record size and memory usage:

```python path=null start=null
# Enable adaptive batching (default)
sink = IcebergRESTSink(
    config=config,
    adaptive_batching=True,
    max_buffer_memory_mb=100.0
)

# Disable for fixed batch sizes
sink = IcebergRESTSink(
    config=config,
    adaptive_batching=False,
    batch_size=1000
)
```

### Memory Management

Configure memory limits to prevent excessive buffering:

```python path=null start=null
import psutil

# Set memory limit as percentage of available RAM
available_ram_mb = psutil.virtual_memory().available / (1024 * 1024)
memory_limit = min(available_ram_mb * 0.10, 200)  # 10% of RAM, max 200MB

sink = IcebergRESTSink(
    config=config,
    max_buffer_memory_mb=memory_limit,
    adaptive_batching=True
)

# Monitor memory usage
stats = sink.get_stats()
memory_usage_pct = (stats['buffer_memory_mb'] / stats['max_buffer_memory_mb']) * 100
if memory_usage_pct > 80:
    logger.warning(f"High memory usage: {memory_usage_pct:.1f}%")
    sink.flush()  # Force flush to free memory
```

### JSON Performance

Install optional dependencies for faster JSON serialization:

```bash path=null start=null
# Best performance (3-10x faster)
pip install orjson>=3.8.0

# Good performance (2-3x faster)
pip install ujson>=5.0.0

# Falls back to standard library json if neither is available
```

### Connection Pooling

The client automatically optimizes HTTP connections:

```python path=null start=null
# Connection pooling is automatic and configured optimally:
# - 20 connection pools per host
# - 100 maximum connections per pool
# - Connection reuse across requests
# - Automatic connection cleanup

# No manual configuration required!
```

## Examples

### 1. Local Development Setup

```python path=null start=null
"""Complete local development setup with Docker Compose"""

from quixstreams import Application
from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_rest_config
import logging

# Enable debugging
logging.basicConfig(level=logging.DEBUG)

# Create configuration for local services
config = create_local_rest_config(
    table_name="user_events",
    catalog_host="localhost",
    catalog_port=8181,  # Lakekeeper REST catalog
    minio_host="localhost", 
    minio_port=9000,    # MinIO object storage
    warehouse_id="local"
)

# Create sink with local development optimizations
sink = IcebergRESTSink(
    config=config,
    batch_size=100,          # Smaller batches for development
    max_buffer_memory_mb=10.0,  # Smaller memory limit
    request_timeout=30.0,    # Longer timeout for debugging
    adaptive_batching=True
)

# QuixStreams application
app = Application(
    broker_address="localhost:9092",
    consumer_group="iceberg-sink-dev"
)

input_topic = app.topic("user_events")
sdf = app.dataframe(input_topic)

# Add metadata
sdf = sdf.apply(lambda row: {
    **row,
    "processed_at": datetime.utcnow().isoformat(),
    "sink_version": "1.0.0"
})

# Write to Iceberg
sdf.sink(sink)

if __name__ == "__main__":
    try:
        app.run()
    finally:
        sink.flush()  # Ensure data is written on shutdown
```

### 2. Production AWS Deployment

```python path=null start=null
"""Production deployment with AWS S3 and Tabular catalog"""

import os
from quixstreams import Application
from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_s3_rest_config

# Production configuration
config = create_s3_rest_config(
    catalog_uri=os.getenv("TABULAR_CATALOG_URI"),
    warehouse_id=os.getenv("TABULAR_WAREHOUSE_ID"),
    table_name="production_events",
    aws_region="us-east-1",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    catalog_token=os.getenv("TABULAR_TOKEN")
)

# Production-optimized sink
sink = IcebergRESTSink(
    config=config,
    batch_size=1000,             # Larger batches for throughput
    max_buffer_memory_mb=200.0,  # Higher memory limit
    request_timeout=10.0,        # Reasonable timeout
    max_retries=5,               # More retries for reliability
    adaptive_batching=True
)

app = Application(
    broker_address=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    consumer_group="iceberg-sink-prod",
    auto_offset_reset="earliest"
)

# Production topic processing
input_topic = app.topic("production_events")
sdf = app.dataframe(input_topic)

# Data validation and enrichment
def validate_and_enrich(row):
    # Validate required fields
    required_fields = ["user_id", "event_type", "timestamp"]
    for field in required_fields:
        if field not in row:
            raise ValueError(f"Missing required field: {field}")
    
    # Add processing metadata
    return {
        **row,
        "ingestion_time": datetime.utcnow().isoformat(),
        "partition_date": datetime.fromisoformat(row["timestamp"]).date().isoformat()
    }

sdf = sdf.apply(validate_and_enrich)
sdf.sink(sink)

# Production monitoring
def log_stats():
    stats = sink.get_stats()
    health = sink.health_check()
    
    logger.info(f"Sink Stats - Records: {stats['total_records_written']}, "
               f"Batches: {stats['total_batches_sent']}, "
               f"Memory: {stats['buffer_memory_mb']:.1f}MB")
    
    if health["status"] != "healthy":
        logger.error(f"Sink unhealthy: {health}")

# Schedule stats logging every 60 seconds
import threading
timer = threading.Timer(60.0, log_stats)
timer.start()

if __name__ == "__main__":
    try:
        app.run()
    finally:
        sink.flush()
        timer.cancel()
```

### 3. Multi-Provider Configuration

```python path=null start=null
"""Example showing different provider configurations"""

import os
from quixstreams.sinks.community.iceberg_rest import *

# Provider selection based on environment
PROVIDER = os.getenv("ICEBERG_PROVIDER", "local").lower()

if PROVIDER == "local":
    # Local development with MinIO + Lakekeeper
    config = create_local_rest_config(
        table_name="events",
        catalog_port=8181,
        minio_port=9000
    )
    
elif PROVIDER == "aws":
    # AWS S3 with Tabular.io
    config = create_s3_rest_config(
        catalog_uri="https://tabular.io/api/v1",
        warehouse_id="production",
        table_name="events",
        aws_region="us-east-1",
        catalog_token=os.getenv("TABULAR_TOKEN")
    )
    
elif PROVIDER == "r2":
    # Cloudflare R2 with custom catalog
    config = create_r2_config(
        account_id=os.getenv("CF_ACCOUNT_ID"),
        table_name="events",
        catalog_uri=os.getenv("CATALOG_URI"),
        access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
        secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
        catalog_token=os.getenv("CATALOG_TOKEN")
    )
    
else:
    raise ValueError(f"Unknown provider: {PROVIDER}")

# Create sink with provider-optimized settings
if PROVIDER == "local":
    sink_config = {
        "batch_size": 100,
        "max_buffer_memory_mb": 10.0,
        "request_timeout": 30.0
    }
else:
    sink_config = {
        "batch_size": 1000,
        "max_buffer_memory_mb": 100.0,
        "request_timeout": 10.0,
        "max_retries": 5
    }

sink = IcebergRESTSink(config=config, **sink_config)
```

### 4. Error Handling and Recovery

```python path=null start=null
"""Comprehensive error handling example"""

import time
from quixstreams.sinks.community.iceberg_rest import *
from quixstreams.sinks.community.iceberg_rest.errors import *

def create_resilient_sink(config):
    """Create sink with comprehensive error handling"""
    
    max_attempts = 3
    backoff_delay = 1.0
    
    for attempt in range(max_attempts):
        try:
            sink = IcebergRESTSink(
                config=config,
                max_retries=5,
                backoff_factor=0.5,
                adaptive_batching=True
            )
            
            # Test connectivity
            health = sink.health_check()
            if health["status"] != "healthy":
                raise NetworkError(f"Sink unhealthy: {health}")
                
            return sink
            
        except ConfigurationError as e:
            logger.error(f"Configuration error (attempt {attempt + 1}): {e}")
            if attempt == max_attempts - 1:
                raise
                
        except NetworkError as e:
            logger.warning(f"Network error (attempt {attempt + 1}): {e}")
            if attempt == max_attempts - 1:
                raise
            time.sleep(backoff_delay * (2 ** attempt))

def write_with_recovery(sink, records):
    """Write records with automatic recovery"""
    
    max_attempts = 3
    backoff_delay = 1.0
    
    for attempt in range(max_attempts):
        try:
            sink.write(records)
            return  # Success
            
        except BufferError as e:
            logger.warning(f"Buffer full, flushing: {e}")
            sink.flush()
            # Retry the write after flushing
            
        except NetworkError as e:
            logger.warning(f"Network error (attempt {attempt + 1}): {e}")
            if attempt == max_attempts - 1:
                # Store failed records for later retry
                store_failed_records(records, e)
                raise
            time.sleep(backoff_delay * (2 ** attempt))
            
        except CatalogError as e:
            logger.error(f"Catalog error: {e}")
            # Catalog errors are usually not recoverable
            raise

def store_failed_records(records, error):
    """Store failed records for later processing"""
    import json
    import datetime
    
    failed_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "error": str(error),
        "records": records
    }
    
    with open("failed_records.jsonl", "a") as f:
        f.write(json.dumps(failed_data) + "\n")
    
    logger.info(f"Stored {len(records)} failed records for later retry")

# Usage example
config = create_local_rest_config(table_name="events")
sink = create_resilient_sink(config)

# Application with error handling
app = Application(broker_address="localhost:9092")
sdf = app.dataframe(app.topic("events"))

def process_batch(records):
    """Process batch with error handling"""
    try:
        write_with_recovery(sink, records)
    except Exception as e:
        logger.error(f"Failed to write batch after all retries: {e}")
        # Could implement dead letter queue here

sdf.sink(process_batch)
```

---

This API reference provides comprehensive documentation for all classes, methods, and usage patterns of the Apache Iceberg REST Sink. For additional examples and deployment guides, refer to the main specification document.