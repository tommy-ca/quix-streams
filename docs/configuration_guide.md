# Apache Iceberg REST Sink - Configuration Guide

This guide provides comprehensive configuration instructions for different deployment scenarios, from local development to production environments.

## Table of Contents

1. [Configuration Overview](#configuration-overview)
2. [Local Development](#local-development)
3. [AWS S3 Deployment](#aws-s3-deployment)
4. [Cloudflare R2 Deployment](#cloudflare-r2-deployment)
5. [Environment Variables](#environment-variables)
6. [Configuration Files](#configuration-files)
7. [Performance Tuning](#performance-tuning)
8. [Security Configuration](#security-configuration)
9. [Troubleshooting](#troubleshooting)

## Configuration Overview

### Configuration Classes

The Iceberg REST Sink uses a layered configuration approach:

```python
@dataclass
class RESTIcebergConfig:
    """Main configuration class for REST catalog and storage settings."""
    
    # Required settings
    catalog_uri: str              # REST catalog endpoint
    table_name: str               # Target Iceberg table
    warehouse_id: str             # Catalog warehouse identifier
    
    # S3-compatible storage (optional)
    s3_endpoint_url: Optional[str] = None
    s3_region: Optional[str] = None
    s3_access_key_id: Optional[str] = None
    s3_secret_access_key: Optional[str] = None
    s3_session_token: Optional[str] = None
    
    # Authentication (optional)
    catalog_token: Optional[str] = None
    auth_type: Literal["none", "bearer_token"] = "none"
```

### Factory Functions

Use these convenience functions for common scenarios:

- `create_local_rest_config()` - Local development with MinIO + Lakekeeper
- `create_s3_rest_config()` - AWS S3 with REST catalog
- `create_r2_config()` - Cloudflare R2 with custom catalog

## Local Development

### Docker Compose Setup

First, create a complete local stack with `docker-compose.yml`:

```yaml
version: '3.8'
services:
  # Apache Kafka
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  # MinIO S3-compatible storage
  minio:
    image: minio/minio:RELEASE.2024-01-16T16-07-38Z
    ports:
      - "9000:9000"      # S3 API
      - "9001:9001"      # Web Console
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data

  # Lakekeeper Iceberg REST catalog
  lakekeeper:
    image: lakekeeper/lakekeeper:latest
    ports:
      - "8181:8181"      # REST API
    environment:
      RUST_LOG: info
      CATALOG_STORAGE_TYPE: s3
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_ACCESS_KEY: minioadmin
      CATALOG_S3_SECRET_KEY: minioadmin
      CATALOG_S3_BUCKET: warehouse
    depends_on:
      - minio
    restart: unless-stopped

volumes:
  minio_data:
```

### Start Local Services

```bash
# Start all services
docker-compose up -d

# Verify services
curl http://localhost:8181/v1/config          # Lakekeeper
curl http://localhost:9000/minio/health/live  # MinIO

# Create test topic
docker exec -it $(docker-compose ps -q kafka) kafka-topics --create \
    --topic user_events \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1
```

### Local Configuration

```python
from quixstreams.sinks.community.iceberg_rest import create_local_rest_config, IcebergRESTSink

# Basic local configuration
config = create_local_rest_config(
    table_name="user_events",
    catalog_host="localhost",
    catalog_port=8181,
    minio_host="localhost",
    minio_port=9000,
    warehouse_id="local"
)

# Development-optimized sink
sink = IcebergRESTSink(
    config=config,
    batch_size=100,              # Smaller batches for testing
    max_buffer_memory_mb=10.0,   # Lower memory limit
    request_timeout=30.0,        # Longer timeout for debugging
    adaptive_batching=True
)
```

### Local Environment Variables

```bash
# Optional: Override defaults via environment
export ICEBERG_REST_CATALOG_URI="http://localhost:8181"
export ICEBERG_REST_WAREHOUSE_ID="local"
export ICEBERG_REST_S3_ENDPOINT_URL="http://localhost:9000"
export ICEBERG_REST_S3_ACCESS_KEY_ID="minioadmin"
export ICEBERG_REST_S3_SECRET_ACCESS_KEY="minioadmin"
export ICEBERG_REST_BATCH_SIZE="100"
export ICEBERG_REST_MAX_BUFFER_MEMORY_MB="10"
```

## AWS S3 Deployment

### Prerequisites

1. **AWS Account** with S3 access
2. **REST Catalog Service** (e.g., Tabular.io, custom implementation)
3. **IAM Permissions** for S3 operations

### IAM Policy Example

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject", 
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-iceberg-warehouse/*",
                "arn:aws:s3:::your-iceberg-warehouse"
            ]
        }
    ]
}
```

### Configuration with Tabular.io

```python
import os
from quixstreams.sinks.community.iceberg_rest import create_s3_rest_config, IcebergRESTSink

# Production configuration
config = create_s3_rest_config(
    catalog_uri="https://tabular.io/api/v1",
    warehouse_id="production",
    table_name="events",
    aws_region="us-east-1",
    
    # Credentials (use IAM roles in production)
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),  # For STS
    
    # Catalog authentication
    catalog_token=os.getenv("TABULAR_TOKEN")
)

# Production-optimized sink
sink = IcebergRESTSink(
    config=config,
    batch_size=1000,             # Larger batches for throughput
    max_buffer_memory_mb=200.0,  # Higher memory limit
    request_timeout=10.0,        # Production timeout
    max_retries=5,               # More retries for reliability
    adaptive_batching=True
)
```

### AWS Environment Variables

```bash
# AWS Credentials (prefer IAM roles)
export AWS_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_SESSION_TOKEN="your-session-token"  # For temporary credentials

# Catalog Configuration
export TABULAR_TOKEN="your-tabular-token"
export ICEBERG_REST_CATALOG_URI="https://tabular.io/api/v1"
export ICEBERG_REST_WAREHOUSE_ID="production"

# Performance Settings
export ICEBERG_REST_BATCH_SIZE="1000"
export ICEBERG_REST_MAX_BUFFER_MEMORY_MB="200"
export ICEBERG_REST_MAX_RETRIES="5"
export ICEBERG_REST_ADAPTIVE_BATCHING="true"
```

### Custom REST Catalog

```python
# For custom REST catalog implementations
config = create_s3_rest_config(
    catalog_uri="https://your-catalog.company.com/api/v1",
    warehouse_id="production",
    table_name="events",
    aws_region="us-east-1",
    
    # Custom authentication
    catalog_token=os.getenv("CUSTOM_CATALOG_TOKEN")
)
```

## Cloudflare R2 Deployment

### Prerequisites

1. **Cloudflare Account** with R2 enabled
2. **R2 API Token** with read/write permissions
3. **REST Catalog Service** compatible with S3 API

### R2 Configuration

```python
import os
from quixstreams.sinks.community.iceberg_rest import create_r2_config, IcebergRESTSink

# R2 configuration
config = create_r2_config(
    account_id="your-cloudflare-account-id",
    table_name="analytics_events",
    catalog_uri="https://catalog.company.com/api/v1",
    warehouse_id="analytics",
    
    # R2 credentials
    access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
    secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
    
    # Catalog authentication
    catalog_token=os.getenv("CATALOG_TOKEN")
)

sink = IcebergRESTSink(
    config=config,
    batch_size=1500,            # R2 handles larger batches well
    max_buffer_memory_mb=150.0,
    adaptive_batching=True
)
```

### R2 Environment Variables

```bash
# Cloudflare R2 Configuration
export CLOUDFLARE_ACCOUNT_ID="your-account-id"
export R2_ACCESS_KEY_ID="your-r2-access-key"
export R2_SECRET_ACCESS_KEY="your-r2-secret-key"

# Catalog Configuration
export CATALOG_TOKEN="your-catalog-token"
export ICEBERG_REST_CATALOG_URI="https://catalog.company.com/api/v1"
export ICEBERG_REST_WAREHOUSE_ID="analytics"

# R2-Optimized Settings
export ICEBERG_REST_BATCH_SIZE="1500"
export ICEBERG_REST_MAX_BUFFER_MEMORY_MB="150"
```

## Environment Variables

### Complete Environment Variable Reference

The sink supports configuration via environment variables with the `ICEBERG_REST_` prefix:

```bash
# Catalog Settings
ICEBERG_REST_CATALOG_URI="https://catalog.example.com/api/v1"
ICEBERG_REST_WAREHOUSE_ID="production"
ICEBERG_REST_CATALOG_TOKEN="your-bearer-token"
ICEBERG_REST_AUTH_TYPE="bearer_token"  # or "none"

# Storage Settings
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
ICEBERG_REST_BACKOFF_FACTOR="0.3"
ICEBERG_REST_ADAPTIVE_BATCHING="true"

# Development/Debugging
ICEBERG_REST_LOG_LEVEL="INFO"  # DEBUG, INFO, WARNING, ERROR
ICEBERG_REST_ENABLE_COMPRESSION="true"
ICEBERG_REST_JSON_ENCODER="auto"  # auto, orjson, ujson, standard
```

### Environment Variable Loading

The sink automatically loads environment variables when configuration objects are created:

```python
from quixstreams.sinks.community.iceberg_rest import load_config_from_env

# Automatically load from environment
config = load_config_from_env(
    table_name="events",
    # Any missing values will be loaded from ICEBERG_REST_* environment variables
)

sink = IcebergRESTSink(config=config)
```

## Configuration Files

### YAML Configuration

Create `iceberg_rest_config.yaml`:

```yaml
# Iceberg REST Sink Configuration
catalog:
  uri: "${ICEBERG_REST_CATALOG_URI:-https://catalog.example.com/api/v1}"
  warehouse_id: "${ICEBERG_REST_WAREHOUSE_ID:-production}"
  auth_type: "${ICEBERG_REST_AUTH_TYPE:-bearer_token}"
  token: "${ICEBERG_REST_CATALOG_TOKEN}"

storage:
  provider: "s3"  # s3, r2, minio
  region: "${ICEBERG_REST_S3_REGION:-us-east-1}"
  endpoint_url: "${ICEBERG_REST_S3_ENDPOINT_URL}"  # null for AWS S3
  credentials:
    access_key_id: "${ICEBERG_REST_S3_ACCESS_KEY_ID}"
    secret_access_key: "${ICEBERG_REST_S3_SECRET_ACCESS_KEY}"
    session_token: "${ICEBERG_REST_S3_SESSION_TOKEN}"

performance:
  batch_size: 1000
  max_buffer_memory_mb: 100
  adaptive_batching: true
  request_timeout: 10.0
  max_retries: 3
  backoff_factor: 0.3

logging:
  level: "INFO"
  enable_debug_metrics: false
```

### JSON Configuration

Create `iceberg_rest_config.json`:

```json
{
    "catalog": {
        "uri": "https://catalog.example.com/api/v1",
        "warehouse_id": "production",
        "auth_type": "bearer_token",
        "token": "${CATALOG_TOKEN}"
    },
    "storage": {
        "provider": "s3",
        "region": "us-east-1",
        "endpoint_url": null,
        "credentials": {
            "access_key_id": "${AWS_ACCESS_KEY_ID}",
            "secret_access_key": "${AWS_SECRET_ACCESS_KEY}",
            "session_token": "${AWS_SESSION_TOKEN}"
        }
    },
    "performance": {
        "batch_size": 1000,
        "max_buffer_memory_mb": 100,
        "adaptive_batching": true,
        "request_timeout": 10.0,
        "max_retries": 3
    }
}
```

### Load Configuration from File

```python
from quixstreams.sinks.community.iceberg_rest import load_config_from_file

# Load from YAML
config = load_config_from_file(
    "iceberg_rest_config.yaml",
    table_name="events"
)

# Load from JSON
config = load_config_from_file(
    "iceberg_rest_config.json", 
    table_name="events"
)

sink = IcebergRESTSink(config=config)
```

## Performance Tuning

### Memory Management

Configure memory limits based on your deployment environment:

```python
# Development (low memory)
sink = IcebergRESTSink(
    config=config,
    max_buffer_memory_mb=10.0,
    batch_size=100
)

# Production (high throughput)
sink = IcebergRESTSink(
    config=config,
    max_buffer_memory_mb=200.0,
    batch_size=2000,
    adaptive_batching=True
)

# Memory-constrained production
import psutil
available_ram_mb = psutil.virtual_memory().available / (1024 * 1024)
memory_limit = min(available_ram_mb * 0.05, 100)  # 5% of RAM, max 100MB

sink = IcebergRESTSink(
    config=config,
    max_buffer_memory_mb=memory_limit,
    adaptive_batching=True
)
```

### Batch Size Guidelines

| Record Size | Recommended Batch Size | Memory Impact |
|-------------|------------------------|---------------|
| Small (<1KB) | 1000-2000 records | Low (~1-2MB) |
| Medium (1-100KB) | 500-1000 records | Medium (~50-100MB) |
| Large (100KB-1MB) | 50-250 records | High (~50-250MB) |
| Very Large (>1MB) | 10-50 records | Very High (>50MB) |

```python
# Auto-configure based on expected record size
def create_optimized_sink(config, expected_record_size_kb):
    if expected_record_size_kb < 1:
        # Small records
        return IcebergRESTSink(
            config=config,
            batch_size=2000,
            max_buffer_memory_mb=50.0,
            adaptive_batching=True
        )
    elif expected_record_size_kb < 100:
        # Medium records  
        return IcebergRESTSink(
            config=config,
            batch_size=1000,
            max_buffer_memory_mb=150.0,
            adaptive_batching=True
        )
    else:
        # Large records
        return IcebergRESTSink(
            config=config,
            batch_size=100,
            max_buffer_memory_mb=300.0,
            adaptive_batching=True
        )
```

### Connection Optimization

```python
# High-throughput configuration
sink = IcebergRESTSink(
    config=config,
    request_timeout=5.0,      # Aggressive timeout for fast networks
    max_retries=3,            # Fewer retries for speed
    backoff_factor=0.1,       # Fast retry for transient issues
    adaptive_batching=True
)

# High-reliability configuration
sink = IcebergRESTSink(
    config=config,
    request_timeout=30.0,     # Patient timeout for slow networks
    max_retries=8,            # More retries for unreliable networks
    backoff_factor=0.5,       # Longer backoff to avoid overwhelming
    adaptive_batching=True
)
```

### JSON Performance

Install performance packages for optimal JSON handling:

```bash
# Best performance (3-10x faster)
pip install orjson>=3.8.0

# Good performance (2-3x faster)
pip install ujson>=5.0.0
```

The sink automatically detects and uses the fastest available JSON library.

## Security Configuration

### Credential Management Best Practices

1. **Never hardcode credentials** in source code
2. **Use environment variables** for sensitive values
3. **Prefer IAM roles** over access keys when possible
4. **Rotate tokens regularly** for better security
5. **Use temporary credentials** (STS) when available

### Authentication Methods

#### Bearer Token Authentication

```python
# Recommended for REST catalogs
config = RESTIcebergConfig(
    catalog_uri="https://catalog.example.com/api/v1",
    table_name="events",
    warehouse_id="prod",
    auth_type="bearer_token",
    catalog_token=os.getenv("CATALOG_TOKEN")  # From environment
)
```

#### AWS IAM Role (Recommended)

```python
# No explicit credentials needed
config = create_s3_rest_config(
    catalog_uri="https://tabular.io/api/v1",
    table_name="events",
    warehouse_id="prod"
    # Credentials automatically loaded from:
    # 1. IAM instance/task role
    # 2. ~/.aws/credentials
    # 3. Environment variables
)
```

#### Temporary Credentials

```python
import boto3

# Generate temporary credentials
sts = boto3.client('sts')
response = sts.assume_role(
    RoleArn='arn:aws:iam::account:role/IcebergWriteRole',
    RoleSessionName='iceberg-sink-session'
)

credentials = response['Credentials']

config = create_s3_rest_config(
    catalog_uri="https://tabular.io/api/v1",
    table_name="events",
    warehouse_id="prod",
    aws_access_key_id=credentials['AccessKeyId'],
    aws_secret_access_key=credentials['SecretAccessKey'],
    aws_session_token=credentials['SessionToken']
)
```

### Network Security

```python
# HTTPS only for production
config = RESTIcebergConfig(
    catalog_uri="https://catalog.example.com/api/v1",  # HTTPS required
    # ... other settings
)

# Custom TLS verification (advanced)
import ssl
import requests

session = requests.Session()
session.verify = "/path/to/custom/ca-bundle.crt"
# Note: Custom session injection not yet supported, future enhancement
```

## Troubleshooting

### Common Configuration Issues

#### 1. Connection Refused

**Error:**
```
NetworkError: Connection refused to http://localhost:8181
```

**Solutions:**
```bash
# Check if catalog service is running
curl http://localhost:8181/v1/config

# Check Docker containers
docker-compose ps

# Check network connectivity
telnet localhost 8181
```

#### 2. Authentication Failed

**Error:**
```
AuthenticationError: 401 Unauthorized
```

**Solutions:**
```python
# Verify token is set
import os
print("Token:", os.getenv("CATALOG_TOKEN"))

# Test token manually
import requests
response = requests.get(
    "https://catalog.example.com/api/v1/config",
    headers={"Authorization": f"Bearer {os.getenv('CATALOG_TOKEN')}"}
)
print(response.status_code, response.text)
```

#### 3. Buffer Memory Exceeded

**Error:**
```
BufferError: Buffer memory limit exceeded: 55.2MB / 50.0MB
```

**Solutions:**
```python
# Increase memory limit
sink = IcebergRESTSink(
    config=config,
    max_buffer_memory_mb=100.0  # Increase limit
)

# Or reduce batch size
sink = IcebergRESTSink(
    config=config,
    batch_size=500,  # Reduce from 1000
    adaptive_batching=True
)

# Monitor memory usage
stats = sink.get_stats()
print(f"Memory: {stats['buffer_memory_mb']:.1f}MB / {stats['max_buffer_memory_mb']:.1f}MB")
```

#### 4. Table Not Found

**Error:**
```
CatalogError: Table 'events' not found in warehouse 'prod'
```

**Solutions:**
```python
# Verify table exists
health = sink.health_check()
print("Health:", health)

# Check warehouse ID
print("Warehouse:", config.warehouse_id)

# List available tables (if catalog supports it)
# Implementation varies by catalog
```

### Debug Configuration

Enable detailed logging for troubleshooting:

```python
import logging

# Enable debug logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Specific logger for the sink
logger = logging.getLogger("quixstreams.sinks.community.iceberg_rest")
logger.setLevel(logging.DEBUG)

# Create sink with debug info
sink = IcebergRESTSink(config=config)

# Check configuration
print("Config:", sink.config)
print("Client:", sink.client)

# Test connectivity
health = sink.health_check()
print("Health:", health)
```

### Configuration Validation

```python
from quixstreams.sinks.community.iceberg_rest import validate_rest_config

try:
    validate_rest_config(config)
    print("Configuration is valid")
except Exception as e:
    print(f"Configuration error: {e}")
```

### Performance Diagnostics

```python
import time

# Monitor performance
start_time = time.time()
sink.write(large_batch_of_records)
end_time = time.time()

stats = sink.get_stats()
print(f"Write time: {end_time - start_time:.2f}s")
print(f"Records/sec: {len(large_batch_of_records) / (end_time - start_time):.0f}")
print(f"Memory usage: {stats['buffer_memory_mb']:.1f}MB")
print(f"Compression ratio: {stats.get('compression_ratio', 'N/A')}")
```

---

This configuration guide provides comprehensive setup instructions for all supported deployment scenarios. For additional examples and troubleshooting, refer to the [API Reference](api_reference.md) and [README](../quixstreams/sinks/community/iceberg_rest/README.md).