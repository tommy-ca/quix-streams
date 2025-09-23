# Local Development Stack

## Overview

This Docker Compose stack provides a complete local development environment for testing crypto sources with Iceberg REST catalog sinks. It includes:

- **Redpanda**: Kafka-compatible streaming platform
- **Lakekeeper**: REST catalog for Iceberg tables
- **MinIO**: S3-compatible object storage
- **PostgreSQL**: Database backend for Lakekeeper
- **Redpanda Console**: Web UI for Kafka management

## Quick Start

### 1. Start the Stack
```bash
cd infra/local-dev-stack
docker-compose up -d
```

### 2. Wait for Services to Start
```bash
# Check all services are healthy
docker-compose ps

# Watch the logs
docker-compose logs -f lakekeeper-bootstrap
```

### 3. Verify Setup
```bash
# Test Lakekeeper REST catalog
curl -s http://localhost:8181/management/v1/health | jq

# Test MinIO S3 API  
curl -s http://localhost:9000/minio/health/live

# Test Redpanda
rpk cluster info --brokers localhost:19092
```

## Service Endpoints

| Service | Endpoint | Purpose |
|---------|----------|---------|
| **Redpanda (Kafka)** | `localhost:19092` | Kafka broker |
| **Schema Registry** | `localhost:18081` | Avro/JSON schema registry |
| **Redpanda Console** | `http://localhost:8080` | Kafka web UI |
| **Lakekeeper** | `http://localhost:8181` | REST catalog API |
| **MinIO Console** | `http://localhost:9001` | S3 storage web UI |
| **MinIO S3 API** | `http://localhost:9000` | S3-compatible API |
| **PostgreSQL** | `localhost:5432` | Database (lakekeeper/postgres/postgres) |

## Default Credentials

### MinIO
- **Access Key**: `minioadmin`
- **Secret Key**: `minioadmin`
- **Buckets**: `crypto-dev`, `lakehouse`

### PostgreSQL
- **Database**: `lakekeeper`
- **Username**: `postgres` 
- **Password**: `postgres`

### Lakekeeper
- **Warehouse**: `local`
- **Authentication**: None (development only)

## Usage Examples

### Basic Crypto Pipeline
```python
from quixstreams import Application
from quixstreams.sources.community.crypto import BinanceS3Source
from quixstreams.sinks.community.iceberg_rest import LocalIcebergSink

app = Application(broker_address="localhost:19092")

# Source: Binance S3 historical data
source = BinanceS3Source(
    bucket="binance-public-data",
    prefix="data/spot/daily/trades/BTCUSDT/2024-01-01/",
    unsigned=True
)

# Sink: Local Iceberg with REST catalog
sink = LocalIcebergSink(
    table_name="crypto.binance.trades_spot",
    bucket="crypto-dev"
)

# Pipeline
sdf = app.dataframe(source=source)
sdf = sdf.apply(lambda x: {**x, "ingest_ts": time.time() * 1000})
sdf.sink(sink)

app.run()
```

### Manual Table Creation
```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import StringType, TimestampType, DoubleType

# Connect to local REST catalog
catalog = load_catalog("local", **{
    "type": "rest",
    "uri": "http://localhost:8181",
    "warehouse": "local"
})

# Create namespace
catalog.create_namespace(("crypto", "binance"))

# Define schema
schema = Schema([
    (1, "exchange", StringType(), True),
    (2, "symbol", StringType(), True),
    (3, "price", DoubleType(), True),
    (4, "ts_event", TimestampType(), True),
])

# Create table
table = catalog.create_table(
    identifier=("crypto", "binance", "trades"),
    schema=schema
)

print(f"Created table: {table}")
```

### Query Data with DuckDB
```python
import duckdb

# Connect to MinIO via DuckDB
conn = duckdb.connect()

# Install S3 extension
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")

# Configure S3 credentials for MinIO
conn.execute("""
    SET s3_region='us-east-1';
    SET s3_url_style='path';
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='minioadmin';
    SET s3_secret_access_key='minioadmin';
    SET s3_use_ssl=false;
""")

# Query Iceberg table via S3
result = conn.execute("""
    SELECT * FROM 's3://lakehouse/warehouse/crypto/binance/trades/*/*.parquet'
    ORDER BY ts_event DESC 
    LIMIT 10;
""").fetchall()

print(result)
```

## Troubleshooting

### Services Won't Start
```bash
# Check Docker resources
docker system df

# Clean up if needed
docker-compose down -v
docker system prune -f

# Restart services
docker-compose up -d
```

### Lakekeeper Bootstrap Failed
```bash
# Check bootstrap logs
docker-compose logs lakekeeper-bootstrap

# Manually bootstrap
curl -X POST http://localhost:8181/management/v1/bootstrap \
  -H 'Content-Type: application/json' \
  -d '{"accept-terms-of-use": true}'
```

### MinIO Bucket Issues
```bash
# Access MinIO container
docker exec -it quix-minio mc ls minio/

# Recreate buckets
docker exec -it quix-minio mc mb minio/crypto-dev
docker exec -it quix-minio mc mb minio/lakehouse
```

### Redpanda Connection Issues
```bash
# Check cluster health
rpk cluster info --brokers localhost:19092

# Create test topic
rpk topic create test-topic --brokers localhost:19092

# Produce test message
echo "test message" | rpk topic produce test-topic --brokers localhost:19092

# Consume test message  
rpk topic consume test-topic --brokers localhost:19092 --offset start --num 1
```

## Development Workflow

### 1. Code Development
- Edit source code in your IDE
- Use the local stack for testing
- Services are available at standard ports

### 2. Testing Changes
- Run unit tests against local services
- Test end-to-end pipelines
- Validate data in MinIO console

### 3. Debugging
- Check service logs: `docker-compose logs <service>`
- Access service containers: `docker exec -it <container> bash`
- Use web consoles for visual debugging

### 4. Cleanup
```bash
# Stop services
docker-compose down

# Remove volumes (full reset)
docker-compose down -v

# Remove images (full cleanup)
docker-compose down --rmi all -v
```

## Performance Tuning

### For Development
The default configuration is optimized for development with minimal resource usage:
- Single Redpanda node
- Minimal JVM settings
- Small buffer sizes

### For Load Testing
Edit `docker-compose.yml` to increase resources:
- Add more Redpanda brokers
- Increase memory limits
- Tune Kafka settings for throughput

## Security Notes

⚠️ **This stack is for DEVELOPMENT ONLY**

- No authentication/authorization configured
- Default passwords used
- Services exposed on host network
- Not suitable for production use

For production deployments, configure:
- Strong passwords and secrets
- Network isolation
- TLS encryption
- Access controls

## Next Steps

1. **Start Development**: Use this stack for crypto sources development
2. **Add Tests**: Create integration tests using these services  
3. **Extend Stack**: Add monitoring (Grafana, Prometheus) if needed
4. **Production Setup**: Configure secure production deployment