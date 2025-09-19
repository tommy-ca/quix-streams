# REST Catalog Iceberg Sink Implementation Plan

## Strategic Overview

This plan outlines the implementation of REST catalog support for Iceberg sinks by copying the existing production `IcebergSink` to a separate file and extending it with REST catalog capabilities, specifically targeting:

- **Cloudflare R2** with REST catalog support
- **Local development stack**: Lakekeeper + MinIO + Redpanda
- **Production flexibility**: Any REST catalog-compatible deployment with generic S3-compatible storage

## Approach: Copy and Extend Strategy

### Why Copy from AWS Glue Implementation?
1. **Zero Risk**: Original production code remains completely untouched
2. **Proven Foundation**: Start from battle-tested, production-ready codebase
3. **Faster Development**: Leverage existing schema evolution, error handling, and batching logic
4. **Parallel Development**: No interference with production sink during development
5. **Code Reuse**: Minimize duplication while enabling specialized features
6. **Safe Experimentation**: Test and iterate on copy without affecting original

### Implementation Path
```
Current State: iceberg.py (AWS Glue) ✅ Production Ready (UNTOUCHED)
                     ↓
              Copy File to iceberg-rest.py
                     ↓
New Development: iceberg-rest.py → Extended with REST + S3-Compatible
                     ↓  
Future State: Two specialized sinks serving different use cases
```

## Target Architecture

### End-to-End Data Flow
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Crypto Sources │    │   Redpanda      │    │  IcebergSink    │
│  - BinanceS3    │───▶│   Topics        │───▶│ - REST Catalog  │
│  - CCXT         │    │                 │    │ - S3 Compatible │
│  - Cryptofeed   │    │                 │    │ - Lakekeeper    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        ↓
                                              ┌─────────────────┐
                                              │ Iceberg Tables  │
                                              │ - Partitioned   │
                                              │ - Optimized     │
                                              │ - Queryable     │
                                              └─────────────────┘
```

### Deployment Targets

#### 1. Production Stack (S3-Compatible)
```yaml
# Generic S3-Compatible Configuration (works with AWS S3, R2, MinIO, etc.)
storage:
  endpoint_url: https://account-id.r2.cloudflarestorage.com  # or s3.amazonaws.com
  bucket: crypto-lakehouse
  access_key_id: ${ACCESS_KEY}
  secret_access_key: ${SECRET_KEY}
  region: auto  # or us-east-1, etc.
  
catalog:
  type: rest
  uri: https://catalog.yourdomain.com/api/v1
  warehouse: production-warehouse
  auth:
    type: bearer_token
    token: ${CATALOG_TOKEN}
```

#### 2. Local Development Stack  
```yaml
# Local Stack Configuration (MinIO + Lakekeeper)
storage:
  endpoint_url: http://localhost:9000
  bucket: crypto-dev
  access_key_id: minioadmin
  secret_access_key: minioadmin
  region: us-east-1
  
catalog:
  type: rest  
  uri: http://localhost:8181  # Lakekeeper
  warehouse: local-warehouse
  auth:
    type: none  # Development only
```

## Implementation Phases

### Phase 1: Copy and Extend Implementation (Sprint 1-2)
**Duration**: 2-3 weeks | **Priority**: Critical

#### Deliverables
- [ ] Copy `quixstreams/sinks/community/iceberg.py` to `quixstreams/sinks/community/iceberg-rest.py`
- [ ] **DO NOT MODIFY** the original `iceberg.py` file during development
- [ ] Add REST catalog support alongside existing AWS Glue functionality in the copy
- [ ] Extend configuration to support generic S3-compatible endpoints
- [ ] Add REST catalog client integration to the copied implementation
- [ ] Ensure copied version maintains all existing AWS Glue functionality

#### Technical Tasks
```python
# Copy iceberg.py → iceberg-rest.py (or iceberg_rest.py for importability)
# Extended sink implementation in the COPIED file only
class IcebergSink(BatchingSink):  # Same class name in copied file
    """
    Iceberg sink supporting multiple catalog types (in copied file):
    - AWS Glue (copied from original, maintained)
    - REST catalogs (new functionality added to copy)
    
    Storage support:
    - AWS S3 (copied from original)
    - Any S3-compatible storage (new: R2, MinIO, etc.)
    """
    
    def __init__(
        self,
        table_name: str,
        config: Union[AWSIcebergConfig, RESTIcebergConfig],  # Extended union
        data_catalog_spec: Literal["aws_glue", "rest"],     # Extended options
        **kwargs
    ):
        # Implementation builds on copied existing code
```

#### Configuration Schema (extends existing in copied file)
```python
# Extend existing BaseIcebergConfig in the copied iceberg-rest.py file
class RESTIcebergConfig(BaseIcebergConfig):
    def __init__(
        self,
        rest_uri: str,
        warehouse_id: str,
        # Standard S3 parameters (works with any S3-compatible storage)
        endpoint_url: Optional[str] = None,    # Custom endpoint (R2, MinIO, etc.)
        aws_region: Optional[str] = None,      # S3 region
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        # REST-specific auth
        auth_type: Literal["none", "bearer_token", "basic"] = "none",
        auth_token: Optional[str] = None,
        auth_username: Optional[str] = None,
        auth_password: Optional[str] = None,
    ):
        # Use existing location pattern but allow custom endpoints
        location = endpoint_url or "s3://"
        
        super().__init__(location=location, auth={
            "client.region": aws_region,
            "client.access-key-id": aws_access_key_id,
            "client.secret-access-key": aws_secret_access_key,
            "client.session-token": aws_session_token,
            "s3.endpoint": endpoint_url,  # Key difference for S3-compatible
        })
        
        self.rest_uri = rest_uri
        self.warehouse_id = warehouse_id
        self.auth_type = auth_type
        self.auth_token = auth_token
        # ... rest auth config
```

### Phase 2: S3-Compatible Storage Integration (Sprint 2-3)
**Duration**: 2-3 weeks | **Priority**: High

#### Deliverables
- [ ] Generic S3-compatible endpoint configuration
- [ ] End-to-end testing with multiple storage providers (R2, MinIO, AWS S3)
- [ ] Performance benchmarking across storage types
- [ ] Documentation and examples for different S3-compatible providers
- [ ] Configuration helpers for common scenarios

#### S3-Compatible Configuration Pattern
```python
# Generic approach - no provider-specific code
config = RESTIcebergConfig(
    rest_uri="http://localhost:8181",
    warehouse_id="local",
    # Works with any S3-compatible storage
    endpoint_url="https://account-id.r2.cloudflarestorage.com",  # R2
    # endpoint_url="http://localhost:9000",                     # MinIO
    # endpoint_url=None,                                         # AWS S3
    aws_region="auto",  # or "us-east-1" for others
    aws_access_key_id="your-key",
    aws_secret_access_key="your-secret"
)

# No R2-specific classes needed - just documentation examples
```

#### S3-Compatible Integration Features
- [ ] Automatic endpoint detection and configuration
- [ ] Generic multipart upload optimization
- [ ] Storage-agnostic error handling and retry logic
- [ ] Performance monitoring across different storage types
- [ ] Configuration validation for different S3-compatible providers

### Phase 3: Local Development Stack (Sprint 3-4)
**Duration**: 1-2 weeks | **Priority**: High

#### Deliverables
- [ ] Docker Compose stack: Redpanda + Lakekeeper + MinIO
- [ ] Local development configuration templates
- [ ] Integration tests with local stack
- [ ] Developer quickstart documentation
- [ ] CI/CD integration with local stack testing

#### Local Stack Components
```yaml
# docker-compose.dev.yml
version: '3.8'
services:
  redpanda:
    image: vectorized/redpanda:latest
    ports: ["9092:9092", "9644:9644"]
    
  minio:
    image: minio/minio:latest
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    
  lakekeeper:
    image: quay.io/lakekeeper/catalog:latest-main
    ports: ["8181:8181"]
    depends_on: [postgres, minio]
    environment:
      LAKEKEEPER__PG_DATABASE_URL_READ: postgresql://postgres:postgres@postgres:5432/lakekeeper
      LAKEKEEPER__PG_DATABASE_URL_WRITE: postgresql://postgres:postgres@postgres:5432/lakekeeper
      
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: lakekeeper
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
```

### Phase 4: End-to-End Integration (Sprint 4-5)
**Duration**: 2-3 weeks | **Priority**: High

#### Deliverables
- [ ] Complete crypto sources → REST sink pipelines
- [ ] Performance testing with realistic crypto data volumes
- [ ] Schema evolution testing across the pipeline
- [ ] Monitoring and observability integration
- [ ] Production deployment guides

#### Integration Examples
```python
# Complete end-to-end pipeline using the copied and extended sink
from quixstreams import Application
from quixstreams.sources.community.crypto import BinanceS3Source, CCXTSource
# Import from the copied and extended file (may be renamed to iceberg_rest.py)
from quixstreams.sinks.community.iceberg_rest import IcebergSink, RESTIcebergConfig

app = Application(broker_address="localhost:9092")

# Crypto data source
binance_source = BinanceS3Source(
    bucket="binance-public-data",
    prefix="data/spot/daily/trades/BTCUSDT/",
    unsigned=True
)

# REST Iceberg sink with any S3-compatible storage
# Example 1: Cloudflare R2
r2_config = RESTIcebergConfig(
    rest_uri="https://catalog.yourdomain.com/api/v1",
    warehouse_id="main",
    endpoint_url="https://account-id.r2.cloudflarestorage.com",
    aws_region="auto",
    aws_access_key_id=os.environ["CF_R2_TOKEN_ID"],
    aws_secret_access_key=os.environ["CF_R2_TOKEN_SECRET"]
)

# Example 2: Local MinIO
local_config = RESTIcebergConfig(
    rest_uri="http://localhost:8181",
    warehouse_id="local",
    endpoint_url="http://localhost:9000",
    aws_region="us-east-1",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

iceberg_sink = IcebergSink(
    table_name="crypto.binance.trades_spot",
    config=r2_config,  # or local_config
    data_catalog_spec="rest"
)

# Pipeline (same as before)
sdf = app.dataframe(source=binance_source)
sdf = sdf.apply(lambda x: {**x, "ingest_ts": time.time() * 1000})
sdf.sink(iceberg_sink)

app.run()
```

## Updated Task Priority Matrix

### 🔴 CRITICAL (Sprint 1-2) - Copy and Extend Core
| Task ID | Description | Story Points | Dependencies |
|---------|------------|--------------|--------------|
| COPY-001 | Copy iceberg.py to iceberg-rest.py (preserve original) | 2 | None |
| REST-001 | Extend copied sink with REST catalog support | 8 | COPY-001 |
| REST-002 | Add REST catalog client integration | 5 | REST-001 |
| REST-003 | Implement S3-compatible storage layer | 5 | REST-001 |
| REST-004 | Create configuration management | 3 | REST-002, REST-003 |
| REST-005 | Add basic error handling and retries | 5 | REST-001-004 |

### 🟡 HIGH (Sprint 2-3) - Generic S3-Compatible Storage Integration  
| Task ID | Description | Story Points | Dependencies |
|---------|------------|--------------|--------------|
| S3COMPAT-001 | Generic S3-compatible endpoint support | 5 | REST-003 |
| S3COMPAT-002 | Generic S3 authentication (works with R2, MinIO, S3) | 3 | S3COMPAT-001 |
| S3COMPAT-003 | Generic multipart upload optimization | 5 | S3COMPAT-001, S3COMPAT-002 |
| S3COMPAT-004 | End-to-end testing with R2, MinIO, and S3 | 8 | S3COMPAT-001-003 |
| S3COMPAT-005 | Performance benchmarking across providers | 5 | S3COMPAT-004 |

### 🟢 MEDIUM (Sprint 3-4) - Local Development Stack
| Task ID | Description | Story Points | Dependencies |
|---------|------------|--------------|--------------|
| LOCAL-001 | Create Docker Compose dev stack | 5 | REST-001-005 |
| LOCAL-002 | Local stack configuration templates | 3 | LOCAL-001 |
| LOCAL-003 | Integration testing with local stack | 5 | LOCAL-001, LOCAL-002 |
| LOCAL-004 | Developer documentation and guides | 5 | LOCAL-001-003 |
| LOCAL-005 | CI/CD integration for local testing | 8 | LOCAL-001-004 |

### 🔵 LOW (Sprint 4-5) - E2E Integration & Optimization
| Task ID | Description | Story Points | Dependencies |
|---------|------------|--------------|--------------|
| E2E-001 | Complete pipeline integration tests | 13 | All previous |
| E2E-002 | Performance testing and optimization | 8 | E2E-001 |
| E2E-003 | Monitoring and observability | 8 | E2E-001 |
| E2E-004 | Production deployment guides | 5 | E2E-001-003 |
| E2E-005 | Advanced features (compaction, etc.) | 13 | E2E-001-004 |

## File Structure

```
quixstreams/
├── sinks/
│   ├── community/
│   │   ├── iceberg.py              # Original AWS Glue sink (UNTOUCHED)
│   │   └── iceberg-rest.py         # Copied and extended with REST support
│   └── core/
├── examples/
│   ├── crypto_to_iceberg_s3_compatible.py  # Generic S3-compatible example
│   ├── crypto_to_iceberg_local.py          # Local stack example
│   └── crypto_pipeline_complete.py         # End-to-end example
├── infra/
│   ├── local-dev-stack/
│   │   ├── docker-compose.yml      # Complete local stack
│   │   ├── redpanda/              # Redpanda config
│   │   ├── lakekeeper/            # Lakekeeper config  
│   │   └── minio/                 # MinIO config
│   └── s3-compatible-examples/
│       ├── cloudflare-r2/         # R2 configuration examples
│       ├── minio/                 # MinIO examples
│       └── aws-s3/                # AWS S3 with REST catalog examples
└── docs/
    ├── sinks/
    │   └── iceberg-extended.md     # Extended sink documentation
    ├── deployment/
    │   ├── s3-compatible.md        # Generic S3-compatible guide
    │   └── local-development.md    # Local stack guide
    └── migration/
        └── aws-glue-to-rest.md     # Migration guide
```

## Testing Strategy

### Unit Testing
- [ ] REST catalog client operations
- [ ] S3-compatible storage operations  
- [ ] Configuration validation
- [ ] Error handling scenarios
- [ ] Schema evolution operations

### Integration Testing
- [ ] Local stack end-to-end tests
- [ ] Cloudflare R2 integration tests
- [ ] Performance tests with realistic data volumes
- [ ] Failure recovery and retry tests
- [ ] Schema evolution integration tests

### Performance Testing
- [ ] Throughput benchmarks (records/second)
- [ ] Latency measurements (end-to-end)
- [ ] Storage efficiency tests (compression ratios)
- [ ] Query performance tests (partition pruning)
- [ ] Cost optimization validation

## Success Metrics

### Phase 1 Success Criteria
- [ ] Successfully copy iceberg.py to iceberg-rest.py without modifying original
- [ ] Extended sink creates tables successfully with REST catalogs
- [ ] Basic data ingestion works with local Lakekeeper
- [ ] Configuration system handles all required parameters
- [ ] Error handling covers common failure scenarios
- [ ] Original iceberg.py functionality completely preserved

### Phase 2 Success Criteria
- [ ] Generic S3-compatible storage works with R2, MinIO, and AWS S3
- [ ] Performance meets or exceeds original AWS Glue sink baseline
- [ ] Cost-effective storage utilization across all S3-compatible providers
- [ ] Production-ready error handling and monitoring

### Phase 3 Success Criteria
- [ ] Complete local development environment in <5 minutes setup
- [ ] All integration tests pass with local stack
- [ ] Developer documentation enables rapid onboarding
- [ ] CI/CD pipeline validates changes automatically

### Phase 4 Success Criteria
- [ ] End-to-end crypto data pipelines operational
- [ ] Performance targets met (>10k records/sec)
- [ ] Production deployment successfully completed
- [ ] Monitoring and alerting fully operational

## Risk Mitigation

### High Risk Items
1. **Cloudflare R2 API Compatibility**: Extensive testing with R2-specific features
2. **Performance Differences**: Benchmark against AWS Glue implementation
3. **REST Catalog Reliability**: Implement robust retry and fallback mechanisms

### Medium Risk Items
1. **Configuration Complexity**: Provide comprehensive examples and validation
2. **Local Stack Stability**: Use stable versions and health checks
3. **Schema Evolution**: Test extensively with real crypto data patterns

## Resource Requirements

### Development Team (4-5 Sprints)
- **Senior Python Developer**: Full-time (REST sink implementation)
- **DevOps Engineer**: Part-time (Infrastructure, Docker, deployment)
- **QA Engineer**: Part-time (Testing, validation)

### Infrastructure Requirements
- **Cloudflare R2 Account**: For integration testing and validation
- **Development Environment**: Local Docker stack for all developers
- **CI/CD Resources**: Automated testing with both local and cloud stacks

## Migration Strategy

### Current Users (AWS Glue)
- **Zero Impact**: Original `iceberg.py` file completely untouched
- **Optional Migration**: Users can switch imports when ready (`iceberg` → `iceberg_rest`)
- **Feature Parity**: Ensure copied sink maintains all AWS Glue capabilities plus REST

### New Users
- **Clear Documentation**: Guide users to appropriate sink choice
- **Decision Matrix**: Help users choose between AWS Glue and REST
- **Migration Tools**: Provide utilities for future migrations if needed

## Conclusion

This plan enables rapid development of REST catalog support for Iceberg sinks while ensuring zero risk to existing implementations. The copy-based approach provides complete safety - the original production code remains untouched while we develop and test REST catalog functionality in parallel. The focus on generic S3-compatible storage (R2, MinIO, AWS S3) and local development stacks provides immediate value for both production and development use cases, establishing Quix Streams as a leader in cloud-agnostic lakehouse solutions.

**Key Benefits of Copy Approach:**
- **Zero Risk**: Original production code completely untouched
- **Parallel Development**: No interference with existing functionality
- **Safe Experimentation**: Test and iterate on copy without affecting users
- **Clear Migration Path**: Simple import statement change when ready
- **Code Reuse**: Leverage proven implementation as foundation

**Next Steps**: Begin Phase 1 with COPY-001 (copy iceberg.py to iceberg-rest.py) followed by REST catalog integration in the copied file.
