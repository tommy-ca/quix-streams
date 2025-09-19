# REST Iceberg Sink Implementation Tasks (Copy Approach)

## Overview
Comprehensive task breakdown for implementing REST catalog support by copying `iceberg.py` to `iceberg-rest.py` and extending it with REST catalog functionality while keeping the original completely untouched.

## Epic Structure
- **Epic Duration**: 4-5 sprints (8-10 weeks)
- **Resource Investment**: 1.5-2 engineers
- **Risk Level**: Low (original code untouched)
- **Approach**: Copy existing production code and extend

---

## 🔴 SPRINT 1: Copy and Core REST Implementation (Week 1-2)

### COPY-001: Create Working Copy of Iceberg Sink
**Story Points**: 2 | **Priority**: Critical | **Dependencies**: None | **Owner**: Senior Developer

#### Description
Create an exact working copy of the production `IcebergSink` implementation as the foundation for REST catalog development.

#### Acceptance Criteria
- [ ] File `quixstreams/sinks/community/iceberg-rest.py` exists as exact copy of `iceberg.py`
- [ ] Original `quixstreams/sinks/community/iceberg.py` remains completely unchanged
- [ ] Copied file can be imported as Python module (rename to `iceberg_rest.py` if needed)
- [ ] All existing AWS Glue functionality works identically in copied file
- [ ] Copy passes all existing unit tests when isolated

#### Technical Tasks
- [ ] Execute: `cp quixstreams/sinks/community/iceberg.py quixstreams/sinks/community/iceberg_rest.py`
- [ ] Update import statements in copied file to avoid conflicts
- [ ] Verify copied file can be imported independently
- [ ] Run existing test suite against copied implementation
- [ ] Document that original file must never be modified

#### Definition of Done
- [ ] Copy exists and is functionally identical to original
- [ ] Original file completely untouched
- [ ] Copy can be imported and used independently
- [ ] All tests pass when using copied implementation

---

### REST-001: Add REST Catalog Configuration Support
**Story Points**: 5 | **Priority**: Critical | **Dependencies**: COPY-001 | **Owner**: Senior Developer

#### Description
Extend the copied sink with REST catalog configuration alongside existing AWS Glue support.

#### Acceptance Criteria
- [ ] `RESTIcebergConfig` class created in copied file extending `BaseIcebergConfig`
- [ ] `data_catalog_spec` supports `Literal["aws_glue", "rest"]`
- [ ] REST configuration handles authentication (none, bearer_token, basic)
- [ ] Configuration validation prevents invalid combinations
- [ ] Existing AWS Glue configuration completely preserved

#### Technical Tasks
```python
# In iceberg_rest.py - ADD these classes
@dataclass
class RESTIcebergConfig(BaseIcebergConfig):
    """Configuration for REST catalog Iceberg sink."""
    
    def __init__(
        self,
        rest_uri: str,
        warehouse_id: str,
        # S3-compatible storage
        endpoint_url: Optional[str] = None,
        aws_region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None, 
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        # REST authentication
        auth_type: Literal["none", "bearer_token", "basic"] = "none",
        auth_token: Optional[str] = None,
        auth_username: Optional[str] = None,
        auth_password: Optional[str] = None,
    ):
        # Configuration implementation
        
# Update DataCatalogSpec to include "rest"
DataCatalogSpec = Literal["aws_glue", "rest"]
```

#### Definition of Done
- [ ] REST configuration class fully functional
- [ ] Both AWS Glue and REST configurations supported
- [ ] Configuration validation works correctly
- [ ] Unit tests cover all configuration scenarios

---

### REST-002: Implement REST Catalog Client Integration  
**Story Points**: 8 | **Priority**: Critical | **Dependencies**: REST-001 | **Owner**: Senior Developer

#### Description
Integrate PyIceberg REST catalog client into the copied sink implementation.

#### Acceptance Criteria
- [ ] REST catalog client initializes correctly with configuration
- [ ] Table creation works with REST catalogs
- [ ] Schema registration and evolution supported
- [ ] Error handling for REST catalog operations
- [ ] Connection retry logic implemented

#### Technical Tasks
```python
# In iceberg_rest.py - MODIFY existing methods
def _import_data_catalog(self):
    """Import and initialize data catalog (AWS Glue or REST)."""
    if self._data_catalog_spec == "aws_glue":
        # Existing AWS Glue code (copied, unchanged)
        from pyiceberg.catalog.glue import GlueCatalog
        return GlueCatalog(**self._iceberg_config.catalog_config())
    
    elif self._data_catalog_spec == "rest":
        # New REST catalog support
        from pyiceberg.catalog.rest import RestCatalog
        
        auth_config = {}
        if self._iceberg_config.auth_type == "bearer_token":
            auth_config["token"] = self._iceberg_config.auth_token
        elif self._iceberg_config.auth_type == "basic":
            auth_config["credential"] = f"{self._iceberg_config.auth_username}:{self._iceberg_config.auth_password}"
            
        return RestCatalog(
            name="rest_catalog",
            uri=self._iceberg_config.rest_uri,
            warehouse=self._iceberg_config.warehouse_id,
            **auth_config
        )
    else:
        raise ValueError(f"Unsupported catalog type: {self._data_catalog_spec}")
```

#### Definition of Done
- [ ] REST catalog client integration complete
- [ ] Table operations work with REST catalogs
- [ ] Error handling robust and informative
- [ ] Integration tests pass with local REST catalog

---

### S3COMPAT-001: Add Generic S3-Compatible Storage Support
**Story Points**: 5 | **Priority**: Critical | **Dependencies**: REST-001 | **Owner**: Senior Developer

#### Description
Extend the copied sink to support generic S3-compatible endpoints (R2, MinIO, AWS S3) through endpoint_url configuration.

#### Acceptance Criteria
- [ ] Custom `endpoint_url` parameter supported in configuration
- [ ] Works with Cloudflare R2, MinIO, and AWS S3 using same API
- [ ] No provider-specific code required
- [ ] Existing S3 functionality preserved when endpoint_url is None
- [ ] Connection validation and error handling

#### Technical Tasks
```python
# In iceberg_rest.py - EXTEND configuration handling
def _setup_s3_client(self):
    """Setup S3 client with custom endpoint support."""
    s3_config = {
        "aws_access_key_id": self._iceberg_config.aws_access_key_id,
        "aws_secret_access_key": self._iceberg_config.aws_secret_access_key,
        "region_name": self._iceberg_config.aws_region,
    }
    
    # Add custom endpoint if specified (for R2, MinIO, etc.)
    if hasattr(self._iceberg_config, 'endpoint_url') and self._iceberg_config.endpoint_url:
        s3_config["endpoint_url"] = self._iceberg_config.endpoint_url
        
    return boto3.client('s3', **s3_config)
```

#### Definition of Done
- [ ] Generic S3-compatible endpoint support working
- [ ] Tested with MinIO, R2, and AWS S3
- [ ] No provider-specific code in implementation
- [ ] Error handling consistent across providers

---

### SCHEMA-001: Verify Schema Management with REST Catalogs
**Story Points**: 3 | **Priority**: Critical | **Dependencies**: REST-002 | **Owner**: Python Developer

#### Description
Ensure existing schema evolution and partition management works with REST catalogs.

#### Acceptance Criteria
- [ ] Default crypto schemas (trades, klines) work with REST catalogs
- [ ] Schema evolution functions correctly
- [ ] Partition specifications compatible with REST metadata
- [ ] Time-series partitioning optimized for crypto data

#### Technical Tasks
- [ ] Test existing schema creation logic with REST catalogs
- [ ] Verify partition pruning works with REST metadata
- [ ] Test schema evolution scenarios
- [ ] Validate time-based partitioning strategies

#### Definition of Done
- [ ] All schema operations work identically with REST catalogs
- [ ] Partition strategies optimized for time-series data
- [ ] Schema evolution tested and working

---

## 🟡 SPRINT 2: Testing and Local Development Stack (Week 3-4)

### LOCAL-001: Create Local Development Stack
**Story Points**: 8 | **Priority**: High | **Dependencies**: REST-002, S3COMPAT-001 | **Owner**: DevOps Engineer + Python Developer

#### Description
Build complete local development stack with Redpanda, Lakekeeper, and MinIO for testing the copied sink.

#### Acceptance Criteria
- [ ] Docker Compose stack with all components
- [ ] Copied sink works with local Lakekeeper + MinIO
- [ ] Health checks and initialization scripts
- [ ] One-command startup for developers
- [ ] Configuration helpers for local development

#### Technical Tasks
- [ ] Create `infra/local-dev-stack/docker-compose.yml`
- [ ] Add Lakekeeper REST catalog service
- [ ] Add MinIO S3-compatible storage
- [ ] Add Redpanda Kafka service
- [ ] Create initialization scripts for buckets and tables
- [ ] Add health check endpoints
- [ ] Create configuration helpers for local setup

#### Components
```yaml
version: '3.8'
services:
  redpanda:
    image: vectorized/redpanda:latest
    ports: ["9092:9092"]
    
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
    
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: lakekeeper
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
```

#### Definition of Done
- [ ] Complete stack starts with `docker-compose up`
- [ ] Copied sink works end-to-end with local stack
- [ ] Developer setup takes <5 minutes
- [ ] All services healthy and interconnected

---

### CONFIG-001: Create Configuration Helpers and Examples
**Story Points**: 3 | **Priority**: High | **Dependencies**: LOCAL-001 | **Owner**: Python Developer

#### Description
Build configuration helpers and comprehensive examples for different deployment scenarios using the copied sink.

#### Acceptance Criteria
- [ ] Factory functions for common configurations
- [ ] Examples for R2, MinIO, and S3 with REST catalogs
- [ ] Local development configuration templates
- [ ] Production deployment examples
- [ ] Clear migration examples from original sink

#### Technical Tasks
```python
# Create configuration helper functions
def create_local_rest_config():
    """Helper for local development with Lakekeeper + MinIO."""
    return RESTIcebergConfig(
        rest_uri="http://localhost:8181",
        warehouse_id="local",
        endpoint_url="http://localhost:9000",
        aws_region="us-east-1",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        auth_type="none"
    )

def create_r2_config(account_id: str, access_key: str, secret_key: str):
    """Helper for Cloudflare R2 deployment."""
    return RESTIcebergConfig(
        rest_uri="https://your-catalog.com/api/v1",
        warehouse_id="main", 
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_region="auto",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        auth_type="bearer_token",
        auth_token=os.environ.get("CATALOG_TOKEN")
    )
```

#### Definition of Done
- [ ] Helper functions simplify common configurations
- [ ] Clear examples for each storage provider
- [ ] Migration path from original sink documented
- [ ] Local development requires minimal configuration

---

### TEST-001: Comprehensive Testing Framework
**Story Points**: 8 | **Priority**: High | **Dependencies**: CONFIG-001 | **Owner**: QA Engineer + Python Developer

#### Description
Implement comprehensive testing for the copied sink covering unit, integration, and performance scenarios.

#### Acceptance Criteria
- [ ] Unit tests for all copied sink functionality (>90% coverage)
- [ ] Integration tests with local stack
- [ ] Performance benchmarks against original sink
- [ ] Cross-storage provider compatibility tests
- [ ] Backward compatibility tests ensuring original sink unaffected

#### Test Structure
```python
# Test structure for copied sink
tests/
├── unit/
│   ├── test_rest_config.py         # REST configuration tests
│   ├── test_rest_catalog.py        # REST catalog client tests
│   └── test_s3_compatible.py       # S3-compatible storage tests
├── integration/
│   ├── test_local_stack.py         # Local development stack tests  
│   ├── test_storage_providers.py   # R2, MinIO, S3 tests
│   └── test_schema_evolution.py    # Schema management tests
└── performance/
    ├── test_throughput.py          # Performance benchmarks
    └── test_compatibility.py       # Original vs copied sink tests
```

#### Definition of Done
- [ ] Test coverage >90% for copied sink functionality
- [ ] All tests pass consistently in CI/CD
- [ ] Performance matches or exceeds original sink
- [ ] Original sink tests continue to pass unchanged

---

## 🟢 SPRINT 3: End-to-End Integration and Storage Testing (Week 5-6)

### STORAGE-001: Multi-Provider Storage Integration Testing
**Story Points**: 8 | **Priority**: Medium | **Dependencies**: TEST-001 | **Owner**: Senior Developer

#### Description
Test and optimize the copied sink with multiple S3-compatible storage providers.

#### Acceptance Criteria
- [ ] End-to-end testing with Cloudflare R2, MinIO, and AWS S3
- [ ] Performance comparison across storage providers
- [ ] Generic optimizations (no provider-specific code)
- [ ] Error handling consistent across providers
- [ ] Configuration examples for each provider

#### Testing Matrix
| Storage Provider | Endpoint | Authentication | Features Tested |
|-----------------|----------|----------------|-----------------|
| AWS S3 | `None` (default) | IAM/Access Keys | Multipart upload, versioning |
| Cloudflare R2 | `https://{account}.r2.cloudflarestorage.com` | R2 API tokens | Custom domains, edge locations |
| MinIO | `http://localhost:9000` | Access/Secret keys | Local development, self-hosted |

#### Definition of Done
- [ ] All storage providers work with identical configuration pattern
- [ ] Performance within 10% across equivalent operations
- [ ] Generic error handling and retry logic
- [ ] Clear documentation for each provider

---

### E2E-001: Complete Crypto Pipeline Integration
**Story Points**: 13 | **Priority**: Medium | **Dependencies**: STORAGE-001 | **Owner**: Python Developer + QA Engineer

#### Description
Implement and test complete end-to-end crypto data pipelines using the copied REST-enabled sink.

#### Acceptance Criteria
- [ ] BinanceS3Source → Copied Sink (REST) pipeline working
- [ ] CCXTSource → Copied Sink (REST) pipeline working  
- [ ] Complete examples for local, R2, and S3 deployments
- [ ] Performance testing with realistic crypto data volumes
- [ ] Schema evolution testing across pipeline restarts

#### Pipeline Examples
```python
# Example: Crypto to Local Stack
from quixstreams import Application
from quixstreams.sources.community.crypto import BinanceS3Source
from quixstreams.sinks.community.iceberg_rest import IcebergSink
from config_helpers import create_local_rest_config

app = Application(broker_address="localhost:9092")

source = BinanceS3Source(
    bucket="binance-public-data", 
    prefix="data/spot/daily/trades/BTCUSDT/",
    unsigned=True
)

sink = IcebergSink(
    table_name="crypto.binance.trades_spot",
    config=create_local_rest_config(),
    data_catalog_spec="rest"
)

sdf = app.dataframe(source=source)
sdf = sdf.apply(lambda x: {**x, "ingest_ts": time.time() * 1000})
sdf.sink(sink)

app.run()
```

#### Definition of Done
- [ ] All crypto sources work with copied sink + REST catalogs
- [ ] Performance matches original sink baseline (within 5%)
- [ ] Schema evolution works across pipeline restarts
- [ ] Production deployment examples available

---

### DOC-001: Documentation and Migration Guides
**Story Points**: 5 | **Priority**: Medium | **Dependencies**: E2E-001 | **Owner**: Technical Writer + Python Developer

#### Description
Create comprehensive documentation for the copied sink and migration guidance.

#### Acceptance Criteria
- [ ] API reference documentation for copied sink
- [ ] Configuration examples for all storage providers
- [ ] Deployment guides for production scenarios
- [ ] Migration guide from original sink to copied sink
- [ ] Troubleshooting guide for common issues

#### Documentation Structure
```
docs/
├── sinks/
│   └── iceberg-rest.md            # Copied sink API reference
├── deployment/
│   ├── s3-compatible.md           # Generic S3-compatible guide  
│   ├── cloudflare-r2.md           # R2 specific deployment
│   ├── minio.md                   # MinIO deployment
│   └── local-development.md       # Local stack guide
├── migration/
│   └── iceberg-to-iceberg-rest.md # Migration guide
└── troubleshooting/
    └── iceberg-rest-issues.md     # Common issues and solutions
```

#### Definition of Done
- [ ] Complete documentation enables self-service onboarding
- [ ] Migration path clearly documented with examples
- [ ] Troubleshooting guide covers 90% of common issues
- [ ] All storage provider scenarios documented

---

## 🔵 SPRINT 4: Advanced Features and Production Readiness (Week 7-8)

### PERF-001: Performance Optimization and Monitoring
**Story Points**: 8 | **Priority**: Low | **Dependencies**: E2E-001 | **Owner**: Senior Developer + DevOps Engineer

#### Description
Implement performance monitoring and optimization for the copied sink across storage providers.

#### Acceptance Criteria
- [ ] Throughput and latency metrics collection
- [ ] Performance comparison dashboards (original vs copied sink)
- [ ] Storage provider performance analysis
- [ ] Generic optimization strategies (no provider-specific code)
- [ ] Integration with monitoring systems

#### Monitoring Metrics
```python
# Performance metrics to track
metrics = {
    "throughput": "records/second",
    "latency_p50": "milliseconds", 
    "latency_p95": "milliseconds",
    "error_rate": "percentage",
    "storage_efficiency": "compression_ratio",
    "catalog_response_time": "milliseconds"
}
```

#### Definition of Done
- [ ] Performance monitoring integrated
- [ ] Optimization delivers measurable improvements
- [ ] Cross-provider performance comparison available
- [ ] Production-grade monitoring and alerting

---

### AUTH-001: Advanced Authentication Features  
**Story Points**: 5 | **Priority**: Low | **Dependencies**: E2E-001 | **Owner**: Senior Developer

#### Description
Implement advanced authentication mechanisms for production REST catalogs.

#### Acceptance Criteria
- [ ] OAuth2 authentication flow support
- [ ] Token refresh and rotation mechanisms
- [ ] Enhanced credential management and security
- [ ] Authentication error handling and retry logic
- [ ] Integration with secret management systems

#### Authentication Methods
```python
# Extended authentication support
class RESTIcebergConfig:
    auth_type: Literal[
        "none",           # Development only
        "bearer_token",   # Simple token auth
        "basic",          # Username/password
        "oauth2",         # OAuth2 flow (new)
        "aws_iam"         # AWS IAM roles (new)
    ]
```

#### Definition of Done
- [ ] Advanced authentication methods implemented
- [ ] Token refresh mechanisms working
- [ ] Production-grade security practices
- [ ] Integration with common secret management systems

---

### DEPLOY-001: Production Deployment Automation
**Story Points**: 8 | **Priority**: Low | **Dependencies**: PERF-001, AUTH-001 | **Owner**: DevOps Engineer

#### Description
Create production deployment automation and infrastructure-as-code for copied sink deployments.

#### Acceptance Criteria
- [ ] Terraform modules for infrastructure setup
- [ ] Kubernetes deployment configurations
- [ ] CI/CD pipeline for copied sink deployment
- [ ] Production configuration validation
- [ ] Rollback and disaster recovery procedures

#### Infrastructure Components
```
infra/
├── terraform/
│   ├── cloudflare-r2/       # R2 bucket and access setup
│   ├── aws-s3/              # S3 bucket with REST catalog
│   └── kubernetes/          # K8s deployment configs  
├── helm/
│   └── iceberg-rest-sink/   # Helm chart for copied sink
└── ci-cd/
    ├── github-actions/      # CI/CD workflows
    └── gitlab-ci/           # GitLab CI configurations
```

#### Definition of Done
- [ ] Automated infrastructure deployment
- [ ] CI/CD pipeline validates and deploys changes
- [ ] Production monitoring and alerting operational
- [ ] Disaster recovery procedures tested

---

## Task Summary

### Total Story Points: 89
- **Sprint 1 (Critical)**: 23 points
- **Sprint 2 (High)**: 19 points  
- **Sprint 3 (Medium)**: 26 points
- **Sprint 4 (Low)**: 21 points

### Key Milestones
1. **Week 2**: Copied sink with basic REST catalog support working
2. **Week 4**: Local development stack and comprehensive testing complete
3. **Week 6**: End-to-end integration and multi-provider storage validated  
4. **Week 8**: Production-ready with advanced features and deployment automation

### Risk Mitigation
- **Original Code Safety**: All development in copied file, original never touched
- **Parallel Development**: No disruption to existing users during development
- **Incremental Validation**: Each sprint builds on validated foundation
- **Comprehensive Testing**: >90% coverage with performance benchmarking

### Resource Requirements
- **Senior Python Developer**: Full-time (core implementation)
- **DevOps Engineer**: 50% time (infrastructure, deployment)
- **QA Engineer**: 25% time (testing, validation)
- **Technical Writer**: 25% time (documentation)

## Success Criteria

### Sprint 1 Success
- [ ] Copied sink exists and works identically to original
- [ ] REST catalog support functional with local Lakekeeper
- [ ] Generic S3-compatible storage working with MinIO
- [ ] Original sink completely untouched and unaffected

### Sprint 2 Success  
- [ ] Local development stack operational
- [ ] Comprehensive testing framework in place
- [ ] Configuration helpers simplify development
- [ ] Performance baseline established

### Sprint 3 Success
- [ ] Multi-provider storage integration complete
- [ ] End-to-end crypto pipelines operational
- [ ] Documentation enables user self-service
- [ ] Migration path from original sink clear

### Sprint 4 Success
- [ ] Production-grade performance and monitoring
- [ ] Advanced authentication for enterprise use
- [ ] Automated deployment capabilities
- [ ] Full feature parity with enhanced capabilities

## Next Actions

1. **Immediate**: Execute COPY-001 to create working copy
2. **Week 1**: Complete REST catalog configuration and client integration
3. **Week 2**: Add S3-compatible storage support and schema validation
4. **Week 3**: Build local development stack and testing framework

This task breakdown provides a comprehensive roadmap for implementing REST catalog support while maintaining complete safety for the existing production sink.