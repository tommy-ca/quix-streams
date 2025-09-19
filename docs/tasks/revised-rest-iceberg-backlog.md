# REST Iceberg Sink Task Backlog (Revised - Fork Approach)

## Epic Overview
Development backlog for extending the existing `IcebergSink` to support REST catalogs and generic S3-compatible storage, while maintaining full backward compatibility with AWS Glue.

## Strategic Context
- **Approach**: Fork and extend existing AWS Glue IcebergSink (preserve 100% compatibility)
- **Target Deployments**: Any S3-compatible storage (R2, MinIO, AWS S3) + REST catalogs
- **Timeline**: 4-5 sprints (8-10 weeks)  
- **Resource Investment**: 1.5-2 engineers
- **Key Benefit**: Leverage proven, production-ready codebase as foundation

---

## 🔴 CRITICAL PRIORITY (Sprint 1) - Fork and Extend

### FORK-001: Fork IcebergSink and Add REST Catalog Support
**Story Points**: 8 | **Priority**: Critical | **Dependencies**: None | **Owner**: Senior Developer

#### Description
Create a separate working copy of the production `IcebergSink` implementation and extend it to support REST catalogs alongside AWS Glue, while keeping the original file untouched.

- Copy `quixstreams/sinks/community/iceberg.py` to `quixstreams/sinks/community/iceberg-rest.py` as the starting point
- Do NOT modify the original `iceberg.py` during development

Note: If this module needs to be imported as Python code during development, consider renaming to `iceberg_rest.py` (underscores are valid in module names; hyphens are not). The file name in this plan follows the requested `iceberg-rest.py` copy action.

#### Acceptance Criteria
- [ ] New file `quixstreams/sinks/community/iceberg-rest.py` exists as an exact initial copy of `iceberg.py`
- [ ] Original `quixstreams/sinks/community/iceberg.py` remains unchanged
- [ ] Add `RESTIcebergConfig` class extending `BaseIcebergConfig`
- [ ] Extend `data_catalog_spec` to support `Literal["aws_glue", "rest"]`
- [ ] REST catalog client setup with PyIceberg
- [ ] All existing AWS Glue tests (targeting original `iceberg.py`) continue to pass
- [ ] New REST catalog functionality in `iceberg-rest.py` tested

#### Technical Tasks
- [ ] Copy `iceberg.py` to `iceberg-rest.py` as the foundation
- [ ] Create `RESTIcebergConfig` extending `BaseIcebergConfig` pattern
- [ ] Add "rest" to `DataCatalogSpec` union type
- [ ] Implement REST catalog factory in `_import_data_catalog()`
- [ ] Add REST catalog initialization in existing setup flow
- [ ] Implement REST-specific authentication (token, basic auth)
- [ ] Preserve all existing AWS Glue code paths unchanged in both files

#### Definition of Done
- [ ] All existing AWS Glue functionality in original `iceberg.py` works exactly as before
- [ ] New REST catalog support functional with local Lakekeeper in `iceberg-rest.py`
- [ ] Unit tests >90% coverage for new REST functionality
- [ ] Integration test passing with local stack
- [ ] Code review approved

---

### S3COMPAT-001: Add S3-Compatible Endpoint Support
**Story Points**: 5 | **Priority**: Critical | **Dependencies**: FORK-001 | **Owner**: Senior Developer

#### Description
Extend the existing S3 storage integration to support custom endpoints for any S3-compatible storage provider (R2, MinIO, etc.).

#### Acceptance Criteria
- [ ] Generic S3-compatible endpoint configuration via `endpoint_url` parameter
- [ ] Works with Cloudflare R2, MinIO, and AWS S3 using same configuration pattern
- [ ] Leverage existing boto3 S3 client with custom endpoint support
- [ ] No provider-specific code or classes needed
- [ ] Comprehensive error handling for different storage providers

#### Technical Tasks
- [ ] Extend RESTIcebergConfig to accept endpoint_url parameter
- [ ] Configure boto3 S3 client with custom endpoints when provided
- [ ] Add endpoint validation and connection testing
- [ ] Update existing multipart upload logic for S3-compatible endpoints
- [ ] Add storage provider detection for optimized error handling
- [ ] Maintain existing S3 behavior when endpoint_url is None

#### Definition of Done
- [ ] MinIO working: endpoint_url="http://localhost:9000"
- [ ] Cloudflare R2 working: endpoint_url="https://account.r2.cloudflarestorage.com"
- [ ] AWS S3 working: endpoint_url=None (existing behavior preserved)
- [ ] Performance tests showing equivalent throughput across providers
- [ ] Error handling provides consistent behavior

---

### SCHEMA-001: Extend Schema Management for REST Catalogs
**Story Points**: 3 | **Priority**: Critical | **Dependencies**: FORK-001 | **Owner**: Python Developer

#### Description
Adapt existing schema management and partitioning to work with REST catalogs while preserving AWS Glue functionality.

#### Acceptance Criteria
- [ ] Existing schema evolution logic works with REST catalogs
- [ ] Default crypto schemas (trades, klines) work with both catalog types
- [ ] Partition specifications work with REST catalog metadata
- [ ] Schema compatibility validation works across catalog types

#### Technical Tasks
- [ ] Test existing schema evolution with REST catalogs
- [ ] Ensure default partition specs work with PyIceberg REST client
- [ ] Add REST catalog-specific schema validation if needed
- [ ] Maintain schema evolution compatibility between catalog types

#### Definition of Done
- [ ] Schema evolution works identically for AWS Glue and REST catalogs
- [ ] Default crypto schemas create successfully in both catalog types
- [ ] Partitioning strategies work optimally for time-series data
- [ ] Schema compatibility maintained across catalog migrations

---

## 🟡 HIGH PRIORITY (Sprint 2) - Testing & Integration

### STORAGE-001: S3-Compatible Storage Testing and Optimization
**Story Points**: 8 | **Priority**: High | **Dependencies**: S3COMPAT-001 | **Owner**: Senior Developer

#### Description
Test and optimize the generic S3-compatible storage integration across multiple providers.

#### Acceptance Criteria
- [ ] End-to-end testing with Cloudflare R2, MinIO, and AWS S3
- [ ] Performance benchmarking across all storage providers
- [ ] Provider-agnostic optimizations (no provider-specific code)
- [ ] Configuration examples and documentation for each provider
- [ ] Generic error handling that works across providers

#### Technical Tasks
- [ ] Create integration tests for R2, MinIO, and S3 storage
- [ ] Benchmark upload/download performance across providers
- [ ] Optimize multipart upload thresholds generically
- [ ] Add provider-agnostic error classification and retry logic
- [ ] Create configuration examples for each storage type
- [ ] Add connection validation and health checks

#### Definition of Done
- [ ] All storage providers working with same configuration pattern
- [ ] Performance within 10% across providers for equivalent operations
- [ ] Error handling provides consistent behavior regardless of provider
- [ ] Clear documentation with examples for R2, MinIO, and S3

---

### LOCAL-001: Local Development Stack Setup
**Story Points**: 8 | **Priority**: High | **Dependencies**: SCHEMA-001 | **Owner**: DevOps Engineer + Python Developer

#### Description
Create complete local development stack with Redpanda, Lakekeeper, and MinIO using the new `iceberg-rest.py` sink copy (leaving the original `iceberg.py` untouched).

#### Acceptance Criteria
- [ ] Docker Compose stack with all components
- [ ] Extended IcebergSink works with local Lakekeeper + MinIO
- [ ] Health checks for all components
- [ ] Quick start documentation
- [ ] Configuration helpers for local development

#### Technical Tasks
- [ ] Use existing Docker Compose stack from infra/local-dev-stack
- [ ] Create configuration helpers for local REST + MinIO setup
- [ ] Add integration tests using local stack
- [ ] Create initialization scripts for tables and buckets
- [ ] Document local development workflow

#### Definition of Done
- [ ] Complete stack starts with single command
- [ ] Extended IcebergSink working with local REST catalog
- [ ] End-to-end test from crypto source to local Iceberg successful
- [ ] Developer documentation enables <5 minute setup

---

### CONFIG-001: Configuration Helpers and Examples
**Story Points**: 3 | **Priority**: High | **Dependencies**: STORAGE-001, LOCAL-001 | **Owner**: Python Developer

#### Description
Create configuration helpers and comprehensive examples for different deployment scenarios.

#### Acceptance Criteria
- [ ] Configuration factory functions for common scenarios
- [ ] Examples for R2, MinIO, and S3 with REST catalogs
- [ ] Local development configuration helpers
- [ ] Production deployment examples
- [ ] Migration examples from AWS Glue to REST

#### Technical Tasks
- [ ] Create `create_rest_config()` helper functions
- [ ] Add configuration examples for each storage provider
- [ ] Create local development configuration templates
- [ ] Add production deployment configuration examples
- [ ] Document migration patterns from AWS Glue to REST

#### Definition of Done
- [ ] Each storage provider has clear configuration examples
- [ ] Local development requires minimal configuration
- [ ] Production examples cover common deployment patterns
- [ ] Migration guidance available for existing AWS Glue users

---

## 🟢 MEDIUM PRIORITY (Sprint 3) - End-to-End Integration

### E2E-001: Complete Crypto Pipeline Integration
**Story Points**: 13 | **Priority**: Medium | **Dependencies**: CONFIG-001 | **Owner**: Python Developer + QA Engineer

#### Description
Implement complete end-to-end pipelines from crypto sources to the REST-enabled sink in `iceberg-rest.py` with REST catalogs.

#### Acceptance Criteria
- [ ] BinanceS3Source → IcebergSink(REST) pipeline working
- [ ] CCXTSource → IcebergSink(REST) pipeline working
- [ ] Complete examples for local, R2, and S3 deployments
- [ ] Performance testing with realistic crypto data volumes
- [ ] Schema evolution testing across pipeline restarts

#### Technical Tasks
- [ ] Create end-to-end integration examples using extended IcebergSink
- [ ] Implement performance benchmarks against existing AWS Glue baseline
- [ ] Add schema evolution integration tests
- [ ] Create failure recovery test scenarios
- [ ] Document best practices and deployment patterns

#### Definition of Done
- [ ] All crypto sources work with extended IcebergSink + REST catalogs
- [ ] Performance matches AWS Glue baseline (within 5%)
- [ ] Failure recovery scenarios tested and documented
- [ ] Production deployment guides available

---

### TEST-001: Comprehensive Testing Suite
**Story Points**: 8 | **Priority**: Medium | **Dependencies**: E2E-001 | **Owner**: QA Engineer + Python Developer

#### Description
Implement comprehensive testing covering unit, integration, and performance scenarios for the REST-enabled sink in `iceberg-rest.py` while ensuring the original `iceberg.py` remains fully backward compatible.

#### Acceptance Criteria
- [ ] Unit tests for all extended sink components (>90% coverage)
- [ ] Integration tests with local stack and multiple storage providers
- [ ] Performance benchmarks and regression tests
- [ ] Backward compatibility tests for AWS Glue functionality
- [ ] CI/CD pipeline integration

#### Technical Tasks
- [ ] Expand unit test coverage for REST catalog functionality
- [ ] Create integration test suite covering all storage providers
- [ ] Implement performance benchmark tests with regression detection
- [ ] Add comprehensive backward compatibility tests
- [ ] Integrate all tests into CI/CD pipeline

#### Definition of Done
- [ ] Test coverage meets quality standards (>90%)
- [ ] All tests pass consistently in CI/CD
- [ ] Performance regression detection working
- [ ] Backward compatibility guaranteed for AWS Glue users

---

### DOC-001: Documentation and Migration Guides
**Story Points**: 5 | **Priority**: Medium | **Dependencies**: E2E-001 | **Owner**: Technical Writer + Python Developer

#### Description
Create comprehensive documentation for the extended IcebergSink functionality.

#### Acceptance Criteria
- [ ] Updated API reference documentation
- [ ] Configuration examples for all supported scenarios
- [ ] Deployment guides for different storage providers
- [ ] Migration guide from AWS Glue to REST catalogs
- [ ] Best practices and troubleshooting guides

#### Technical Tasks
- [ ] Update existing API reference with REST catalog options
- [ ] Create configuration examples for each storage provider
- [ ] Write deployment guides for production scenarios
- [ ] Document migration strategies from AWS Glue to REST
- [ ] Create troubleshooting guide covering common issues

#### Definition of Done
- [ ] Documentation enables rapid user onboarding
- [ ] All storage provider scenarios covered with examples
- [ ] Migration path from AWS Glue clearly documented
- [ ] Troubleshooting guide covers most common issues

---

## 🔵 LOW PRIORITY (Sprint 4) - Advanced Features

### AUTH-001: Advanced Authentication Support
**Story Points**: 5 | **Priority**: Low | **Dependencies**: E2E-001 | **Owner**: Senior Developer

#### Description
Implement advanced authentication mechanisms for production REST catalogs.

#### Acceptance Criteria
- [ ] OAuth2 authentication support
- [ ] Enhanced basic authentication with credential management
- [ ] Token refresh mechanisms
- [ ] Authentication error handling and retry

#### Technical Tasks
- [ ] Implement OAuth2 authentication flow for REST catalogs
- [ ] Add token refresh and rotation mechanisms
- [ ] Enhance authentication error handling
- [ ] Add authentication configuration validation

---

### MONITOR-001: Performance Monitoring and Metrics
**Story Points**: 8 | **Priority**: Low | **Dependencies**: E2E-001 | **Owner**: DevOps Engineer + Python Developer

#### Description
Add comprehensive performance monitoring and metrics collection for extended functionality.

#### Acceptance Criteria
- [ ] Throughput and latency metrics for both catalog types
- [ ] Error rate and retry metrics
- [ ] Storage provider performance comparison
- [ ] Integration with monitoring systems (Prometheus, etc.)

#### Technical Tasks
- [ ] Implement metrics collection framework
- [ ] Add performance counters comparing AWS Glue vs REST performance
- [ ] Create monitoring dashboards
- [ ] Integrate with alerting systems

---

## File Structure (Updated)

```
quixstreams/
├── sinks/
│   ├── community/
│   │   ├── iceberg.py              # Original: AWS Glue-only (unchanged)
│   │   └── iceberg-rest.py         # New copy: REST-enabled development (do not modify original)
│   └── core/
├── examples/
│   ├── crypto_to_iceberg_s3_compatible.py  # Generic S3-compatible example
│   ├── crypto_to_iceberg_local.py          # Local stack example  
│   └── crypto_pipeline_complete.py         # End-to-end example
├── infra/
│   ├── local-dev-stack/            # Existing Docker Compose stack
│   └── s3-compatible-examples/
│       ├── cloudflare-r2/          # R2 configuration examples
│       ├── minio/                  # MinIO examples
│       └── aws-s3/                 # AWS S3 with REST catalog examples
└── docs/
    ├── sinks/
    │   └── iceberg-extended.md     # Updated documentation
    ├── deployment/
    │   ├── s3-compatible.md        # Generic S3-compatible guide
    │   └── local-development.md    # Local stack guide
    └── migration/
        └── aws-glue-to-rest.md     # Migration guide
```

Note: For importability during development, `iceberg-rest.py` may be renamed to `iceberg_rest.py` in a subsequent step if needed, since Python modules cannot have hyphens in filenames.

## Success Metrics

### Sprint 1 Success Criteria
- [ ] Extended IcebergSink supports both AWS Glue and REST catalogs
- [ ] 100% backward compatibility with existing AWS Glue usage
- [ ] S3-compatible storage working with R2, MinIO, and S3
- [ ] Unit test coverage >90% for new functionality

### Sprint 2 Success Criteria
- [ ] All storage providers (R2, MinIO, S3) working with same API
- [ ] Local development stack functional with extended sink
- [ ] Performance equivalent across storage providers
- [ ] Configuration examples available for all scenarios

### Sprint 3 Success Criteria
- [ ] End-to-end crypto pipelines working with REST catalogs
- [ ] Performance matches AWS Glue baseline
- [ ] Comprehensive testing suite validates all scenarios
- [ ] Migration documentation enables smooth transitions

### Sprint 4 Success Criteria
- [ ] Advanced authentication provides production-grade security
- [ ] Monitoring enables operational visibility
- [ ] Performance optimization delivers measurable improvements
- [ ] Feature set complete and production-ready

## Key Benefits of Fork Approach

1. **Proven Foundation**: Start with battle-tested, production-ready code
2. **Risk Mitigation**: Existing AWS Glue users unaffected
3. **Faster Development**: Leverage existing patterns and infrastructure
4. **Consistent API**: Same familiar interface with extended capabilities
5. **Easier Migration**: Clear upgrade path from AWS Glue to REST catalogs
6. **Code Reuse**: Minimize duplication while enabling new capabilities

## Configuration Examples

### AWS Glue (Existing - Unchanged)
```python
from quixstreams.sinks.community.iceberg import IcebergSink, AWSIcebergConfig

config = AWSIcebergConfig(
    aws_s3_uri="s3://my-bucket/warehouse/",
    aws_region="us-east-1"
)

sink = IcebergSink(
    table_name="glue.crypto_trades",
    config=config,
    data_catalog_spec="aws_glue"  # Existing
)
```

### REST Catalog with Cloudflare R2 (New)
```python  
# Note: Import from iceberg-rest.py copy (may be renamed to iceberg_rest.py for importability)
from quixstreams.sinks.community.iceberg_rest import IcebergSink, RESTIcebergConfig

config = RESTIcebergConfig(
    rest_uri="https://catalog.example.com/api/v1",
    warehouse_id="main",
    endpoint_url="https://account-id.r2.cloudflarestorage.com",
    aws_region="auto",
    aws_access_key_id="your-r2-token-id",
    aws_secret_access_key="your-r2-token-secret"
)

sink = IcebergSink(
    table_name="crypto.trades",
    config=config,
    data_catalog_spec="rest"  # New
)
```

### Local Development (New)
```python
from quixstreams.sinks.community.iceberg_rest import IcebergSink, RESTIcebergConfig

# Helper function for common local setup
def create_local_rest_config():
    return RESTIcebergConfig(
        rest_uri="http://localhost:8181",
        warehouse_id="local",
        endpoint_url="http://localhost:9000",
        aws_region="us-east-1",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        auth_type="none"
    )

sink = IcebergSink(
    table_name="crypto.trades",
    config=create_local_rest_config(),
    data_catalog_spec="rest"
)
```

## Conclusion

This revised approach leverages the proven AWS Glue implementation as a foundation by copying it to a separate file (`iceberg-rest.py`) and extending it with REST catalog support and generic S3-compatible storage. The copy strategy minimizes risk, accelerates development, and provides a clear migration path for existing users.

**Key Advantages:**
- **Zero Risk**: Original functionality completely untouched
- **Fast Development**: Build on proven codebase as foundation
- **Generic Storage**: Works with any S3-compatible provider without provider-specific code
- **Parallel Development**: Original sink unaffected during REST development
- **Clear Migration**: Existing users can migrate by switching import statements

**Recommended Next Steps:** Begin Sprint 1 with FORK-001 to establish the foundation.