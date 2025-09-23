# Crypto Sources & Iceberg Sinks Task Backlog

## Epic Overview
Development backlog for completing crypto data sources and Iceberg lakehouse integration features.

## Sprint Planning Summary
- **Total Tasks**: 32
- **Story Points**: 178
- **Estimated Duration**: 8-10 sprints (16-20 weeks)
- **Critical Path**: REST catalog integration → Production readiness

---

## 🔴 CRITICAL PRIORITY (Sprint 1-2)

### CRI-001: Implement REST Catalog Support in Community IcebergSink
**Story Points**: 13 | **Priority**: Critical | **Dependencies**: None

#### Description
Add REST catalog support to the community IcebergSink to enable Lakekeeper and other REST catalog deployments.

#### Acceptance Criteria
- [ ] Create `RESTIcebergConfig` class with authentication support
- [ ] Implement REST catalog factory in `_import_data_catalog()`
- [ ] Add environment variable support for REST configuration
- [ ] Update `DataCatalogSpec` to include "rest" option
- [ ] Add error handling for REST-specific failures
- [ ] Write comprehensive unit tests
- [ ] Update documentation with REST catalog examples

#### Technical Tasks
- [ ] Extend `BaseIcebergConfig` for REST catalog parameters
- [ ] Implement authentication (token, basic auth)
- [ ] Add connection pooling and timeout configuration
- [ ] Implement catalog health checks
- [ ] Add REST-specific error classification

#### Validation
- [ ] Integration tests with Lakekeeper deployment
- [ ] Performance tests with large batches
- [ ] Failover testing with catalog unavailability

---

### CRI-002: Fix Iceberg Data Flattening for Nested Structures
**Story Points**: 8 | **Priority**: Critical | **Dependencies**: CRI-001

#### Description
Resolve the critical TODO in IcebergSink that prevents handling of nested JSON properties.

#### Acceptance Criteria
- [ ] Implement recursive flattening for nested objects
- [ ] Handle arrays and complex data types
- [ ] Preserve data types during flattening
- [ ] Add configuration for flattening depth limits
- [ ] Maintain backward compatibility with existing schemas

#### Technical Tasks
- [ ] Design flattening strategy (dot notation vs separate columns)
- [ ] Implement recursive flattening algorithm  
- [ ] Add type preservation for nested fields
- [ ] Handle array data types appropriately
- [ ] Add configuration options for flattening behavior

#### Validation
- [ ] Unit tests with complex nested JSON
- [ ] Integration tests with crypto data containing metadata
- [ ] Performance tests with deeply nested structures

---

### CRI-003: Optimize Iceberg Batch Transformations
**Story Points**: 5 | **Priority**: Critical | **Dependencies**: CRI-002

#### Description
Optimize the multiple iterative batch transformations in Iceberg sink serialization.

#### Acceptance Criteria
- [ ] Reduce batch transformation iterations from 3+ to 1-2
- [ ] Implement streaming/vectorized operations where possible
- [ ] Maintain memory efficiency for large batches
- [ ] Preserve existing functionality and error handling

#### Technical Tasks
- [ ] Profile current batch transformation performance
- [ ] Implement single-pass batch processing
- [ ] Add streaming transformations for large batches
- [ ] Optimize PyArrow table construction
- [ ] Add memory usage monitoring

#### Validation
- [ ] Performance benchmarks with 10k+ record batches
- [ ] Memory usage tests with various batch sizes
- [ ] Correctness tests comparing old vs new transformations

### CRI-004: Restore Binance S3 Configuration Compatibility
**Story Points**: 5 | **Priority**: Critical | **Dependencies**: None

#### Description
Reintroduce the legacy configuration entry points required by Binance S3 consumers (`.config`, `load_from_env`, `AWSAuth`) while retaining the simplified `simple_config` module.

#### Acceptance Criteria
- [ ] Recreate `quixstreams.sources.community.crypto.config` as a thin compatibility layer that delegates to the new simple configs
- [ ] Ensure `load_from_env` supports all legacy environment variables used in docs/tests
- [ ] Update `BinanceS3Source` to import from the compatibility layer without regression
- [ ] Maintain deprecation warnings guiding users to the new API

#### Technical Tasks
- [ ] Implement shim module exporting `AuthProvider`, `RetryConfig`, etc., or update tests to new namespaces
- [ ] Add unit tests covering `load_from_env` and the compatibility imports
- [ ] Audit documentation to point to the new API while keeping migration notes

#### Validation
- [ ] `tests/test_quixstreams/test_sources/test_community/test_crypto/test_unified_config.py` passes without modifications
- [ ] Manual smoke test constructing `BinanceS3Source` via both legacy kwargs and new config object

### CRI-005: Initialise BinanceS3Source Runtime Flags
**Story Points**: 3 | **Priority**: Critical | **Dependencies**: CRI-004

#### Description
Define `_dry_run`, `_replay_speed`, `_checksum_mode`, `_extract_metadata`, and other runtime flags during construction to prevent `AttributeError` at runtime.

#### Acceptance Criteria
- [ ] All runtime flags have explicit defaults aligned with legacy behaviour
- [ ] `pytest tests/.../test_binance_s3_source.py` passes without monkeypatching private attributes
- [ ] Add regression tests covering dry-run, checksum, and replay-speed branches

#### Technical Tasks
- [ ] Set defaults in `__init__` after config resolution
- [ ] Update type hints and docstrings describing the flags
- [ ] Extend tests to cover dry-run and checksum paths without relying on global state

#### Validation
- [ ] No AttributeErrors during integration tests
- [ ] Behaviour documented in README / API reference

### CRI-006: Reinstate Backwards-Compatible Crypto Source Constructors
**Story Points**: 5 | **Priority**: Critical | **Dependencies**: CRI-004

#### Description
Provide backwards-compatible constructors (or helper factories) for `CCXTSource` and `CryptofeedSource` so existing call sites using keyword arguments do not break.

#### Acceptance Criteria
- [ ] Support both config-object and keyword-argument construction paths
- [ ] Deprecation warnings emitted for legacy signatures
- [ ] Update docs with migration guidance and examples for both styles

#### Technical Tasks
- [ ] Implement alternate constructors or `@classmethod from_kwargs`
- [ ] Adjust tests to cover both instantiation paths
- [ ] Ensure type hints/docstrings reflect accepted argument forms

#### Validation
- [ ] `tests/test_ccxt_source.py` and `tests/test_cryptofeed_source.py` green without modifications
- [ ] Manual instantiation in notebooks/scripts succeeds with legacy kwargs

### CRI-007: Align Source Integration Test Suites *(Completed)*
**Outcome**: Both integration suites now consume the compatibility layer and pass under the new behaviour.

---

## 🟡 HIGH PRIORITY (Sprint 3-4)

### HIGH-001: Complete Authentication and Connection Management for REST Catalogs
**Story Points**: 8 | **Priority**: High | **Dependencies**: CRI-001

#### Description
Implement comprehensive authentication and connection management for REST catalogs.

#### Acceptance Criteria
- [ ] Support bearer token authentication
- [ ] Support basic authentication (username/password)  
- [ ] Implement connection pooling for REST clients
- [ ] Add retry logic with exponential backoff
- [ ] Support SSL certificate verification options

#### Technical Tasks
- [ ] Create authentication strategy pattern
- [ ] Implement token refresh mechanisms
- [ ] Add connection pool configuration
- [ ] Implement circuit breaker for REST calls
- [ ] Add SSL/TLS configuration options

---

### HIGH-002: Add Comprehensive Error Handling and Circuit Breakers
**Story Points**: 13 | **Priority**: High | **Dependencies**: None

#### Description
Implement robust error handling with circuit breakers across all crypto sources and sinks.

#### Acceptance Criteria
- [ ] Classify errors into retriable/non-retriable categories
- [ ] Implement circuit breakers for external dependencies
- [ ] Add configurable retry strategies per component
- [ ] Implement error metrics and alerting hooks
- [ ] Add graceful degradation patterns

#### Technical Tasks
- [ ] Create error classification system
- [ ] Implement circuit breaker base class
- [ ] Add retry configuration per source/sink
- [ ] Implement metrics hooks for monitoring
- [ ] Add error recovery patterns

---

### HIGH-003: Implement State Persistence for CCXTSource
**Story Points**: 8 | **Priority**: High | **Dependencies**: None

#### Description
Add external state persistence for CCXTSource cursor management to survive restarts.

#### Acceptance Criteria
- [ ] Support pluggable state storage backends (file, Redis, PostgreSQL)
- [ ] Implement cursor checkpointing with configurable intervals
- [ ] Add state recovery on source restart
- [ ] Ensure exactly-once processing semantics
- [ ] Add state cleanup for inactive symbols

#### Technical Tasks
- [ ] Design state persistence interface
- [ ] Implement file-based state backend
- [ ] Add Redis state backend support
- [ ] Implement cursor checkpointing logic
- [ ] Add state recovery mechanisms

---

### HIGH-004: Enhance BinanceS3Source Performance and Reliability
**Story Points**: 5 | **Priority**: High | **Dependencies**: None

#### Description
Address performance and reliability issues in BinanceS3Source for large-scale deployments.

#### Acceptance Criteria
- [ ] Optimize memory usage during large prefix expansions
- [ ] Implement streaming prefix iteration for memory efficiency
- [ ] Enhance checksum retry logic with better error handling
- [ ] Add configurable parallelism for S3 operations
- [ ] Implement smart prefetch strategies

#### Technical Tasks
- [ ] Implement streaming prefix expansion
- [ ] Add memory usage monitoring and limits
- [ ] Enhance checksum verification retry logic
- [ ] Add parallel S3 operations with throttling
- [ ] Implement intelligent prefetch patterns

---

## 🟢 MEDIUM PRIORITY (Sprint 5-6)

### MED-001: Add Multi-Catalog Support (SQL, Nessie)
**Story Points**: 13 | **Priority**: Medium | **Dependencies**: HIGH-001

#### Description
Extend catalog support to include SQL and Nessie catalogs for development and advanced use cases.

#### Acceptance Criteria
- [ ] Implement SQL catalog configuration (SQLite, PostgreSQL)
- [ ] Add Nessie catalog support with branch management
- [ ] Update catalog factory pattern for extensibility
- [ ] Add catalog-specific optimization patterns
- [ ] Support catalog migration utilities

---

### MED-002: Implement WebSocket Support in CCXTSource
**Story Points**: 8 | **Priority**: Medium | **Dependencies**: HIGH-003

#### Description
Complete WebSocket mode implementation in CCXTSource using ccxtpro.

#### Acceptance Criteria
- [ ] Implement WebSocket connection management
- [ ] Add support for real-time klines, trades, orderbook
- [ ] Implement connection monitoring and reconnection
- [ ] Add WebSocket-specific error handling
- [ ] Support multiple WebSocket feeds per source

---

### MED-003: Enhance CryptofeedSource with Channel-Specific Topics
**Story Points**: 5 | **Priority**: Medium | **Dependencies**: None

#### Description
Add support for channel-specific topic routing in CryptofeedSource.

#### Acceptance Criteria
- [ ] Implement configurable topic routing per channel
- [ ] Add topic naming strategies (exchange.channel, unified, custom)
- [ ] Support dynamic topic creation
- [ ] Maintain backward compatibility with single topic mode
- [ ] Add topic routing configuration validation

---

### MED-004: Add Performance Monitoring and Metrics
**Story Points**: 8 | **Priority**: Medium | **Dependencies**: HIGH-002

#### Description
Implement comprehensive performance monitoring and metrics across all components.

#### Acceptance Criteria
- [ ] Add throughput metrics (records/second, bytes/second)
- [ ] Implement latency tracking (end-to-end, per-component)
- [ ] Add error rate monitoring by category
- [ ] Implement resource utilization tracking (CPU, memory, I/O)
- [ ] Support multiple metrics backends (Prometheus, StatsD, CloudWatch)

---

### MED-005: Implement Schema Caching and Optimization
**Story Points**: 5 | **Priority**: Medium | **Dependencies**: CRI-001

#### Description
Optimize Iceberg sink performance by implementing schema caching and metadata optimization.

#### Acceptance Criteria
- [ ] Cache table metadata to avoid repeated catalog calls
- [ ] Implement schema evolution caching
- [ ] Add configurable cache TTL and invalidation
- [ ] Optimize schema comparison operations
- [ ] Add cache hit rate metrics

---

## 🔵 LOW PRIORITY (Sprint 7-8)

### LOW-001: Add Advanced Partitioning Strategies
**Story Points**: 8 | **Priority**: Low | **Dependencies**: MED-005

#### Description
Implement advanced partitioning strategies for optimal query performance.

#### Acceptance Criteria
- [ ] Support custom partition strategies per data type
- [ ] Implement adaptive partitioning based on data volume
- [ ] Add support for partition evolution
- [ ] Implement partition pruning optimization
- [ ] Support multi-dimensional partitioning schemes

---

### LOW-002: Implement Data Quality and Validation Framework
**Story Points**: 13 | **Priority**: Low | **Dependencies**: MED-004

#### Description
Add comprehensive data quality checks and validation framework.

#### Acceptance Criteria
- [ ] Implement schema validation rules
- [ ] Add data quality checks (null rates, value ranges, patterns)
- [ ] Support custom validation rules per data type
- [ ] Add data quality metrics and alerting
- [ ] Implement data quality reports and dashboards

---

### LOW-003: Add Automatic Compaction and Optimization
**Story Points**: 8 | **Priority**: Low | **Dependencies**: LOW-001

#### Description
Implement automatic file compaction and table optimization for Iceberg tables.

#### Acceptance Criteria
- [ ] Add configurable compaction triggers (file count, size, age)
- [ ] Implement background compaction processes
- [ ] Support partition-aware compaction
- [ ] Add compaction metrics and monitoring
- [ ] Implement Z-order optimization for query performance

---

## 🟠 TESTING & DOCUMENTATION (Ongoing)

### DOC-001: Update API Documentation and Examples
**Story Points**: 8 | **Priority**: Medium | **Dependencies**: Multiple

#### Description
Comprehensive documentation update for all crypto sources and Iceberg sink features.

#### Acceptance Criteria
- [ ] Update API reference documentation
- [ ] Create comprehensive configuration examples
- [ ] Add end-to-end tutorial guides
- [ ] Document best practices and patterns
- [ ] Add troubleshooting guides

---

### TEST-001: Implement End-to-End Integration Tests
**Story Points**: 13 | **Priority**: High | **Dependencies**: CRI-001, HIGH-002

#### Description
Create comprehensive end-to-end integration test suite.

#### Acceptance Criteria
- [ ] E2E tests for each crypto source to Iceberg sink
- [ ] Performance tests with realistic data volumes
- [ ] Failure simulation and recovery tests
- [ ] Multi-catalog compatibility tests
- [ ] Schema evolution integration tests

---

## Task Dependencies Graph

```
CRI-001 (REST Catalog) → HIGH-001 (Auth) → MED-001 (Multi-catalog)
    ↓
CRI-002 (Data Flattening) → CRI-003 (Optimization) → MED-005 (Schema Caching)
    ↓
TEST-001 (E2E Tests) → LOW-002 (Data Quality)

HIGH-002 (Error Handling) → MED-004 (Monitoring) → LOW-003 (Compaction)
    ↓
HIGH-003 (State Persistence) → MED-002 (WebSocket)
    ↓
HIGH-004 (Performance) → LOW-001 (Advanced Partitioning)
```

## Resource Requirements

### Development Team
- **Senior Backend Developer**: Full-time (Iceberg sinks, architecture)
- **Python Developer**: Full-time (Crypto sources, integrations)  
- **DevOps Engineer**: Part-time (Infrastructure, testing)
- **Technical Writer**: Part-time (Documentation)

### Infrastructure
- **Development Environment**: Kafka, Lakekeeper, S3-compatible storage
- **Testing Environment**: Multi-exchange sandbox accounts, performance test data
- **CI/CD Pipeline**: Automated testing, integration test environments

## Success Metrics

### Phase 1 (Critical Priority Complete)
- **REST catalog support**: 100% feature parity with AWS Glue
- **Data reliability**: 0 data loss incidents due to flattening issues
- **Performance**: < 10% performance degradation from optimizations

### Phase 2 (High Priority Complete)  
- **Error handling**: < 0.1% error rate for transient failures
- **State management**: 100% cursor recovery success rate
- **Performance**: 2x throughput improvement for large batches

### Phase 3 (Medium Priority Complete)
- **Multi-catalog support**: 3+ catalog types fully supported
- **Observability**: 100% component coverage for key metrics
- **Feature completeness**: WebSocket support for all applicable sources

### Phase 4 (Low Priority Complete)
- **Advanced features**: Custom partitioning, automatic optimization
- **Data quality**: 99.9% data quality score
- **Operational efficiency**: 50% reduction in manual intervention

## Risk Assessment

### High Risk
- **REST catalog integration complexity**: Mitigation - Incremental implementation, extensive testing
- **Performance regression from optimizations**: Mitigation - Benchmark-driven development
- **Schema evolution breaking changes**: Mitigation - Backward compatibility testing

### Medium Risk  
- **Third-party dependency changes**: Mitigation - Version pinning, adapter patterns
- **Scalability limits at high volume**: Mitigation - Performance testing, capacity planning

### Low Risk
- **Documentation completeness**: Mitigation - Documentation-driven development
- **Testing coverage gaps**: Mitigation - TDD approach, coverage requirements

## Conclusion

This backlog provides a comprehensive roadmap for completing the crypto sources and Iceberg sinks integration. The critical priority tasks address production-blocking issues, while the subsequent phases add advanced features and operational excellence capabilities.

**Recommended approach**: Execute in phases with continuous integration and testing to ensure production readiness at each milestone.
