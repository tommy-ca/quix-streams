# TDD Test Cycles Plan for Iceberg REST Sink

This document outlines comprehensive Test-Driven Development cycles for validating the Iceberg REST sink in the `feature/crypto-sources-lakehouse` branch.

## Overview

We'll follow classic TDD cycles: **RED → GREEN → REFACTOR** for each component, building from unit tests to integration tests to E2E tests with the Lakekeeper catalog and MinIO storage.

## Test Strategy

### Test Pyramid
```
    E2E Tests (Few)
   ├─ Full crypto-to-lakehouse pipeline
   ├─ Schema evolution scenarios
   └─ Performance & reliability tests
   
 Integration Tests (Some)
├─ Sink → Lakekeeper → MinIO integration
├─ Schema management & evolution
├─ Multi-table and partitioning
└─ Configuration system integration

Unit Tests (Many)
├─ Configuration validation
├─ Schema inference & mapping
├─ Batch processing logic
├─ Error handling & retry
└─ REST client operations
```

## TDD Cycle 1: Configuration System Validation

### 🔴 RED Phase - Write Failing Tests

**File**: `tests/e2e/iceberg_sink/test_config_validation.py`

```python
class TestIcebergSinkConfigValidation:
    def test_catalog_config_requires_uri_and_warehouse(self):
        # Should fail - missing required catalog configuration
    
    def test_storage_config_validates_s3_parameters(self):
        # Should fail - invalid S3 configuration combinations
        
    def test_iceberg_config_composition_validation(self):
        # Should fail - invalid catalog + storage combinations
        
    def test_auth_provider_validation_for_different_providers(self):
        # Should fail - provider-specific validation
        
    def test_config_factory_functions_produce_valid_configs(self):
        # Should fail - factory function validation
```

**File**: `tests/e2e/iceberg_sink/test_config_backward_compatibility.py`

```python
class TestConfigurationBackwardCompatibility:
    def test_deprecated_config_functions_show_warnings(self):
        # Should fail - missing deprecation warnings
        
    def test_old_config_patterns_still_work(self):
        # Should fail - backward compatibility broken
        
    def test_migration_from_old_to_new_config(self):
        # Should fail - migration path validation
```

### 🟢 GREEN Phase - Make Tests Pass

1. **Fix Configuration Validation**
   - Complete validation in `IcebergConfig.__post_init__()`
   - Add cross-validation between catalog and storage configs
   - Implement provider-specific validation logic

2. **Fix Factory Functions**
   - Ensure all factory functions produce valid configurations
   - Add proper error messages for invalid combinations
   - Test edge cases and boundary conditions

3. **Maintain Backward Compatibility**
   - Add deprecation warnings to old factory functions
   - Ensure old configuration patterns continue to work
   - Provide clear migration documentation

### 🔄 REFACTOR Phase - Improve Code Quality

1. **Extract Configuration Validation Logic**
2. **Improve Error Messages with Context**
3. **Add Configuration Examples and Documentation**
4. **Optimize Configuration Loading Performance**

---

## TDD Cycle 2: REST Client and Catalog Integration

### 🔴 RED Phase - Write Failing Tests

**File**: `tests/e2e/iceberg_sink/test_rest_client_integration.py`

```python
class TestRestClientIntegration:
    def test_rest_client_connects_to_lakekeeper(self):
        # Should fail - connection establishment
        
    def test_rest_client_creates_namespace(self):
        # Should fail - namespace creation
        
    def test_rest_client_creates_table_with_schema(self):
        # Should fail - table creation with schema
        
    def test_rest_client_handles_authentication_errors(self):
        # Should fail - auth error handling
        
    def test_rest_client_retry_logic_on_failures(self):
        # Should fail - retry behavior validation
```

**File**: `tests/e2e/iceberg_sink/test_catalog_operations.py`

```python
class TestCatalogOperations:
    def test_catalog_table_creation_and_schema_inference(self):
        # Should fail - schema inference from crypto data
        
    def test_catalog_schema_evolution_on_new_fields(self):
        # Should fail - automatic schema evolution
        
    def test_catalog_partition_management(self):
        # Should fail - partition creation and management
        
    def test_catalog_metadata_operations(self):
        # Should fail - table metadata updates
```

### 🟢 GREEN Phase - Make Tests Pass

1. **Complete REST Client Implementation**
   - Implement all required Iceberg REST operations
   - Add proper error handling and retry logic
   - Implement authentication for different providers

2. **Schema Management**
   - Implement schema inference from crypto data
   - Add schema evolution capabilities
   - Handle complex nested data structures

3. **Catalog Integration**
   - Complete table and namespace operations
   - Implement partition management
   - Add metadata operations support

### 🔄 REFACTOR Phase - Improve Code Quality

1. **Extract REST Operation Patterns**
2. **Optimize Schema Processing**
3. **Improve Error Context and Logging**
4. **Add Operation Caching Where Appropriate**

---

## TDD Cycle 3: Data Processing and Serialization

### 🔴 RED Phase - Write Failing Tests

**File**: `tests/e2e/iceberg_sink/test_data_processing.py`

```python
class TestDataProcessing:
    def test_crypto_trade_data_serialization(self):
        # Should fail - crypto trade to Iceberg format
        
    def test_crypto_ticker_data_serialization(self):
        # Should fail - ticker data processing
        
    def test_crypto_orderbook_data_serialization(self):
        # Should fail - orderbook data flattening and processing
        
    def test_batch_processing_with_mixed_data_types(self):
        # Should fail - mixed crypto data in single batch
        
    def test_data_validation_and_cleaning(self):
        # Should fail - invalid data handling
```

**File**: `tests/e2e/iceberg_sink/test_parquet_generation.py`

```python
class TestParquetGeneration:
    def test_parquet_file_creation_from_crypto_data(self):
        # Should fail - Parquet file generation
        
    def test_parquet_schema_compatibility(self):
        # Should fail - schema compatibility validation
        
    def test_parquet_compression_and_optimization(self):
        # Should fail - file compression and optimization
        
    def test_parquet_partitioning_strategies(self):
        # Should fail - partition-aware file writing
```

### 🟢 GREEN Phase - Make Tests Pass

1. **Complete Data Processing Pipeline**
   - Implement crypto-specific data serializers
   - Add data validation and cleaning logic
   - Handle various crypto data formats efficiently

2. **Parquet Generation**
   - Implement efficient Parquet file generation
   - Add compression and optimization
   - Implement partition-aware writing

3. **Schema Management**
   - Add dynamic schema adaptation
   - Handle schema evolution gracefully
   - Optimize for crypto data patterns

### 🔄 REFACTOR Phase - Improve Code Quality

1. **Extract Data Type Processors**
2. **Optimize Memory Usage in Batch Processing**
3. **Improve Parquet Generation Performance**
4. **Add Data Quality Monitoring**

---

## TDD Cycle 4: S3/MinIO Storage Integration

### 🔴 RED Phase - Write Failing Tests

**File**: `tests/e2e/iceberg_sink/test_s3_storage_integration.py`

```python
class TestS3StorageIntegration:
    def test_s3_file_upload_with_different_providers(self):
        # Should fail - multi-provider S3 uploads
        
    def test_s3_file_organization_and_partitioning(self):
        # Should fail - proper file organization
        
    def test_s3_concurrent_upload_handling(self):
        # Should fail - concurrent upload management
        
    def test_s3_error_recovery_and_retry(self):
        # Should fail - S3 operation retry logic
        
    def test_s3_large_file_upload_optimization(self):
        # Should fail - large file handling
```

**File**: `tests/e2e/iceberg_sink/test_minio_integration.py`

```python
class TestMinIOIntegration:
    def test_minio_bucket_operations(self):
        # Should fail - bucket creation and management
        
    def test_minio_object_lifecycle_management(self):
        # Should fail - object lifecycle operations
        
    def test_minio_multipart_upload_handling(self):
        # Should fail - multipart upload for large files
        
    def test_minio_path_style_access_configuration(self):
        # Should fail - path-style access configuration
```

### 🟢 GREEN Phase - Make Tests Pass

1. **Complete S3 Integration**
   - Support all major S3 providers (AWS, MinIO, CloudFlare R2)
   - Implement efficient upload strategies
   - Add proper error handling and retry logic

2. **File Organization**
   - Implement Iceberg-compliant file organization
   - Add partition-aware file placement
   - Optimize for query performance

3. **Performance Optimization**
   - Implement concurrent uploads
   - Add multipart upload for large files
   - Optimize network utilization

### 🔄 REFACTOR Phase - Improve Code Quality

1. **Extract S3 Provider Abstraction**
2. **Optimize Upload Batching Strategies**
3. **Improve Error Recovery Logic**
4. **Add Upload Progress Monitoring**

---

## TDD Cycle 5: End-to-End Pipeline Integration

### 🔴 RED Phase - Write Failing Tests

**File**: `tests/e2e/iceberg_sink/test_crypto_to_iceberg_e2e.py`

```python
class TestCryptoToIcebergE2E:
    def test_full_pipeline_crypto_trades_to_iceberg(self):
        # Should fail - complete pipeline validation
        
    def test_multiple_crypto_sources_to_single_table(self):
        # Should fail - multi-source aggregation
        
    def test_schema_evolution_with_new_crypto_fields(self):
        # Should fail - dynamic schema adaptation
        
    def test_partition_management_over_time(self):
        # Should fail - time-based partitioning
        
    def test_table_maintenance_and_optimization(self):
        # Should fail - table optimization operations
```

**File**: `tests/e2e/iceberg_sink/test_performance_and_scalability.py`

```python
class TestPerformanceAndScalability:
    def test_high_throughput_crypto_data_ingestion(self):
        # Should fail - throughput benchmarking
        
    def test_large_batch_processing_memory_efficiency(self):
        # Should fail - memory efficiency validation
        
    def test_concurrent_table_writing(self):
        # Should fail - concurrent write handling
        
    def test_query_performance_on_ingested_data(self):
        # Should fail - query performance validation
```

### 🟢 GREEN Phase - Make Tests Pass

1. **Complete E2E Pipeline**
   - Ensure seamless crypto data to Iceberg flow
   - Add comprehensive error handling
   - Implement monitoring and observability

2. **Performance Optimization**
   - Optimize for high-throughput scenarios
   - Implement efficient batching strategies
   - Add resource usage optimization

3. **Scalability Features**
   - Support multiple concurrent writers
   - Implement efficient table maintenance
   - Add automatic optimization triggers

### 🔄 REFACTOR Phase - Improve Code Quality

1. **Extract Pipeline Orchestration Logic**
2. **Optimize Resource Utilization**
3. **Improve Monitoring and Alerting**
4. **Add Comprehensive Documentation**

---

## TDD Cycle 6: Error Handling and Reliability

### 🔴 RED Phase - Write Failing Tests

**File**: `tests/e2e/iceberg_sink/test_error_scenarios.py`

```python
class TestErrorScenarios:
    def test_lakekeeper_service_unavailable_handling(self):
        # Should fail - catalog service unavailability
        
    def test_s3_service_errors_and_recovery(self):
        # Should fail - storage service error handling
        
    def test_data_corruption_detection_and_recovery(self):
        # Should fail - data integrity validation
        
    def test_partial_batch_failure_handling(self):
        # Should fail - partial failure recovery
        
    def test_network_partition_and_reconnection(self):
        # Should fail - network resilience testing
```

**File**: `tests/e2e/iceberg_sink/test_data_consistency.py`

```python
class TestDataConsistency:
    def test_transaction_consistency_guarantees(self):
        # Should fail - ACID properties validation
        
    def test_concurrent_writer_conflict_resolution(self):
        # Should fail - concurrent write conflict handling
        
    def test_schema_evolution_consistency(self):
        # Should fail - consistent schema evolution
        
    def test_partition_consistency_across_operations(self):
        # Should fail - partition-level consistency
```

### 🟢 GREEN Phase - Make Tests Pass

1. **Comprehensive Error Handling**
   - Implement robust error recovery for all failure modes
   - Add circuit breaker patterns for external services
   - Implement data validation and consistency checks

2. **Reliability Features**
   - Add transaction support where possible
   - Implement conflict resolution strategies
   - Add data integrity validation

3. **Monitoring and Observability**
   - Add comprehensive metrics and logging
   - Implement health checks and alerts
   - Add performance monitoring

### 🔄 REFACTOR Phase - Improve Code Quality

1. **Extract Error Handling Patterns**
2. **Optimize Recovery Strategies**
3. **Improve Observability Coverage**
4. **Add Comprehensive Testing Documentation**

---

## Special Test Scenarios for Crypto Data

### Crypto-Specific Test Cases

**File**: `tests/e2e/iceberg_sink/test_crypto_specific_scenarios.py`

```python
class TestCryptoSpecificScenarios:
    def test_high_frequency_trade_data_batching(self):
        # Crypto trades can be very high frequency
        
    def test_variable_orderbook_depth_handling(self):
        # Order book depth varies significantly
        
    def test_exchange_specific_field_mapping(self):
        # Different exchanges have different field formats
        
    def test_price_precision_handling(self):
        # Crypto prices require high precision
        
    def test_timestamp_normalization_across_exchanges(self):
        # Different timestamp formats from exchanges
        
    def test_symbol_normalization_and_mapping(self):
        # Symbol formats vary across exchanges
```

### Schema Evolution Test Cases

**File**: `tests/e2e/iceberg_sink/test_crypto_schema_evolution.py`

```python
class TestCryptoSchemaEvolution:
    def test_new_exchange_fields_schema_evolution(self):
        # New exchanges may have additional fields
        
    def test_crypto_metadata_field_additions(self):
        # Market metadata fields may be added over time
        
    def test_backward_compatibility_with_old_crypto_data(self):
        # Ensure old data remains accessible
        
    def test_data_type_evolution_for_crypto_fields(self):
        # Handle precision changes or type promotions
```

## Test Execution Strategy

### Local Development Environment
```bash
# Start local infrastructure
docker-compose -f infra/e2e-crypto-lakehouse/docker-compose.yml up -d

# Wait for services to be healthy
./infra/e2e-crypto-lakehouse/scripts/wait-for-services.sh

# Run unit tests
pytest tests/e2e/iceberg_sink/test_config_validation.py -v

# Run integration tests
pytest tests/e2e/iceberg_sink/test_*_integration.py -v

# Run E2E tests
pytest tests/e2e/iceberg_sink/test_*_e2e.py -v

# Run performance tests
pytest tests/e2e/iceberg_sink/test_performance_*.py -v --benchmark-only
```

### CI/CD Pipeline
```bash
# Quick validation (unit tests only)
pytest tests/e2e/iceberg_sink/ -k "not integration and not e2e" --timeout=60

# Integration testing with infrastructure
pytest tests/e2e/iceberg_sink/ --integration --timeout=300

# Full E2E validation
pytest tests/e2e/iceberg_sink/ --e2e --timeout=600

# Performance benchmarking
pytest tests/e2e/iceberg_sink/ --benchmark --timeout=900
```

### Test Data Management

#### Crypto Test Data Sets
1. **Small Test Set**: 100 records per data type
2. **Medium Test Set**: 10K records per data type  
3. **Large Test Set**: 1M records per data type
4. **Schema Evolution Set**: Data with evolving schemas
5. **Multi-Exchange Set**: Data from multiple exchanges

#### Data Generation
```python
def generate_crypto_test_data(data_type, count, exchange="mock"):
    """Generate realistic crypto test data."""
    if data_type == "trades":
        return generate_trade_data(count, exchange)
    elif data_type == "ticker":
        return generate_ticker_data(count, exchange)
    elif data_type == "orderbook":
        return generate_orderbook_data(count, exchange)
```

## Performance Baselines

### Throughput Targets
- **Crypto Trades**: 10,000 trades/second sustained
- **Crypto Tickers**: 1,000 tickers/second sustained  
- **Order Books**: 100 snapshots/second sustained

### Latency Targets
- **End-to-End**: < 500ms for 95th percentile
- **Batch Processing**: < 5s for 10K record batch
- **Schema Operations**: < 2s for schema evolution

### Resource Usage Targets
- **Memory**: < 1GB peak for normal operations
- **CPU**: < 70% sustained utilization
- **Network**: Efficient S3 upload utilization
- **Storage**: < 2x data size for Parquet files

## Success Criteria

### Each TDD Cycle Must Achieve:
1. ✅ **All tests pass** (GREEN phase complete)
2. ✅ **Code coverage** > 95% for sink components
3. ✅ **Performance benchmarks** meet targets
4. ✅ **Integration tests** validate real Lakekeeper/MinIO
5. ✅ **Error scenarios** handled gracefully
6. ✅ **Documentation** updated with examples

### Final Validation Criteria:
1. ✅ **Crypto data integrity** preserved end-to-end
2. ✅ **Schema evolution** works seamlessly
3. ✅ **Multi-provider support** (AWS S3, MinIO, CloudFlare R2)
4. ✅ **Performance targets** met consistently
5. ✅ **Error recovery** robust under all failure modes
6. ✅ **Query performance** optimized for crypto analytics
7. ✅ **Monitoring** provides full observability

## Integration with Crypto Sources TDD

### Pipeline Testing Strategy
1. **Unit Tests**: Individual components in isolation
2. **Component Integration**: Source + Kafka + Sink
3. **Service Integration**: With Lakekeeper + MinIO
4. **E2E Integration**: Full crypto-to-lakehouse pipeline
5. **Performance Testing**: Under realistic load
6. **Reliability Testing**: Failure scenarios and recovery

### Coordination Points
- **Schema Consistency**: Ensure crypto source normalization matches sink expectations
- **Error Propagation**: Consistent error handling across pipeline
- **Monitoring Integration**: End-to-end observability
- **Configuration Alignment**: Unified configuration system usage

## Next Steps

After completing Iceberg sink TDD cycles, proceed to full E2E pipeline validation combining both crypto sources and Iceberg sink with comprehensive performance and reliability testing.