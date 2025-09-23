# TDD Test Cycles Plan for Crypto Sources

This document outlines comprehensive Test-Driven Development cycles for validating crypto sources in the `feature/crypto-sources-lakehouse` branch.

## Overview

We'll follow classic TDD cycles: **RED → GREEN → REFACTOR** for each component, building from unit tests to integration tests to E2E tests.

## Test Strategy

### Test Pyramid
```
    E2E Tests (Few)
   ├─ Full pipeline with real services
   └─ Performance & reliability tests
   
 Integration Tests (Some)
├─ Sources → Kafka integration
├─ Mock exchange integration  
└─ Configuration system integration

Unit Tests (Many)
├─ Configuration validation
├─ Error handling
├─ Retry logic
├─ Data normalization
└─ Authentication providers
```

## TDD Cycle 1: Configuration System Validation

### 🔴 RED Phase - Write Failing Tests

**File**: `tests/e2e/crypto_sources/test_config_validation.py`

```python
class TestCryptoSourceConfigValidation:
    def test_cryptofeed_config_requires_exchanges_and_channels(self):
        # Should fail - missing required fields
    
    def test_ccxt_config_validates_mode_and_symbols(self):
        # Should fail - invalid mode values
        
    def test_binance_s3_config_validates_s3_parameters(self):
        # Should fail - invalid bucket/prefix combinations
        
    def test_environment_variable_loading_with_invalid_types(self):
        # Should fail - type conversion errors
        
    def test_auth_provider_interface_compliance(self):
        # Should fail - incomplete implementations
```

**File**: `tests/e2e/crypto_sources/test_config_integration.py`

```python
class TestConfigurationIntegration:
    def test_config_serialization_roundtrip(self):
        # Should fail - serialization/deserialization issues
        
    def test_backward_compatibility_warnings(self):
        # Should fail - missing deprecation warnings
        
    def test_config_factory_functions_consistency(self):
        # Should fail - factory function mismatches
```

### 🟢 GREEN Phase - Make Tests Pass

1. **Fix Configuration Validation**
   - Implement proper validation in dataclass `__post_init__`
   - Add type checking and constraint validation
   - Handle edge cases for required/optional fields

2. **Fix Environment Loading**
   - Robust type conversion in `load_from_env()`
   - Proper error handling for malformed env vars
   - Support for complex types (lists, nested objects)

3. **Fix Authentication Providers**
   - Ensure all auth providers implement interface correctly
   - Add validation for credential completeness
   - Handle missing/invalid credentials gracefully

### 🔄 REFACTOR Phase - Improve Code Quality

1. **Extract Common Validation Logic**
2. **Improve Error Messages**
3. **Add Type Hints and Documentation**
4. **Optimize Performance**

---

## TDD Cycle 2: Mock Exchange Integration

### 🔴 RED Phase - Write Failing Tests

**File**: `tests/e2e/crypto_sources/test_mock_exchange.py`

```python
class TestMockExchangeIntegration:
    def test_mock_exchange_websocket_connection(self):
        # Should fail - connection establishment
        
    def test_mock_exchange_rest_api_endpoints(self):
        # Should fail - API response validation
        
    def test_mock_exchange_data_generation(self):
        # Should fail - data format validation
        
    def test_mock_exchange_subscription_management(self):
        # Should fail - WebSocket subscription handling
```

**File**: `tests/e2e/crypto_sources/test_ccxt_mock_integration.py`

```python
class TestCCXTMockIntegration:
    def test_ccxt_source_connects_to_mock_exchange(self):
        # Should fail - connection setup
        
    def test_ccxt_source_fetches_trades_from_mock(self):
        # Should fail - data retrieval and parsing
        
    def test_ccxt_source_handles_mock_rate_limits(self):
        # Should fail - rate limiting simulation
        
    def test_ccxt_source_retry_logic_with_mock_failures(self):
        # Should fail - retry behavior validation
```

### 🟢 GREEN Phase - Make Tests Pass

1. **Complete Mock Exchange Implementation**
   - Fix WebSocket server implementation
   - Implement all required REST endpoints
   - Add realistic data generation algorithms

2. **Enhance CCXT Source**
   - Add mock exchange support to CCXT source
   - Implement proper error handling for mock responses
   - Add configuration for mock endpoints

3. **Integration Testing Framework**
   - Create test helpers for mock exchange interaction
   - Add docker-compose test orchestration
   - Implement test data fixtures

### 🔄 REFACTOR Phase - Improve Code Quality

1. **Extract Mock Exchange as Reusable Component**
2. **Optimize Data Generation Algorithms**
3. **Improve Test Helper Functions**
4. **Add Comprehensive Logging**

---

## TDD Cycle 3: CryptofeedSource Integration

### 🔴 RED Phase - Write Failing Tests

**File**: `tests/e2e/crypto_sources/test_cryptofeed_integration.py`

```python
class TestCryptofeedSourceIntegration:
    def test_cryptofeed_source_initialization_with_config(self):
        # Should fail - config-based initialization
        
    def test_cryptofeed_source_connects_to_mock_websocket(self):
        # Should fail - WebSocket connection handling
        
    def test_cryptofeed_source_processes_trade_messages(self):
        # Should fail - message processing and normalization
        
    def test_cryptofeed_source_handles_connection_failures(self):
        # Should fail - error recovery and retry logic
        
    def test_cryptofeed_source_produces_to_kafka(self):
        # Should fail - Kafka message production
```

**File**: `tests/e2e/crypto_sources/test_cryptofeed_e2e.py`

```python
class TestCryptofeedE2E:
    def test_full_cryptofeed_pipeline(self):
        # Should fail - end-to-end data flow
        
    def test_cryptofeed_with_multiple_exchanges(self):
        # Should fail - multi-exchange handling
        
    def test_cryptofeed_reconnection_scenarios(self):
        # Should fail - connection resilience testing
```

### 🟢 GREEN Phase - Make Tests Pass

1. **Complete CryptofeedSource Refactor**
   - Implement unified configuration system usage
   - Add mock exchange adapter for cryptofeed
   - Implement proper error handling and retry logic

2. **Kafka Integration**
   - Ensure proper Kafka producer configuration
   - Add message serialization and key generation
   - Implement topic management

3. **E2E Pipeline Setup**
   - Create test orchestration for full pipeline
   - Add data validation at each step
   - Implement test cleanup and teardown

### 🔄 REFACTOR Phase - Improve Code Quality

1. **Extract Common Source Patterns**
2. **Optimize Message Processing**
3. **Improve Error Handling Granularity**
4. **Add Performance Monitoring**

---

## TDD Cycle 4: CCXTSource Comprehensive Testing

### 🔴 RED Phase - Write Failing Tests

**File**: `tests/e2e/crypto_sources/test_ccxt_comprehensive.py`

```python
class TestCCXTSourceComprehensive:
    def test_ccxt_source_klines_mode_integration(self):
        # Should fail - klines data fetching and processing
        
    def test_ccxt_source_orderbook_mode_integration(self):
        # Should fail - orderbook data handling
        
    def test_ccxt_source_trades_mode_integration(self):
        # Should fail - trades data processing
        
    def test_ccxt_source_rate_limiting_behavior(self):
        # Should fail - rate limiting compliance
        
    def test_ccxt_source_error_recovery_patterns(self):
        # Should fail - various error scenarios
```

**File**: `tests/e2e/crypto_sources/test_ccxt_multi_exchange.py`

```python
class TestCCXTMultiExchangeSupport:
    def test_ccxt_with_binance_config(self):
        # Should fail - Binance-specific configuration
        
    def test_ccxt_with_coinbase_config(self):
        # Should fail - Coinbase-specific configuration
        
    def test_ccxt_exchange_switching(self):
        # Should fail - dynamic exchange switching
```

### 🟢 GREEN Phase - Make Tests Pass

1. **Complete CCXT Source Implementation**
   - Implement all modes (klines, trades, orderbook)
   - Add proper rate limiting and retry logic
   - Implement exchange-specific optimizations

2. **Multi-Exchange Support**
   - Add configuration templates for popular exchanges
   - Implement exchange-specific error handling
   - Add validation for exchange capabilities

3. **Performance Optimization**
   - Implement efficient data fetching patterns
   - Add connection pooling where appropriate
   - Optimize memory usage for large datasets

### 🔄 REFACTOR Phase - Improve Code Quality

1. **Extract Exchange-Specific Logic**
2. **Optimize Data Processing Pipeline**
3. **Improve Configuration Validation**
4. **Add Comprehensive Documentation**

---

## TDD Cycle 5: BinanceS3Source Testing

### 🔴 RED Phase - Write Failing Tests

**File**: `tests/e2e/crypto_sources/test_binance_s3_integration.py`

```python
class TestBinanceS3Integration:
    def test_binance_s3_source_with_public_data(self):
        # Should fail - public data access via MinIO
        
    def test_binance_s3_source_file_format_detection(self):
        # Should fail - automatic format detection
        
    def test_binance_s3_source_compression_handling(self):
        # Should fail - gzip/zip file processing
        
    def test_binance_s3_source_templated_prefixes(self):
        # Should fail - templated prefix expansion
        
    def test_binance_s3_source_checksum_validation(self):
        # Should fail - data integrity checking
```

**File**: `tests/e2e/crypto_sources/test_binance_s3_performance.py`

```python
class TestBinanceS3Performance:
    def test_large_file_processing_memory_usage(self):
        # Should fail - memory efficiency
        
    def test_concurrent_file_processing(self):
        # Should fail - concurrent download/processing
        
    def test_streaming_file_processing(self):
        # Should fail - streaming large files
```

### 🟢 GREEN Phase - Make Tests Pass

1. **Complete BinanceS3Source Implementation**
   - Implement unified configuration system
   - Add comprehensive file format support
   - Implement efficient streaming processing

2. **Performance Optimization**
   - Add streaming file processing
   - Implement concurrent downloads
   - Optimize memory usage for large files

3. **Data Integrity**
   - Implement checksum validation
   - Add error recovery for corrupted files
   - Handle partial downloads gracefully

### 🔄 REFACTOR Phase - Improve Code Quality

1. **Extract File Processing Utilities**
2. **Optimize S3 Client Configuration**
3. **Improve Error Handling and Logging**
4. **Add Progress Monitoring**

---

## Test Execution Strategy

### Local Development
```bash
# Run unit tests
pytest tests/e2e/crypto_sources/test_config_validation.py -v

# Run integration tests  
pytest tests/e2e/crypto_sources/test_*_integration.py -v

# Run E2E tests with infrastructure
docker-compose -f infra/e2e-crypto-lakehouse/docker-compose.yml up -d
pytest tests/e2e/crypto_sources/ -v --integration
```

### Automated CI/CD
```bash
# Fast feedback loop - unit tests only
pytest tests/e2e/crypto_sources/ -k "not integration and not e2e"

# Full integration testing
pytest tests/e2e/crypto_sources/ --integration --e2e
```

### Test Data Management
- **Mock data**: Generated by mock exchange
- **Sample files**: Small representative datasets in git
- **Large datasets**: Downloaded on-demand or cached locally
- **Test isolation**: Each test gets clean environment

### Performance Baselines
- **Throughput**: Messages per second for each source
- **Latency**: End-to-end processing time
- **Memory usage**: Peak memory consumption
- **Error rate**: Percentage of failed operations

## Success Criteria

### Each TDD Cycle Must Achieve:
1. ✅ **All tests pass** (GREEN phase complete)
2. ✅ **Code coverage** > 90% for new/modified code
3. ✅ **Performance benchmarks** meet established baselines
4. ✅ **Integration tests** validate real-world scenarios
5. ✅ **Documentation** updated with examples
6. ✅ **Backward compatibility** maintained

### Final Validation:
1. ✅ **E2E pipeline** works end-to-end
2. ✅ **Error recovery** handles all failure modes
3. ✅ **Performance** meets production requirements
4. ✅ **Monitoring** provides adequate observability
5. ✅ **Documentation** enables easy adoption

## Next Steps

After completing all TDD cycles for crypto sources, we'll proceed with similar comprehensive testing for the Iceberg REST sink, followed by full E2E pipeline validation.