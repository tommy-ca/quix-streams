# Technical Design Document

## Engineering Principles

This initiative strictly follows project engineering principles:
- **KISS**: Simple documentation of existing patterns without over-engineering
- **SOLID**: Document single responsibility patterns already implemented
- **DRY**: Identify shared patterns for potential extraction  
- **YAGNI**: Focus on actual documentation needs, avoid speculative architecture
- **NO MOCKS**: Document real implementation testing patterns
- **NO LEGACY**: Clean pattern analysis without backward compatibility concerns
- **NO COMPATIBILITY**: No requirement to maintain old patterns
- **START SMALL**: Minimal documentation scope focused on crypto sources and Iceberg REST sink
- **CONSISTENT NAMING**: Simple names without prefixes
- **TDD**: Document test-first development patterns already implemented
- **FRs over NFRs**: Focus on functional patterns, not performance optimization

## System Overview

This initiative documents architectural patterns from two production-ready components:

1. **Crypto Sources Module** (2110 lines, 100% complete) - Real-time and historical crypto data ingestion
2. **Iceberg REST Sink** (6232 lines, 95%+ complete) - High-performance lakehouse data storage

**Objective**: Document proven patterns, create integration templates, identify minimal alignment opportunities.

## Architecture Patterns

### 1. Configuration Patterns

#### Crypto Sources Pattern
```python
@dataclass
class CryptofeedConfig:
    """Type-safe configuration with validation."""
    exchanges: List[str]
    channels: List[str]
    symbols: List[str] = field(default_factory=list)
    auth_provider: AuthProvider = field(default_factory=NoAuth)
    
    def __post_init__(self) -> None:
        # Validation in __post_init__
        self._validate()

# AuthProvider interface pattern
class AuthProvider(ABC):
    @abstractmethod
    def get_credentials(self) -> Dict[str, Any]:
        """Return credential dictionary."""
```

#### Iceberg REST Sink Pattern
```python
@dataclass 
class IcebergConfig:
    """Unified configuration for Iceberg REST sink."""
    catalog_uri: str
    table_name: str
    storage: StorageConfig
    
    def validate(self) -> None:
        # Explicit validation method
        if not self.catalog_uri:
            raise ValueError("catalog_uri required")
```

#### Pattern Analysis
**Similarities:**
- Both use dataclasses with type hints
- Both implement validation
- Both use factory functions

**Differences:**
- Validation timing: `__post_init__` vs explicit `validate()`
- Authentication: AuthProvider interface vs dictionary
- Structure: flat vs nested configuration

### 2. Error Handling Patterns

#### Crypto Sources Pattern
```python
class CryptoSourceError(Exception):
    """Base exception with context."""
    def __init__(self, message: str, source: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.source = source
        self.context = context or {}

# Convenience functions
def connection_error(source: str, original_error: Exception) -> CryptoSourceConnectionError:
    """Create standardized connection error."""
    return CryptoSourceConnectionError(
        f"{source} connection failed: {str(original_error)}",
        source=source,
        context={"original": str(original_error)}
    )
```

#### Iceberg REST Sink Pattern
```python
class IcebergRESTError(Exception):
    """Base exception for Iceberg operations."""
    pass

class ConfigurationError(IcebergRESTError):
    """Configuration validation errors."""
    pass

class NetworkError(IcebergRESTError):
    """Network and connection errors."""
    pass
```

#### Pattern Analysis
**Similarities:**
- Both use hierarchical exception types
- Both provide operation-specific context

**Differences:**
- Context handling: structured context vs simple inheritance
- Convenience functions: crypto sources have them, Iceberg doesn't

### 3. Testing Patterns

#### Shared Testing Approach
```python
# No-mocks pattern from both components
class TestComponent:
    def test_configuration_validation(self):
        """Test with real configuration objects."""
        config = ComponentConfig(required_field="test")
        config.validate()  # Real validation
        
    def test_integration_real_service(self):
        """Test with real services using test containers."""
        # Use real implementations, not mocks
        pass
```

**Common Patterns:**
- TDD with comprehensive test coverage
- Real implementation testing (no mocks)
- Configuration validation tests
- Integration tests with test containers

### 4. Integration Pattern

#### Current Integration
```python
from quixstreams import Application
from quixstreams.sources.community.crypto import CryptofeedSource, create_cryptofeed_config
from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_config

app = Application(broker_address="localhost:9092")

# Source configuration
source_config = create_cryptofeed_config(
    exchanges=["binance"],
    channels=["trades"],
    symbols=["BTCUSDT"]
)

# Sink configuration  
sink_config = create_local_config(table_name="crypto.binance_trades")

# Integration
source = CryptofeedSource(source_config)
sink = IcebergRESTSink(sink_config)

topic = app.topic("crypto.trades")
sdf = app.dataframe(topic)
sdf.sink(sink)

app.run(source)
```

**Integration Strengths:**
- Factory functions simplify configuration
- Schema compatibility works automatically
- Minimal configuration required

## Simple Improvements

### 1. Authentication Pattern Alignment
**Current**: Crypto sources use AuthProvider interface, Iceberg sink uses auth dictionary
**Improvement**: Consider AuthProvider interface adoption in Iceberg sink
**Effort**: Low (1-2 days)
**Benefit**: Consistent authentication pattern

### 2. Error Context Standardization  
**Current**: Different context parameter patterns
**Improvement**: Standardize context dictionary format
**Effort**: Low (1-2 days)
**Benefit**: Consistent error debugging

### 3. Integration Templates
**Current**: Manual configuration required
**Improvement**: Pre-built templates for common crypto patterns
**Effort**: Medium (3-5 days)
**Benefit**: Faster pipeline deployment

### 4. Shared Testing Utilities
**Current**: Similar test patterns in both components
**Improvement**: Extract common test fixtures
**Effort**: Medium (3-5 days)
**Benefit**: Reduced test code duplication

## Template Design

### Real-time Trading Template
```yaml
# templates/real-time-trading.yaml
crypto_source:
  type: cryptofeed
  exchanges: ["binance"]
  channels: ["trades"]
  symbols: ["BTCUSDT", "ETHUSDT"]
  
iceberg_sink:
  catalog_uri: "http://localhost:8181"
  table_name: "crypto.realtime_trades"
  
kafka:
  bootstrap_servers: "localhost:9092"
```

### Historical Analysis Template
```yaml
# templates/historical-analysis.yaml
crypto_source:
  type: binance_s3
  bucket: "binance-public-data"
  prefix: "data/spot/daily/trades/BTCUSDT/"
  date_from: "2024-01-01"
  
iceberg_sink:
  catalog_uri: "http://localhost:8181"
  table_name: "crypto.historical_trades"
```

## Implementation Approach

### Phase 1: Documentation (Week 1)
1. Document configuration patterns from both components
2. Document error handling patterns
3. Document testing patterns

### Phase 2: Templates (Week 2)
1. Create integration templates for common patterns
2. Validate templates with real crypto data
3. Document template usage

### Phase 3: Simple Improvements (Week 3)
1. Minor authentication pattern alignment if beneficial
2. Error context standardization if helpful
3. Shared testing utilities extraction

## Summary

**Documented Patterns:**
- **Configuration**: Dataclass-based with validation
- **Error Handling**: Hierarchical exceptions with context
- **Testing**: TDD with real implementations
- **Integration**: Factory functions and minimal configuration

**Simple Improvements:**
- Authentication pattern alignment
- Error context standardization  
- Integration templates
- Shared testing utilities

**Engineering Principles Applied:**
- **KISS**: Simple pattern documentation
- **YAGNI**: Focus on actual needs, no speculative architecture
- **START SMALL**: Minimal improvements only
- **FRs over NFRs**: Functional patterns, not performance optimization