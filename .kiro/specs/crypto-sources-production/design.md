# Technical Design Document

## Engineering Principles

This implementation strictly follows project engineering principles:
- **KISS**: Simple, focused source classes with clear data flow patterns
- **SOLID**: Single responsibility per source, unified configuration interface, dependency injection
- **DRY**: Shared configuration system, common error handling, reusable normalization utilities
- **YAGNI**: Built for actual crypto data use cases, no speculative features
- **NO MOCKS**: Tests use real implementations with dependency injection
- **NO LEGACY**: Clean dataclass-based configuration, deprecation warnings for old APIs
- **START SMALL**: Three focused source types, extensible architecture
- **CONSISTENT NAMING**: CryptofeedSource, CCXTSource, BinanceS3Source (no prefixes)
- **TDD**: Test-first development with comprehensive coverage
- **FRs over NFRs**: Focus on data ingestion functionality, performance optimization when needed

## System Overview

The Crypto Sources module provides three specialized source implementations for different crypto data access patterns:

1. **CryptofeedSource**: Real-time websocket data from multiple exchanges
2. **CCXTSource**: REST API polling with optional websocket upgrades  
3. **BinanceS3Source**: Historical data replay from S3 archives

All sources share a unified configuration system, error handling, and data normalization layer.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Crypto Sources Module                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │  Cryptofeed     │  │      CCXT       │  │   Binance S3    │ │
│  │    Source       │  │     Source      │  │     Source      │ │
│  │  (Websockets)   │  │  (REST/WS)      │  │  (Historical)   │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              Unified Configuration System                   │ │
│  │  Config Classes │ Auth Providers │ Validation │ Env Loading │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                Shared Infrastructure                        │ │
│  │  Error Handling │ Retry Logic │ Normalization │ Utilities   │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
                    ┌─────────────────────┐
                    │   QuixStreams       │
                    │     Topics          │
                    └─────────────────────┘
```

## Core Components

### 1. Source Implementations

#### CryptofeedSource (`cryptofeed_source.py`)

**Single Responsibility**: Real-time crypto data ingestion via websockets using the cryptofeed library.

```python
class CryptofeedSource(Source):
    """Real-time crypto data source using cryptofeed websockets."""
    
    def __init__(self, config: CryptofeedConfig, topic_builder: TopicBuilder):
        self.config = config
        self.topic_builder = topic_builder
        self._feed_handler = None
        
    def setup(self) -> None:
        """Setup cryptofeed FeedHandler with exchange callbacks."""
        # Lazy import cryptofeed
        # Configure exchanges, channels, symbols
        # Register normalized data callbacks
        
    def run(self) -> None:
        """Start websocket connections with retry logic."""
        # Start feed handler
        # Handle disconnections with exponential backoff
        # Emit normalized data to topics
```

**Key Features**:
- Multiple exchange support (Binance, Kraken, Coinbase, etc.)
- Automatic reconnection with configurable retry logic
- Data normalization for trades, tickers, orderbooks, klines
- Exchange-specific topic routing
- Graceful error handling and recovery

#### CCXTSource (`ccxt_source.py`)

**Single Responsibility**: Flexible crypto data access via REST APIs with optional websocket upgrades.

```python
class CCXTSource(Source):
    """Crypto data source using CCXT library for REST/WebSocket access."""
    
    def __init__(self, config: CCXTConfig, topic_builder: TopicBuilder):
        self.config = config
        self.topic_builder = topic_builder
        self._exchange = None
        
    def setup(self) -> None:
        """Setup CCXT exchange client with authentication."""
        # Initialize exchange with config
        # Configure rate limiting
        # Setup authentication if required
        
    def run(self) -> None:
        """Poll REST endpoints or maintain websocket connections."""
        # REST polling mode or websocket streaming
        # Respect rate limits
        # Emit normalized data
```

**Key Features**:
- Dual REST/WebSocket modes
- Comprehensive exchange coverage via CCXT
- Built-in rate limiting and retry logic
- Authentication support for private endpoints
- Configurable polling intervals

#### BinanceS3Source (`binance_s3_source.py`)

**Single Responsibility**: Historical crypto data replay from Binance public S3 archives.

```python
class BinanceS3Source(Source):
    """Historical crypto data replay from Binance S3 archives."""
    
    def __init__(self, config: BinanceS3Config, topic_builder: TopicBuilder):
        self.config = config
        self.topic_builder = topic_builder
        self._s3_client = None
        
    def setup(self) -> None:
        """Setup S3 client and validate access."""
        # Configure AWS/S3-compatible client
        # Validate bucket access
        # Build file list from prefixes
        
    def run(self) -> None:
        """Process historical files with optional replay throttling."""
        # Iterate through historical files
        # Apply replay speed throttling
        # Validate checksums if enabled
        # Emit historical data
```

**Key Features**:
- Multiple data types (trades, klines, ticker, orderbook)
- Date range filtering
- Replay speed control for simulation
- Checksum validation for data integrity
- Template-based prefix handling

### 2. Unified Configuration System (`config.py`)

**Single Responsibility**: Type-safe, validated configuration for all crypto sources.

```python
@dataclass
class CryptofeedConfig:
    """Configuration for real-time websocket data ingestion."""
    exchanges: List[str]
    channels: List[str]
    symbols: List[str] = field(default_factory=list)
    auth_provider: AuthProvider = field(default_factory=NoAuth)
    reconnect: bool = True
    normalize: bool = True
    retry_config: RetryConfig = field(default_factory=RetryConfig)

@dataclass  
class CCXTConfig:
    """Configuration for REST API and websocket access."""
    exchange: str
    mode: str  # 'trades', 'ticker', 'orderbook', 'klines'
    symbols: List[str]
    interval: Optional[str] = None
    rate_limit: bool = True
    use_websocket: bool = False
    auth_provider: AuthProvider = field(default_factory=NoAuth)

@dataclass
class BinanceS3Config:
    """Configuration for historical data replay."""
    bucket: str
    prefix: str
    datatype: str = "trades"
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    replay_speed: float = 0.0  # 0 = unlimited
    auth_provider: AuthProvider = field(default_factory=lambda: AWSAuth("", ""))
```

#### Authentication Providers

```python
class AuthProvider(ABC):
    """Base authentication provider interface."""
    
    @abstractmethod
    def get_credentials(self) -> Dict[str, Any]:
        """Return credentials dictionary for downstream clients."""

@dataclass
class NoAuth(AuthProvider):
    """No authentication required."""
    
@dataclass  
class APIKeyAuth(AuthProvider):
    """API key authentication for exchanges."""
    api_key: str
    api_secret: Optional[str] = None
    
@dataclass
class AWSAuth(AuthProvider):  
    """AWS credentials for S3 access."""
    access_key_id: str
    secret_access_key: str
    region_name: str = "us-east-1"
    session_token: Optional[str] = None
```

### 3. Error Handling System (`errors.py`)

**Single Responsibility**: Structured, hierarchical error handling with context.

```python
class CryptoSourceError(Exception):
    """Base exception with source context."""
    
class CryptoSourceConfigError(CryptoSourceError):
    """Configuration validation errors."""
    
class CryptoSourceConnectionError(CryptoSourceError):
    """Network and connection errors with retry information."""
    
class CryptoSourceRateLimitError(CryptoSourceConnectionError):
    """Rate limit errors with retry-after info."""
    
class CryptoSourceDependencyError(CryptoSourceError, ImportError):
    """Missing dependency errors with installation guidance."""
```

**Convenience Functions**:
```python
def missing_dependency_error(package: str, source_name: str) -> CryptoSourceDependencyError:
    """Generate standardized dependency error with install command."""
    
def connection_error(source: str, original_error: Exception) -> CryptoSourceConnectionError:
    """Wrap connection errors with retry context."""
    
def rate_limit_error(source: str, retry_after: float) -> CryptoSourceRateLimitError:
    """Generate rate limit error with backoff information."""
```

### 4. Retry Logic (`retry.py`)

**Single Responsibility**: Configurable exponential backoff for resilient connections.

```python
@dataclass
class RetryConfig:
    """Retry configuration with exponential backoff."""
    enabled: bool = True
    base_delay: float = 0.5
    max_delay: float = 30.0
    backoff_factor: float = 2.0
    max_retries: int = 3

def retry_with_backoff(func: Callable, retry_config: RetryConfig) -> Any:
    """Execute function with configurable retry logic."""
    # Implement exponential backoff
    # Respect max_retries and delay limits
    # Handle retryable vs non-retryable errors
```

### 5. Data Normalization (`utils.py`)

**Single Responsibility**: Consistent data schemas across exchanges and sources.

```python
def normalize_trade(data: Dict[str, Any], exchange: str) -> Dict[str, Any]:
    """Normalize trade data to standard schema."""
    return {
        "exchange": exchange,
        "symbol": data.get("symbol"),
        "trade_id": data.get("id"),
        "side": data.get("side"),
        "price": float(data.get("price", 0)),
        "qty": float(data.get("amount", 0)),
        "ts_event": data.get("timestamp")
    }

def normalize_ticker(data: Dict[str, Any], exchange: str) -> Dict[str, Any]:
    """Normalize ticker data to standard schema."""
    return {
        "exchange": exchange,
        "symbol": data.get("symbol"),
        "bid": float(data.get("bid", 0)),
        "ask": float(data.get("ask", 0)),
        "last": float(data.get("last", 0)),
        "ts_event": data.get("timestamp")
    }
```

## Integration Patterns

### Environment Configuration

```python
# Load from environment variables
config = load_from_env("CRYPTOFEED")  # Loads CRYPTOFEED_* env vars

# Factory functions for common patterns
config = create_cryptofeed_config(
    exchanges=["binance", "kraken"],
    channels=["trades", "ticker"],
    symbols=["BTCUSDT", "ETHUSDT"]
)

config = create_ccxt_config(
    exchange="binance",
    mode="trades", 
    symbols=["BTC/USDT"]
)

config = create_binance_s3_config(
    bucket="binance-public-data",
    prefix="data/spot/daily/trades/BTCUSDT/",
    date_from="2024-01-01"
)
```

### QuixStreams Integration

```python
from quixstreams import Application
from quixstreams.sources.community.crypto import CryptofeedSource, create_cryptofeed_config

app = Application(broker_address="localhost:9092")

config = create_cryptofeed_config(
    exchanges=["binance"],
    channels=["trades"],
    symbols=["BTCUSDT"]
)

source = CryptofeedSource(config)
topic = app.topic("crypto.trades")

sdf = app.dataframe(topic)
sdf = sdf.filter(lambda x: x["price"] > 0)  # Data validation
sdf.print()

app.run(source)
```

## Testing Strategy (TDD)

### Test Structure

```python
class TestCryptofeedSource:
    """Test real-time websocket data ingestion."""
    
    def test_config_validation(self):
        """Test configuration validation with invalid inputs."""
        
    def test_source_creation(self):
        """Test source creation with dependency injection."""
        
    def test_normalization(self):
        """Test data normalization across exchanges."""
        
    def test_retry_logic(self):
        """Test exponential backoff with simulated failures."""

class TestUnifiedConfig:
    """Test configuration system with real validation."""
    
    def test_environment_loading(self):
        """Test loading configuration from environment variables."""
        
    def test_auth_providers(self):
        """Test different authentication provider implementations."""
        
    def test_backward_compatibility(self):
        """Test deprecated APIs with proper warnings."""
```

### No Mocks Policy

Tests use real implementations:
- Real configuration classes with validation
- Real error classes with context
- Real retry logic with actual delays (shortened for tests)
- Real normalization functions with exchange data
- Real environment variable loading

## File Structure

```
quixstreams/sources/community/crypto/
├── __init__.py              # Public API exports
├── config.py                # Unified configuration system
├── errors.py                # Error hierarchy
├── retry.py                 # Retry logic and backoff
├── utils.py                 # Normalization and utilities
├── cryptofeed_source.py     # Real-time websocket source
├── ccxt_source.py           # REST/WebSocket API source  
├── binance_s3_source.py     # Historical replay source
├── simple_config.py         # Simplified configuration helpers
├── README_UNIFIED_CONFIG.md # Configuration documentation
├── GREENFIELD_SUMMARY.md    # Implementation summary
└── tests/                   # Comprehensive test suite
    ├── test_config.py
    ├── test_sources.py
    ├── test_errors.py
    └── test_integration.py
```

## Summary

This design follows engineering principles by:

- **KISS**: Three focused source classes with clear responsibilities
- **SOLID**: Unified configuration interface, dependency injection, single responsibility
- **DRY**: Shared error handling, retry logic, and normalization utilities
- **YAGNI**: Built for actual crypto data use cases, no speculative features
- **TDD**: Comprehensive test coverage with real implementations
- **Simple naming**: Clear, descriptive class names without prefixes

The module provides production-ready crypto data ingestion with:
- **Type safety**: Dataclass-based configuration with validation
- **Resilience**: Comprehensive error handling and retry mechanisms
- **Flexibility**: Multiple data access patterns (real-time, REST, historical)
- **Consistency**: Unified configuration and normalized data schemas
- **Extensibility**: Clean interfaces for adding new sources and authentication providers