# Unified Configuration System for Crypto Sources

This document describes the new unified configuration system for crypto sources, following SOLID, KISS, and DRY principles similar to the Iceberg sink refactor.

## Overview

The unified configuration system provides:
- **Type-safe configuration** using dataclasses with validation
- **Unified authentication** across all crypto sources
- **Standardized retry logic** with exponential backoff
- **Environment variable support** for deployment flexibility
- **Full backward compatibility** with deprecation warnings
- **Comprehensive error handling** with standardized exception hierarchy

## Quick Start

### Using the New Configuration System

```python
from quixstreams.sources.community.crypto.config import (
    CryptofeedConfig, 
    CCXTConfig, 
    BinanceS3Config,
    APIKeyAuth,
    RetryConfig
)
from quixstreams.sources.community.crypto import (
    CryptofeedSource,
    CCXTSource, 
    BinanceS3Source
)

# Cryptofeed source with new config
config = CryptofeedConfig(
    exchanges=["binance", "coinbase"],
    channels=["trades", "ticker"],
    symbols=["BTC-USD", "ETH-USD"],
    normalize=True,
    auth_provider=APIKeyAuth(api_key="your_key"),
    retry_config=RetryConfig(max_retries=5, base_delay=1.0)
)
source = CryptofeedSource(config)

# CCXT source with new config
config = CCXTConfig(
    exchange="binance",
    mode="trades",
    symbols=["BTC/USDT", "ETH/USDT"],
    rate_limit=True,
    normalize=True
)
source = CCXTSource(config)

# Binance S3 source with new config
config = BinanceS3Config(
    bucket="binance-public-data",
    prefix="data/spot/daily/trades/BTCUSDT/",
    unsigned=True,
    datatype="trades"
)
source = BinanceS3Source(config)
```

### Environment Variable Support

```bash
# Set environment variables
export CRYPTO_EXCHANGES="binance,coinbase"
export CRYPTO_CHANNELS="trades,ticker"
export CRYPTO_SYMBOLS="BTC-USD,ETH-USD"
export CRYPTO_NORMALIZE="true"
export CRYPTO_RECONNECT="true"

# Load from environment
from quixstreams.sources.community.crypto.config import load_from_env, CryptofeedConfig

config = load_from_env(CryptofeedConfig)
source = CryptofeedSource(config)
```

## Migration Guide

### Migrating from Old Constructor Pattern

**Before (Deprecated):**
```python
# Old way - will show deprecation warning
source = CryptofeedSource(
    exchanges=["binance"],
    channels=["trades"],
    symbols=["BTC-USD"],
    normalize=True,
    reconnect=True,
    max_retries=3
)
```

**After (Recommended):**
```python
# New way - using configuration object
from quixstreams.sources.community.crypto.config import CryptofeedConfig, RetryConfig

config = CryptofeedConfig(
    exchanges=["binance"],
    channels=["trades"],
    symbols=["BTC-USD"],
    normalize=True,
    retry_config=RetryConfig(
        enabled=True,
        max_retries=3
    )
)
source = CryptofeedSource(config)
```

### Authentication Migration

**Before:**
```python
# Old way - credentials mixed with other parameters
source = BinanceS3Source(
    bucket="my-bucket",
    prefix="data/",
    aws_access_key_id="ACCESS_KEY",
    aws_secret_access_key="SECRET_KEY",
    region_name="us-west-2"
)
```

**After:**
```python
# New way - dedicated authentication provider
from quixstreams.sources.community.crypto.config import BinanceS3Config, AWSAuth

config = BinanceS3Config(
    bucket="my-bucket",
    prefix="data/",
    auth_provider=AWSAuth(
        aws_access_key_id="ACCESS_KEY",
        aws_secret_access_key="SECRET_KEY",
        region_name="us-west-2"
    )
)
source = BinanceS3Source(config)
```

## Configuration Reference

### Base Configuration

All source configurations inherit from `BaseSourceConfig`:

```python
@dataclass(frozen=True)
class BaseSourceConfig:
    name: Optional[str] = None
    normalize: bool = True
    auth_provider: AuthProvider = field(default_factory=NoAuth)
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    shutdown_timeout: float = 10.0
```

### CryptofeedConfig

Configuration for real-time Cryptofeed sources:

```python
@dataclass(frozen=True)
class CryptofeedConfig(BaseSourceConfig):
    exchanges: List[str]           # Required: ["binance", "coinbase"]
    channels: List[str]            # Required: ["trades", "ticker"]  
    symbols: List[str]             # Optional: ["BTC-USD", "ETH-USD"]
    reconnect: bool = True         # Enable reconnection on disconnect
```

**Example:**
```python
config = CryptofeedConfig(
    exchanges=["binance", "kraken"],
    channels=["trades", "ticker"],
    symbols=["BTC-USD", "ETH-USD", "DOT-USD"],
    reconnect=True,
    normalize=True
)
```

### CCXTConfig

Configuration for CCXT-based sources:

```python
@dataclass(frozen=True)
class CCXTConfig(BaseSourceConfig):
    exchange: str                  # Required: "binance"
    mode: str                      # Required: "trades", "klines", "orderbook"
    symbols: List[str]             # Required: ["BTC/USDT", "ETH/USDT"]
    interval: Optional[str] = None # For klines: "1m", "5m", "1h", "1d"
    use_ws: bool = False           # Use WebSocket (future)
    rest_poll_interval: float = 0.0 # REST polling interval
    rate_limit: bool = True        # Respect exchange rate limits
```

**Examples:**
```python
# Trades config
config = CCXTConfig(
    exchange="binance",
    mode="trades",
    symbols=["BTC/USDT", "ETH/USDT"],
    rate_limit=True
)

# Klines config  
config = CCXTConfig(
    exchange="coinbase", 
    mode="klines",
    symbols=["BTC-USD"],
    interval="1h",
    rest_poll_interval=60.0
)

# Orderbook config
config = CCXTConfig(
    exchange="kraken",
    mode="orderbook", 
    symbols=["BTC/EUR"],
    rest_poll_interval=5.0
)
```

### BinanceS3Config

Configuration for Binance S3 archive sources:

```python
@dataclass(frozen=True)
class BinanceS3Config(BaseSourceConfig):
    bucket: str                           # Required: S3 bucket name
    prefix: str                           # Required: Object prefix
    unsigned: bool = False                # Use unsigned requests (public data)
    file_format: str = "infer"           # "csv", "jsonl", or "infer"
    compression: Optional[str] = "infer"  # "gzip", "zip", or "infer"
    datatype: str = "trades"             # Data type identifier
    replay_speed: float = 0.0            # Replay speed multiplier
    
    # Advanced options
    access_mode: str = "direct_prefix"    # "direct_prefix" or "templated_prefixes"
    prefix_template: Optional[str] = None # Template for prefix expansion
    # ... (other S3 specific options)
```

**Examples:**
```python
# Simple public data access
config = BinanceS3Config(
    bucket="binance-public-data",
    prefix="data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2023-01-01.zip",
    unsigned=True,
    datatype="trades"
)

# Templated access for multiple symbols/dates
config = BinanceS3Config(
    bucket="binance-public-data", 
    prefix="data/",
    access_mode="templated_prefixes",
    prefix_template="{root}{market}/daily/{datatype}/{symbol}/{symbol}-{datatype}-{date}.zip",
    market="spot",
    symbols=["BTCUSDT", "ETHUSDT"],
    date_from="2023-01-01",
    date_to="2023-01-31",
    unsigned=True
)
```

## Authentication Providers

### NoAuth (Default)

For public data sources requiring no authentication:

```python
from quixstreams.sources.community.crypto.config import NoAuth

config = CryptofeedConfig(
    exchanges=["binance"],
    channels=["trades"],
    auth_provider=NoAuth()  # Default
)
```

### APIKeyAuth

For exchange APIs requiring API keys:

```python
from quixstreams.sources.community.crypto.config import APIKeyAuth

config = CCXTConfig(
    exchange="binance",
    mode="trades", 
    symbols=["BTC/USDT"],
    auth_provider=APIKeyAuth(
        api_key="your_api_key",
        api_secret="your_api_secret"  # Optional
    )
)
```

### AWSAuth

For S3-compatible storage authentication:

```python
from quixstreams.sources.community.crypto.config import AWSAuth

config = BinanceS3Config(
    bucket="private-bucket",
    prefix="data/",
    auth_provider=AWSAuth(
        aws_access_key_id="ACCESS_KEY",
        aws_secret_access_key="SECRET_KEY",
        region_name="us-west-2",
        endpoint_url="https://s3.amazonaws.com"  # Optional
    )
)
```

### Custom Authentication

You can create custom authentication providers:

```python
from quixstreams.sources.community.crypto.config import AuthProvider

class CustomAuth(AuthProvider):
    def __init__(self, token: str):
        self.token = token
    
    def get_credentials(self) -> Dict[str, Any]:
        return {"authorization": f"Bearer {self.token}"}

config = CryptofeedConfig(
    exchanges=["custom_exchange"],
    channels=["trades"],
    auth_provider=CustomAuth("your_token")
)
```

## Retry Configuration

Configure retry behavior with exponential backoff:

```python
from quixstreams.sources.community.crypto.config import RetryConfig

retry_config = RetryConfig(
    enabled=True,           # Enable retries
    max_retries=5,         # Maximum retry attempts
    base_delay=1.0,        # Initial delay (seconds)
    max_delay=30.0,        # Maximum delay (seconds)
    backoff_factor=2.0     # Exponential backoff multiplier
)

config = CryptofeedConfig(
    exchanges=["binance"],
    channels=["trades"], 
    retry_config=retry_config
)
```

## Error Handling

The unified system provides a comprehensive error hierarchy:

```python
from quixstreams.sources.community.crypto.errors import (
    CryptoSourceError,           # Base error
    CryptoSourceConfigError,     # Configuration errors
    CryptoSourceConnectionError, # Network/connection errors
    CryptoSourceRateLimitError,  # Rate limiting errors
    CryptoSourceTimeoutError,    # Timeout errors
    CryptoSourceDataError,       # Data parsing errors
    CryptoSourceDependencyError  # Missing dependency errors
)

try:
    source = CryptofeedSource(config)
    source.setup()
except CryptoSourceDependencyError as e:
    print(f"Missing dependency: {e.package}")
    print(f"Install with: {e.install_command}")
except CryptoSourceConfigError as e:
    print(f"Configuration error: {e}")
except CryptoSourceError as e:
    print(f"General crypto source error: {e}")
    print(f"Source: {e.source}")
    print(f"Context: {e.context}")
```

## Advanced Usage

### Loading from External Configuration Files

```python
import json
from quixstreams.sources.community.crypto.config import CryptofeedConfig

# Load from JSON file
with open("crypto_config.json") as f:
    config_data = json.load(f)

config = CryptofeedConfig(**config_data)
```

### Dynamic Configuration

```python
def create_dynamic_config(symbols: List[str]) -> CCXTConfig:
    """Create configuration dynamically based on runtime parameters."""
    return CCXTConfig(
        exchange="binance",
        mode="trades",
        symbols=symbols,
        rate_limit=True,
        retry_config=RetryConfig(
            max_retries=len(symbols)  # Scale retries with symbol count
        )
    )

config = create_dynamic_config(["BTC/USDT", "ETH/USDT", "DOT/USDT"])
```

### Configuration Validation

```python
def validate_config(config: CryptofeedConfig) -> None:
    """Custom validation logic."""
    if "binance" in config.exchanges and len(config.symbols) > 100:
        raise ValueError("Binance supports max 100 symbols per connection")
    
    for channel in config.channels:
        if channel not in ["trades", "ticker", "book"]:
            raise ValueError(f"Unsupported channel: {channel}")

try:
    config = CryptofeedConfig(exchanges=["binance"], channels=["trades"])
    validate_config(config)
except ValueError as e:
    print(f"Validation error: {e}")
```

## Best Practices

### 1. Use Configuration Objects
- Always prefer configuration objects over individual parameters
- Take advantage of type safety and validation

### 2. Environment Variables for Deployment
- Use environment variables for sensitive data (API keys, credentials)
- Load base configuration from environment, override specific values as needed

### 3. Custom Authentication Providers
- Implement custom `AuthProvider` classes for specialized authentication
- Keep authentication logic separate from source configuration

### 4. Error Handling
- Catch specific error types rather than generic exceptions
- Use error context information for debugging

### 5. Retry Configuration
- Configure appropriate retry behavior based on your use case
- Consider rate limits when setting retry parameters

### 6. Validation
- Validate configurations early in your application startup
- Use the built-in validation provided by dataclasses

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```
   ImportError: CryptofeedSource requires 'cryptofeed'. Install: pip install quixstreams[crypto]
   ```
   **Solution:** Install the required optional dependencies

2. **Configuration Validation Errors**
   ```
   ValueError: exchanges cannot be empty
   ```
   **Solution:** Ensure all required fields are provided

3. **Environment Variable Loading**
   ```
   ValueError: Either config or exchanges/channels parameters must be provided
   ```
   **Solution:** Set required environment variables or provide config object

4. **Deprecation Warnings**
   ```
   DeprecationWarning: Using individual parameters is deprecated. Use CryptofeedConfig instead.
   ```
   **Solution:** Migrate to using configuration objects

### Debug Mode

Enable debug logging to troubleshoot configuration issues:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Configuration and source creation will now provide detailed logs
config = CryptofeedConfig(exchanges=["binance"], channels=["trades"])
source = CryptofeedSource(config)
```

## Migration Timeline

- **Phase 1 (Current):** New configuration system available alongside old parameters
- **Phase 2 (Next release):** Deprecation warnings for old parameter usage
- **Phase 3 (Future release):** Remove old parameter support (breaking change)

We recommend migrating to the new configuration system as soon as possible to take advantage of improved type safety, validation, and features.

## Contributing

When adding new crypto sources or extending existing ones:

1. Follow the established configuration patterns
2. Extend `BaseSourceConfig` for new source configurations
3. Implement appropriate `AuthProvider` if needed
4. Add comprehensive tests following the existing test patterns
5. Update this documentation with examples

For questions or issues, please refer to the project's issue tracker.