# Pydantic v2 Integration Plan: Crypto Sources & Iceberg REST Sinks TDD Cycles

## Executive Summary

This document outlines a comprehensive Test-Driven Development (TDD) approach for integrating Pydantic v2 validation across the crypto sources and Iceberg REST sinks ecosystem, following global development rules and agent-based methodology.

## 🎯 Project Scope & Objectives

### Current State Analysis
- ✅ **Iceberg REST Sink**: Pydantic v2 configuration system implemented with backward compatibility
- ⚠️ **Crypto Sources**: Legacy dataclass-based configuration system requiring modernization
- 🔄 **Integration**: Need unified validation approach across both systems

### Target State
- **Unified Pydantic v2 Ecosystem**: Consistent validation patterns across all components
- **Production-Grade Validation**: Enhanced error messages, type safety, environment loading
- **Backward Compatibility**: Seamless migration path for existing configurations
- **Agent-Based Architecture**: Modular, testable, and maintainable codebase

---

## 🏗️ Architecture Overview

### Global Rules & Principles

#### 1. **SOLID Principles**
- **Single Responsibility**: Each Pydantic model handles one configuration concern
- **Open/Closed**: Extensible for new crypto providers without modification
- **Liskov Substitution**: All config models implement common interfaces
- **Interface Segregation**: Provider-specific settings cleanly separated
- **Dependency Inversion**: Abstract interfaces over concrete implementations

#### 2. **Validation Standards**
```python
# Global validation rules for all configurations
ValidationRules = {
    "immutable": True,                    # All configs frozen after creation
    "extra_fields": "forbid",            # Prevent typos and misconfigurations
    "str_strip": True,                   # Clean string inputs automatically
    "validate_assignment": True,         # Validate on attribute changes
    "use_enum_values": True,            # Serialize enums to their values
    "json_encoders": {SecretStr: lambda v: "***"},  # Secure secret display
}
```

#### 3. **Agent-Based Design**
```python
# Each component is an autonomous agent with:
class Agent:
    config: PydanticModel           # Validated configuration
    state: StateModel              # Current operational state  
    actions: List[ActionModel]     # Available operations
    events: EventBus               # Inter-agent communication
    metrics: MetricsCollector      # Observability
```

---

## 📋 TDD Cycle Implementation Plan

### Phase 1: Foundation (RED → GREEN → REFACTOR)

#### 🔴 **RED Phase: Create Comprehensive Test Specifications**

##### Test Structure
```
tests/
├── unit/
│   ├── config/
│   │   ├── test_crypto_config_validation.py
│   │   ├── test_iceberg_config_validation.py
│   │   └── test_unified_validation.py
│   ├── models/
│   │   ├── test_pydantic_models.py
│   │   └── test_serialization.py
│   └── agents/
│       ├── test_crypto_agent.py
│       └── test_iceberg_agent.py
├── integration/
│   ├── test_cross_component_validation.py
│   ├── test_migration_compatibility.py
│   └── test_environment_loading.py
└── e2e/
    ├── test_crypto_to_iceberg_pipeline.py
    └── test_production_scenarios.py
```

##### Core Test Specifications

**File: `tests/unit/config/test_crypto_config_validation.py`**
```python
import pytest
from pydantic import ValidationError
from quixstreams.sources.community.crypto.config_v2 import (
    CryptofeedConfig, CCXTConfig, BinanceS3Config, 
    AuthType, CryptoProvider, RetryPolicy
)

class TestCryptoConfigValidation:
    """Comprehensive validation tests for crypto source configurations."""
    
    def test_cryptofeed_config_requires_exchanges_and_channels(self):
        """CryptofeedConfig should validate required fields with descriptive errors."""
        with pytest.raises(ValidationError) as exc_info:
            CryptofeedConfig()  # Missing required fields
        
        errors = exc_info.value.errors()
        assert any("exchanges" in str(error["loc"]) for error in errors)
        assert any("channels" in str(error["loc"]) for error in errors)
    
    def test_ccxt_config_validates_mode_enum(self):
        """CCXTConfig should validate mode field against enum values."""
        with pytest.raises(ValidationError) as exc_info:
            CCXTConfig(
                exchange="binance",
                symbols=["BTC/USDT"],
                mode="invalid_mode"  # Should fail
            )
        
        error = exc_info.value.errors()[0]
        assert "mode" in str(error["loc"])
        assert "Input should be" in error["msg"]
    
    def test_auth_provider_validation_patterns(self):
        """Auth providers should validate credentials according to their type."""
        # Test API key auth validation
        with pytest.raises(ValidationError):
            APIKeyAuth(api_key="")  # Empty key should fail
        
        # Test AWS auth validation
        with pytest.raises(ValidationError):
            AWSAuth(
                aws_access_key_id="key", 
                aws_secret_access_key=""  # Empty secret should fail
            )
    
    def test_retry_config_validates_constraints(self):
        """RetryConfig should validate logical constraints between fields."""
        with pytest.raises(ValidationError):
            RetryPolicy(
                base_delay=10.0,
                max_delay=5.0  # max_delay < base_delay should fail
            )
    
    def test_environment_variable_loading_type_conversion(self):
        """Environment loading should handle type conversion with validation."""
        import os
        os.environ.update({
            "CRYPTO_EXCHANGES": "invalid_json",  # Should fail JSON parsing
            "CRYPTO_RETRY_MAX_ATTEMPTS": "not_a_number",  # Should fail int conversion
        })
        
        with pytest.raises(ValidationError):
            CryptofeedSettings().model_validate({})
    
    def test_cross_field_validation_rules(self):
        """Configurations should validate logical relationships between fields."""
        with pytest.raises(ValidationError) as exc_info:
            CCXTConfig(
                exchange="binance",
                symbols=["BTC/USDT"],
                mode="klines",
                interval=None  # Required when mode=klines
            )
        
        assert "interval required for klines mode" in str(exc_info.value)

class TestConfigurationSerialization:
    """Test configuration serialization and deserialization."""
    
    def test_config_roundtrip_serialization(self):
        """Configurations should serialize/deserialize without data loss."""
        original = CryptofeedConfig(
            exchanges=["binance", "coinbase"],
            channels=["trades", "ticker"],
            symbols=["BTC-USDT"],
            auth=APIKeyAuth(api_key="test", api_secret="secret")
        )
        
        # Serialize to dict
        serialized = original.model_dump()
        
        # Deserialize back
        restored = CryptofeedConfig.model_validate(serialized)
        
        # Should be equivalent (except secrets are masked)
        assert restored.exchanges == original.exchanges
        assert restored.channels == original.channels
        assert restored.symbols == original.symbols
    
    def test_json_schema_generation(self):
        """All configs should generate valid JSON schemas."""
        schema = CryptofeedConfig.model_json_schema()
        
        assert "properties" in schema
        assert "required" in schema
        assert "exchanges" in schema["properties"]
        assert "channels" in schema["properties"]
    
    def test_secret_field_masking(self):
        """Secret fields should be masked in serialized output."""
        config = CCXTConfig(
            exchange="binance",
            symbols=["BTC/USDT"],
            auth=APIKeyAuth(api_key="public", api_secret="secret")
        )
        
        serialized = config.model_dump()
        # Secrets should be masked but structure preserved
        assert "***" in str(serialized)  # Secret is masked
        assert "public" in str(serialized)  # Non-secret is visible
```

**File: `tests/integration/test_cross_component_validation.py`**
```python
class TestCrossComponentValidation:
    """Test validation patterns across crypto sources and iceberg sinks."""
    
    def test_unified_auth_provider_interface(self):
        """Auth providers should work consistently across all components."""
        aws_auth = AWSAuth(
            aws_access_key_id="key",
            aws_secret_access_key="secret",
            region_name="us-east-1"
        )
        
        # Should work with crypto sources
        crypto_config = BinanceS3Config(
            bucket="crypto-data",
            prefix="trades/",
            auth=aws_auth
        )
        
        # Should work with iceberg sinks  
        iceberg_config = IcebergConfig(
            table_name="crypto.trades",
            catalog_uri="http://localhost:8181/api/v1",
            warehouse_id="crypto",
            storage=StorageConfig(
                provider=StorageProvider.AWS,
                region="us-east-1",
                access_key_id="key",
                secret_access_key="secret"
            )
        )
        
        # Both should validate successfully
        assert crypto_config.auth.aws_access_key_id == "key"
        assert iceberg_config.storage.access_key_id == "key"
    
    def test_configuration_migration_compatibility(self):
        """Legacy configurations should migrate to Pydantic v2 seamlessly."""
        from quixstreams.sources.community.crypto.config import CryptofeedConfig as LegacyConfig
        from quixstreams.sources.community.crypto.config_v2 import migrate_from_legacy
        
        # Create legacy config
        legacy = LegacyConfig(
            exchanges=["binance"],
            channels=["trades"],
            symbols=["BTC-USDT"]
        )
        
        # Migrate to Pydantic v2
        modern = migrate_from_legacy(legacy)
        
        # Should preserve all data and add enhancements
        assert modern.exchanges == ["binance"]
        assert modern.channels == ["trades"] 
        assert modern.symbols == ["BTC-USDT"]
        assert isinstance(modern.auth, NoAuth)  # Default auth provider
        assert isinstance(modern.retry, RetryPolicy)  # Default retry config
```

##### Expected Test Results (RED Phase)
```bash
# All tests should FAIL initially - this is correct for TDD
pytest tests/unit/config/ -v
# Expected: 45 failed, 0 passed

pytest tests/integration/ -v  
# Expected: 23 failed, 0 passed

pytest tests/e2e/ -v
# Expected: 12 failed, 0 passed
```

#### 🟢 **GREEN Phase: Implement Pydantic v2 Configuration System**

##### Core Implementation Files

**File: `quixstreams/sources/community/crypto/config_v2.py`**
```python
"""
Pydantic v2 Configuration System for Crypto Sources

Modern, type-safe configuration system with comprehensive validation,
environment loading, and production-ready features.
"""

import os
import warnings
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union, Annotated

from pydantic import (
    BaseModel, ConfigDict, Field, SecretStr, 
    computed_field, field_validator, model_validator
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from quixstreams.utils.settings import BaseSettings as QuixBaseSettings

__all__ = [
    # Core Models
    "CryptofeedConfig", "CCXTConfig", "BinanceS3Config",
    # Auth Providers 
    "AuthProvider", "NoAuth", "APIKeyAuth", "AWSAuth",
    # Enums
    "CryptoProvider", "AuthType", "OperationalMode",
    # Settings Classes
    "CryptofeedSettings", "CCXTSettings", "BinanceS3Settings",
    # Utilities
    "create_config", "load_from_env", "validate_config"
]

class CryptoProvider(str, Enum):
    """Supported crypto data providers with validation."""
    CRYPTOFEED = "cryptofeed"
    CCXT = "ccxt"
    BINANCE_S3 = "binance_s3"
    
class AuthType(str, Enum):
    """Authentication types for crypto providers."""
    NONE = "none"
    API_KEY = "api_key" 
    AWS = "aws"
    OAUTH = "oauth"

class OperationalMode(str, Enum):
    """Operational modes for crypto sources."""
    TRADES = "trades"
    KLINES = "klines" 
    ORDERBOOK = "orderbook"
    TICKER = "ticker"

# Base Auth Provider Models
class AuthProvider(BaseModel):
    """Base class for all authentication providers."""
    model_config = ConfigDict(
        frozen=True,
        extra='forbid',
        str_strip_whitespace=True,
        validate_assignment=True,
    )
    
    auth_type: AuthType = AuthType.NONE
    
    @computed_field
    @property
    def credentials(self) -> Dict[str, Any]:
        """Get sanitized credentials dict."""
        return self.get_credentials()
    
    def get_credentials(self) -> Dict[str, Any]:
        """Override in subclasses to provide credentials."""
        return {}

class NoAuth(AuthProvider):
    """No authentication for public data sources."""
    auth_type: AuthType = Field(default=AuthType.NONE, frozen=True)

class APIKeyAuth(AuthProvider):
    """API key-based authentication with validation."""
    model_config = ConfigDict(
        frozen=True,
        extra='forbid',
        str_strip_whitespace=True,
    )
    
    auth_type: AuthType = Field(default=AuthType.API_KEY, frozen=True)
    api_key: str = Field(..., min_length=1, description="API key for authentication")
    api_secret: Optional[SecretStr] = Field(default=None, description="API secret")
    passphrase: Optional[SecretStr] = Field(default=None, description="API passphrase")
    
    @field_validator('api_key')
    @classmethod
    def validate_api_key(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("API key cannot be empty")
        return v.strip()
    
    def get_credentials(self) -> Dict[str, Any]:
        creds = {"api_key": self.api_key}
        if self.api_secret:
            creds["api_secret"] = self.api_secret.get_secret_value()
        if self.passphrase:
            creds["passphrase"] = self.passphrase.get_secret_value()
        return creds

class AWSAuth(AuthProvider):
    """AWS authentication with S3 compatibility."""
    model_config = ConfigDict(
        frozen=True,
        extra='forbid',
        str_strip_whitespace=True,
    )
    
    auth_type: AuthType = Field(default=AuthType.AWS, frozen=True)
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[SecretStr] = None
    region_name: str = Field(default="us-east-1")
    endpoint_url: Optional[str] = None
    
    @field_validator('region_name')
    @classmethod
    def validate_region(cls, v: str) -> str:
        if not v:
            raise ValueError("Region name cannot be empty")
        return v
    
    def get_credentials(self) -> Dict[str, Any]:
        creds = {"region_name": self.region_name}
        if self.aws_access_key_id:
            creds["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            creds["aws_secret_access_key"] = self.aws_secret_access_key.get_secret_value()
        if self.endpoint_url:
            creds["endpoint_url"] = self.endpoint_url
        return creds

# Retry Configuration
class RetryPolicy(BaseModel):
    """Retry policy configuration with validation."""
    model_config = ConfigDict(frozen=True)
    
    enabled: bool = True
    max_attempts: int = Field(default=3, ge=1, le=10)
    base_delay: float = Field(default=0.5, gt=0, le=60.0)
    max_delay: float = Field(default=30.0, gt=0, le=300.0)
    backoff_factor: float = Field(default=2.0, gt=1.0, le=10.0)
    
    @model_validator(mode='after')
    def validate_delays(self) -> 'RetryPolicy':
        if self.max_delay <= self.base_delay:
            raise ValueError("max_delay must be greater than base_delay")
        return self

# Base Configuration
class BaseCryptoConfig(BaseModel):
    """Base configuration for all crypto sources."""
    model_config = ConfigDict(
        frozen=True,
        extra='forbid',
        str_strip_whitespace=True,
        validate_assignment=True,
        use_enum_values=True,
    )
    
    name: Optional[str] = Field(default=None, description="Source instance name")
    normalize: bool = Field(default=True, description="Normalize data format")
    auth: AuthProvider = Field(default_factory=NoAuth, description="Authentication provider")
    retry: RetryPolicy = Field(default_factory=RetryPolicy, description="Retry configuration")
    shutdown_timeout: float = Field(default=10.0, gt=0, le=300, description="Shutdown timeout in seconds")

# Provider-Specific Configurations
class CryptofeedConfig(BaseCryptoConfig):
    """Configuration for CryptofeedSource with comprehensive validation."""
    
    provider: CryptoProvider = Field(default=CryptoProvider.CRYPTOFEED, frozen=True)
    exchanges: List[str] = Field(..., min_length=1, description="List of exchanges to connect to")
    channels: List[str] = Field(..., min_length=1, description="Data channels to subscribe to")
    symbols: List[str] = Field(default_factory=list, description="Trading pairs to monitor")
    reconnect: bool = Field(default=True, description="Auto-reconnect on connection loss")
    max_depth: Optional[int] = Field(default=None, ge=1, le=1000, description="Order book depth")
    
    @field_validator('exchanges')
    @classmethod
    def validate_exchanges(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("At least one exchange must be specified")
        return [exchange.lower().strip() for exchange in v if exchange.strip()]
    
    @field_validator('channels')
    @classmethod
    def validate_channels(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("At least one channel must be specified")
        valid_channels = {"trades", "ticker", "funding", "orderbook", "candles"}
        for channel in v:
            if channel.lower() not in valid_channels:
                raise ValueError(f"Invalid channel: {channel}. Must be one of {valid_channels}")
        return [channel.lower().strip() for channel in v]
    
    @computed_field
    @property
    def connection_string(self) -> str:
        """Generate connection string for debugging."""
        return f"cryptofeed://{','.join(self.exchanges)}/{','.join(self.channels)}"

class CCXTConfig(BaseCryptoConfig):
    """Configuration for CCXTSource with mode-specific validation."""
    
    provider: CryptoProvider = Field(default=CryptoProvider.CCXT, frozen=True)
    exchange: str = Field(..., min_length=1, description="CCXT exchange identifier")
    mode: OperationalMode = Field(default=OperationalMode.TRADES, description="Data collection mode")
    symbols: List[str] = Field(..., min_length=1, description="Trading pairs to collect")
    interval: Optional[str] = Field(default=None, description="Timeframe for klines/candles")
    use_websocket: bool = Field(default=False, description="Use WebSocket connection if available")
    rest_poll_interval: float = Field(default=1.0, ge=0.1, le=3600, description="REST polling interval in seconds")
    rate_limit: bool = Field(default=True, description="Respect exchange rate limits")
    
    @field_validator('exchange')
    @classmethod
    def validate_exchange(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Exchange identifier cannot be empty")
        return v.lower().strip()
    
    @model_validator(mode='after')
    def validate_mode_requirements(self) -> 'CCXTConfig':
        if self.mode == OperationalMode.KLINES and not self.interval:
            raise ValueError("interval is required when mode is 'klines'")
        return self
    
    @computed_field
    @property
    def connection_info(self) -> Dict[str, Any]:
        """Get connection information for the source."""
        return {
            "exchange": self.exchange,
            "mode": self.mode,
            "websocket": self.use_websocket,
            "symbols_count": len(self.symbols)
        }

class BinanceS3Config(BaseCryptoConfig):
    """Configuration for BinanceS3Source with S3 validation."""
    
    provider: CryptoProvider = Field(default=CryptoProvider.BINANCE_S3, frozen=True)
    bucket: str = Field(..., min_length=1, description="S3 bucket name")
    prefix: str = Field(..., min_length=1, description="S3 prefix for data files")
    datatype: str = Field(default="trades", description="Type of data to process")
    file_format: str = Field(default="infer", description="File format (csv, parquet, json)")
    compression: Optional[str] = Field(default="infer", description="Compression type")
    replay_speed: float = Field(default=0.0, ge=0, description="Replay speed multiplier (0 = no delay)")
    
    # Advanced S3 options
    unsigned: bool = Field(default=False, description="Use unsigned S3 requests")
    has_partition_folders: bool = Field(default=False, description="Data is partitioned by folders")
    checksum_mode: str = Field(default="skip", description="Checksum validation mode")
    
    @field_validator('bucket')
    @classmethod
    def validate_bucket(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("S3 bucket name cannot be empty")
        # Basic S3 bucket name validation
        bucket = v.lower().strip()
        if not bucket.replace('-', '').replace('.', '').isalnum():
            raise ValueError("Invalid S3 bucket name format")
        return bucket
    
    @field_validator('prefix')
    @classmethod
    def validate_prefix(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("S3 prefix cannot be empty")
        return v.strip().rstrip('/') + '/'  # Ensure trailing slash
    
    @computed_field
    @property
    def s3_path(self) -> str:
        """Get full S3 path."""
        return f"s3://{self.bucket}/{self.prefix}"

# Configuration Union Type
CryptoConfig = Union[CryptofeedConfig, CCXTConfig, BinanceS3Config]

# Environment Loading Classes
class CryptofeedSettings(QuixBaseSettings):
    """Environment settings for CryptofeedSource."""
    model_config = SettingsConfigDict(
        env_prefix="CRYPTOFEED_",
        env_ignore_empty=True,
        case_sensitive=False,
    )
    
    exchanges: List[str] = Field(default_factory=lambda: ["binance"])
    channels: List[str] = Field(default_factory=lambda: ["trades"])
    symbols: List[str] = Field(default_factory=list)
    reconnect: bool = True
    normalize: bool = True
    
    def to_config(self) -> CryptofeedConfig:
        return CryptofeedConfig(**self.model_dump())

class CCXTSettings(QuixBaseSettings):
    """Environment settings for CCXTSource."""
    model_config = SettingsConfigDict(
        env_prefix="CCXT_",
        env_ignore_empty=True,
        case_sensitive=False,
    )
    
    exchange: str = "binance"
    mode: OperationalMode = OperationalMode.TRADES
    symbols: List[str] = Field(default_factory=lambda: ["BTC/USDT"])
    interval: Optional[str] = None
    use_websocket: bool = False
    rest_poll_interval: float = 1.0
    
    def to_config(self) -> CCXTConfig:
        return CCXTConfig(**self.model_dump())

# Factory Functions
def create_config(
    provider: Union[CryptoProvider, str],
    **kwargs
) -> CryptoConfig:
    """Create a configuration for the specified provider."""
    if isinstance(provider, str):
        provider = CryptoProvider(provider)
    
    config_map = {
        CryptoProvider.CRYPTOFEED: CryptofeedConfig,
        CryptoProvider.CCXT: CCXTConfig,
        CryptoProvider.BINANCE_S3: BinanceS3Config,
    }
    
    config_class = config_map.get(provider)
    if not config_class:
        raise ValueError(f"Unsupported provider: {provider}")
    
    return config_class(**kwargs)

def load_from_env(
    provider: Union[CryptoProvider, str],
    **overrides
) -> CryptoConfig:
    """Load configuration from environment variables."""
    if isinstance(provider, str):
        provider = CryptoProvider(provider)
    
    settings_map = {
        CryptoProvider.CRYPTOFEED: CryptofeedSettings,
        CryptoProvider.CCXT: CCXTSettings,
        # BinanceS3 settings would be added here
    }
    
    settings_class = settings_map.get(provider)
    if not settings_class:
        raise ValueError(f"Environment loading not supported for provider: {provider}")
    
    settings = settings_class(**overrides)
    return settings.to_config()

def validate_config(config: CryptoConfig) -> bool:
    """Validate a crypto configuration."""
    if not isinstance(config, (CryptofeedConfig, CCXTConfig, BinanceS3Config)):
        raise ValueError("Invalid configuration type")
    return True

# Migration utilities
def migrate_from_legacy(legacy_config: Any) -> CryptoConfig:
    """Migrate from legacy dataclass configuration to Pydantic v2."""
    # Implementation would extract data from legacy config
    # and create appropriate Pydantic v2 config
    pass
```

**File: `quixstreams/sources/community/crypto/agents.py`**
```python
"""
Agent-based architecture for crypto sources using Pydantic v2 configurations.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

from .config_v2 import CryptoConfig, CryptofeedConfig, CCXTConfig

class AgentState(str, Enum):
    """Agent operational states."""
    INITIALIZING = "initializing"
    CONNECTED = "connected"
    STREAMING = "streaming"
    RECONNECTING = "reconnecting"
    STOPPED = "stopped"
    ERROR = "error"

@dataclass
class AgentMetrics:
    """Metrics collection for crypto source agents."""
    messages_processed: int = 0
    connections_established: int = 0
    reconnection_attempts: int = 0
    errors_encountered: int = 0
    last_message_timestamp: Optional[float] = None

class CryptoSourceAgent(ABC):
    """Base agent class for crypto sources."""
    
    def __init__(self, config: CryptoConfig):
        self.config = config
        self.state = AgentState.INITIALIZING
        self.metrics = AgentMetrics()
    
    @abstractmethod
    async def start(self) -> None:
        """Start the agent."""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Stop the agent."""
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """Check agent health status."""
        pass

class CryptofeedAgent(CryptoSourceAgent):
    """Agent for CryptofeedSource with Pydantic v2 configuration."""
    
    def __init__(self, config: CryptofeedConfig):
        super().__init__(config)
        self.config: CryptofeedConfig = config  # Type hint for IDE
    
    async def start(self) -> None:
        """Start cryptofeed data streaming."""
        self.state = AgentState.CONNECTED
        # Implementation would use self.config.exchanges, etc.
    
    async def stop(self) -> None:
        """Stop cryptofeed streaming."""
        self.state = AgentState.STOPPED
    
    async def health_check(self) -> Dict[str, Any]:
        """Check cryptofeed agent health."""
        return {
            "agent_type": "cryptofeed",
            "state": self.state.value,
            "config_valid": True,
            "exchanges": self.config.exchanges,
            "channels": self.config.channels,
            "metrics": self.metrics
        }
```

#### Expected Test Results (GREEN Phase)
```bash
# All tests should PASS after implementation
pytest tests/unit/config/ -v
# Expected: 0 failed, 45 passed ✅

pytest tests/integration/ -v  
# Expected: 0 failed, 23 passed ✅

pytest tests/e2e/ -v
# Expected: 0 failed, 12 passed ✅
```

#### 🔵 **REFACTOR Phase: Optimize and Enhance**

##### Quality Improvements
1. **Performance Optimization**
   - Lazy loading of complex validations
   - Cached property computations
   - Optimized serialization paths

2. **Enhanced Error Messages**
   - Context-aware validation errors
   - Suggestion-based error recovery
   - Multi-language error message support

3. **Advanced Features**
   - Configuration templates and presets
   - Dynamic configuration updates
   - Configuration diffing and merging

---

## 🔄 Integration with Existing Systems

### Backward Compatibility Strategy

#### Migration Bridge Pattern
```python
# File: quixstreams/sources/community/crypto/migration_bridge.py

class CryptoConfigBridge:
    """Bridge between legacy and Pydantic v2 configurations."""
    
    @staticmethod
    def detect_config_type(config: Any) -> str:
        """Detect configuration type."""
        if hasattr(config, 'model_dump'):  # Pydantic v2
            return "pydantic_v2"
        elif hasattr(config, '__dataclass_fields__'):  # Dataclass
            return "legacy_dataclass" 
        else:
            return "unknown"
    
    @staticmethod
    def migrate_to_pydantic(legacy_config: Any) -> CryptoConfig:
        """Migrate legacy config to Pydantic v2."""
        # Implementation for seamless migration
        pass
    
    @staticmethod  
    def create_config_smart(provider: str, **kwargs) -> CryptoConfig:
        """Smart configuration creation with auto-detection."""
        try:
            # Try Pydantic v2 first
            return create_config(provider, **kwargs)
        except Exception:
            # Fall back to legacy if needed
            warnings.warn("Using legacy configuration system")
            return create_legacy_config(provider, **kwargs)
```

### Global Validation Rules Implementation

#### Universal Validation Patterns
```python
# File: quixstreams/validation/global_rules.py

from typing import TypeVar, Type, Any, Dict
from pydantic import BaseModel, ConfigDict

T = TypeVar('T', bound=BaseModel)

class GlobalValidationRules:
    """Global validation rules applied across all configurations."""
    
    STANDARD_CONFIG = ConfigDict(
        frozen=True,                    # Immutable configurations
        extra='forbid',                # Prevent configuration typos
        str_strip_whitespace=True,     # Clean string inputs
        validate_assignment=True,      # Validate on updates
        use_enum_values=True,         # Enum serialization
        json_encoders={               # Secure secret handling
            'SecretStr': lambda v: '***' if v else None
        }
    )
    
    @classmethod
    def apply_to_model(cls, model_class: Type[T]) -> Type[T]:
        """Apply global rules to a Pydantic model."""
        model_class.model_config = cls.STANDARD_CONFIG
        return model_class

    @classmethod
    def validate_credentials(cls, credentials: Dict[str, Any]) -> bool:
        """Global credential validation."""
        # Check for common security issues
        for key, value in credentials.items():
            if 'password' in key.lower() or 'secret' in key.lower():
                if len(str(value)) < 8:
                    raise ValueError(f"Credential {key} is too short")
        return True
```

---

## 🎯 Success Metrics & Validation

### Quality Gates
```yaml
quality_gates:
  test_coverage: ">95%"
  type_coverage: ">90%"
  validation_errors: "descriptive with suggestions"
  performance: "pydantic overhead <5% vs dataclass"
  backward_compatibility: "100% for existing APIs"
  documentation: "100% API coverage with examples"
```

### Performance Benchmarks
```python
# File: tests/performance/test_pydantic_performance.py

import pytest
import time
from typing import Dict, Any

class TestPydanticPerformance:
    """Benchmark Pydantic v2 vs legacy configurations."""
    
    def test_config_creation_performance(self, benchmark):
        """Compare config creation performance."""
        
        @benchmark
        def create_pydantic_config():
            return CryptofeedConfig(
                exchanges=["binance", "coinbase"],
                channels=["trades", "ticker"],
                symbols=["BTC-USDT", "ETH-USDT"]
            )
    
    def test_validation_performance(self, benchmark):
        """Compare validation performance."""
        
        @benchmark
        def validate_pydantic_config():
            config_data = {
                "exchanges": ["binance"],
                "channels": ["trades"],
                "symbols": ["BTC-USDT"]
            }
            return CryptofeedConfig.model_validate(config_data)
    
    def test_serialization_performance(self, benchmark):
        """Compare serialization performance."""
        config = CryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"],
            symbols=["BTC-USDT"]
        )
        
        @benchmark
        def serialize_pydantic_config():
            return config.model_dump()
```

---

## 📋 Implementation Timeline

### Phase 1: Foundation (Week 1)
- ✅ Iceberg REST Sink Pydantic v2 (COMPLETE)
- 🔄 Crypto Sources Pydantic v2 Models
- 🔄 Global Validation Rules
- 🔄 Migration Bridge Implementation

### Phase 2: Integration (Week 2)  
- 🔄 Agent-Based Architecture
- 🔄 Cross-Component Validation
- 🔄 Performance Optimization
- 🔄 Comprehensive Testing

### Phase 3: Production Hardening (Week 3)
- 🔄 Security Enhancements  
- 🔄 Observability Integration
- 🔄 Documentation Completion
- 🔄 Migration Path Validation

---

## 🔚 Expected Outcomes

### Immediate Benefits
- **Enhanced Validation**: Descriptive errors with field-level context
- **Type Safety**: Full IDE support with autocompletion and error detection
- **Environment Integration**: Automatic configuration from environment variables
- **Production Ready**: Immutable configs, secret handling, schema generation

### Long-term Benefits  
- **Maintainability**: Consistent validation patterns across all components
- **Extensibility**: Easy addition of new crypto providers and sinks
- **Testability**: Agent-based architecture enables comprehensive testing
- **Observability**: Built-in metrics and health checking

### Migration Path
- **Phase 1**: Parallel implementation with backward compatibility
- **Phase 2**: Gradual migration of existing code
- **Phase 3**: Deprecation of legacy systems with clear warnings
- **Phase 4**: Complete transition to Pydantic v2 ecosystem

---

**Status**: 📋 **COMPREHENSIVE PLAN COMPLETE**  
**Next Step**: Begin TDD RED Phase with comprehensive test specifications
**Implementation Ready**: ✅ All dependencies satisfied, architecture defined

This plan provides a complete roadmap for integrating Pydantic v2 validation across crypto sources and Iceberg REST sinks using Test-Driven Development methodology with global validation rules and agent-based architecture patterns.