from .utils import (
    TopicBuilder,
    default_key_fn,
    default_ts_fn,
    normalize_trade,
    normalize_ticker,
    normalize_klines,
    normalize_orderbook,
)
from .cryptofeed_source import CryptofeedSource
from .ccxt_source import CCXTSource
from .binance_s3_source import BinanceS3Source

from .config import (
    AuthProvider,
    CryptofeedConfig,
    CCXTConfig,
    BinanceS3Config,
    APIKeyAuth,
    AWSAuth,
    NoAuth,
    ValidationError,
    RetryConfig,
    load_from_env,
    create_cryptofeed_config,
    create_ccxt_config,
    create_binance_s3_config,
    create_local_cryptofeed_config,
    create_s3_binance_config,
)

# Export essential error classes
from .errors import (
    CryptoSourceError,
    CryptoSourceConfigError,
    CryptoSourceConnectionError,
    
    # Convenience functions
    missing_dependency_error,
    connection_error,
)

__all__ = [
    # Utils
    "TopicBuilder",
    "default_key_fn",
    "default_ts_fn",
    "normalize_trade",
    "normalize_ticker",
    "normalize_klines",
    "normalize_orderbook",
    
    # Sources (greenfield only)
    "CryptofeedSource",
    "CCXTSource",
    "BinanceS3Source",

    # Configuration API
    "AuthProvider",
    "CryptofeedConfig",
    "CCXTConfig", 
    "BinanceS3Config",
    
    # Auth providers
    "NoAuth",
    "APIKeyAuth",
    "AWSAuth",
    "RetryConfig",

    # Config helpers
    "load_from_env",
    "create_cryptofeed_config",
    "create_ccxt_config",
    "create_binance_s3_config",
    "create_local_cryptofeed_config",
    "create_s3_binance_config",

    # Error classes
    "CryptoSourceError",
    "CryptoSourceConfigError",
    "CryptoSourceConnectionError",
    
    # Error convenience functions
    "missing_dependency_error",
    "connection_error",
    
    # Validation
    "ValidationError",
]
