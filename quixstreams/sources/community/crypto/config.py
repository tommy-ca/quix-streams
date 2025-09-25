"""Unified configuration API for crypto sources (compatibility layer)."""

from __future__ import annotations

import os
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Type, TypeVar, Union

from .retry import RetryConfig


class ValidationError(ValueError):
    """Raised when configuration validation fails."""


class AuthProvider(ABC):
    """Base class for authentication providers."""

    @abstractmethod
    def get_credentials(self) -> Dict[str, Any]:
        """Return credential dictionary suitable for downstream clients."""

    def validate(self) -> bool:  # pragma: no cover - trivial by default
        return True


@dataclass
class NoAuth(AuthProvider):
    """Authentication provider that returns no credentials."""

    def get_credentials(self) -> Dict[str, Any]:
        return {}


@dataclass
class APIKeyAuth(AuthProvider):
    """Simple API key authentication."""

    api_key: str
    api_secret: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.api_key:
            raise ValidationError("api_key cannot be empty")

    def get_credentials(self) -> Dict[str, Any]:
        credentials = {"api_key": self.api_key}
        if self.api_secret:
            credentials["api_secret"] = self.api_secret
        return credentials


@dataclass
class AWSAuth(AuthProvider):
    """AWS style authentication provider used by S3-compatible sources."""

    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    region_name: str = "us-east-1"
    endpoint_url: Optional[str] = None
    session_token: Optional[str] = None

    def get_credentials(self) -> Dict[str, Any]:
        creds = {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "region_name": self.region_name,
            "endpoint_url": self.endpoint_url,
        }
        if self.session_token:
            creds["aws_session_token"] = self.session_token
        return creds


class CryptoProvider(str, Enum):
    """Enumeration of supported crypto providers."""

    CRYPTOFEED = "cryptofeed"
    CCXT = "ccxt"
    BINANCE_S3 = "binance_s3"


def _ensure_list(value: Optional[Union[str, Sequence[str]]]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return list(value)


def _parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    value = value.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValidationError(f"Cannot parse boolean value from '{value}'")


def _deprecated(message: str) -> None:
    warnings.warn(message, DeprecationWarning, stacklevel=2)


@dataclass
class CryptofeedConfig:
    """Configuration for the Cryptofeed source."""

    exchanges: List[str]
    channels: List[str]
    symbols: List[str] = field(default_factory=list)
    auth_provider: AuthProvider = field(default_factory=NoAuth)
    reconnect: bool = True
    normalize: bool = True
    name: Optional[str] = None
    shutdown_timeout: float = 10.0
    retry_config: RetryConfig = field(default_factory=RetryConfig)

    SUPPORTED_EXCHANGES: Tuple[str, ...] = (
        "binance",
        "kraken",
        "coinbase",
        "bitfinex",
        "bybit",
        "okx",
    )
    SUPPORTED_CHANNELS: Tuple[str, ...] = ("trades", "ticker", "orderbook", "klines")

    def __post_init__(self) -> None:
        self.exchanges = [exchange.lower() for exchange in self.exchanges]
        self.channels = [channel.lower() for channel in self.channels]
        if not self.exchanges:
            raise ValidationError("exchanges cannot be empty")
        if not self.channels:
            raise ValidationError("channels cannot be empty")
        for exchange in self.exchanges:
            if exchange not in self.SUPPORTED_EXCHANGES:
                raise ValidationError(f"Unsupported exchange: {exchange}")
        for channel in self.channels:
            if channel not in self.SUPPORTED_CHANNELS:
                raise ValidationError(f"Unsupported channel: {channel}")
        if not isinstance(self.retry_config, RetryConfig):
            raise ValidationError("retry_config must be a RetryConfig instance")

    def validate(self) -> bool:
        return True


@dataclass
class CCXTConfig:
    """Configuration for the CCXT source."""

    exchange: str
    mode: str
    symbols: List[str]
    interval: Optional[str] = None
    rate_limit: bool = True
    normalize: bool = True
    rest_poll_interval: float = 0.0
    shutdown_timeout: float = 10.0
    name: Optional[str] = None
    auth_provider: AuthProvider = field(default_factory=NoAuth)
    use_websocket: bool = False

    SUPPORTED_MODES: Tuple[str, ...] = ("trades", "ticker", "orderbook", "klines")

    def __post_init__(self) -> None:
        self.exchange = self.exchange.lower()
        self.mode = self.mode.lower()
        if not self.exchange:
            raise ValidationError("exchange cannot be empty")
        if self.mode not in self.SUPPORTED_MODES:
            raise ValidationError("mode must be one of {'trades', 'ticker', 'orderbook', 'klines'}")
        if not self.symbols:
            raise ValidationError("symbols cannot be empty")

    def validate(self) -> bool:
        return True


@dataclass
class BinanceS3Config:
    """Configuration for replaying Binance public data from S3."""

    bucket: str
    prefix: str
    datatype: str = "trades"
    unsigned: bool = False
    file_format: str = "infer"
    compression: str = "infer"
    replay_speed: float = 0.0
    has_partition_folders: bool = False
    checksum_mode: str = "skip"
    extract_metadata: bool = True
    access_mode: str = "direct_prefix"
    prefix_template: Optional[str] = None
    root: Optional[str] = None
    market: Optional[str] = None
    segments: List[str] = field(default_factory=lambda: ["daily"])
    datatypes_list: List[str] = field(default_factory=list)
    symbols: List[str] = field(default_factory=list)
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    interval: Optional[str] = None
    dry_run: bool = False
    name: Optional[str] = None
    shutdown_timeout: float = 30.0
    auth_provider: AuthProvider = field(default_factory=lambda: AWSAuth("", ""))
    validate_prefix_template: bool = field(default=True, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValidationError("bucket cannot be empty")
        if not self.prefix:
            raise ValidationError("prefix cannot be empty")
        if (
            self.validate_prefix_template
            and self.access_mode == "templated_prefixes"
            and not self.prefix_template
        ):
            raise ValidationError("prefix_template is required when access_mode='templated_prefixes'")
        if not self.datatypes_list:
            self.datatypes_list = [self.datatype]
        if self.unsigned:
            self.auth_provider = NoAuth()

    def validate(self) -> bool:
        return True


ConfigT = TypeVar("ConfigT", CryptofeedConfig, CCXTConfig, BinanceS3Config)


def load_from_env(config_cls: Type[ConfigT], **overrides: Any) -> ConfigT:
    """Load configuration from environment variables with optional overrides."""

    if config_cls is CryptofeedConfig:
        exchanges = _ensure_list(os.getenv("CRYPTO_EXCHANGES"))
        channels = _ensure_list(os.getenv("CRYPTO_CHANNELS"))
        symbols = _ensure_list(os.getenv("CRYPTO_SYMBOLS"))
        normalize = _parse_bool(os.getenv("CRYPTO_NORMALIZE"), True)
        reconnect = _parse_bool(os.getenv("CRYPTO_RECONNECT"), True)
        data = dict(
            exchanges=exchanges,
            channels=channels,
            symbols=symbols,
            normalize=normalize,
            reconnect=reconnect,
        )
    elif config_cls is CCXTConfig:
        exchange = os.getenv("CRYPTO_EXCHANGE", "")
        mode = os.getenv("CRYPTO_MODE", "")
        symbols = _ensure_list(os.getenv("CRYPTO_SYMBOLS"))
        interval = os.getenv("CRYPTO_INTERVAL")
        rate_limit = _parse_bool(os.getenv("CRYPTO_RATE_LIMIT"), True)
        rest_poll = os.getenv("CRYPTO_REST_POLL_INTERVAL")
        rest_poll_interval = float(rest_poll) if rest_poll else 0.0
        data = dict(
            exchange=exchange,
            mode=mode,
            symbols=symbols,
            interval=interval,
            rate_limit=rate_limit,
            rest_poll_interval=rest_poll_interval,
        )
    elif config_cls is BinanceS3Config:
        bucket = os.getenv("CRYPTO_BINANCE_BUCKET", "")
        prefix = os.getenv("CRYPTO_BINANCE_PREFIX", "")
        datatype = os.getenv("CRYPTO_BINANCE_DATATYPE", "trades")
        unsigned = _parse_bool(os.getenv("CRYPTO_BINANCE_UNSIGNED"), False)
        access_mode = os.getenv("CRYPTO_BINANCE_ACCESS_MODE", "direct_prefix")
        prefix_template = os.getenv("CRYPTO_BINANCE_PREFIX_TEMPLATE")
        dry_run = _parse_bool(os.getenv("CRYPTO_BINANCE_DRY_RUN"), False)
        data = dict(
            bucket=bucket,
            prefix=prefix,
            datatype=datatype,
            unsigned=unsigned,
            access_mode=access_mode,
            prefix_template=prefix_template,
            dry_run=dry_run,
        )
    else:  # pragma: no cover - defensive fallback
        raise TypeError(f"Unsupported config class: {config_cls}")

    data.update(overrides)
    return config_cls(**data)  # type: ignore[arg-type]


def create_cryptofeed_config(**kwargs: Any) -> CryptofeedConfig:
    return CryptofeedConfig(**kwargs)


def create_ccxt_config(**kwargs: Any) -> CCXTConfig:
    return CCXTConfig(**kwargs)


def create_binance_s3_config(**kwargs: Any) -> BinanceS3Config:
    return BinanceS3Config(**kwargs)


def create_local_cryptofeed_config(**kwargs: Any) -> CryptofeedConfig:
    _deprecated("create_local_cryptofeed_config is deprecated; use create_cryptofeed_config")
    defaults = dict(exchanges=["binance"], channels=["trades"], name="cryptofeed-local")
    defaults.update(kwargs)
    return create_cryptofeed_config(**defaults)


def create_s3_binance_config(**kwargs: Any) -> BinanceS3Config:
    _deprecated("create_s3_binance_config is deprecated; use create_binance_s3_config")
    defaults = dict(datatype="trades", access_mode="direct_prefix")
    defaults.update(kwargs)
    return create_binance_s3_config(**defaults)


def resolve_provider(provider: Union[CryptoProvider, str]) -> CryptoProvider:
    if isinstance(provider, CryptoProvider):
        return provider
    return CryptoProvider(provider)


__all__ = [
    "AuthProvider",
    "NoAuth",
    "APIKeyAuth",
    "AWSAuth",
    "ValidationError",
    "CryptofeedConfig",
    "CCXTConfig",
    "BinanceS3Config",
    "CryptoProvider",
    "load_from_env",
    "create_cryptofeed_config",
    "create_ccxt_config",
    "create_binance_s3_config",
    "create_local_cryptofeed_config",
    "create_s3_binance_config",
    "resolve_provider",
    "RetryConfig",
]
