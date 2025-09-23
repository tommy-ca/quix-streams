"""
Tests for the unified crypto source configuration system.

This module validates the SOLID, KISS, and DRY principles compliance,
backward compatibility, environment variable support, and interface consistency.
"""

import os
import warnings
import pytest
from unittest.mock import patch

from quixstreams.sources.community.crypto.config import (
    CryptofeedConfig,
    CCXTConfig,
    BinanceS3Config,
    AuthProvider,
    NoAuth,
    APIKeyAuth,
    AWSAuth,
    RetryConfig,
    CryptoProvider,
    load_from_env,
    create_cryptofeed_config,
    create_ccxt_config,
    create_binance_s3_config,
    create_local_cryptofeed_config,
    create_s3_binance_config,
)
from quixstreams.sources.community.crypto.errors import (
    CryptoSourceError,
    CryptoSourceConfigError,
    CryptoSourceConnectionError,
    CryptoSourceRateLimitError,
    missing_dependency_error,
)
from quixstreams.sources.community.crypto.retry import (
    ExponentialBackoff,
    RetryManager,
    retry_with_backoff,
)


class TestRetryConfig:
    """Test RetryConfig validation."""
    
    def test_valid_config(self):
        config = RetryConfig()
        assert config.enabled is True
        assert config.base_delay == 0.5
        assert config.max_delay == 30.0
        assert config.backoff_factor == 2.0
    
    def test_invalid_base_delay(self):
        with pytest.raises(ValueError, match="base_delay must be positive"):
            RetryConfig(base_delay=-1.0)
    
    def test_invalid_max_delay(self):
        with pytest.raises(ValueError, match="max_delay must be positive"):
            RetryConfig(max_delay=-5.0)
    
    def test_invalid_backoff_factor(self):
        with pytest.raises(ValueError, match="backoff_factor must be greater than 1"):
            RetryConfig(backoff_factor=0.5)


class TestAuthProviders:
    """Test authentication providers."""
    
    def test_no_auth(self):
        auth = NoAuth()
        assert auth.get_credentials() == {}
    
    def test_api_key_auth(self):
        auth = APIKeyAuth(api_key="test_key")
        assert auth.get_credentials() == {"api_key": "test_key"}
        
    def test_api_key_with_secret(self):
        auth = APIKeyAuth(api_key="test_key", api_secret="test_secret")
        expected = {"api_key": "test_key", "api_secret": "test_secret"}
        assert auth.get_credentials() == expected
    
    def test_aws_auth(self):
        auth = AWSAuth(
            aws_access_key_id="access_key",
            aws_secret_access_key="secret_key",
            region_name="us-west-2"
        )
        expected = {
            "aws_access_key_id": "access_key",
            "aws_secret_access_key": "secret_key", 
            "region_name": "us-west-2",
            "endpoint_url": None
        }
        assert auth.get_credentials() == expected


class TestCryptofeedConfig:
    """Test CryptofeedConfig validation and creation."""
    
    def test_valid_config(self):
        config = CryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"]
        )
        assert config.exchanges == ["binance"]
        assert config.channels == ["trades"]
        assert config.normalize is True
        assert config.reconnect is True
    
    def test_empty_exchanges(self):
        with pytest.raises(ValueError, match="exchanges cannot be empty"):
            CryptofeedConfig(exchanges=[], channels=["trades"])
    
    def test_empty_channels(self):
        with pytest.raises(ValueError, match="channels cannot be empty"):
            CryptofeedConfig(exchanges=["binance"], channels=[])


class TestCCXTConfig:
    """Test CCXTConfig validation and creation."""
    
    def test_valid_config(self):
        config = CCXTConfig(
            exchange="binance",
            mode="trades",
            symbols=["BTC/USDT"]
        )
        assert config.exchange == "binance"
        assert config.mode == "trades"
        assert config.symbols == ["BTC/USDT"]
    
    def test_empty_exchange(self):
        with pytest.raises(ValueError, match="exchange cannot be empty"):
            CCXTConfig(exchange="", mode="trades", symbols=["BTC/USDT"])
    
    def test_invalid_mode(self):
        with pytest.raises(ValueError, match="mode must be one of"):
            CCXTConfig(exchange="binance", mode="invalid", symbols=["BTC/USDT"])
    
    def test_empty_symbols(self):
        with pytest.raises(ValueError, match="symbols cannot be empty"):
            CCXTConfig(exchange="binance", mode="trades", symbols=[])


class TestBinanceS3Config:
    """Test BinanceS3Config validation and creation."""
    
    def test_valid_config(self):
        config = BinanceS3Config(
            bucket="test-bucket",
            prefix="data/"
        )
        assert config.bucket == "test-bucket"
        assert config.prefix == "data/"
        assert config.unsigned is False
    
    def test_empty_bucket(self):
        with pytest.raises(ValueError, match="bucket cannot be empty"):
            BinanceS3Config(bucket="", prefix="data/")
    
    def test_empty_prefix(self):
        with pytest.raises(ValueError, match="prefix cannot be empty"):
            BinanceS3Config(bucket="test-bucket", prefix="")
    
    def test_templated_mode_requires_template(self):
        with pytest.raises(ValueError, match="prefix_template is required"):
            BinanceS3Config(
                bucket="test-bucket",
                prefix="data/",
                access_mode="templated_prefixes"
            )


class TestEnvironmentLoading:
    """Test configuration loading from environment variables."""
    
    def test_load_cryptofeed_from_env(self):
        env_vars = {
            "CRYPTO_EXCHANGES": "binance,coinbase",
            "CRYPTO_CHANNELS": "trades,ticker",
            "CRYPTO_NORMALIZE": "true",
            "CRYPTO_RECONNECT": "false"
        }
        
        with patch.dict(os.environ, env_vars):
            config = load_from_env(CryptofeedConfig)
            assert config.exchanges == ["binance", "coinbase"]
            assert config.channels == ["trades", "ticker"]
            assert config.normalize is True
            assert config.reconnect is False
    
    def test_load_ccxt_from_env(self):
        env_vars = {
            "CRYPTO_EXCHANGE": "binance",
            "CRYPTO_MODE": "klines",
            "CRYPTO_SYMBOLS": "BTC/USDT,ETH/USDT",
            "CRYPTO_INTERVAL": "1h",
            "CRYPTO_RATE_LIMIT": "false"
        }
        
        with patch.dict(os.environ, env_vars):
            config = load_from_env(CCXTConfig)
            assert config.exchange == "binance"
            assert config.mode == "klines"
            assert config.symbols == ["BTC/USDT", "ETH/USDT"]
            assert config.interval == "1h"
            assert config.rate_limit is False
    
    def test_env_overrides(self):
        env_vars = {
            "CRYPTO_EXCHANGES": "binance",
            "CRYPTO_CHANNELS": "trades"
        }
        
        with patch.dict(os.environ, env_vars):
            config = load_from_env(
                CryptofeedConfig,
                reconnect=False,  # override
                normalize=False   # override
            )
            assert config.exchanges == ["binance"]
            assert config.channels == ["trades"] 
            assert config.reconnect is False  # overridden
            assert config.normalize is False   # overridden


class TestBackwardCompatibility:
    """Test backward compatibility with deprecation warnings."""
    
    def test_factory_functions(self):
        # Test new factory functions work
        config = create_cryptofeed_config(
            exchanges=["binance"],
            channels=["trades"]
        )
        assert isinstance(config, CryptofeedConfig)
        
        config = create_ccxt_config(
            exchange="binance",
            mode="trades", 
            symbols=["BTC/USDT"]
        )
        assert isinstance(config, CCXTConfig)
        
        config = create_binance_s3_config(
            bucket="test-bucket",
            prefix="data/"
        )
        assert isinstance(config, BinanceS3Config)
    
    def test_deprecated_factory_functions(self):
        # Test deprecated functions with warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            config = create_local_cryptofeed_config(
                exchanges=["binance"],
                channels=["trades"]
            )
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "deprecated" in str(w[0].message)
            assert isinstance(config, CryptofeedConfig)
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            config = create_s3_binance_config(
                bucket="test-bucket",
                prefix="data/"
            )
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert isinstance(config, BinanceS3Config)


class TestErrorHierarchy:
    """Test standardized error hierarchy."""
    
    def test_base_error(self):
        error = CryptoSourceError("test error", source="test", context={"key": "value"})
        assert str(error) == "test error"
        assert error.source == "test"
        assert error.context == {"key": "value"}
    
    def test_connection_error_retryable(self):
        error = CryptoSourceConnectionError("connection failed", retryable=True)
        assert error.retryable is True
    
    def test_rate_limit_error(self):
        error = CryptoSourceRateLimitError("rate limited", retry_after=30.0)
        assert error.retry_after == 30.0
        assert error.retryable is True  # inherited
    
    def test_convenience_functions(self):
        error = missing_dependency_error("requests", "TestSource", "http")
        assert "TestSource requires 'requests'" in str(error)
        assert "pip install quixstreams[http]" in str(error)
        assert error.package == "requests"


class TestRetryLogic:
    """Test unified retry and backoff logic."""
    
    def test_exponential_backoff(self):
        config = RetryConfig(base_delay=1.0, backoff_factor=2.0, max_delay=10.0)
        backoff = ExponentialBackoff(config)
        
        delays = [next(backoff) for _ in range(5)]
        assert delays == [1.0, 2.0, 4.0, 8.0, 10.0]  # capped at max_delay
    
    def test_retry_manager_should_retry(self):
        config = RetryConfig(max_retries=3)
        manager = RetryManager(config, "test")
        
        # Should retry connection errors
        conn_error = CryptoSourceConnectionError("failed", retryable=True)
        assert manager.should_retry(conn_error, 1) is True
        assert manager.should_retry(conn_error, 4) is False  # exceeds max_retries
        
        # Should not retry non-retryable connection errors
        non_retryable = CryptoSourceConnectionError("failed", retryable=False)
        assert manager.should_retry(non_retryable, 1) is False
        
        # Should not retry other crypto source errors
        config_error = CryptoSourceConfigError("config error")
        assert manager.should_retry(config_error, 1) is False
    
    @patch('time.sleep')
    def test_retry_with_backoff_success(self, mock_sleep):
        config = RetryConfig(base_delay=1.0)
        
        call_count = [0]
        def failing_func():
            call_count[0] += 1
            if call_count[0] < 3:
                raise CryptoSourceConnectionError("temp failure")
            return "success"
        
        result = retry_with_backoff(failing_func, config, "test")
        assert result == "success"
        assert call_count[0] == 3
        assert mock_sleep.call_count == 2  # 2 retries
    
    @patch('time.sleep')  
    def test_retry_with_backoff_exhausted(self, mock_sleep):
        config = RetryConfig(max_retries=2)
        
        def always_failing():
            raise CryptoSourceConnectionError("always fails")
        
        with pytest.raises(CryptoSourceConnectionError):
            retry_with_backoff(always_failing, config, "test")
        
        # With max_retries=2: first attempt fails, then 1 retry (2nd attempt) fails, then give up
        # So we sleep once between attempt 1 and 2
        assert mock_sleep.call_count == 1
    

class TestSOLIDPrinciples:
    """Test compliance with SOLID principles."""
    
    def test_single_responsibility(self):
        """Each config class has a single responsibility."""
        # CryptofeedConfig only handles Cryptofeed configuration
        config = CryptofeedConfig(exchanges=["binance"], channels=["trades"])
        assert hasattr(config, 'exchanges')
        assert hasattr(config, 'channels')
        assert not hasattr(config, 'bucket')  # Not mixed with S3 config
        
        # AuthProvider only handles authentication
        auth = APIKeyAuth(api_key="test")
        assert hasattr(auth, 'get_credentials')
        assert not hasattr(auth, 'retry_config')  # Not mixed with retry logic
    
    def test_open_closed_principle(self):
        """System is open for extension, closed for modification."""
        # Can extend AuthProvider without modifying existing code
        class CustomAuth(AuthProvider):
            def get_credentials(self):
                return {"custom": "auth"}
        
        custom_auth = CustomAuth()
        assert custom_auth.get_credentials() == {"custom": "auth"}
        
        # Can use custom auth in existing config
        config = CryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"],
            auth_provider=custom_auth
        )
        assert isinstance(config.auth_provider, CustomAuth)
    
    def test_liskov_substitution(self):
        """Subtypes must be substitutable for their base types."""
        # Any AuthProvider should work in place of the base
        auth_providers = [NoAuth(), APIKeyAuth("test"), AWSAuth()]
        
        for auth in auth_providers:
            config = CryptofeedConfig(
                exchanges=["binance"],
                channels=["trades"],
                auth_provider=auth
            )
            # All should work without breaking
            assert isinstance(auth.get_credentials(), dict)
    
    def test_interface_segregation(self):
        """Clients shouldn't depend on interfaces they don't use."""
        # Config classes only expose relevant methods
        crypto_config = CryptofeedConfig(exchanges=["binance"], channels=["trades"])
        # No unused methods from other domains
        assert not hasattr(crypto_config, 'connect')
        assert not hasattr(crypto_config, 'execute_query')
    
    def test_dependency_inversion(self):
        """Depend on abstractions, not concretions."""
        # Config accepts AuthProvider abstraction, not concrete implementations
        config = CryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"],
            auth_provider=NoAuth()  # Could be any AuthProvider implementation
        )
        # Works with abstraction
        assert isinstance(config.auth_provider, AuthProvider)