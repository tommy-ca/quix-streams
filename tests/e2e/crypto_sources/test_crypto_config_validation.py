"""
TDD Cycle 1 - RED Phase: Crypto Source Configuration Validation Tests

These tests are designed to FAIL initially, defining the expected behavior 
for the unified crypto source configuration system. They represent the 
requirements that need to be implemented.
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
    load_from_env,
)
from quixstreams.sources.community.crypto.errors import CryptoSourceConfigError


class TestCryptoSourceConfigValidation:
    """Test configuration validation for all crypto sources."""

    def test_cryptofeed_config_requires_exchanges_and_channels(self):
        """CryptofeedConfig should validate required fields."""
        # Should pass - valid configuration
        config = CryptofeedConfig(
            exchanges=["binance", "coinbase"],
            channels=["trades", "ticker"]
        )
        assert config.exchanges == ["binance", "coinbase"]
        assert config.channels == ["trades", "ticker"]

        # Should fail - empty exchanges
        with pytest.raises(ValueError, match="exchanges cannot be empty"):
            CryptofeedConfig(exchanges=[], channels=["trades"])

        # Should fail - empty channels  
        with pytest.raises(ValueError, match="channels cannot be empty"):
            CryptofeedConfig(exchanges=["binance"], channels=[])

        # Should fail - missing exchanges completely (becomes empty list via default)
        with pytest.raises(ValueError, match="exchanges cannot be empty"):
            CryptofeedConfig(channels=["trades"])  # exchanges defaults to empty list

    def test_ccxt_config_validates_mode_and_symbols(self):
        """CCXTConfig should validate mode values and required symbols."""
        # Should pass - valid configuration
        config = CCXTConfig(
            exchange="binance",
            mode="trades",
            symbols=["BTC/USDT", "ETH/USDT"]
        )
        assert config.exchange == "binance"
        assert config.mode == "trades"
        assert config.symbols == ["BTC/USDT", "ETH/USDT"]

        # Should fail - invalid mode
        with pytest.raises(ValueError, match="mode must be one of"):
            CCXTConfig(
                exchange="binance",
                mode="invalid_mode",
                symbols=["BTC/USDT"]
            )

        # Should fail - empty exchange
        with pytest.raises(ValueError, match="exchange cannot be empty"):
            CCXTConfig(
                exchange="",
                mode="trades",
                symbols=["BTC/USDT"]
            )

        # Should fail - empty symbols
        with pytest.raises(ValueError, match="symbols cannot be empty"):
            CCXTConfig(
                exchange="binance", 
                mode="trades",
                symbols=[]
            )

        # Should fail - negative rest_poll_interval
        with pytest.raises(ValueError, match="rest_poll_interval cannot be negative"):
            CCXTConfig(
                exchange="binance",
                mode="trades", 
                symbols=["BTC/USDT"],
                rest_poll_interval=-1.0
            )

    def test_binance_s3_config_validates_s3_parameters(self):
        """BinanceS3Config should validate S3-specific parameters."""
        # Should pass - valid configuration
        config = BinanceS3Config(
            bucket="binance-public-data",
            prefix="data/spot/daily/trades/"
        )
        assert config.bucket == "binance-public-data"
        assert config.prefix == "data/spot/daily/trades/"

        # Should fail - empty bucket
        with pytest.raises(ValueError, match="bucket cannot be empty"):
            BinanceS3Config(bucket="", prefix="data/")

        # Should fail - empty prefix
        with pytest.raises(ValueError, match="prefix cannot be empty"):
            BinanceS3Config(bucket="test-bucket", prefix="")

        # Should fail - negative replay_speed
        with pytest.raises(ValueError, match="replay_speed cannot be negative"):
            BinanceS3Config(
                bucket="test-bucket",
                prefix="data/",
                replay_speed=-1.0
            )

        # Should fail - templated mode without template
        with pytest.raises(ValueError, match="prefix_template is required"):
            BinanceS3Config(
                bucket="test-bucket",
                prefix="data/", 
                access_mode="templated_prefixes"
            )

    def test_environment_variable_loading_with_invalid_types(self):
        """load_from_env should handle type conversion errors gracefully."""
        # Test boolean conversion
        with patch.dict(os.environ, {
            "CRYPTO_EXCHANGES": "binance,coinbase",
            "CRYPTO_CHANNELS": "trades,ticker", 
            "CRYPTO_NORMALIZE": "invalid_bool"
        }):
            # Should still work - invalid boolean becomes False
            config = load_from_env(CryptofeedConfig)
            assert config.normalize in [True, False]

        # Test integer conversion error
        with patch.dict(os.environ, {
            "CRYPTO_EXCHANGE": "binance",
            "CRYPTO_MODE": "trades",
            "CRYPTO_SYMBOLS": "BTC/USDT",
            "CRYPTO_REST_POLL_INTERVAL": "not_a_number"
        }):
            # Should raise ValueError for invalid numeric conversion
            with pytest.raises(ValueError):
                load_from_env(CCXTConfig)

        # Test list parsing edge cases
        with patch.dict(os.environ, {
            "CRYPTO_EXCHANGES": "",  # Empty string 
            "CRYPTO_CHANNELS": "trades",
        }):
            # Should fail - empty exchanges list from environment
            with pytest.raises(ValueError, match="exchanges cannot be empty"):
                load_from_env(CryptofeedConfig)

    def test_auth_provider_interface_compliance(self):
        """All AuthProviders should implement interface correctly."""
        # Test NoAuth
        no_auth = NoAuth()
        credentials = no_auth.get_credentials()
        assert isinstance(credentials, dict)
        assert credentials == {}

        # Test APIKeyAuth
        api_auth = APIKeyAuth(api_key="test_key", api_secret="test_secret")
        credentials = api_auth.get_credentials()
        assert credentials["api_key"] == "test_key"
        assert credentials["api_secret"] == "test_secret"

        # Test AWSAuth
        aws_auth = AWSAuth(
            aws_access_key_id="access_key",
            aws_secret_access_key="secret_key",
            region_name="us-west-2"
        )
        credentials = aws_auth.get_credentials()
        assert credentials["aws_access_key_id"] == "access_key"
        assert credentials["aws_secret_access_key"] == "secret_key"
        assert credentials["region_name"] == "us-west-2"

        # Test custom auth provider
        class CustomAuth(AuthProvider):
            def get_credentials(self):
                return {"token": "custom_token"}

        custom_auth = CustomAuth()
        credentials = custom_auth.get_credentials()
        assert credentials["token"] == "custom_token"

    def test_retry_config_validation(self):
        """RetryConfig should validate retry parameters."""
        # Should pass - valid configuration
        config = RetryConfig(
            enabled=True,
            max_retries=5,
            base_delay=1.0,
            max_delay=30.0,
            backoff_factor=2.0
        )
        assert config.enabled is True
        assert config.max_retries == 5

        # Should fail - invalid base_delay
        with pytest.raises(ValueError, match="base_delay must be positive"):
            RetryConfig(base_delay=-1.0)

        # Should fail - invalid max_delay
        with pytest.raises(ValueError, match="max_delay must be positive"):
            RetryConfig(max_delay=-5.0)

        # Should fail - invalid backoff_factor
        with pytest.raises(ValueError, match="backoff_factor must be greater than 1"):
            RetryConfig(backoff_factor=0.5)

    def test_base_source_config_validation(self):
        """BaseSourceConfig should validate common parameters."""
        # Test with CryptofeedConfig inheriting BaseSourceConfig
        
        # Should fail - negative shutdown_timeout
        with pytest.raises(ValueError, match="shutdown_timeout must be positive"):
            CryptofeedConfig(
                exchanges=["binance"],
                channels=["trades"],
                shutdown_timeout=-1.0
            )

        # Should pass - valid timeout
        config = CryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"],
            shutdown_timeout=30.0
        )
        assert config.shutdown_timeout == 30.0

    def test_configuration_immutability(self):
        """Configuration objects should be immutable (frozen dataclasses)."""
        config = CryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"]
        )
        
        # Should fail - configuration is frozen
        with pytest.raises(AttributeError):
            config.exchanges = ["coinbase"]

        with pytest.raises(AttributeError):
            config.normalize = False


class TestConfigurationIntegration:
    """Test configuration integration and factory functions."""

    def test_config_serialization_roundtrip(self):
        """Configuration objects should be serializable."""
        original_config = CryptofeedConfig(
            exchanges=["binance", "coinbase"],
            channels=["trades", "ticker"],
            symbols=["BTC-USD", "ETH-USD"],
            normalize=True,
            reconnect=False
        )

        # Should be able to convert to dict
        config_dict = {
            "exchanges": original_config.exchanges,
            "channels": original_config.channels,
            "symbols": original_config.symbols,
            "normalize": original_config.normalize,
            "reconnect": original_config.reconnect
        }

        # Should be able to recreate from dict
        recreated_config = CryptofeedConfig(**config_dict)
        
        assert recreated_config.exchanges == original_config.exchanges
        assert recreated_config.channels == original_config.channels
        assert recreated_config.symbols == original_config.symbols
        assert recreated_config.normalize == original_config.normalize
        assert recreated_config.reconnect == original_config.reconnect

    def test_backward_compatibility_warnings(self):
        """Deprecated functions should show deprecation warnings."""
        from quixstreams.sources.community.crypto.config import (
            create_local_cryptofeed_config,
            create_s3_binance_config
        )

        # Should show deprecation warning for cryptofeed
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = create_local_cryptofeed_config(
                exchanges=["binance"],
                channels=["trades"]
            )
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "deprecated" in str(w[0].message).lower()
            assert isinstance(config, CryptofeedConfig)

        # Should show deprecation warning for binance s3
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            config = create_s3_binance_config(
                bucket="test-bucket",
                prefix="data/"
            )
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert isinstance(config, BinanceS3Config)

    def test_config_factory_functions_consistency(self):
        """Factory functions should produce consistent, valid configurations."""
        from quixstreams.sources.community.crypto.config import (
            create_cryptofeed_config,
            create_ccxt_config,
            create_binance_s3_config
        )

        # Test cryptofeed factory
        config = create_cryptofeed_config(
            exchanges=["binance"],
            channels=["trades"], 
            symbols=["BTC-USD"],
            normalize=False
        )
        assert isinstance(config, CryptofeedConfig)
        assert config.exchanges == ["binance"]
        assert config.channels == ["trades"]
        assert config.symbols == ["BTC-USD"]
        assert config.normalize is False

        # Test CCXT factory
        config = create_ccxt_config(
            exchange="binance",
            mode="klines",
            symbols=["BTC/USDT"],
            interval="1h",
            rate_limit=False
        )
        assert isinstance(config, CCXTConfig)
        assert config.exchange == "binance"
        assert config.mode == "klines"
        assert config.symbols == ["BTC/USDT"]
        assert config.interval == "1h"
        assert config.rate_limit is False

        # Test Binance S3 factory
        config = create_binance_s3_config(
            bucket="test-bucket",
            prefix="data/",
            unsigned=True,
            datatype="klines"
        )
        assert isinstance(config, BinanceS3Config)
        assert config.bucket == "test-bucket"
        assert config.prefix == "data/"
        assert config.unsigned is True
        assert config.datatype == "klines"

    def test_environment_variable_override_behavior(self):
        """Environment variables should be overridden by explicit parameters."""
        env_vars = {
            "CRYPTO_EXCHANGES": "coinbase,kraken",
            "CRYPTO_CHANNELS": "ticker",
            "CRYPTO_NORMALIZE": "false"
        }
        
        with patch.dict(os.environ, env_vars):
            # Override environment with explicit parameters
            config = load_from_env(
                CryptofeedConfig,
                exchanges=["binance"],  # Override env
                normalize=True          # Override env
            )
            
            assert config.exchanges == ["binance"]  # Overridden
            assert config.channels == ["ticker"]    # From env
            assert config.normalize is True         # Overridden

    def test_configuration_validation_error_messages(self):
        """Configuration validation should provide helpful error messages."""
        # Test detailed error messages
        try:
            CCXTConfig(
                exchange="binance",
                mode="invalid_mode",
                symbols=["BTC/USDT"]
            )
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "klines" in str(e)  # Should mention valid options
            assert "trades" in str(e)
            assert "orderbook" in str(e)

        # Test error context for complex validation
        try:
            BinanceS3Config(
                bucket="test-bucket",
                prefix="data/",
                access_mode="templated_prefixes"
            )
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "prefix_template" in str(e)
            assert "required" in str(e)
            assert "templated_prefixes" in str(e)