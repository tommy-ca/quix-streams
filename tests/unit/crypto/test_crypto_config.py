"""
Greenfield Crypto Configuration Tests

NO MOCKS, NO LEGACY, NO COMPATIBILITY, START SMALL, SOLID, KISS, DRY, CONSISTENT NAMING

For greenfield projects, we only need:
1. Create config agents with simple data
2. Validate configurations
3. Use configurations

Author: QuixStreams Engineering Team
Date: September 20, 2025
Version: 3.0.0 (Greenfield - Pure)
"""

import pytest
from quixstreams.sources.community.crypto.simple_config import (
    CryptofeedConfig, CCXTConfig, BinanceS3Config,
    APIKeyAuth, AWSAuth, NoAuth,
    ValidationError
)


class TestCryptofeedConfig:
    """Test Cryptofeed configuration creation and validation."""
    
    def test_create_basic_cryptofeed_config(self):
        """Should create basic cryptofeed configuration."""
        config = CryptofeedConfig(
            exchanges=["binance", "kraken"],
            channels=["trades", "ticker"],
            symbols=["BTC-USDT", "ETH-USDT"]
        )
        
        assert config.exchanges == ["binance", "kraken"]
        assert config.channels == ["trades", "ticker"]
        assert config.symbols == ["BTC-USDT", "ETH-USDT"]
        assert config.validate()
    
    def test_create_cryptofeed_with_auth(self):
        """Should create cryptofeed configuration with authentication."""
        auth = APIKeyAuth(api_key="test_key", api_secret="test_secret")
        
        config = CryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"],
            auth=auth
        )
        
        assert isinstance(config.auth, APIKeyAuth)
        assert config.auth.api_key == "test_key"
        assert config.validate()
    
    def test_cryptofeed_validation_fails_for_invalid_exchange(self):
        """Should fail validation for unsupported exchange."""
        with pytest.raises(ValidationError) as exc_info:
            CryptofeedConfig(
                exchanges=["invalid_exchange"],
                channels=["trades"]
            )
        
        assert "unsupported exchange" in str(exc_info.value).lower()
    
    def test_cryptofeed_validation_fails_for_empty_exchanges(self):
        """Should fail validation for empty exchanges list."""
        with pytest.raises(ValidationError) as exc_info:
            CryptofeedConfig(
                exchanges=[],
                channels=["trades"]
            )
        
        assert "exchanges" in str(exc_info.value).lower()


class TestCCXTConfig:
    """Test CCXT configuration creation and validation."""
    
    def test_create_basic_ccxt_config(self):
        """Should create basic CCXT configuration."""
        config = CCXTConfig(
            exchange="binance",
            mode="trades",
            symbols=["BTC/USDT", "ETH/USDT"]
        )
        
        assert config.exchange == "binance"
        assert config.mode == "trades"
        assert config.symbols == ["BTC/USDT", "ETH/USDT"]
        assert config.validate()
    
    def test_create_ccxt_with_websocket(self):
        """Should create CCXT configuration with WebSocket."""
        config = CCXTConfig(
            exchange="binance",
            mode="trades",
            symbols=["BTC/USDT"],
            use_websocket=True
        )
        
        assert config.use_websocket is True
        assert config.validate()
    
    def test_ccxt_validation_requires_exchange(self):
        """Should require exchange field."""
        with pytest.raises(ValidationError) as exc_info:
            CCXTConfig(
                exchange="",
                mode="trades",
                symbols=["BTC/USDT"]
            )
        
        assert "exchange" in str(exc_info.value).lower()


class TestBinanceS3Config:
    """Test BinanceS3 configuration creation and validation."""
    
    def test_create_basic_binance_s3_config(self):
        """Should create basic BinanceS3 configuration."""
        config = BinanceS3Config(
            bucket="crypto-data",
            prefix="binance/trades/",
            datatype="trades"
        )
        
        assert config.bucket == "crypto-data"
        assert config.prefix == "binance/trades/"
        assert config.datatype == "trades"
        assert config.validate()
    
    def test_create_binance_s3_with_aws_auth(self):
        """Should create BinanceS3 configuration with AWS auth."""
        auth = AWSAuth(
            aws_access_key_id="test_key_id",
            aws_secret_access_key="test_secret_key",
            region_name="us-east-1"
        )
        
        config = BinanceS3Config(
            bucket="crypto-data",
            prefix="data/",
            auth=auth
        )
        
        assert isinstance(config.auth, AWSAuth)
        assert config.auth.aws_access_key_id == "test_key_id"
        assert config.validate()
    
    def test_binance_s3_validation_requires_bucket(self):
        """Should require bucket field."""
        with pytest.raises(ValidationError) as exc_info:
            BinanceS3Config(
                bucket="",
                prefix="data/"
            )
        
        assert "bucket" in str(exc_info.value).lower()


class TestAuthProviders:
    """Test authentication provider creation and validation."""
    
    def test_create_no_auth(self):
        """Should create no authentication."""
        auth = NoAuth()
        
        assert auth.auth_type == "none"
        assert auth.get_credentials() == {}
        assert auth.validate()
    
    def test_create_api_key_auth(self):
        """Should create API key authentication."""
        auth = APIKeyAuth(
            api_key="test_api_key",
            api_secret="test_api_secret"
        )
        
        assert auth.auth_type == "api_key"
        assert auth.api_key == "test_api_key"
        assert auth.api_secret == "test_api_secret"
        
        credentials = auth.get_credentials()
        assert credentials["api_key"] == "test_api_key"
        assert credentials["api_secret"] == "test_api_secret"
        assert auth.validate()
    
    def test_create_aws_auth(self):
        """Should create AWS authentication."""
        auth = AWSAuth(
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region_name="us-west-2"
        )
        
        assert auth.auth_type == "aws"
        assert auth.aws_access_key_id == "AKIAIOSFODNN7EXAMPLE"
        assert auth.region_name == "us-west-2"
        
        credentials = auth.get_credentials()
        assert credentials["aws_access_key_id"] == "AKIAIOSFODNN7EXAMPLE"
        assert auth.validate()
    
    def test_api_key_auth_validation_requires_key(self):
        """Should require API key."""
        with pytest.raises(ValidationError) as exc_info:
            APIKeyAuth(api_key="", api_secret="secret")
        
        assert "api key" in str(exc_info.value).lower()


class TestConfigIntegration:
    """Test configuration integration scenarios."""
    
    def test_multiple_configs_work_independently(self):
        """Should create multiple configs without interference."""
        cryptofeed = CryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"]
        )
        
        ccxt = CCXTConfig(
            exchange="kraken",
            mode="ticker",
            symbols=["BTC/USD"]
        )
        
        binance_s3 = BinanceS3Config(
            bucket="data-bucket",
            prefix="crypto/"
        )
        
        # All should be valid and independent
        assert cryptofeed.validate()
        assert ccxt.validate()
        assert binance_s3.validate()
        
        # Verify independence
        assert cryptofeed.exchanges != ccxt.exchange
        assert ccxt.symbols != binance_s3.bucket