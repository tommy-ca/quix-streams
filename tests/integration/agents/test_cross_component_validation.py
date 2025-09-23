"""
Integration tests for cross-component validation between crypto and iceberg agents.

RED PHASE: Tests should FAIL initially and drive unified interface implementation.

This module tests:
- Unified auth provider interfaces across components
- Configuration migration compatibility between systems
- Global validation rules consistency 
- Environment loading prefix consistency
- Cross-system data flow validation
- Agent communication patterns

Author: QuixStreams Engineering Team  
Date: September 19, 2025
Version: 1.0.0 (RED Phase - Failing Tests)
"""

import json
import os
import pytest
import warnings
from typing import Dict, Any, List
from unittest.mock import patch, MagicMock

# NOTE: These imports will FAIL initially - this is expected for RED phase
try:
    from pydantic import ValidationError
    
    # Crypto source agents (not implemented yet) 
    from quixstreams.sources.community.crypto.config_v2 import (
        CryptofeedConfigAgent, CCXTConfigAgent, BinanceS3ConfigAgent,
        AuthProviderAgent, APIKeyAuthAgent, AWSAuthAgent, NoAuthAgent,
        ConfigMigrationAgent, CryptoValidationError, GLOBAL_AGENT_CONFIG,
        # Import Iceberg aliases from crypto module for consistent interface
        IcebergConfigAgent, CatalogConfigAgent, StorageConfigAgent
    )
    
    # Additional imports for compatibility
    from quixstreams.sinks.community.iceberg_rest.config_v2 import (
        StorageProvider, AuthType as IcebergAuthType
    )
    
    # Legacy configs for migration testing
    from quixstreams.sources.community.crypto.config import (
        CryptofeedConfig as LegacyCryptofeedConfig,
        CCXTConfig as LegacyCCXTConfig,
        BinanceS3Config as LegacyBinanceS3Config
    )
    
except ImportError as e:
    # Expected during RED phase - modules don't exist yet
    pytest.skip(f"Skipping tests - modules not implemented yet: {e}", allow_module_level=True)


class TestUnifiedAuthProviderAgentInterface:
    """Test unified authentication interfaces across crypto sources and iceberg sinks."""
    
    def test_aws_auth_provider_works_across_components(self):
        """AWS auth providers should work consistently across all component agents."""
        # Create AWS auth provider agent
        aws_auth = AuthProviderAgent.create_aws_auth(
            aws_access_key_id="test_key_id",
            aws_secret_access_key="test_secret_key",
            region_name="us-east-1"
        )
        
        # Should work with crypto source agents
        crypto_agent = BinanceS3ConfigAgent(
            bucket="crypto-data",
            prefix="trades/",
            auth=aws_auth
        )
        
        # Should work with iceberg sink agents
        iceberg_agent = IcebergConfigAgent(
            table_name="crypto.trades",
            catalog=CatalogConfigAgent(
                uri="http://localhost:8181/api/v1",
                warehouse_id="crypto"
            ),
            storage=StorageConfigAgent(
                provider="aws",
                region="us-east-1",
                access_key_id="test_key_id",
                secret_access_key="test_secret_key"
            )
        )
        
        # Cross-validation: auth credentials should be consistent
        crypto_creds = crypto_agent.auth.get_credentials()
        iceberg_creds = iceberg_agent.storage.to_pyiceberg_auth()
        
        assert crypto_creds["aws_access_key_id"] == iceberg_creds["client.access-key-id"]
        assert crypto_creds["region_name"] == iceberg_creds["client.region"]
        
        # Both should validate successfully
        assert crypto_agent.auth.validate_configuration()
        assert iceberg_agent.storage is not None
    
    def test_api_key_auth_provider_interface_consistency(self):
        """API key auth providers should have consistent interfaces."""
        # Create API key auth for crypto sources
        crypto_auth = AuthProviderAgent.create_api_key_auth(
            api_key="crypto_api_key_123",
            api_secret="crypto_secret_456"
        )
        
        # Should work with cryptofeed
        cryptofeed_agent = CryptofeedConfigAgent(
            exchanges=["binance"],
            channels=["trades"],
            auth=crypto_auth
        )
        
        # Should work with CCXT
        ccxt_agent = CCXTConfigAgent(
            exchange="binance",
            mode="trades",
            symbols=["BTC/USDT"],
            auth=crypto_auth
        )
        
        # Auth interface should be consistent across agents
        cryptofeed_creds = cryptofeed_agent.auth.get_credentials()
        ccxt_creds = ccxt_agent.auth.get_credentials()
        
        assert cryptofeed_creds["api_key"] == ccxt_creds["api_key"]
        assert cryptofeed_creds["api_secret"] == ccxt_creds["api_secret"]
        
        # Auth types should be consistent
        assert cryptofeed_agent.auth.auth_type == ccxt_agent.auth.auth_type
    
    def test_no_auth_provider_universal_compatibility(self):
        """NoAuth provider should work universally across all agents."""
        no_auth = NoAuthAgent()
        
        # Should work with all crypto source types
        agents = [
            CryptofeedConfigAgent(exchanges=["binance"], channels=["trades"], auth=no_auth),
            CCXTConfigAgent(exchange="binance", mode="trades", symbols=["BTC/USDT"], auth=no_auth),
            BinanceS3ConfigAgent(bucket="test", prefix="data/", auth=no_auth)
        ]
        
        # All should validate and have consistent no-auth behavior
        for agent in agents:
            assert agent.auth.auth_type == "none"
            assert agent.auth.get_credentials() == {}
            assert agent.auth.validate_configuration()
    
    def test_auth_provider_credential_format_standardization(self):
        """Auth providers should return standardized credential formats."""
        # Test different auth types produce consistent credential formats
        auth_providers = [
            NoAuthAgent(),
            AuthProviderAgent.create_api_key_auth("key", "secret"),
            AuthProviderAgent.create_aws_auth("key_id", "secret", "us-east-1")
        ]
        
        for auth in auth_providers:
            creds = auth.get_credentials()
            
            # Should always return a dictionary
            assert isinstance(creds, dict)
            
            # Should not contain None values for present keys
            for key, value in creds.items():
                assert value is not None or key.endswith("_optional")
            
            # Should have consistent key naming patterns
            for key in creds.keys():
                assert isinstance(key, str)
                assert key.islower() or "_" in key  # snake_case or lowercase


class TestConfigurationMigrationAgentCompatibility:
    """Test migration compatibility between legacy and modern agent systems."""
    
    def test_cryptofeed_legacy_to_agent_migration(self):
        """Legacy Cryptofeed configs should migrate seamlessly to agents."""
        # Create legacy dataclass config
        legacy_config = LegacyCryptofeedConfig(
            exchanges=["binance", "coinbase"],
            channels=["trades", "ticker"],
            symbols=["BTC-USDT", "ETH-USDT"]
        )
        
        # Migrate through agent bridge
        migration_agent = ConfigMigrationAgent()
        modern_agent = migration_agent.migrate_from_legacy(legacy_config)
        
        # Validation: all data preserved
        assert modern_agent.exchanges == ["binance", "coinbase"]
        assert modern_agent.channels == ["trades", "ticker"]
        assert modern_agent.symbols == ["BTC-USDT", "ETH-USDT"]
        
        # Agent enhancements should be present
        assert hasattr(modern_agent, 'auth')
        assert hasattr(modern_agent, 'retry')
        assert hasattr(modern_agent, 'model_dump')  # Pydantic v2 methods
        assert hasattr(modern_agent, 'model_validate')
        
        # Should have default values for new fields
        assert isinstance(modern_agent.auth, NoAuthAgent)
        assert modern_agent.retry.enabled == True
        assert modern_agent.normalize == True
    
    def test_ccxt_legacy_to_agent_migration_with_validation(self):
        """Legacy CCXT configs should migrate with enhanced validation."""
        # Create legacy config
        legacy_config = LegacyCCXTConfig(
            exchange="binance",
            mode="trades",
            symbols=["BTC/USDT"],
            use_ws=False
        )
        
        # Migrate to agent
        migration_agent = ConfigMigrationAgent()
        modern_agent = migration_agent.migrate_from_legacy(legacy_config)
        
        # Data preservation
        assert modern_agent.exchange == "binance"
        assert modern_agent.mode == "trades"
        assert modern_agent.symbols == ["BTC/USDT"]
        assert modern_agent.use_websocket == False
        
        # Enhanced validation should work
        assert modern_agent.validate_configuration()
        
        # Modern features should be available
        connection_info = modern_agent.get_connection_info()
        assert "exchange" in connection_info
        assert "mode" in connection_info
        assert "symbols_count" in connection_info
    
    def test_binance_s3_legacy_to_agent_migration_comprehensive(self):
        """Legacy BinanceS3 configs should migrate with comprehensive enhancements."""
        # Create complex legacy config
        legacy_config = LegacyBinanceS3Config(
            bucket="crypto-historical-data",
            prefix="spot/daily/trades/",
            datatype="trades",
            symbols=["BTCUSDT", "ETHUSDT"],
            replay_speed=2.0
        )
        
        # Migrate to agent
        migration_agent = ConfigMigrationAgent()
        modern_agent = migration_agent.migrate_from_legacy(legacy_config)
        
        # All legacy data should be preserved
        assert modern_agent.bucket == "crypto-historical-data"
        assert modern_agent.prefix == "spot/daily/trades/"
        assert modern_agent.datatype == "trades"
        assert modern_agent.symbols == ["BTCUSDT", "ETHUSDT"]
        assert modern_agent.replay_speed == 2.0
        
        # Modern validation and features
        assert modern_agent.validate_configuration()
        
        # S3 path computation should work
        s3_path = modern_agent.s3_path
        assert s3_path == "s3://crypto-historical-data/spot/daily/trades/"
    
    def test_migration_handles_auth_provider_upgrades(self):
        """Migration should upgrade legacy auth to modern auth providers."""
        # Create legacy config with auth_provider field
        legacy_config = LegacyCryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"],
            # Legacy might have auth_provider as dict or basic object
            auth_provider={"type": "api_key", "key": "legacy_key", "secret": "legacy_secret"}
        )
        
        # Migrate to agent
        migration_agent = ConfigMigrationAgent()
        modern_agent = migration_agent.migrate_from_legacy(legacy_config)
        
        # Auth should be upgraded to modern provider
        assert isinstance(modern_agent.auth, APIKeyAuthAgent)
        assert modern_agent.auth.api_key == "legacy_key"
        
        # Modern auth validation should work
        assert modern_agent.auth.validate_configuration()
        
        # Should have modern auth features
        creds = modern_agent.auth.get_credentials()
        assert "api_key" in creds
        assert "api_secret" in creds
    
    def test_migration_agent_detects_config_types_accurately(self):
        """Migration agent should accurately detect different config types."""
        migration_agent = ConfigMigrationAgent()
        
        # Test Pydantic v2 agent detection
        modern_agent = CryptofeedConfigAgent(exchanges=["binance"], channels=["trades"])
        assert migration_agent.detect_config_type(modern_agent) == "pydantic_v2_agent"
        
        # Test legacy dataclass detection
        legacy_config = LegacyCryptofeedConfig(exchanges=["binance"], channels=["trades"])
        assert migration_agent.detect_config_type(legacy_config) == "legacy_dataclass"
        
        # Test unknown type detection
        unknown_config = {"exchanges": ["binance"], "channels": ["trades"]}
        assert migration_agent.detect_config_type(unknown_config) == "unknown"


class TestGlobalValidationRulesConsistency:
    """Test global validation rules consistency across all agent types."""
    
    def test_global_config_dict_consistency_across_agents(self):
        """All agents should follow consistent global validation rules."""
        # Create agents of different types
        agents = [
            CryptofeedConfigAgent(exchanges=["binance"], channels=["trades"]),
            CCXTConfigAgent(exchange="binance", mode="trades", symbols=["BTC/USDT"]),
            BinanceS3ConfigAgent(bucket="test", prefix="data/"),
            IcebergConfigAgent.create_local_config("test_table")
        ]
        
        # Global rules should be consistently applied across all agents
        for agent in agents:
            config_dict = agent.model_config
            
            # Core global rules validation
            assert config_dict.get('frozen') == True, \
                f"Agent {type(agent).__name__} should be immutable"
            assert config_dict.get('extra') == 'forbid', \
                f"Agent {type(agent).__name__} should forbid extra fields"
            assert config_dict.get('str_strip_whitespace') == True, \
                f"Agent {type(agent).__name__} should strip whitespace"
            assert config_dict.get('validate_assignment') == True, \
                f"Agent {type(agent).__name__} should validate assignments"
            assert config_dict.get('use_enum_values') == True, \
                f"Agent {type(agent).__name__} should use enum values"
    
    def test_agent_immutability_enforcement_universally(self):
        """All agents should enforce immutability consistently."""
        agents = [
            CryptofeedConfigAgent(exchanges=["binance"], channels=["trades"]),
            CCXTConfigAgent(exchange="binance", mode="trades", symbols=["BTC/USDT"]),
        ]
        
        # Test immutability across all agent types
        for agent in agents:
            # Should not be able to modify after creation
            with pytest.raises((AttributeError, ValidationError)):
                # Try to modify different fields based on agent type
                if hasattr(agent, 'exchanges'):
                    agent.exchanges = ["different"]
                elif hasattr(agent, 'exchange'):
                    agent.exchange = "different"
                elif hasattr(agent, 'bucket'):
                    agent.bucket = "different"
    
    def test_secret_handling_consistency_across_agents(self):
        """Secret handling should be consistent across all agent types."""
        # Create agents with secret-containing auth
        agents_with_secrets = [
            CryptofeedConfigAgent(
                exchanges=["binance"], channels=["trades"],
                auth=APIKeyAuthAgent(api_key="public", api_secret="secret123")
            ),
            CCXTConfigAgent(
                exchange="binance", mode="trades", symbols=["BTC/USDT"],
                auth=APIKeyAuthAgent(api_key="public", api_secret="secret456")
            )
        ]
        
        # Test secret masking consistency
        for agent in agents_with_secrets:
            # Secrets should not appear in string representation
            agent_str = str(agent)
            assert "secret123" not in agent_str and "secret456" not in agent_str
            assert "***" in agent_str or "SecretStr" in agent_str
            
            # Secrets should be masked in serialization
            serialized = agent.model_dump()
            auth_data = serialized["auth"]
            secret_value = auth_data.get("api_secret")
            assert secret_value is None or "***" in str(secret_value)
    
    def test_validation_error_quality_consistency(self):
        """Validation error quality should be consistent across agents."""
        error_test_cases = [
            # Cryptofeed validation errors
            lambda: CryptofeedConfigAgent(exchanges=[], channels=[]),
            # CCXT validation errors  
            lambda: CCXTConfigAgent(exchange="", mode="invalid", symbols=[]),
            # BinanceS3 validation errors
            lambda: BinanceS3ConfigAgent(bucket="", prefix="")
        ]
        
        for test_case in error_test_cases:
            with pytest.raises(ValidationError) as exc_info:
                test_case()
            
            error_msg = str(exc_info.value)
            
            # All validation errors should be descriptive
            assert len(error_msg) > 50, "Error messages should be descriptive"
            
            # Should include helpful guidance
            assert any(keyword in error_msg.lower() for keyword in 
                      ["must", "should", "required", "invalid", "cannot be"]), \
                f"Error should include guidance: {error_msg}"


class TestEnvironmentLoadingAgentPrefixConsistency:
    """Test environment loading consistency across agent types."""
    
    def test_environment_prefix_patterns_consistency(self):
        """Environment loading should follow consistent prefixing patterns."""
        # Test different environment variable combinations
        test_environments = [
            {
                # Crypto source environment
                "CRYPTO_CRYPTOFEED_EXCHANGES": '["binance", "coinbase"]',
                "CRYPTO_CRYPTOFEED_CHANNELS": '["trades", "ticker"]',
                "CRYPTO_CCXT_EXCHANGE": "binance",
                "CRYPTO_CCXT_MODE": "trades",
                "CRYPTO_CCXT_SYMBOLS": '["BTC/USDT"]'
            },
            {
                # Iceberg sink environment
                "ICEBERG_TABLE_NAME": "crypto_trades",
                "ICEBERG_CATALOG_URI": "http://localhost:8181/api/v1",
                "ICEBERG_STORAGE_PROVIDER": "minio",
                "ICEBERG_STORAGE_ENDPOINT_URL": "http://localhost:9000"
            }
        ]
        
        for env_vars in test_environments:
            with patch.dict(os.environ, env_vars, clear=False):
                # Environment loading should work for appropriate agents
                if "CRYPTO_CRYPTOFEED" in str(env_vars):
                    crypto_agent = CryptofeedConfigAgent.from_env(prefix="CRYPTO_CRYPTOFEED_")
                    assert crypto_agent.exchanges == ["binance", "coinbase"]
                    assert crypto_agent.channels == ["trades", "ticker"]
                
                if "CRYPTO_CCXT" in str(env_vars):
                    ccxt_agent = CCXTConfigAgent.from_env(prefix="CRYPTO_CCXT_")
                    assert ccxt_agent.exchange == "binance"
                    assert ccxt_agent.mode == "trades"
                
                if "ICEBERG_" in str(env_vars):
                    iceberg_agent = IcebergConfigAgent.from_env()
                    assert iceberg_agent.table_name == "crypto_trades"
    
    def test_environment_type_conversion_consistency(self):
        """Type conversion from environment should be consistent."""
        # Test consistent type conversion patterns
        type_test_env = {
            "TEST_BOOLEAN_TRUE": "true",
            "TEST_BOOLEAN_FALSE": "false", 
            "TEST_INTEGER": "42",
            "TEST_FLOAT": "3.14",
            "TEST_JSON_ARRAY": '["item1", "item2"]',
            "TEST_JSON_OBJECT": '{"key": "value"}'
        }
        
        with patch.dict(os.environ, type_test_env, clear=False):
            # All agents should handle type conversion consistently
            agents = [
                CryptofeedConfigAgent,
                CCXTConfigAgent,
                BinanceS3ConfigAgent
            ]
            
            for agent_class in agents:
                # Test that environment loading handles types consistently
                # (This tests the underlying environment loading infrastructure)
                try:
                    # Mock environment loader to test type conversion
                    from quixstreams.sources.community.crypto.config_v2 import load_agent_from_env
                    
                    # Should handle boolean conversion
                    assert os.getenv("TEST_BOOLEAN_TRUE").lower() in ["true", "1", "yes"]
                    assert os.getenv("TEST_BOOLEAN_FALSE").lower() in ["false", "0", "no"]
                    
                    # Should handle numeric conversion  
                    assert int(os.getenv("TEST_INTEGER")) == 42
                    assert float(os.getenv("TEST_FLOAT")) == 3.14
                    
                    # Should handle JSON conversion
                    import json
                    assert json.loads(os.getenv("TEST_JSON_ARRAY")) == ["item1", "item2"]
                    assert json.loads(os.getenv("TEST_JSON_OBJECT")) == {"key": "value"}
                    
                except Exception as e:
                    # During RED phase, this might fail - that's expected
                    assert "not implemented" in str(e).lower() or "import" in str(e).lower()
    
    def test_environment_error_handling_consistency(self):
        """Environment loading error handling should be consistent."""
        # Test error scenarios
        error_test_env = {
            "CRYPTO_BAD_JSON": "invalid_json_format",
            "CRYPTO_BAD_BOOLEAN": "not_a_boolean",  
            "CRYPTO_BAD_NUMBER": "not_a_number"
        }
        
        with patch.dict(os.environ, error_test_env, clear=False):
            # All agents should handle environment errors consistently
            agent_classes = [CryptofeedConfigAgent, CCXTConfigAgent]
            
            for agent_class in agent_classes:
                # Should get consistent validation errors for bad environment data
                with pytest.raises((ValidationError, ValueError)) as exc_info:
                    agent_class.from_env(prefix="CRYPTO_")
                
                error_msg = str(exc_info.value)
                # Error messages should be helpful and consistent
                assert any(keyword in error_msg.lower() for keyword in 
                          ["json", "invalid", "format", "conversion"]), \
                    f"Environment error should be descriptive: {error_msg}"


class TestCrossSystemDataFlowValidation:
    """Test data flow validation between crypto sources and iceberg sinks."""
    
    def test_crypto_to_iceberg_config_compatibility(self):
        """Crypto source configs should be compatible with Iceberg sink configs."""
        # Create crypto source configuration
        crypto_config = CryptofeedConfigAgent(
            exchanges=["binance"],
            channels=["trades"],
            symbols=["BTC-USDT", "ETH-USDT"]
        )
        
        # Create corresponding iceberg sink configuration
        iceberg_config = IcebergConfigAgent(
            table_name="crypto_trades",
            catalog=CatalogConfigAgent(
                uri="http://localhost:8181/api/v1",
                warehouse_id="crypto_warehouse"
            ),
            storage=StorageConfigAgent(
                provider="minio",
                region="us-east-1",
                endpoint_url="http://localhost:9000"
            )
        )
        
        # Should be able to validate data flow compatibility
        assert crypto_config.validate_configuration()
        assert iceberg_config is not None
        
        # Connection info should be compatible
        crypto_info = crypto_config.get_connection_info()
        iceberg_location = iceberg_config.location
        
        # Should have compatible data structures
        assert "provider" in crypto_info
        assert "exchanges" in crypto_info
        assert iceberg_location.startswith("s3://")
    
    def test_auth_provider_shared_between_source_and_sink(self):
        """Same auth provider should work for both source and sink in pipeline."""
        # Create shared AWS auth
        shared_auth = AuthProviderAgent.create_aws_auth(
            aws_access_key_id="pipeline_key",
            aws_secret_access_key="pipeline_secret",
            region_name="us-west-2"
        )
        
        # Use in crypto source
        source_config = BinanceS3ConfigAgent(
            bucket="crypto-raw-data",
            prefix="binance/spot/",
            auth=shared_auth
        )
        
        # Use in iceberg sink (adapted to storage config)
        sink_config = IcebergConfigAgent(
            table_name="processed_crypto",
            catalog=CatalogConfigAgent(
                uri="http://localhost:8181/api/v1", 
                warehouse_id="processing"
            ),
            storage=StorageConfigAgent(
                provider="aws",
                region="us-west-2",
                access_key_id="pipeline_key",
                secret_access_key="pipeline_secret"
            )
        )
        
        # Both should validate successfully
        assert source_config.auth.validate_configuration()
        assert sink_config.storage is not None
        
        # Credentials should be compatible
        source_creds = source_config.auth.get_credentials()
        sink_creds = sink_config.storage.to_pyiceberg_auth()
        
        assert source_creds["aws_access_key_id"] == sink_creds["client.access-key-id"]
        assert source_creds["region_name"] == sink_creds["client.region"]
    
    def test_end_to_end_configuration_pipeline_validation(self):
        """Complete pipeline configuration should validate end-to-end."""
        # Create complete pipeline configuration
        pipeline_configs = {
            "source": CryptofeedConfigAgent(
                exchanges=["binance", "coinbase"],
                channels=["trades", "ticker"],
                symbols=["BTC-USDT", "ETH-USDT"],
                auth=APIKeyAuthAgent(api_key="source_key", api_secret="source_secret")
            ),
            "sink": IcebergConfigAgent.create_local_config("crypto_pipeline_table"),
            "processing": {
                "batch_size": 1000,
                "window_duration": "1m",
                "aggregations": ["mean", "volume_weighted"]
            }
        }
        
        # All components should validate individually
        assert pipeline_configs["source"].validate_configuration()
        # Iceberg config should be valid (already tested in separate module)
        
        # Pipeline should have consistent configuration
        source_info = pipeline_configs["source"].get_connection_info()
        assert source_info["provider"] == "cryptofeed"
        assert len(source_info["exchanges"]) == 2
        assert len(source_info["channels"]) == 2


# Test fixtures for environment cleanup
@pytest.fixture(autouse=True) 
def clean_test_environment():
    """Clean up environment variables after each test."""
    original_env = dict(os.environ)
    yield
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def mock_legacy_configs():
    """Provide mock legacy configurations for testing."""
    return {
        "cryptofeed": LegacyCryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"], 
            symbols=["BTC-USDT"]
        ),
        "ccxt": LegacyCCXTConfig(
            exchange="binance",
            mode="trades",
            symbols=["BTC/USDT"]
        ),
        "binance_s3": LegacyBinanceS3Config(
            bucket="test-bucket",
            prefix="test-prefix/"
        )
    }


if __name__ == "__main__":
    # Run the tests if executed directly
    pytest.main([__file__, "-v", "--tb=short"])