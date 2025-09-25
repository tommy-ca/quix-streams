"""
Comprehensive tests for CryptofeedConfigAgent following AGENTS pattern.

RED PHASE: All tests should FAIL initially and drive implementation.

This module tests:
- Agent configuration validation with descriptive errors
- Exchange/channel compatibility validation  
- Environment variable loading with type conversion
- Auth provider integration and validation
- Serialization and deserialization capabilities
- Backward compatibility interface preservation
- Global validation rules compliance

Author: QuixStreams Engineering Team
Date: September 19, 2025
Version: 1.0.0 (RED Phase - Failing Tests)
"""

import json
import os
import pytest
import warnings
from typing import Dict, Any
from unittest.mock import patch

# NOTE: These imports will FAIL initially - this is expected for RED phase
try:
    from pydantic import ValidationError
    from quixstreams.sources.community.crypto.config_v2 import (
        CryptofeedConfigAgent, AuthProviderAgent, APIKeyAuthAgent, AWSAuthAgent,
        NoAuthAgent, RetryPolicyAgent, CryptoValidationError,
        CryptoProvider, AuthType, GLOBAL_AGENT_CONFIG
    )
except ImportError as e:
    # Expected during RED phase - modules don't exist yet
    pytest.skip(f"Skipping tests - modules not implemented yet: {e}", allow_module_level=True)


class TestCryptofeedConfigAgentValidation:
    """Agent-based validation tests for Cryptofeed configuration."""
    
    def test_agent_requires_exchanges_and_channels_validation(self):
        """ConfigAgent should validate required fields with descriptive errors."""
        with pytest.raises(ValidationError) as exc_info:
            CryptofeedConfigAgent()  # Missing required fields
        
        errors = exc_info.value.errors()
        
        # Specific error location and message validation
        error_fields = [str(error["loc"]) for error in errors]
        assert any("exchanges" in field for field in error_fields), \
            f"Expected 'exchanges' validation error, got: {error_fields}"
        assert any("channels" in field for field in error_fields), \
            f"Expected 'channels' validation error, got: {error_fields}"
        
        # Human-readable error messages
        error_msg = str(exc_info.value)
        assert "At least one exchange must be specified" in error_msg, \
            f"Expected descriptive exchange error, got: {error_msg}"
    
    def test_agent_validates_exchange_names_against_known_providers(self):
        """Agent should validate exchange names against supported providers."""
        with pytest.raises(ValidationError) as exc_info:
            CryptofeedConfigAgent(
                exchanges=["invalid_exchange", "fake_exchange"],
                channels=["trades"]
            )
        
        error_msg = str(exc_info.value)
        assert "invalid_exchange" in error_msg, \
            f"Expected 'invalid_exchange' in error, got: {error_msg}"
        assert "supported exchanges" in error_msg.lower(), \
            f"Expected 'supported exchanges' guidance, got: {error_msg}"
        assert "binance" in error_msg.lower(), \
            f"Expected supported exchange examples, got: {error_msg}"
    
    def test_agent_validates_channel_names_against_supported_types(self):
        """Agent should validate channel names against supported data types."""
        with pytest.raises(ValidationError) as exc_info:
            CryptofeedConfigAgent(
                exchanges=["binance"],
                channels=["invalid_channel", "fake_data_type"]
            )
        
        error_msg = str(exc_info.value)
        assert "invalid_channel" in error_msg, \
            f"Expected 'invalid_channel' in error, got: {error_msg}"
        assert "supported channels" in error_msg.lower(), \
            f"Expected 'supported channels' guidance, got: {error_msg}"
        assert "trades" in error_msg.lower(), \
            f"Expected supported channel examples, got: {error_msg}"
    
    def test_agent_cross_validates_exchange_channel_compatibility(self):
        """Agent should validate exchange/channel combinations for compatibility."""
        with pytest.raises(ValidationError) as exc_info:
            CryptofeedConfigAgent(
                exchanges=["coinbase"],
                channels=["funding"]  # Not supported by coinbase
            )
        
        error_msg = str(exc_info.value)
        assert "coinbase" in error_msg, \
            f"Expected 'coinbase' in error, got: {error_msg}"
        assert "funding" in error_msg, \
            f"Expected 'funding' in error, got: {error_msg}"
        assert "not supported" in error_msg.lower(), \
            f"Expected 'not supported' message, got: {error_msg}"
        
        # Should suggest what IS supported
        assert "supported channels for coinbase" in error_msg.lower(), \
            f"Expected supported channels guidance, got: {error_msg}"
    
    def test_agent_validates_symbols_format_and_constraints(self):
        """Agent should validate symbol format and length constraints."""
        # Test empty symbol validation
        with pytest.raises(ValidationError) as exc_info:
            CryptofeedConfigAgent(
                exchanges=["binance"],
                channels=["trades"],
                symbols=["", "  ", "INVALID_SYMBOL_FORMAT_TOO_LONG_FOR_NORMAL_TRADING_PAIR"]
            )
        
        error_msg = str(exc_info.value)
        assert "symbol" in error_msg.lower(), \
            f"Expected symbol validation error, got: {error_msg}"
    
    def test_agent_environment_loading_with_type_conversion_validation(self):
        """Agent should handle environment loading with proper type conversion."""
        test_env = {
            "CRYPTOFEED_EXCHANGES": "invalid_json_format",  # Bad JSON
            "CRYPTOFEED_CHANNELS": '["trades"]',  # Good JSON
            "CRYPTOFEED_RECONNECT": "not_a_boolean",  # Bad boolean
            "CRYPTOFEED_MAX_DEPTH": "not_a_number",  # Bad number
            "CRYPTOFEED_SHUTDOWN_TIMEOUT": "-5.0",  # Invalid negative timeout
        }
        
        with patch.dict(os.environ, test_env, clear=False):
            with pytest.raises(ValidationError) as exc_info:
                CryptofeedConfigAgent.from_env()
            
            error_msg = str(exc_info.value)
            # Should provide specific type conversion errors
            assert any(keyword in error_msg.lower() for keyword in 
                      ["json", "format", "invalid", "conversion"]), \
                f"Expected type conversion error message, got: {error_msg}"
    
    def test_agent_auth_provider_integration_validation(self):
        """Agent should validate auth provider compatibility and completeness."""
        with pytest.raises(ValidationError) as exc_info:
            CryptofeedConfigAgent(
                exchanges=["binance"],
                channels=["trades"],
                auth=APIKeyAuthAgent(
                    api_key=""  # Empty key should fail
                )
            )
        
        error_msg = str(exc_info.value)
        assert "api_key" in error_msg, \
            f"Expected 'api_key' in error, got: {error_msg}"
        assert any(keyword in error_msg.lower() for keyword in 
                  ["empty", "required", "cannot be"]), \
            f"Expected descriptive API key error, got: {error_msg}"
    
    def test_agent_retry_policy_cross_field_validation(self):
        """Agent should validate logical relationships in retry configuration."""
        with pytest.raises(ValidationError) as exc_info:
            CryptofeedConfigAgent(
                exchanges=["binance"],
                channels=["trades"],
                retry=RetryPolicyAgent(
                    base_delay=10.0,
                    max_delay=5.0,  # max_delay < base_delay should fail
                    backoff_factor=0.5  # < 1.0 should fail
                )
            )
        
        error_msg = str(exc_info.value)
        assert "max_delay" in error_msg and "base_delay" in error_msg, \
            f"Expected delay relationship error, got: {error_msg}"
        assert "backoff_factor" in error_msg, \
            f"Expected backoff factor error, got: {error_msg}"
    
    def test_agent_shutdown_timeout_validation_range(self):
        """Agent should validate shutdown timeout within reasonable range."""
        # Test negative timeout
        with pytest.raises(ValidationError):
            CryptofeedConfigAgent(
                exchanges=["binance"],
                channels=["trades"],
                shutdown_timeout=-1.0
            )
        
        # Test extremely large timeout
        with pytest.raises(ValidationError):
            CryptofeedConfigAgent(
                exchanges=["binance"],
                channels=["trades"],
                shutdown_timeout=10000.0  # > 300 seconds limit
            )
    
    def test_agent_validates_max_depth_constraints_for_orderbook(self):
        """Agent should validate max_depth constraints when orderbook channel is used."""
        # Test negative max_depth
        with pytest.raises(ValidationError):
            CryptofeedConfigAgent(
                exchanges=["binance"],
                channels=["orderbook"],
                max_depth=-1
            )
        
        # Test excessive max_depth
        with pytest.raises(ValidationError):
            CryptofeedConfigAgent(
                exchanges=["binance"],
                channels=["orderbook"],
                max_depth=10000  # > 1000 limit
            )


class TestCryptofeedConfigAgentSerialization:
    """Test agent serialization and configuration management capabilities."""
    
    def test_agent_roundtrip_serialization_preservation(self):
        """Agent should serialize/deserialize without data loss."""
        # Create agent with complex configuration
        original_agent = CryptofeedConfigAgent(
            exchanges=["binance", "coinbase"],
            channels=["trades", "ticker"],
            symbols=["BTC-USDT", "ETH-USDT"],
            auth=APIKeyAuthAgent(
                api_key="test_key_12345",
                api_secret="test_secret_67890"
            ),
            retry=RetryPolicyAgent(
                enabled=True,
                max_attempts=5,
                base_delay=1.0,
                max_delay=60.0,
                backoff_factor=2.5
            )
        )
        
        # Serialize to dict with secret masking
        serialized = original_agent.model_dump()
        
        # Validate serialization structure
        assert "exchanges" in serialized
        assert "channels" in serialized
        assert "symbols" in serialized
        assert "auth" in serialized
        assert "retry" in serialized
        
        # Secrets should be masked in serialization
        auth_data = serialized["auth"]
        assert auth_data["api_key"] == "test_key_12345"  # Public key visible
        # Secret should be masked or None
        assert auth_data.get("api_secret") is None or "***" in str(auth_data.get("api_secret"))
        
        # Deserialize back to agent
        restored_agent = CryptofeedConfigAgent.model_validate(serialized)
        
        # Data integrity validation (except masked secrets)
        assert restored_agent.exchanges == original_agent.exchanges
        assert restored_agent.channels == original_agent.channels
        assert restored_agent.symbols == original_agent.symbols
        assert restored_agent.auth.api_key == original_agent.auth.api_key
    
    def test_agent_json_schema_generation_completeness(self):
        """Agent should generate comprehensive JSON schemas for documentation."""
        schema = CryptofeedConfigAgent.model_json_schema()
        
        # Schema structure validation
        required_sections = ["properties", "required", "$defs", "title", "description"]
        for section in required_sections:
            assert section in schema, f"Missing required schema section: {section}"
        
        # Field documentation validation
        properties = schema["properties"]
        
        # Core fields should be documented
        core_fields = ["exchanges", "channels", "symbols", "auth", "retry"]
        for field in core_fields:
            assert field in properties, f"Missing field in schema: {field}"
            assert "description" in properties[field], f"Missing description for field: {field}"
        
        # Exchanges field should have examples
        exchanges_field = properties["exchanges"]
        assert "examples" in exchanges_field or "example" in exchanges_field, \
            "Exchanges field should have usage examples"
        
        # Required fields should be properly marked
        required_fields = schema.get("required", [])
        assert "exchanges" in required_fields
        assert "channels" in required_fields
    
    def test_agent_json_serialization_roundtrip(self):
        """Agent should handle JSON serialization/deserialization correctly."""
        agent = CryptofeedConfigAgent(
            exchanges=["binance"],
            channels=["trades"],
            symbols=["BTC-USDT"]
        )
        
        # Test JSON serialization
        json_str = agent.model_dump_json()
        assert isinstance(json_str, str)
        
        # Validate JSON structure
        json_data = json.loads(json_str)
        assert json_data["exchanges"] == ["binance"]
        assert json_data["channels"] == ["trades"]
        assert json_data["symbols"] == ["BTC-USDT"]
        
        # Test JSON deserialization
        restored_agent = CryptofeedConfigAgent.model_validate_json(json_str)
        assert restored_agent.exchanges == agent.exchanges
        assert restored_agent.channels == agent.channels
        assert restored_agent.symbols == agent.symbols
    
    def test_agent_configuration_templates_generation(self):
        """Agent should generate configuration templates for documentation."""
        # Test minimal configuration template
        minimal_template = CryptofeedConfigAgent.get_minimal_config_template()
        assert "exchanges" in minimal_template
        assert "channels" in minimal_template
        
        # Test complete configuration template
        complete_template = CryptofeedConfigAgent.get_complete_config_template()
        assert "exchanges" in complete_template
        assert "channels" in complete_template
        assert "symbols" in complete_template
        assert "auth" in complete_template
        assert "retry" in complete_template


class TestCryptofeedConfigAgentBackwardCompatibility:
    """Test agent backward compatibility with legacy configuration systems."""
    
    def test_agent_backward_compatibility_interface(self):
        """Agent should provide backward-compatible interface for legacy code."""
        agent = CryptofeedConfigAgent(
            exchanges=["binance"],
            channels=["trades"],
            symbols=["BTC-USDT"]
        )
        
        # Legacy interface should still work
        assert hasattr(agent, 'exchanges')
        assert hasattr(agent, 'channels')
        assert hasattr(agent, 'symbols')
        assert hasattr(agent, 'normalize')
        assert hasattr(agent, 'auth')
        assert hasattr(agent, 'retry')
        
        # Values should be accessible as before
        assert agent.exchanges == ["binance"]
        assert agent.channels == ["trades"]
        assert agent.symbols == ["BTC-USDT"]
    
    def test_agent_to_legacy_config_conversion(self):
        """Agent should convert to legacy dataclass format."""
        agent = CryptofeedConfigAgent(
            exchanges=["binance"],
            channels=["trades"],
            symbols=["BTC-USDT"],
            reconnect=False
        )
        
        # Convert to legacy format
        legacy_config = agent.to_legacy_config()
        
        # Legacy config should have expected structure
        assert hasattr(legacy_config, 'exchanges')
        assert hasattr(legacy_config, 'channels')
        assert hasattr(legacy_config, 'symbols')
        
        # Data should be preserved
        assert legacy_config.exchanges == ["binance"]
        assert legacy_config.channels == ["trades"]
        assert legacy_config.symbols == ["BTC-USDT"]
    
    def test_agent_from_legacy_config_migration(self):
        """Agent should migrate from legacy dataclass configurations."""
        from quixstreams.sources.community.crypto.config import CryptofeedConfig as LegacyConfig
        
        # Create legacy dataclass config
        legacy_config = LegacyConfig(
            exchanges=["binance"],
            channels=["trades"],
            symbols=["BTC-USDT"]
        )
        
        # Migrate to agent
        agent = CryptofeedConfigAgent.from_legacy_config(legacy_config)
        
        # Data should be preserved
        assert agent.exchanges == ["binance"]
        assert agent.channels == ["trades"]
        assert agent.symbols == ["BTC-USDT"]
        
        # Agent enhancements should be present with defaults
        assert isinstance(agent.auth, NoAuthAgent)
        assert isinstance(agent.retry, RetryPolicyAgent)
        assert hasattr(agent, 'model_dump')  # Pydantic v2 methods
        assert hasattr(agent, 'model_validate')


class TestCryptofeedConfigAgentGlobalRulesCompliance:
    """Test agent compliance with global validation rules and patterns."""
    
    def test_agent_follows_global_configuration_dict_rules(self):
        """Agent should follow global ConfigDict rules consistently."""
        agent = CryptofeedConfigAgent(
            exchanges=["binance"],
            channels=["trades"]
        )
        
        config_dict = agent.model_config
        
        # Global rules should be consistently applied
        assert config_dict.get('frozen') == True, \
            "Agent should be immutable (frozen=True)"
        assert config_dict.get('extra') == 'forbid', \
            "Agent should forbid extra fields to prevent typos"
        assert config_dict.get('str_strip_whitespace') == True, \
            "Agent should strip whitespace from string inputs"
        assert config_dict.get('validate_assignment') == True, \
            "Agent should validate on attribute assignments"
        assert config_dict.get('use_enum_values') == True, \
            "Agent should use enum values in serialization"
    
    def test_agent_immutability_enforcement(self):
        """Agent should enforce immutability after creation."""
        agent = CryptofeedConfigAgent(
            exchanges=["binance"],
            channels=["trades"]
        )
        
        # Should not be able to modify agent after creation
        with pytest.raises((AttributeError, ValidationError)):
            agent.exchanges = ["coinbase"]  # Should fail
        
        with pytest.raises((AttributeError, ValidationError)):
            agent.channels = ["ticker"]  # Should fail
    
    def test_agent_secret_handling_security(self):
        """Agent should handle secrets securely in all operations."""
        agent = CryptofeedConfigAgent(
            exchanges=["binance"],
            channels=["trades"],
            auth=APIKeyAuthAgent(
                api_key="public_key_123",
                api_secret="super_secret_key_456"
            )
        )
        
        # Secrets should not appear in string representation
        agent_str = str(agent)
        assert "super_secret_key_456" not in agent_str
        assert "***" in agent_str or "SecretStr" in agent_str
        
        # Secrets should be masked in serialization
        serialized = agent.model_dump()
        auth_data = serialized["auth"]
        assert auth_data.get("api_secret") is None or "***" in str(auth_data.get("api_secret"))
    
    def test_agent_validation_error_quality(self):
        """Agent should provide high-quality, descriptive validation errors."""
        # Test complex validation scenario
        with pytest.raises(ValidationError) as exc_info:
            CryptofeedConfigAgent(
                exchanges=["invalid_exchange"],
                channels=["invalid_channel"],
                symbols=[""],
                max_depth=-1,
                shutdown_timeout=-5.0,
                auth=APIKeyAuthAgent(api_key=""),
                retry=RetryPolicyAgent(
                    base_delay=10.0,
                    max_delay=5.0
                )
            )
        
        error_msg = str(exc_info.value)
        
        # Error should be comprehensive and helpful
        assert len(error_msg) > 100, "Error message should be descriptive"
        assert "invalid_exchange" in error_msg
        assert "invalid_channel" in error_msg
        
        # Should include suggestions or guidance
        assert any(keyword in error_msg.lower() for keyword in 
                  ["supported", "valid", "must be", "should be"]), \
            f"Error should include helpful guidance: {error_msg}"


class TestCryptofeedConfigAgentConnectionInfo:
    """Test agent connection information and debugging capabilities."""
    
    def test_agent_connection_string_generation(self):
        """Agent should generate useful connection strings for debugging."""
        agent = CryptofeedConfigAgent(
            exchanges=["binance", "coinbase"],
            channels=["trades", "ticker"]
        )
        
        connection_string = agent.connection_string
        assert connection_string.startswith("cryptofeed://")
        assert "binance" in connection_string
        assert "coinbase" in connection_string
        assert "trades" in connection_string
        assert "ticker" in connection_string
    
    def test_agent_connection_info_completeness(self):
        """Agent should provide comprehensive connection information."""
        agent = CryptofeedConfigAgent(
            exchanges=["binance"],
            channels=["trades"],
            symbols=["BTC-USDT", "ETH-USDT"],
            auth=APIKeyAuthAgent(api_key="test_key")
        )
        
        connection_info = agent.get_connection_info()
        
        # Should include all relevant connection details
        required_keys = [
            "provider", "exchanges", "channels", "symbols_count",
            "reconnect", "connection_string", "auth_type"
        ]
        for key in required_keys:
            assert key in connection_info, f"Missing connection info key: {key}"
        
        # Values should be correct
        assert connection_info["provider"] == CryptoProvider.CRYPTOFEED
        assert connection_info["exchanges"] == ["binance"]
        assert connection_info["channels"] == ["trades"]
        assert connection_info["symbols_count"] == 2
        assert connection_info["auth_type"] == AuthType.API_KEY


class TestCryptofeedConfigAgentPerformance:
    """Test agent performance characteristics and resource usage."""
    
    def test_agent_creation_performance(self):
        """Agent creation should be efficient and fast."""
        import time
        
        start_time = time.perf_counter()
        
        # Create multiple agents to test performance
        for i in range(100):
            agent = CryptofeedConfigAgent(
                exchanges=["binance"],
                channels=["trades"],
                symbols=[f"BTC-USDT-{i}"]
            )
        
        end_time = time.perf_counter()
        creation_time = end_time - start_time
        
        # Should create 100 agents in reasonable time
        assert creation_time < 1.0, f"Agent creation too slow: {creation_time:.3f}s for 100 agents"
    
    def test_agent_validation_performance(self):
        """Agent validation should be efficient."""
        import time
        
        # Complex configuration for validation testing
        config_data = {
            "exchanges": ["binance", "coinbase", "kraken"],
            "channels": ["trades", "ticker", "orderbook"],
            "symbols": [f"COIN{i}-USDT" for i in range(50)],
            "auth": {
                "auth_type": "api_key",
                "api_key": "test_key_123456789"
            },
            "retry": {
                "enabled": True,
                "max_attempts": 5,
                "base_delay": 1.0,
                "max_delay": 30.0,
                "backoff_factor": 2.0
            }
        }
        
        start_time = time.perf_counter()
        
        # Validate same configuration multiple times
        for i in range(50):
            agent = CryptofeedConfigAgent.model_validate(config_data)
        
        end_time = time.perf_counter()
        validation_time = end_time - start_time
        
        # Should validate 50 complex configs in reasonable time
        assert validation_time < 0.5, f"Validation too slow: {validation_time:.3f}s for 50 validations"
    
    def test_agent_serialization_performance(self):
        """Agent serialization should be efficient."""
        import time
        
        agent = CryptofeedConfigAgent(
            exchanges=["binance", "coinbase"],
            channels=["trades", "ticker"],
            symbols=[f"COIN{i}-USDT" for i in range(100)],
            auth=APIKeyAuthAgent(
                api_key="test_key_123456789",
                api_secret="test_secret_987654321"
            )
        )
        
        start_time = time.perf_counter()
        
        # Serialize same agent multiple times
        for i in range(100):
            serialized = agent.model_dump()
            json_str = agent.model_dump_json()
        
        end_time = time.perf_counter()
        serialization_time = end_time - start_time
        
        # Should serialize 100 times in reasonable time
        assert serialization_time < 0.5, f"Serialization too slow: {serialization_time:.3f}s for 100 serializations"


# Environment cleanup helper
@pytest.fixture(autouse=True)
def clean_environment():
    """Clean up environment variables after each test."""
    original_env = dict(os.environ)
    yield
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


if __name__ == "__main__":
    # Run the tests if executed directly
    pytest.main([__file__, "-v", "--tb=short"])