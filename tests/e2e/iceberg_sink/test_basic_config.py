"""
Basic configuration validation tests for Iceberg sink.

This test suite validates the current available configuration API
and establishes the foundation for TDD improvement.
"""

import pytest
from quixstreams.sinks.community.iceberg_rest import (
    create_local_rest_config,
    validate_rest_config,
    get_config_examples,
)


class TestBasicConfigurationAPI:
    """Test the basic configuration API that's currently available."""

    def test_create_local_rest_config_works(self):
        """Should be able to create local REST configuration."""
        config = create_local_rest_config()
        
        # Should have basic attributes
        assert hasattr(config, 'catalog_uri')
        assert hasattr(config, 'warehouse_id')
        assert "localhost" in config.catalog_uri
        
    def test_validate_rest_config_exists(self):
        """validate_rest_config function should exist and be callable."""
        config = create_local_rest_config()
        
        # Should be able to validate without errors
        result = validate_rest_config(config)
        assert result is True
        
    def test_get_config_examples_returns_examples(self):
        """Should return example configurations."""
        examples = get_config_examples()
        
        assert isinstance(examples, dict)
        assert len(examples) > 0
        
        # Should have local example
        assert "local" in examples
        
        # Each example should be valid
        for name, config in examples.items():
            assert validate_rest_config(config), f"Invalid example: {name}"


class TestAdvancedConfigurationNeeded:
    """Test advanced configuration features that need to be implemented."""
    
    def test_rest_config_should_support_environment_loading(self):
        """RESTIcebergConfig should support loading from environment variables."""
        # This test will fail initially, driving the RED phase
        pytest.skip("Environment loading not yet implemented - RED phase target")
    
    def test_config_should_support_validation_errors(self):
        """Configuration should provide helpful validation errors."""
        # This test will fail initially, driving the RED phase 
        pytest.skip("Enhanced validation not yet implemented - RED phase target")
    
    def test_config_should_support_serialization(self):
        """Configuration should support serialization roundtrip."""
        # This test will fail initially, driving the RED phase
        pytest.skip("Serialization not yet implemented - RED phase target")


class TestCurrentImplementationLimitations:
    """Test cases that expose current implementation limitations."""
    
    def test_config_modification_behavior(self):
        """Test what happens when we try to modify configuration."""
        config = create_local_rest_config()
        
        # Try to understand current mutability behavior
        original_uri = config.catalog_uri
        
        # This test documents current behavior, may fail
        try:
            config.catalog_uri = "modified"  # This may or may not work
            print(f"Config modification allowed: {config.catalog_uri}")
        except AttributeError:
            print("Config is immutable (good)")
        except Exception as e:
            print(f"Unexpected error: {e}")
    
    def test_config_creation_parameter_requirements(self):
        """Test what parameters are actually required for configuration creation."""
        # This test documents current requirements
        try:
            from quixstreams.sinks.community.iceberg_rest import RESTIcebergConfig
            from quixstreams.sinks.community.iceberg_rest import CatalogConfig, StorageConfig, StorageProvider
            
            # Try to create minimal configuration
            catalog = CatalogConfig(uri="http://localhost:8181", warehouse_id="test")
            storage = StorageConfig(provider=StorageProvider.MINIO, region="us-east-1", 
                                   endpoint_url="http://localhost:9000")
            config = RESTIcebergConfig(table_name="test.table", catalog=catalog, storage=storage)
            
            assert config is not None
            print("✅ Successfully created RESTIcebergConfig with new API")
            
        except Exception as e:
            print(f"❌ Could not create RESTIcebergConfig: {e}")
            pytest.fail(f"Current API documentation may be incorrect: {e}")
    
    def test_backward_compatibility_behavior(self):
        """Test how well backward compatibility is maintained."""
        # Test the old vs new API
        local_config = create_local_rest_config()
        
        # Document what properties are available
        available_props = [attr for attr in dir(local_config) if not attr.startswith('_')]
        print(f"Available config properties: {available_props}")
        
        # Test some expected properties
        expected_props = ['catalog_uri', 'warehouse_id', 'auth_type']
        for prop in expected_props:
            if hasattr(local_config, prop):
                print(f"✅ Has {prop}: {getattr(local_config, prop)}")
            else:
                print(f"❌ Missing {prop}")
    
    def test_error_handling_current_behavior(self):
        """Test how errors are currently handled."""
        try:
            # Try invalid configuration
            validate_rest_config(None)
            pytest.fail("Should have raised an error for None config")
        except Exception as e:
            print(f"Error handling works: {type(e).__name__}: {e}")
            # This is good - we have some error handling


if __name__ == "__main__":
    # Run basic diagnostics
    print("=== Iceberg REST Configuration API Diagnostics ===")
    
    try:
        from quixstreams.sinks.community.iceberg_rest import (
            create_local_rest_config, validate_rest_config, get_config_examples
        )
        print("✅ Basic API imports successful")
        
        config = create_local_rest_config()
        print(f"✅ Local config created: {type(config)}")
        
        is_valid = validate_rest_config(config)
        print(f"✅ Validation works: {is_valid}")
        
        examples = get_config_examples()
        print(f"✅ Examples available: {list(examples.keys())}")
        
    except Exception as e:
        print(f"❌ Basic API issue: {e}")
        
    print("=== Ready for TDD Implementation ===")