#!/usr/bin/env python3
"""
Tests for Iceberg REST sink extensibility and configuration hooks.

Following TDD methodology - tests written first, then implementation.
Task 5.2: Extend configuration and extensibility hooks

Requirements tested:
- REQ-2: Storage Provider Abstraction
- REQ-5: Configuration Management (runtime updates)
- REQ-10: Extensibility (pluggable adapters, extension points)
"""

import pytest
import tempfile
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Protocol
from unittest.mock import Mock, patch


class TestRuntimeConfigReloads:
    """Test runtime-safe configuration reloading."""
    
    def test_runtime_config_reload_safe_params(self):
        """Test reloading of safe configuration parameters at runtime."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.config_reloader import ConfigReloader
        
        # Initial configuration
        initial_config = {
            'log_level': 'INFO',
            'batch_timeout': 5.0,
            'metrics_enabled': True,
            'debug_mode': False
        }
        
        reloader = ConfigReloader(initial_config)
        
        # Test reloading safe parameters
        new_config = {
            'log_level': 'DEBUG',
            'batch_timeout': 10.0,
            'metrics_enabled': False
        }
        
        result = reloader.reload_safe_config(new_config)
        
        assert result.success is True
        assert result.updated_params == ['log_level', 'batch_timeout', 'metrics_enabled']
        assert reloader.current_config['log_level'] == 'DEBUG'
        assert reloader.current_config['batch_timeout'] == 10.0
        assert reloader.current_config['metrics_enabled'] is False
        
    def test_runtime_config_reload_unsafe_params_rejected(self):
        """Test that unsafe parameters are rejected during runtime reload."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.config_reloader import ConfigReloader
        
        initial_config = {
            'catalog_uri': 'http://localhost:8181',
            'table_name': 'test_table',
            'log_level': 'INFO'
        }
        
        reloader = ConfigReloader(initial_config)
        
        # Try to reload unsafe parameters
        unsafe_config = {
            'catalog_uri': 'http://newhost:8181',  # Unsafe - requires restart
            'table_name': 'new_table',             # Unsafe - requires restart
            'log_level': 'DEBUG'                   # Safe - can be reloaded
        }
        
        result = reloader.reload_safe_config(unsafe_config)
        
        assert result.success is False
        assert 'catalog_uri' in result.unsafe_params
        assert 'table_name' in result.unsafe_params
        assert 'log_level' not in result.unsafe_params
        assert result.updated_params == []  # Nothing updated due to unsafe params
        
    def test_config_change_validation(self):
        """Test validation of configuration changes before applying."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.config_reloader import ConfigReloader
        
        initial_config = {'batch_size': 1000, 'log_level': 'INFO'}
        reloader = ConfigReloader(initial_config)
        
        # Test valid changes
        valid_config = {'batch_size': 2000, 'log_level': 'DEBUG'}
        validation_result = reloader.validate_config_change(valid_config)
        
        assert validation_result.is_valid is True
        assert len(validation_result.errors) == 0
        
        # Test invalid changes
        invalid_config = {'batch_size': -1, 'log_level': 'INVALID_LEVEL'}
        validation_result = reloader.validate_config_change(invalid_config)
        
        assert validation_result.is_valid is False
        assert len(validation_result.errors) > 0
        assert any('batch_size' in error for error in validation_result.errors)
        assert any('log_level' in error for error in validation_result.errors)


class TestPluggableSerializationAdapters:
    """Test pluggable serialization adapter system."""
    
    def test_serialization_adapter_registration(self):
        """Test registration of custom serialization adapters."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.adapters import SerializationAdapterRegistry
        
        registry = SerializationAdapterRegistry()
        
        # Create a custom adapter
        class CustomJSONAdapter:
            def serialize(self, data: Dict[str, Any]) -> bytes:
                return json.dumps(data, separators=(',', ':')).encode('utf-8')
            
            def deserialize(self, data: bytes) -> Dict[str, Any]:
                return json.loads(data.decode('utf-8'))
        
        custom_adapter = CustomJSONAdapter()
        
        # Register the adapter
        registry.register('custom_json', custom_adapter)
        
        assert 'custom_json' in registry.list_adapters()
        assert registry.get_adapter('custom_json') is custom_adapter
        
    def test_serialization_adapter_usage(self):
        """Test using custom serialization adapters."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.adapters import SerializationAdapterRegistry
        
        registry = SerializationAdapterRegistry()
        
        # Test data
        test_data = {'exchange': 'binance', 'symbol': 'BTCUSDT', 'price': 43250.50}
        
        # Get default adapter
        default_adapter = registry.get_adapter('default')
        serialized = default_adapter.serialize(test_data)
        deserialized = default_adapter.deserialize(serialized)
        
        assert deserialized == test_data
        assert isinstance(serialized, bytes)
        
    def test_adapter_fallback_behavior(self):
        """Test fallback behavior when adapter is not found."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.adapters import SerializationAdapterRegistry
        
        registry = SerializationAdapterRegistry()
        
        # Try to get non-existent adapter
        adapter = registry.get_adapter('non_existent', fallback='default')
        
        assert adapter is not None
        assert adapter == registry.get_adapter('default')
        
        # Try without fallback
        with pytest.raises(KeyError):
            registry.get_adapter('non_existent', fallback=None)


class TestPluggableMonitoringAdapters:
    """Test pluggable monitoring adapter system."""
    
    def test_monitoring_adapter_registration(self):
        """Test registration of custom monitoring adapters."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.adapters import MonitoringAdapterRegistry
        
        registry = MonitoringAdapterRegistry()
        
        # Create a custom monitoring adapter
        class CustomPrometheusAdapter:
            def __init__(self):
                self.metrics = {}
            
            def increment_counter(self, name: str, labels: Dict[str, str] = None):
                key = f"{name}_{labels or {}}"
                self.metrics[key] = self.metrics.get(key, 0) + 1
            
            def record_histogram(self, name: str, value: float, labels: Dict[str, str] = None):
                key = f"{name}_hist_{labels or {}}"
                if key not in self.metrics:
                    self.metrics[key] = []
                self.metrics[key].append(value)
            
            def get_metrics(self) -> Dict[str, Any]:
                return self.metrics.copy()
        
        custom_adapter = CustomPrometheusAdapter()
        
        # Register the adapter
        registry.register('custom_prometheus', custom_adapter)
        
        assert 'custom_prometheus' in registry.list_adapters()
        assert registry.get_adapter('custom_prometheus') is custom_adapter
        
    def test_monitoring_adapter_usage(self):
        """Test using custom monitoring adapters."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.adapters import MonitoringAdapterRegistry
        
        registry = MonitoringAdapterRegistry()
        adapter = registry.get_adapter('default')
        
        # Test counter
        adapter.increment_counter('records_processed', {'table': 'test_table'})
        adapter.increment_counter('records_processed', {'table': 'test_table'})
        
        # Test histogram
        adapter.record_histogram('processing_latency', 150.5, {'operation': 'write'})
        adapter.record_histogram('processing_latency', 200.3, {'operation': 'write'})
        
        metrics = adapter.get_metrics()
        
        assert 'records_processed' in str(metrics)
        assert 'processing_latency' in str(metrics)
        
    def test_multiple_monitoring_adapters(self):
        """Test using multiple monitoring adapters simultaneously."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.adapters import MonitoringAdapterRegistry
        
        registry = MonitoringAdapterRegistry()
        
        # Register multiple adapters
        prometheus_adapter = Mock()
        statsd_adapter = Mock()
        
        registry.register('prometheus', prometheus_adapter)
        registry.register('statsd', statsd_adapter)
        
        # Use multiple adapters
        adapters = registry.get_adapters(['prometheus', 'statsd'])
        
        assert len(adapters) == 2
        assert prometheus_adapter in adapters
        assert statsd_adapter in adapters


class TestExtensionPoints:
    """Test extension point system and documentation."""
    
    def test_extension_point_discovery(self):
        """Test discovery of available extension points."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.extensions import ExtensionRegistry
        
        registry = ExtensionRegistry()
        
        # Get available extension points
        extension_points = registry.list_extension_points()
        
        expected_points = [
            'serialization_adapters',
            'monitoring_adapters', 
            'storage_providers',
            'schema_processors',
            'error_handlers'
        ]
        
        for point in expected_points:
            assert point in extension_points
            
    def test_extension_point_documentation(self):
        """Test extension point documentation generation."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.extensions import ExtensionRegistry
        
        registry = ExtensionRegistry()
        
        # Get documentation for extension point
        docs = registry.get_extension_docs('serialization_adapters')
        
        assert 'interface' in docs
        assert 'examples' in docs
        assert 'required_methods' in docs
        assert docs['interface'] is not None
        
        # Required methods for serialization adapters
        required_methods = docs['required_methods']
        assert 'serialize' in required_methods
        assert 'deserialize' in required_methods
        
    def test_custom_extension_registration(self):
        """Test registration of custom extensions."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.extensions import ExtensionRegistry
        
        registry = ExtensionRegistry()
        
        # Create a custom storage provider
        class CustomS3Provider:
            def __init__(self, endpoint: str, credentials: Dict[str, str]):
                self.endpoint = endpoint
                self.credentials = credentials
            
            def upload_file(self, local_path: str, remote_path: str) -> bool:
                return True  # Mock implementation
            
            def download_file(self, remote_path: str, local_path: str) -> bool:
                return True  # Mock implementation
        
        # Register the extension
        registry.register_extension('storage_providers', 'custom_s3', CustomS3Provider)
        
        # Verify registration
        providers = registry.list_extensions('storage_providers')
        assert 'custom_s3' in providers
        
        # Get the provider
        provider_class = registry.get_extension('storage_providers', 'custom_s3')
        assert provider_class is CustomS3Provider


class TestConfigurationDrivenCustomization:
    """Test configuration-driven customization without code changes."""
    
    def test_config_driven_adapter_selection(self):
        """Test selecting adapters through configuration."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.config_customizer import ConfigCustomizer
        
        config = {
            'serialization_adapter': 'custom_json',
            'monitoring_adapters': ['prometheus', 'statsd'],
            'storage_provider': 'custom_s3',
            'error_handler': 'retry_with_backoff'
        }
        
        customizer = ConfigCustomizer(config)
        
        # Test adapter resolution
        serialization_adapter = customizer.get_serialization_adapter()
        monitoring_adapters = customizer.get_monitoring_adapters()
        storage_provider = customizer.get_storage_provider()
        error_handler = customizer.get_error_handler()
        
        assert serialization_adapter is not None
        assert len(monitoring_adapters) == 2
        assert storage_provider is not None
        assert error_handler is not None
        
    def test_config_validation_for_customization(self):
        """Test validation of customization configuration."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.config_customizer import ConfigCustomizer
        
        # Valid configuration
        valid_config = {
            'serialization_adapter': 'default',
            'monitoring_adapters': ['prometheus'],
            'storage_provider': 'aws_s3'
        }
        
        customizer = ConfigCustomizer(valid_config)
        validation_result = customizer.validate_customization_config()
        
        assert validation_result.is_valid is True
        assert len(validation_result.errors) == 0
        
        # Invalid configuration
        invalid_config = {
            'serialization_adapter': 'non_existent',
            'monitoring_adapters': ['invalid_adapter'],
            'storage_provider': 'unknown_provider'
        }
        
        customizer = ConfigCustomizer(invalid_config)
        validation_result = customizer.validate_customization_config()
        
        assert validation_result.is_valid is False
        assert len(validation_result.errors) > 0


if __name__ == '__main__':
    pytest.main([__file__])