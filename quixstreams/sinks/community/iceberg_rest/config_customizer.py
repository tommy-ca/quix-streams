#!/usr/bin/env python3
"""
Configuration-driven customization for Iceberg REST sink.

Enables adapter selection and customization through configuration
without requiring code changes.
"""

import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from .adapters import SerializationAdapterRegistry, MonitoringAdapterRegistry
from .extensions import ExtensionRegistry
from .config_reloader import ValidationResult


logger = logging.getLogger(__name__)


@dataclass
class CustomizationValidationResult:
    """Result of customization configuration validation."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class ConfigCustomizer:
    """Configuration-driven customization manager."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize customizer with configuration.
        
        Args:
            config: Configuration dictionary with customization settings
        """
        self.config = config.copy()
        self._serialization_registry = SerializationAdapterRegistry()
        self._monitoring_registry = MonitoringAdapterRegistry()
        self._extension_registry = ExtensionRegistry()
        
        # Default configurations
        self._default_serialization = 'default'
        self._default_monitoring = ['default']
        self._default_storage = 'aws_s3'
        self._default_error_handler = 'default'
    
    def get_serialization_adapter(self):
        """Get configured serialization adapter."""
        adapter_name = self.config.get('serialization_adapter', self._default_serialization)
        
        try:
            return self._serialization_registry.get_adapter(adapter_name)
        except KeyError:
            logger.warning(f"Serialization adapter '{adapter_name}' not found, using default")
            return self._serialization_registry.get_adapter(self._default_serialization)
    
    def get_monitoring_adapters(self) -> List:
        """Get configured monitoring adapters."""
        adapter_names = self.config.get('monitoring_adapters', self._default_monitoring)
        
        if not isinstance(adapter_names, list):
            adapter_names = [adapter_names]
        
        adapters = []
        for name in adapter_names:
            try:
                adapter = self._monitoring_registry.get_adapter(name)
                adapters.append(adapter)
            except KeyError:
                logger.warning(f"Monitoring adapter '{name}' not found, skipping")
        
        # Ensure at least one adapter
        if not adapters:
            logger.info("No valid monitoring adapters found, using default")
            adapters.append(self._monitoring_registry.get_adapter(self._default_monitoring[0]))
        
        return adapters
    
    def get_storage_provider(self):
        """Get configured storage provider."""
        provider_name = self.config.get('storage_provider', self._default_storage)
        
        # Mock storage provider for testing
        class MockStorageProvider:
            def __init__(self, name: str):
                self.name = name
            
            def upload_file(self, local_path: str, remote_path: str) -> bool:
                return True
            
            def download_file(self, remote_path: str, local_path: str) -> bool:
                return True
        
        return MockStorageProvider(provider_name)
    
    def get_error_handler(self):
        """Get configured error handler."""
        handler_name = self.config.get('error_handler', self._default_error_handler)
        
        # Mock error handler for testing
        class MockErrorHandler:
            def __init__(self, name: str):
                self.name = name
            
            def handle_error(self, error: Exception) -> bool:
                return True
            
            def should_retry(self, error: Exception) -> bool:
                return True
        
        return MockErrorHandler(handler_name)
    
    def validate_customization_config(self) -> CustomizationValidationResult:
        """Validate customization configuration."""
        errors = []
        warnings = []
        
        # Known/valid adapter names (can be registered later)
        known_serialization = {'default', 'custom_json', 'avro', 'protobuf'}
        known_monitoring = {'default', 'prometheus', 'statsd', 'cloudwatch', 'datadog'}
        
        # Validate serialization adapter
        serialization_adapter = self.config.get('serialization_adapter')
        if serialization_adapter:
            if serialization_adapter not in known_serialization:
                available_adapters = self._serialization_registry.list_adapters()
                errors.append(f"Serialization adapter '{serialization_adapter}' not recognized. "
                            f"Known adapters: {sorted(known_serialization)}, "
                            f"Registered: {available_adapters}")
        
        # Validate monitoring adapters
        monitoring_adapters = self.config.get('monitoring_adapters', [])
        if not isinstance(monitoring_adapters, list):
            monitoring_adapters = [monitoring_adapters]
        
        for adapter_name in monitoring_adapters:
            if adapter_name not in known_monitoring:
                available_monitoring = self._monitoring_registry.list_adapters()
                errors.append(f"Monitoring adapter '{adapter_name}' not recognized. "
                            f"Known adapters: {sorted(known_monitoring)}, "
                            f"Registered: {available_monitoring}")
        
        # Validate storage provider
        storage_provider = self.config.get('storage_provider')
        if storage_provider:
            # List of known/valid storage providers
            valid_providers = ['aws_s3', 'gcs', 'azure_blob', 'local_fs']
            if storage_provider not in valid_providers:
                errors.append(f"Storage provider '{storage_provider}' not recognized. "
                            f"Valid providers: {valid_providers}")
        
        # Validate error handler
        error_handler = self.config.get('error_handler')
        if error_handler:
            # List of known/valid error handlers
            valid_handlers = ['default', 'retry_with_backoff', 'circuit_breaker', 'dead_letter']
            if error_handler not in valid_handlers:
                errors.append(f"Error handler '{error_handler}' not recognized. "
                            f"Valid handlers: {valid_handlers}")
        
        # Check for unknown configuration keys
        known_keys = {
            'serialization_adapter', 'monitoring_adapters', 
            'storage_provider', 'error_handler'
        }
        unknown_keys = set(self.config.keys()) - known_keys
        for key in unknown_keys:
            warnings.append(f"Unknown customization configuration key: '{key}'")
        
        return CustomizationValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def get_customization_summary(self) -> Dict[str, Any]:
        """Get summary of current customization configuration."""
        return {
            'serialization_adapter': self.config.get('serialization_adapter', self._default_serialization),
            'monitoring_adapters': self.config.get('monitoring_adapters', self._default_monitoring),
            'storage_provider': self.config.get('storage_provider', self._default_storage),
            'error_handler': self.config.get('error_handler', self._default_error_handler),
            'available_serialization': self._serialization_registry.list_adapters(),
            'available_monitoring': self._monitoring_registry.list_adapters(),
            'extension_points': self._extension_registry.list_extension_points()
        }