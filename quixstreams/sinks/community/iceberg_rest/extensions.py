#!/usr/bin/env python3
"""
Extension point system for Iceberg REST sink.

Provides extensibility framework for customizing sink behavior through
well-defined extension points and interfaces.
"""

import logging
from typing import Dict, Any, List, Optional, Type
from abc import ABC, abstractmethod


logger = logging.getLogger(__name__)


class ExtensionRegistry:
    """Registry and documentation system for extension points."""
    
    # Extension point definitions with their documentation
    EXTENSION_POINTS = {
        'serialization_adapters': {
            'interface': 'SerializationAdapter',
            'description': 'Pluggable serialization for data transformation',
            'required_methods': ['serialize', 'deserialize'],
            'examples': [
                'DefaultSerializationAdapter: Standard JSON serialization',
                'AvroAdapter: Apache Avro binary serialization',
                'ProtobufAdapter: Protocol Buffers serialization'
            ]
        },
        'monitoring_adapters': {
            'interface': 'MonitoringAdapter', 
            'description': 'Pluggable monitoring and metrics collection',
            'required_methods': ['increment_counter', 'record_histogram', 'get_metrics'],
            'examples': [
                'DefaultMonitoringAdapter: In-memory metrics storage',
                'PrometheusAdapter: Prometheus metrics integration',
                'StatsDAdapter: StatsD metrics publishing'
            ]
        },
        'storage_providers': {
            'interface': 'StorageProvider',
            'description': 'Pluggable storage backend integration',
            'required_methods': ['upload_file', 'download_file', 'list_files'],
            'examples': [
                'S3Provider: Amazon S3 storage integration',
                'GCSProvider: Google Cloud Storage integration',
                'AzureProvider: Azure Blob Storage integration'
            ]
        },
        'schema_processors': {
            'interface': 'SchemaProcessor',
            'description': 'Pluggable schema transformation and validation',
            'required_methods': ['process_schema', 'validate_schema'],
            'examples': [
                'DefaultSchemaProcessor: Basic schema handling',
                'AvroSchemaProcessor: Avro schema integration',
                'JSONSchemaProcessor: JSON Schema validation'
            ]
        },
        'error_handlers': {
            'interface': 'ErrorHandler',
            'description': 'Pluggable error handling and recovery strategies',
            'required_methods': ['handle_error', 'should_retry'],
            'examples': [
                'DefaultErrorHandler: Basic retry with backoff',
                'CircuitBreakerHandler: Circuit breaker pattern',
                'DeadLetterHandler: Dead letter queue integration'
            ]
        }
    }
    
    def __init__(self):
        self._extensions: Dict[str, Dict[str, Type]] = {}
        for point_name in self.EXTENSION_POINTS:
            self._extensions[point_name] = {}
    
    def list_extension_points(self) -> List[str]:
        """Get list of available extension points."""
        return list(self.EXTENSION_POINTS.keys())
    
    def get_extension_docs(self, extension_point: str) -> Dict[str, Any]:
        """Get documentation for an extension point."""
        if extension_point not in self.EXTENSION_POINTS:
            raise KeyError(f"Extension point '{extension_point}' not found")
        
        docs = self.EXTENSION_POINTS[extension_point].copy()
        
        # Add runtime information
        docs['registered_extensions'] = list(self._extensions[extension_point].keys())
        docs['extension_count'] = len(self._extensions[extension_point])
        
        return docs
    
    def register_extension(self, extension_point: str, name: str, extension_class: Type):
        """Register an extension for a specific extension point."""
        if extension_point not in self.EXTENSION_POINTS:
            raise KeyError(f"Extension point '{extension_point}' not found")
        
        self._extensions[extension_point][name] = extension_class
        logger.info(f"Registered extension '{name}' for point '{extension_point}'")
    
    def get_extension(self, extension_point: str, name: str) -> Type:
        """Get an extension class by name."""
        if extension_point not in self._extensions:
            raise KeyError(f"Extension point '{extension_point}' not found")
        
        if name not in self._extensions[extension_point]:
            raise KeyError(f"Extension '{name}' not found in point '{extension_point}'")
        
        return self._extensions[extension_point][name]
    
    def list_extensions(self, extension_point: str) -> List[str]:
        """List registered extensions for an extension point."""
        if extension_point not in self._extensions:
            raise KeyError(f"Extension point '{extension_point}' not found")
        
        return list(self._extensions[extension_point].keys())
    
    def validate_extension(self, extension_point: str, extension_class: Type) -> bool:
        """Validate that an extension class implements required methods."""
        if extension_point not in self.EXTENSION_POINTS:
            return False
        
        required_methods = self.EXTENSION_POINTS[extension_point]['required_methods']
        
        for method_name in required_methods:
            if not hasattr(extension_class, method_name):
                logger.error(f"Extension class missing required method: {method_name}")
                return False
            
            if not callable(getattr(extension_class, method_name)):
                logger.error(f"Extension class method is not callable: {method_name}")
                return False
        
        return True
    
    def get_extension_interface_docs(self, extension_point: str) -> str:
        """Get interface documentation for an extension point."""
        if extension_point not in self.EXTENSION_POINTS:
            raise KeyError(f"Extension point '{extension_point}' not found")
        
        point_info = self.EXTENSION_POINTS[extension_point]
        
        docs = f"""
Extension Point: {extension_point}
Interface: {point_info['interface']}
Description: {point_info['description']}

Required Methods:
"""
        for method in point_info['required_methods']:
            docs += f"  - {method}()\n"
        
        docs += "\nExamples:\n"
        for example in point_info['examples']:
            docs += f"  - {example}\n"
        
        if self._extensions[extension_point]:
            docs += "\nRegistered Extensions:\n"
            for name in self._extensions[extension_point]:
                docs += f"  - {name}\n"
        
        return docs.strip()