"""
Iceberg REST Sink for QuixStreams

A REST-enabled Apache Iceberg sink that supports multiple S3-compatible storage providers
and REST catalog backends. This module provides a complete alternative to AWS Glue-based
Iceberg implementations.

Features:
- REST catalog support (Lakekeeper, Tabular, etc.)
- S3-compatible storage (MinIO, AWS S3, Cloudflare R2, etc.)
- Local development stack integration
- Comprehensive configuration helpers
- Migration utilities from AWS Glue

Author: TDD Sprint 3 - GREEN Phase
Date: September 19, 2025
"""

# Import unified configuration system (SOLID principles refactor)
from .config import (
    # New unified API
    create_config,
    create_local_config,
    load_config_from_env,
    validate_config,
    IcebergConfig,
    StorageProvider,
    CatalogConfig,
    StorageConfig,
    # Backward compatibility (deprecated)
    create_local_rest_config,
    create_r2_config,
    create_s3_rest_config,
    validate_rest_config,
    RESTIcebergConfig,
    AWSIcebergConfig,
    # Additional functions for test compatibility
    create_sink_from_env,
    validate_sink_config,
)

# Import additional helpers from old config_helpers for stack management
from .config_helpers import (
    start_local_stack,
    stop_local_stack,
    check_local_stack_health,
    wait_for_services,
    init_local_stack,
    get_config_examples,
    print_config_example
)

# Import main sink implementation (refactored)
from .sink import IcebergRESTSink
from .table_lifecycle import TableLifecycleManager
from .observability import MetricsCollector
from .schema_presets import load_schema_preset, available_presets

# Import REST catalog client for advanced usage
from .client import RESTCatalogClient
from .deployment import DeploymentReadiness

# Import error hierarchy for better error handling
from .errors import (
    IcebergRESTError,
    ConfigurationError,
    ValidationError,
    NetworkError,
    TimeoutError,
    AuthenticationError,
    CatalogError,
    SchemaError,
    BufferError,
    # Additional error types for test compatibility
    SchemaIncompatibilityError,
    CatalogConnectionError,
    SinkError,
)

# Public API
__all__ = [
    # Core classes
    "IcebergRESTSink",
    "RESTCatalogClient",
    "TableLifecycleManager",
    "MetricsCollector",
    "load_schema_preset",
    "available_presets",
    "DeploymentReadiness",
    
    # Unified Configuration API (New - SOLID principles)
    "create_config",
    "create_local_config",
    "load_config_from_env",
    "validate_config",
    "IcebergConfig",
    "StorageProvider",
    "CatalogConfig",
    "StorageConfig",
    
    # Backward Compatibility (Deprecated)
    "RESTIcebergConfig",
    "AWSIcebergConfig",
    "create_local_rest_config", 
    "create_r2_config",
    "create_s3_rest_config",
    "validate_rest_config",
    "get_config_examples", 
    "print_config_example",
    "create_sink_from_env",
    "validate_sink_config",
    
    # Local stack management
    "start_local_stack",
    "stop_local_stack", 
    "check_local_stack_health",
    "wait_for_services",
    "init_local_stack",
    
    # Error hierarchy
    "IcebergRESTError",
    "ConfigurationError",
    "ValidationError", 
    "NetworkError",
    "TimeoutError",
    "AuthenticationError",
    "CatalogError",
    "SchemaError",
    "BufferError",
    "SchemaIncompatibilityError",
    "CatalogConnectionError",
    "SinkError",
]

# Module metadata
__version__ = "0.1.0"
__author__ = "QuixStreams Community"
__description__ = "REST-enabled Apache Iceberg sink for QuixStreams"
