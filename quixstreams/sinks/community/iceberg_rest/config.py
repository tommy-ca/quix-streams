"""
Unified Configuration System for Apache Iceberg REST Sink

This module provides a clean, unified configuration approach for the Apache Iceberg REST sink
that works with any S3-compatible storage provider. It follows SOLID, KISS, and DRY principles
to eliminate redundancy and simplify the API surface.

Design Principles:
- Single Responsibility: Separate catalog, storage, and provider concerns
- Open/Closed: Extensible for new storage providers without modification
- Interface Segregation: Clean, focused interfaces for different concerns
- KISS: Single configuration approach instead of multiple factory functions
- DRY: Unified S3-compatible logic instead of provider-specific duplication

Architecture:
    CatalogConfig -> Handles REST catalog settings
    StorageConfig -> Handles S3-compatible storage settings
    IcebergConfig -> Composes catalog + storage + validation
    StorageProvider -> Handles provider-specific endpoint resolution

Author: Apache Iceberg REST Sink Team  
Version: 2.0.0 (Configuration Refactor)
Date: September 19, 2025
"""

import os
import warnings
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Literal, Optional, Union
from urllib.parse import urlparse


class StorageProvider(Enum):
    """Supported S3-compatible storage providers."""
    AWS = "aws"
    CLOUDFLARE_R2 = "cloudflare_r2"  
    MINIO = "minio"
    CUSTOM = "custom"


@dataclass
class CatalogConfig:
    """REST catalog configuration (Single Responsibility Principle)."""
    uri: str
    warehouse_id: str
    auth_type: Literal["none", "bearer_token"] = "none"
    token: Optional[str] = None
    
    def __post_init__(self):
        """Validate catalog configuration."""
        if not self.uri:
            raise ValueError("Catalog URI is required")
        if not self.warehouse_id:
            raise ValueError("Warehouse ID is required")
        if self.auth_type == "bearer_token" and not self.token:
            raise ValueError("Token required when auth_type='bearer_token'")
        
        # Validate URI format
        try:
            parsed = urlparse(self.uri)
            if not parsed.scheme or not parsed.netloc:
                raise ValueError(f"Invalid catalog URI: {self.uri}")
        except Exception as e:
            raise ValueError(f"Invalid catalog URI: {self.uri}") from e


@dataclass 
class StorageConfig:
    """S3-compatible storage configuration (Single Responsibility Principle)."""
    provider: StorageProvider
    region: str
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    endpoint_url: Optional[str] = None
    # Provider-specific settings
    account_id: Optional[str] = None  # For Cloudflare R2
    bucket_name: Optional[str] = None  # For custom setups
    
    def __post_init__(self):
        """Resolve provider-specific settings."""
        if self.provider == StorageProvider.CLOUDFLARE_R2:
            if not self.account_id:
                raise ValueError("account_id required for Cloudflare R2")
            if not self.endpoint_url:
                self.endpoint_url = f"https://{self.account_id}.r2.cloudflarestorage.com"
            if self.region != "auto":
                self.region = "auto"  # R2 uses "auto" region
        
        elif self.provider == StorageProvider.AWS:
            # AWS S3 uses default endpoints, no custom endpoint_url needed
            if not self.region:
                self.region = "us-east-1"  # Default AWS region
        
        elif self.provider == StorageProvider.MINIO:
            # MinIO requires explicit endpoint
            if not self.endpoint_url:
                raise ValueError("endpoint_url required for MinIO")
        
        elif self.provider == StorageProvider.CUSTOM:
            # Custom provider requires explicit endpoint
            if not self.endpoint_url:
                raise ValueError("endpoint_url required for custom provider")
    
    def to_auth_dict(self) -> Dict[str, str]:
        """Convert to pyiceberg auth dictionary format."""
        auth = {}
        
        if self.region:
            auth["client.region"] = self.region
        if self.access_key_id:
            auth["client.access-key-id"] = self.access_key_id
        if self.secret_access_key:
            auth["client.secret-access-key"] = self.secret_access_key
        if self.session_token:
            auth["client.session-token"] = self.session_token
        if self.endpoint_url:
            auth["s3.endpoint"] = self.endpoint_url
            
        return auth


@dataclass
class IcebergConfig:
    """
    Unified Apache Iceberg configuration for REST catalog and S3-compatible storage.
    
    This class composes catalog and storage configurations following the 
    Composition over Inheritance principle.
    """
    table_name: str
    catalog: CatalogConfig
    storage: StorageConfig
    
    # Computed properties for backward compatibility
    location: str = field(init=False)
    auth: Dict[str, str] = field(init=False)
    
    def __post_init__(self):
        """Initialize computed properties."""
        # Set location based on warehouse
        self.location = f"s3://warehouse/{self.catalog.warehouse_id}/"
        
        # Set auth dictionary for pyiceberg compatibility
        self.auth = self.storage.to_auth_dict()
    
    @property
    def catalog_uri(self) -> str:
        """Backward compatibility property."""
        return self.catalog.uri
    
    @property
    def warehouse_id(self) -> str:
        """Backward compatibility property."""
        return self.catalog.warehouse_id
    
    @property
    def auth_type(self) -> str:
        """Backward compatibility property."""
        return self.catalog.auth_type
    
    @property
    def catalog_token(self) -> Optional[str]:
        """Backward compatibility property."""
        return self.catalog.token


def create_config(
    table_name: str,
    catalog_uri: str,
    warehouse_id: str,
    provider: Union[StorageProvider, str],
    region: str,
    # Common storage parameters
    access_key_id: Optional[str] = None,
    secret_access_key: Optional[str] = None,
    session_token: Optional[str] = None,
    # Catalog authentication
    catalog_token: Optional[str] = None,
    auth_type: Literal["none", "bearer_token"] = "none",
    # Provider-specific parameters
    account_id: Optional[str] = None,  # Cloudflare R2
    endpoint_url: Optional[str] = None,  # Custom/MinIO
    bucket_name: Optional[str] = None,  # Custom setups
) -> IcebergConfig:
    """
    Create unified configuration for any S3-compatible storage provider.
    
    This function replaces the multiple provider-specific factory functions with
    a single, clean interface following KISS and DRY principles.
    
    Args:
        table_name: Target Iceberg table name
        catalog_uri: REST catalog endpoint URL
        warehouse_id: Catalog warehouse identifier
        provider: Storage provider (aws, cloudflare_r2, minio, custom)
        region: Storage region
        access_key_id: S3-compatible access key
        secret_access_key: S3-compatible secret key
        session_token: Optional session token (AWS STS)
        catalog_token: REST catalog bearer token
        auth_type: Catalog authentication type
        account_id: Cloudflare account ID (required for R2)
        endpoint_url: Custom S3 endpoint (required for MinIO/custom)
        bucket_name: Custom bucket name
        
    Returns:
        IcebergConfig: Unified configuration object
        
    Examples:
        AWS S3:
        >>> config = create_config(
        ...     table_name="events",
        ...     catalog_uri="https://tabular.io/api/v1",
        ...     warehouse_id="production", 
        ...     provider="aws",
        ...     region="us-east-1",
        ...     catalog_token="your-token"
        ... )
        
        Cloudflare R2:
        >>> config = create_config(
        ...     table_name="analytics", 
        ...     catalog_uri="https://catalog.company.com/api/v1",
        ...     warehouse_id="analytics",
        ...     provider="cloudflare_r2",
        ...     region="auto",
        ...     account_id="your-cf-account-id",
        ...     access_key_id="r2-token-id",
        ...     secret_access_key="r2-token-secret"
        ... )
        
        MinIO (local):
        >>> config = create_config(
        ...     table_name="test_table",
        ...     catalog_uri="http://localhost:8181/api/v1", 
        ...     warehouse_id="local",
        ...     provider="minio",
        ...     region="us-east-1",
        ...     endpoint_url="http://localhost:9000",
        ...     access_key_id="minioadmin",
        ...     secret_access_key="minioadmin"
        ... )
    """
    # Handle string provider input
    if isinstance(provider, str):
        try:
            provider = StorageProvider(provider)
        except ValueError:
            raise ValueError(f"Unknown provider: {provider}. "
                           f"Supported: {[p.value for p in StorageProvider]}")
    
    # Auto-detect auth_type if token provided
    if catalog_token and auth_type == "none":
        auth_type = "bearer_token"
    
    # Create catalog configuration
    catalog_config = CatalogConfig(
        uri=catalog_uri,
        warehouse_id=warehouse_id,
        auth_type=auth_type,
        token=catalog_token
    )
    
    # Create storage configuration  
    storage_config = StorageConfig(
        provider=provider,
        region=region,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        endpoint_url=endpoint_url,
        account_id=account_id,
        bucket_name=bucket_name
    )
    
    # Create unified configuration
    return IcebergConfig(
        table_name=table_name,
        catalog=catalog_config,
        storage=storage_config
    )


def create_local_config(
    table_name: str,
    warehouse_id: str = "local",
    catalog_port: int = 8181,
    minio_port: int = 9000,
    catalog_host: str = "localhost",
    minio_host: str = "localhost"
) -> IcebergConfig:
    """
    Create configuration for local development with MinIO and Lakekeeper.
    
    This function provides a clean interface for local development setup,
    automatically configuring MinIO and Lakekeeper with sensible defaults.
    
    Args:
        table_name: Target Iceberg table name
        warehouse_id: Local warehouse identifier
        catalog_port: Lakekeeper REST catalog port
        minio_port: MinIO storage port
        catalog_host: Lakekeeper host
        minio_host: MinIO host
        
    Returns:
        IcebergConfig: Local development configuration
        
    Example:
        >>> config = create_local_config(
        ...     table_name="user_events",
        ...     warehouse_id="development"
        ... )
    """
    return create_config(
        table_name=table_name,
        catalog_uri=f"http://{catalog_host}:{catalog_port}/api/v1",
        warehouse_id=warehouse_id,
        provider=StorageProvider.MINIO,
        region="us-east-1",
        endpoint_url=f"http://{minio_host}:{minio_port}",
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
        auth_type="none"
    )


def load_config_from_env(
    table_name: str,
    warehouse_id: Optional[str] = None,
    provider: Optional[Union[StorageProvider, str]] = None
) -> IcebergConfig:
    """
    Load configuration from environment variables.
    
    Environment variables:
        ICEBERG_CATALOG_URI - REST catalog endpoint URL
        ICEBERG_WAREHOUSE_ID - Catalog warehouse identifier  
        ICEBERG_STORAGE_PROVIDER - Storage provider (aws, cloudflare_r2, minio, custom)
        ICEBERG_REGION - Storage region
        ICEBERG_ACCESS_KEY_ID - S3-compatible access key
        ICEBERG_SECRET_ACCESS_KEY - S3-compatible secret key
        ICEBERG_SESSION_TOKEN - Session token (AWS STS)
        ICEBERG_CATALOG_TOKEN - REST catalog bearer token
        ICEBERG_ACCOUNT_ID - Cloudflare account ID
        ICEBERG_ENDPOINT_URL - Custom S3 endpoint URL
        
    Args:
        table_name: Target Iceberg table name
        warehouse_id: Override warehouse ID from environment
        provider: Override storage provider from environment
        
    Returns:
        IcebergConfig: Configuration loaded from environment
        
    Example:
        >>> config = load_config_from_env(table_name="events")
    """
    # Load required settings
    catalog_uri = os.getenv("ICEBERG_CATALOG_URI")
    if not catalog_uri:
        raise ValueError("ICEBERG_CATALOG_URI environment variable required")
    
    warehouse_id = warehouse_id or os.getenv("ICEBERG_WAREHOUSE_ID")
    if not warehouse_id:
        raise ValueError("ICEBERG_WAREHOUSE_ID environment variable required")
    
    provider_str = provider or os.getenv("ICEBERG_STORAGE_PROVIDER", "aws")
    if isinstance(provider_str, str):
        provider_str = StorageProvider(provider_str)
    
    region = os.getenv("ICEBERG_REGION", "us-east-1")
    
    # Load optional settings
    access_key_id = os.getenv("ICEBERG_ACCESS_KEY_ID")
    secret_access_key = os.getenv("ICEBERG_SECRET_ACCESS_KEY") 
    session_token = os.getenv("ICEBERG_SESSION_TOKEN")
    catalog_token = os.getenv("ICEBERG_CATALOG_TOKEN")
    account_id = os.getenv("ICEBERG_ACCOUNT_ID")
    endpoint_url = os.getenv("ICEBERG_ENDPOINT_URL")
    
    return create_config(
        table_name=table_name,
        catalog_uri=catalog_uri,
        warehouse_id=warehouse_id,
        provider=provider_str,
        region=region,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        catalog_token=catalog_token,
        account_id=account_id,
        endpoint_url=endpoint_url
    )


def validate_config(config: IcebergConfig) -> bool:
    """
    Validate an Iceberg configuration.
    
    Args:
        config: Configuration to validate
        
    Returns:
        bool: True if valid
        
    Raises:
        ValueError: If configuration is invalid
    """
    if not config.table_name:
        raise ValueError("table_name is required")
    
    # Catalog and storage configs self-validate in __post_init__
    # This function mainly exists for explicit validation calls
    return True


# ================================
# Backward Compatibility Functions
# ================================

def create_local_rest_config(
    table_name: str = "default_table",
    catalog_port: int = 8181,
    minio_port: int = 9000,
    warehouse_id: str = "local-warehouse",
    **kwargs
) -> IcebergConfig:
    """
    DEPRECATED: Use create_local_config() instead.
    
    Create configuration for local development.
    """
    warnings.warn(
        "create_local_rest_config() is deprecated. Use create_local_config() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    
    return create_local_config(
        table_name=table_name,
        warehouse_id=warehouse_id,
        catalog_port=catalog_port,
        minio_port=minio_port,
        **kwargs
    )


def create_s3_rest_config(
    catalog_uri: str,
    warehouse_id: str,
    aws_region: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    table_name: str = "default_table",
    aws_session_token: Optional[str] = None,
    catalog_token: Optional[str] = None,
    **kwargs
) -> IcebergConfig:
    """
    DEPRECATED: Use create_config(provider="aws", ...) instead.
    
    Create configuration for AWS S3 with REST catalog.
    """
    warnings.warn(
        "create_s3_rest_config() is deprecated. Use create_config(provider='aws', ...) instead.",
        DeprecationWarning,
        stacklevel=2
    )
    
    return create_config(
        table_name=table_name,
        catalog_uri=catalog_uri,
        warehouse_id=warehouse_id,
        provider=StorageProvider.AWS,
        region=aws_region,
        access_key_id=aws_access_key_id,
        secret_access_key=aws_secret_access_key,
        session_token=aws_session_token,
        catalog_token=catalog_token,
        **kwargs
    )


def create_r2_config(
    account_id: str,
    access_key_id: str,
    secret_access_key: str,
    catalog_uri: str,
    table_name: str = "default_table",
    catalog_token: Optional[str] = None,
    warehouse_id: str = "default",
    **kwargs
) -> IcebergConfig:
    """
    DEPRECATED: Use create_config(provider="cloudflare_r2", ...) instead.
    
    Create configuration for Cloudflare R2.
    """
    warnings.warn(
        "create_r2_config() is deprecated. Use create_config(provider='cloudflare_r2', ...) instead.",
        DeprecationWarning,
        stacklevel=2
    )
    
    return create_config(
        table_name=table_name,
        catalog_uri=catalog_uri,
        warehouse_id=warehouse_id,
        provider=StorageProvider.CLOUDFLARE_R2,
        region="auto",
        account_id=account_id,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        catalog_token=catalog_token,
        **kwargs
    )


# Backward compatibility aliases
RESTIcebergConfig = IcebergConfig  # Direct alias
validate_rest_config = validate_config  # Function alias


if __name__ == "__main__":
    print("✅ Unified Configuration System loaded successfully")
    print("Available functions:")
    print("  - create_config() - Unified configuration for any provider")
    print("  - create_local_config() - Local development setup")
    print("  - load_config_from_env() - Load from environment variables")
    print("  - validate_config() - Configuration validation")
    print()
    print("Supported providers:")
    for provider in StorageProvider:
        print(f"  - {provider.value}")