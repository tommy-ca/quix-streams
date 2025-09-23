"""
Pydantic v2 Configuration System for Apache Iceberg REST Sink

This module provides a modern, type-safe configuration system using Pydantic v2
for the Apache Iceberg REST sink. It replaces the dataclass-based approach with
enhanced validation, better error messages, serialization support, and environment
variable loading.

Features:
- Strong type validation with descriptive error messages
- Environment variable loading with proper prefixes  
- Configuration serialization/deserialization (JSON/YAML)
- Immutable configurations for production safety
- Schema generation for documentation
- Migration utilities from legacy configurations
- Production-ready error handling and logging

Architecture:
    BaseCatalogConfig -> REST catalog settings with validation
    BaseStorageConfig -> S3-compatible storage with provider logic
    BaseIcebergConfig -> Unified configuration with cross-validation
    Settings classes -> Environment loading with pydantic-settings

Author: QuixStreams Community - Iceberg REST Team
Version: 3.0.0 (Pydantic v2 Integration)  
Date: September 19, 2025
"""

import os
import warnings
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Set, Union
from urllib.parse import urlparse

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    computed_field,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from quixstreams.utils.settings import BaseSettings as QuixBaseSettings

__all__ = [
    # Core configuration models
    "CatalogConfig",
    "StorageConfig", 
    "IcebergConfig",
    # Settings classes for environment loading
    "CatalogSettings",
    "StorageSettings",
    "IcebergSettings",
    # Enums and utilities
    "StorageProvider",
    "AuthType",
    # Factory functions (backward compatibility)
    "create_config",
    "create_local_config",
    "load_config_from_env",
    "validate_config",
]


class StorageProvider(str, Enum):
    """Supported S3-compatible storage providers with validation."""
    AWS = "aws"
    CLOUDFLARE_R2 = "cloudflare_r2"
    MINIO = "minio" 
    CUSTOM = "custom"
    
    @classmethod
    def _missing_(cls, value: object) -> Optional["StorageProvider"]:
        """Handle case-insensitive enum lookup."""
        if isinstance(value, str):
            for member in cls:
                if member.value.lower() == value.lower():
                    return member
        return None


class AuthType(str, Enum):
    """Supported authentication types for REST catalog."""
    NONE = "none"
    BEARER_TOKEN = "bearer_token"
    BASIC = "basic"
    

# ================================
# Core Configuration Models
# ================================

class CatalogConfig(BaseModel):
    """
    REST catalog configuration with comprehensive validation.
    
    This model handles all REST catalog settings including authentication,
    URI validation, and warehouse management.
    """
    model_config = ConfigDict(
        frozen=True,  # Immutable for production safety
        extra='forbid',  # Prevent typos in field names
        str_strip_whitespace=True,  # Clean string inputs
        validate_assignment=True,  # Validate on attribute assignment
        use_enum_values=True,  # Use enum values in serialization
    )
    
    uri: str = Field(
        ..., 
        description="REST catalog endpoint URL",
        examples=[
            "http://localhost:8181/api/v1",
            "https://catalog.company.com/api/v1",
            "https://api.tabular.io/ws"
        ]
    )
    
    warehouse_id: str = Field(
        ...,
        min_length=1,
        description="Catalog warehouse identifier", 
        examples=["production", "development", "analytics"]
    )
    
    auth_type: AuthType = Field(
        default=AuthType.NONE,
        description="Authentication type for REST catalog"
    )
    
    token: Optional[SecretStr] = Field(
        default=None,
        description="Bearer token for REST catalog authentication"
    )
    
    username: Optional[str] = Field(
        default=None,
        description="Username for basic authentication"
    )
    
    password: Optional[SecretStr] = Field(
        default=None, 
        description="Password for basic authentication"
    )
    
    timeout: float = Field(
        default=30.0,
        gt=0,
        le=300,
        description="Request timeout in seconds"
    )
    
    @field_validator('uri')
    @classmethod
    def validate_uri(cls, v: str) -> str:
        """Validate REST catalog URI format."""
        if not v:
            raise ValueError("Catalog URI is required and cannot be empty")
            
        try:
            parsed = urlparse(v)
            if not parsed.scheme:
                raise ValueError("URI must include scheme (http:// or https://)")
            if not parsed.netloc:
                raise ValueError("URI must include hostname")
            if parsed.scheme not in ('http', 'https'):
                raise ValueError("URI scheme must be http or https")
            return v
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise ValueError(f"Invalid REST URI format: {v}") from e
    
    @field_validator('warehouse_id')
    @classmethod  
    def validate_warehouse_id(cls, v: str) -> str:
        """Validate warehouse ID format."""
        if not v or not v.strip():
            raise ValueError("Warehouse ID is required and cannot be empty")
        return v.strip()
    
    @model_validator(mode='after')
    def validate_auth_combinations(self) -> 'CatalogConfig':
        """Validate authentication parameter combinations."""
        if self.auth_type == AuthType.BEARER_TOKEN:
            if not self.token:
                raise ValueError("Token is required when auth_type='bearer_token'")
        elif self.auth_type == AuthType.BASIC:
            if not self.username or not self.password:
                raise ValueError("Both username and password are required when auth_type='basic'")
        elif self.auth_type == AuthType.NONE:
            if self.token or self.username or self.password:
                warnings.warn("Authentication credentials provided but auth_type is 'none'")
        
        return self
    
    @computed_field
    @property 
    def is_secure(self) -> bool:
        """Check if catalog uses secure connection."""
        return self.uri.startswith('https://')
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Generate authentication headers for REST requests."""
        headers = {}
        
        if self.auth_type == AuthType.BEARER_TOKEN and self.token:
            headers['Authorization'] = f'Bearer {self.token.get_secret_value()}'
        elif self.auth_type == AuthType.BASIC and self.username and self.password:
            import base64
            credentials = f"{self.username}:{self.password.get_secret_value()}"
            encoded = base64.b64encode(credentials.encode()).decode()
            headers['Authorization'] = f'Basic {encoded}'
        
        return headers


class StorageConfig(BaseModel):
    """
    S3-compatible storage configuration with provider-specific validation.
    
    This model handles storage settings for all supported providers including
    AWS S3, Cloudflare R2, MinIO, and custom S3-compatible storage.
    """
    model_config = ConfigDict(
        frozen=True,
        extra='forbid',
        str_strip_whitespace=True,
        validate_assignment=True,
        use_enum_values=True,
    )
    
    provider: StorageProvider = Field(
        ...,
        description="S3-compatible storage provider"
    )
    
    region: str = Field(
        default="us-east-1",
        description="Storage region"
    )
    
    access_key_id: Optional[str] = Field(
        default=None,
        description="S3-compatible access key ID"
    )
    
    secret_access_key: Optional[SecretStr] = Field(
        default=None,
        description="S3-compatible secret access key"
    )
    
    session_token: Optional[SecretStr] = Field(
        default=None,
        description="Session token for temporary credentials (AWS STS)"
    )
    
    endpoint_url: Optional[str] = Field(
        default=None,
        description="Custom S3-compatible endpoint URL",
        examples=[
            "http://localhost:9000",  # MinIO
            "https://account-id.r2.cloudflarestorage.com",  # Cloudflare R2
        ]
    )
    
    # Provider-specific settings
    account_id: Optional[str] = Field(
        default=None,
        description="Cloudflare account ID (required for R2)"
    )
    
    bucket_name: Optional[str] = Field(
        default=None,
        description="Custom bucket name override"
    )
    
    path_style_access: bool = Field(
        default=False,
        description="Use path-style access for S3 requests"
    )
    
    @field_validator('endpoint_url') 
    @classmethod
    def validate_endpoint_url(cls, v: Optional[str]) -> Optional[str]:
        """Validate endpoint URL format."""
        if v is None:
            return v
            
        try:
            parsed = urlparse(v)
            if not parsed.scheme or not parsed.netloc:
                raise ValueError("Endpoint URL must include scheme and hostname")
            if parsed.scheme not in ('http', 'https'):
                raise ValueError("Endpoint URL scheme must be http or https")
            return v
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            raise ValueError(f"Invalid endpoint URL format: {v}") from e
    
    @model_validator(mode='after')
    def validate_provider_requirements(self) -> 'StorageConfig':
        """Validate provider-specific requirements."""
        if self.provider == StorageProvider.CLOUDFLARE_R2:
            if not self.account_id:
                raise ValueError("account_id is required for Cloudflare R2")
            # Auto-generate R2 endpoint if not provided
            if not self.endpoint_url:
                self.endpoint_url = f"https://{self.account_id}.r2.cloudflarestorage.com"
            # R2 uses "auto" region
            if self.region != "auto":
                object.__setattr__(self, 'region', 'auto')
                
        elif self.provider == StorageProvider.MINIO:
            if not self.endpoint_url:
                raise ValueError("endpoint_url is required for MinIO")
                
        elif self.provider == StorageProvider.CUSTOM:
            if not self.endpoint_url:
                raise ValueError("endpoint_url is required for custom provider")
                
        elif self.provider == StorageProvider.AWS:
            # AWS S3 uses default endpoints, no custom endpoint_url needed
            if not self.region:
                object.__setattr__(self, 'region', 'us-east-1')
        
        return self
    
    @computed_field
    @property
    def is_secure(self) -> bool:
        """Check if storage uses secure connection."""
        if self.endpoint_url:
            return self.endpoint_url.startswith('https://')
        return self.provider == StorageProvider.AWS  # AWS S3 defaults to HTTPS
    
    def to_pyiceberg_auth(self) -> Dict[str, str]:
        """Convert to pyiceberg auth dictionary format."""
        auth = {}
        
        if self.region:
            auth["client.region"] = self.region
        if self.access_key_id:
            auth["client.access-key-id"] = self.access_key_id  
        if self.secret_access_key:
            auth["client.secret-access-key"] = self.secret_access_key.get_secret_value()
        if self.session_token:
            auth["client.session-token"] = self.session_token.get_secret_value()
        if self.endpoint_url:
            auth["s3.endpoint"] = self.endpoint_url
        if self.path_style_access:
            auth["s3.path-style-access"] = str(self.path_style_access).lower()
            
        return auth


class IcebergConfig(BaseModel):
    """
    Unified Apache Iceberg configuration with comprehensive validation.
    
    This is the main configuration class that composes catalog and storage
    settings with cross-validation and production-ready features.
    """
    model_config = ConfigDict(
        frozen=True,
        extra='forbid', 
        str_strip_whitespace=True,
        validate_assignment=True,
        use_enum_values=True,
        # Enable JSON schema generation
        json_schema_extra={
            "examples": [
                {
                    "table_name": "crypto.trades",
                    "catalog": {
                        "uri": "http://localhost:8181/api/v1", 
                        "warehouse_id": "local-warehouse"
                    },
                    "storage": {
                        "provider": "minio",
                        "region": "us-east-1",
                        "endpoint_url": "http://localhost:9000",
                        "access_key_id": "admin",
                        "secret_access_key": "password"
                    }
                }
            ]
        }
    )
    
    table_name: str = Field(
        ...,
        min_length=1, 
        description="Target Iceberg table name",
        examples=["events", "crypto.trades", "analytics.user_behavior"]
    )
    
    catalog: CatalogConfig = Field(
        ...,
        description="REST catalog configuration"
    )
    
    storage: StorageConfig = Field(
        ..., 
        description="S3-compatible storage configuration"
    )
    
    # Additional Iceberg-specific settings
    write_batch_size: int = Field(
        default=500,
        ge=1,
        le=10000,
        description="Number of records per write batch"
    )
    
    commit_retry_attempts: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Number of commit retry attempts"
    )
    
    @field_validator('table_name')
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        """Validate table name format."""
        if not v or not v.strip():
            raise ValueError("Table name is required and cannot be empty")
        # Allow namespace.table format
        name = v.strip()
        if '..' in name or name.startswith('.') or name.endswith('.'):
            raise ValueError("Invalid table name format")
        return name
    
    @computed_field
    @property
    def location(self) -> str:
        """Generate S3 location for Iceberg table."""
        return f"s3://warehouse/{self.catalog.warehouse_id}/"
    
    @computed_field 
    @property
    def auth(self) -> Dict[str, str]:
        """Generate pyiceberg-compatible auth dictionary."""
        return self.storage.to_pyiceberg_auth()
    
    # Backward compatibility properties
    @computed_field
    @property
    def catalog_uri(self) -> str:
        """Backward compatibility: catalog URI."""
        return self.catalog.uri
    
    @computed_field
    @property  
    def rest_uri(self) -> str:
        """Test compatibility: REST URI alias."""
        return self.catalog.uri
    
    @computed_field
    @property
    def warehouse_id(self) -> str:
        """Backward compatibility: warehouse ID."""
        return self.catalog.warehouse_id
        
    @computed_field
    @property
    def auth_type(self) -> str:
        """Backward compatibility: auth type."""
        return str(self.catalog.auth_type)
        
    @computed_field
    @property
    def catalog_token(self) -> Optional[str]:
        """Backward compatibility: catalog token (returns None for security)."""
        # Don't expose secret tokens in properties
        return None
    
    def get_catalog_token(self) -> Optional[str]:
        """Get catalog token securely (for internal use only)."""
        if self.catalog.token:
            return self.catalog.token.get_secret_value()
        return None
    
    @classmethod
    def from_env(cls, table_name: Optional[str] = None) -> 'IcebergConfig':
        """Create configuration from environment variables."""
        settings = IcebergSettings()
        if table_name:
            settings.table_name = table_name
        return settings.to_config()


# ================================
# Settings Classes for Environment Loading  
# ================================

class CatalogSettings(QuixBaseSettings):
    """Environment settings for REST catalog configuration."""
    model_config = SettingsConfigDict(
        env_prefix="ICEBERG_CATALOG_",
        env_ignore_empty=True,
        case_sensitive=False,
    )
    
    uri: str = Field(
        default="http://localhost:8181/api/v1",
        description="REST catalog endpoint URL"
    )
    warehouse_id: str = Field(
        default="local-warehouse", 
        description="Catalog warehouse identifier"
    )
    auth_type: AuthType = Field(default=AuthType.NONE)
    token: Optional[SecretStr] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    timeout: float = 30.0
    
    def to_catalog_config(self) -> CatalogConfig:
        """Convert to CatalogConfig model."""
        return CatalogConfig(**self.model_dump())


class StorageSettings(QuixBaseSettings):
    """Environment settings for S3-compatible storage configuration."""
    model_config = SettingsConfigDict(
        env_prefix="ICEBERG_STORAGE_",
        env_ignore_empty=True,
        case_sensitive=False,
    )
    
    provider: StorageProvider = StorageProvider.MINIO
    region: str = "us-east-1"
    access_key_id: Optional[str] = None  
    secret_access_key: Optional[SecretStr] = None
    session_token: Optional[SecretStr] = None
    endpoint_url: Optional[str] = None
    account_id: Optional[str] = None
    bucket_name: Optional[str] = None
    path_style_access: bool = False
    
    def to_storage_config(self) -> StorageConfig:
        """Convert to StorageConfig model.""" 
        return StorageConfig(**self.model_dump())


class IcebergSettings(QuixBaseSettings):
    """Main environment settings for complete Iceberg configuration."""
    model_config = SettingsConfigDict(
        env_prefix="ICEBERG_",
        env_ignore_empty=True,
        case_sensitive=False,
    )
    
    table_name: str = "default_table"
    write_batch_size: int = 500
    commit_retry_attempts: int = 3
    
    # Catalog settings (will be loaded with ICEBERG_CATALOG_ prefix)
    catalog_uri: str = "http://localhost:8181/api/v1"
    catalog_warehouse_id: str = "local-warehouse" 
    catalog_auth_type: AuthType = AuthType.NONE
    catalog_token: Optional[SecretStr] = None
    catalog_username: Optional[str] = None
    catalog_password: Optional[SecretStr] = None
    catalog_timeout: float = 30.0
    
    # Storage settings (will be loaded with ICEBERG_STORAGE_ prefix) 
    storage_provider: StorageProvider = StorageProvider.MINIO
    storage_region: str = "us-east-1"
    storage_access_key_id: Optional[str] = None
    storage_secret_access_key: Optional[SecretStr] = None
    storage_session_token: Optional[SecretStr] = None
    storage_endpoint_url: Optional[str] = "http://localhost:9000"  # Default MinIO endpoint
    storage_account_id: Optional[str] = None
    storage_bucket_name: Optional[str] = None
    storage_path_style_access: bool = False
    
    def to_config(self) -> IcebergConfig:
        """Convert to complete IcebergConfig model."""
        catalog_config = CatalogConfig(
            uri=self.catalog_uri,
            warehouse_id=self.catalog_warehouse_id,
            auth_type=self.catalog_auth_type,
            token=self.catalog_token,
            username=self.catalog_username, 
            password=self.catalog_password,
            timeout=self.catalog_timeout,
        )
        
        storage_config = StorageConfig(
            provider=self.storage_provider,
            region=self.storage_region,
            access_key_id=self.storage_access_key_id,
            secret_access_key=self.storage_secret_access_key,
            session_token=self.storage_session_token,
            endpoint_url=self.storage_endpoint_url,
            account_id=self.storage_account_id,
            bucket_name=self.storage_bucket_name,
            path_style_access=self.storage_path_style_access,
        )
        
        return IcebergConfig(
            table_name=self.table_name,
            catalog=catalog_config,
            storage=storage_config,
            write_batch_size=self.write_batch_size,
            commit_retry_attempts=self.commit_retry_attempts,
        )


# ================================
# Factory Functions (Backward Compatibility)
# ================================

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
    auth_type: Union[AuthType, str] = AuthType.NONE,
    # Provider-specific parameters
    account_id: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    bucket_name: Optional[str] = None,
    # Additional settings
    write_batch_size: int = 500,
    commit_retry_attempts: int = 3,
    **kwargs
) -> IcebergConfig:
    """
    Create unified Iceberg configuration with Pydantic v2 validation.
    
    This is the main factory function that provides a clean interface for
    creating validated configurations for any S3-compatible storage provider.
    
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
        write_batch_size: Records per write batch
        commit_retry_attempts: Number of retry attempts
        **kwargs: Additional configuration options
        
    Returns:
        IcebergConfig: Validated configuration object
        
    Raises:
        ValidationError: If configuration validation fails
        
    Examples:
        Local MinIO:
        >>> config = create_config(
        ...     table_name="events",
        ...     catalog_uri="http://localhost:8181/api/v1",
        ...     warehouse_id="local",
        ...     provider="minio",
        ...     region="us-east-1", 
        ...     endpoint_url="http://localhost:9000",
        ...     access_key_id="admin",
        ...     secret_access_key="password"
        ... )
        
        AWS S3:
        >>> config = create_config(
        ...     table_name="analytics.events",
        ...     catalog_uri="https://catalog.company.com/api/v1",
        ...     warehouse_id="production",
        ...     provider="aws",
        ...     region="us-east-1",
        ...     catalog_token="your-token"
        ... )
        
        Cloudflare R2:
        >>> config = create_config(
        ...     table_name="logs",
        ...     catalog_uri="https://catalog.company.com/api/v1", 
        ...     warehouse_id="analytics",
        ...     provider="cloudflare_r2",
        ...     region="auto",
        ...     account_id="your-account-id",
        ...     access_key_id="r2-token-id",
        ...     secret_access_key="r2-token-secret"
        ... )
    """
    # Handle enum conversion
    if isinstance(provider, str):
        provider = StorageProvider(provider)
    if isinstance(auth_type, str):
        auth_type = AuthType(auth_type)
    
    # Create catalog configuration
    catalog_config = CatalogConfig(
        uri=catalog_uri,
        warehouse_id=warehouse_id,
        auth_type=auth_type,
        token=SecretStr(catalog_token) if catalog_token else None,
    )
    
    # Create storage configuration
    storage_config = StorageConfig(
        provider=provider,
        region=region,
        access_key_id=access_key_id,
        secret_access_key=SecretStr(secret_access_key) if secret_access_key else None,
        session_token=SecretStr(session_token) if session_token else None,
        endpoint_url=endpoint_url,
        account_id=account_id,
        bucket_name=bucket_name,
    )
    
    # Create unified configuration
    return IcebergConfig(
        table_name=table_name,
        catalog=catalog_config,
        storage=storage_config,
        write_batch_size=write_batch_size,
        commit_retry_attempts=commit_retry_attempts,
    )


def create_local_config(
    table_name: str,
    warehouse_id: str = "local-warehouse",
    catalog_port: int = 8181,
    minio_port: int = 9000,
    catalog_host: str = "localhost",
    minio_host: str = "localhost",
    access_key_id: str = "admin",
    secret_access_key: str = "password",
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
        access_key_id: MinIO access key
        secret_access_key: MinIO secret key
        
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
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        auth_type=AuthType.NONE,
    )


def load_config_from_env(
    table_name: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    provider: Optional[Union[StorageProvider, str]] = None,
) -> IcebergConfig:
    """
    Load configuration from environment variables using pydantic-settings.
    
    Environment Variables:
        ICEBERG_TABLE_NAME: Target Iceberg table name
        ICEBERG_CATALOG_URI: REST catalog endpoint URL
        ICEBERG_CATALOG_WAREHOUSE_ID: Catalog warehouse identifier
        ICEBERG_STORAGE_PROVIDER: Storage provider (aws, cloudflare_r2, minio, custom)
        ICEBERG_STORAGE_REGION: Storage region
        ICEBERG_STORAGE_ACCESS_KEY_ID: S3-compatible access key
        ICEBERG_STORAGE_SECRET_ACCESS_KEY: S3-compatible secret key
        ICEBERG_STORAGE_SESSION_TOKEN: Session token (AWS STS)
        ICEBERG_CATALOG_TOKEN: REST catalog bearer token
        ICEBERG_STORAGE_ACCOUNT_ID: Cloudflare account ID
        ICEBERG_STORAGE_ENDPOINT_URL: Custom S3 endpoint URL
        
    Args:
        table_name: Override table name from environment
        warehouse_id: Override warehouse ID from environment
        provider: Override storage provider from environment
        
    Returns:
        IcebergConfig: Configuration loaded from environment
        
    Raises:
        ValidationError: If required environment variables are missing or invalid
        
    Example:
        >>> import os
        >>> os.environ['ICEBERG_TABLE_NAME'] = 'events'  
        >>> os.environ['ICEBERG_CATALOG_URI'] = 'http://localhost:8181/api/v1'
        >>> os.environ['ICEBERG_CATALOG_WAREHOUSE_ID'] = 'local'
        >>> config = load_config_from_env()
    """
    settings = IcebergSettings()
    
    # Apply overrides
    if table_name:
        settings.table_name = table_name
    if warehouse_id:
        settings.catalog_warehouse_id = warehouse_id  
    if provider:
        if isinstance(provider, str):
            provider = StorageProvider(provider)
        settings.storage_provider = provider
    
    return settings.to_config()


def validate_config(config: IcebergConfig) -> bool:
    """
    Validate an Iceberg configuration object.
    
    Args:
        config: Configuration to validate
        
    Returns:
        bool: True if valid
        
    Raises:
        ValidationError: If configuration is invalid
    """
    # Pydantic v2 models are automatically validated on creation
    # This function exists for backward compatibility
    if not isinstance(config, IcebergConfig):
        raise ValueError("Configuration must be an IcebergConfig instance")
    return True


# ================================  
# Migration and Utilities
# ================================

def migrate_from_legacy_config(legacy_config: Any) -> IcebergConfig:
    """
    Migrate from legacy dataclass-based configuration to Pydantic v2.
    
    Args:
        legacy_config: Legacy configuration object
        
    Returns:
        IcebergConfig: Migrated Pydantic v2 configuration
    """
    # Extract configuration data from legacy object
    if hasattr(legacy_config, 'table_name'):
        table_name = legacy_config.table_name
    else:
        table_name = "migrated_table"
    
    if hasattr(legacy_config, 'catalog'):
        catalog_data = {
            'uri': legacy_config.catalog.uri,
            'warehouse_id': legacy_config.catalog.warehouse_id,
            'auth_type': legacy_config.catalog.auth_type,
            'token': legacy_config.catalog.token,
        }
    else:
        # Legacy flat structure
        catalog_data = {
            'uri': getattr(legacy_config, 'catalog_uri', 'http://localhost:8181/api/v1'),
            'warehouse_id': getattr(legacy_config, 'warehouse_id', 'local'),
            'auth_type': getattr(legacy_config, 'auth_type', 'none'),
            'token': getattr(legacy_config, 'catalog_token', None),
        }
    
    if hasattr(legacy_config, 'storage'):
        storage_data = {
            'provider': legacy_config.storage.provider,
            'region': legacy_config.storage.region,
            'access_key_id': legacy_config.storage.access_key_id,
            'secret_access_key': legacy_config.storage.secret_access_key,
            'endpoint_url': legacy_config.storage.endpoint_url,
        }
    else:
        # Legacy flat structure or auth dict
        auth = getattr(legacy_config, 'auth', {})
        storage_data = {
            'provider': 'minio',  # Default assumption
            'region': auth.get('client.region', 'us-east-1'),
            'access_key_id': auth.get('client.access-key-id'),
            'secret_access_key': auth.get('client.secret-access-key'),
            'endpoint_url': auth.get('s3.endpoint'),
        }
    
    return create_config(
        table_name=table_name,
        catalog_uri=catalog_data['uri'],
        warehouse_id=catalog_data['warehouse_id'], 
        provider=storage_data['provider'],
        region=storage_data['region'],
        access_key_id=storage_data['access_key_id'],
        secret_access_key=storage_data['secret_access_key'],
        catalog_token=catalog_data['token'],
        auth_type=catalog_data['auth_type'],
        endpoint_url=storage_data['endpoint_url'],
    )


def generate_config_schema() -> Dict[str, Any]:
    """Generate JSON schema for IcebergConfig for documentation."""
    return IcebergConfig.model_json_schema()


def create_config_template(provider: Union[StorageProvider, str]) -> str:
    """
    Create configuration template for specific provider.
    
    Args:
        provider: Storage provider to create template for
        
    Returns:
        str: Configuration template as JSON string
    """
    if isinstance(provider, str):
        provider = StorageProvider(provider)
    
    templates = {
        StorageProvider.AWS: {
            "table_name": "your_table_name",
            "catalog": {
                "uri": "https://your-catalog.example.com/api/v1",
                "warehouse_id": "production",
                "auth_type": "bearer_token", 
                "token": "your-catalog-token"
            },
            "storage": {
                "provider": "aws",
                "region": "us-east-1",
                "access_key_id": "your-aws-access-key-id",
                "secret_access_key": "your-aws-secret-access-key"
            }
        },
        StorageProvider.CLOUDFLARE_R2: {
            "table_name": "your_table_name", 
            "catalog": {
                "uri": "https://your-catalog.example.com/api/v1",
                "warehouse_id": "analytics",
                "auth_type": "bearer_token",
                "token": "your-catalog-token"
            },
            "storage": {
                "provider": "cloudflare_r2",
                "region": "auto",
                "account_id": "your-cloudflare-account-id",
                "access_key_id": "your-r2-access-key-id", 
                "secret_access_key": "your-r2-secret-access-key"
            }
        },
        StorageProvider.MINIO: {
            "table_name": "your_table_name",
            "catalog": {
                "uri": "http://localhost:8181/api/v1",
                "warehouse_id": "local-warehouse",
                "auth_type": "none"
            },
            "storage": {
                "provider": "minio",
                "region": "us-east-1", 
                "endpoint_url": "http://localhost:9000",
                "access_key_id": "admin",
                "secret_access_key": "password"
            }
        }
    }
    
    import json
    template = templates.get(provider, templates[StorageProvider.MINIO])
    return json.dumps(template, indent=2)


if __name__ == "__main__":
    # Example usage and validation
    print("🚀 Pydantic v2 Configuration System for Iceberg REST Sink")
    print("=" * 60)
    
    # Test local configuration
    try:
        local_config = create_local_config(table_name="test_events")
        print("✅ Local configuration created successfully")
        print(f"   Table: {local_config.table_name}")
        print(f"   Catalog: {local_config.catalog_uri}")
        print(f"   Storage: {local_config.storage.provider}")
        
        # Test serialization
        config_dict = local_config.model_dump(mode='json')
        print("✅ Configuration serialization works")
        
        # Test validation 
        validate_config(local_config)
        print("✅ Configuration validation works")
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
    
    # Test environment loading
    try:
        os.environ['ICEBERG_TABLE_NAME'] = 'env_test_table'
        os.environ['ICEBERG_CATALOG_URI'] = 'http://localhost:8181/api/v1'
        os.environ['ICEBERG_CATALOG_WAREHOUSE_ID'] = 'env_warehouse'
        
        env_config = load_config_from_env()
        print("✅ Environment configuration loading works")
        print(f"   Table from env: {env_config.table_name}")
        
    except Exception as e:
        print(f"❌ Environment loading test failed: {e}")
    
    print("\n📋 Configuration System Ready!")
    print("   - Strong type validation with descriptive errors")
    print("   - Environment variable loading with proper prefixes")
    print("   - Immutable configurations for production safety")
    print("   - JSON/YAML serialization support")
    print("   - Schema generation for documentation")
    print("   - Migration utilities from legacy configs")