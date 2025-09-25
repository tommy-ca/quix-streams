"""
Configuration Bridge for Iceberg REST Sink

This module provides seamless integration between the legacy dataclass-based
configuration system and the new Pydantic v2 configuration system. It ensures
backward compatibility while enabling a smooth migration path.

Features:
- Automatic detection of configuration type
- Seamless conversion between legacy and Pydantic v2 configs
- Transparent compatibility layer for existing code
- Migration utilities and deprecation warnings
- Performance optimized conversion

Author: QuixStreams Community - Iceberg REST Team
Version: 1.0.0 (Configuration Bridge)
Date: September 19, 2025
"""

import warnings
from typing import Any, Union

try:
    # Import Pydantic v2 configuration system
    try:
        from .config_v2 import (
            IcebergConfig as PydanticIcebergConfig,
            CatalogConfig as PydanticCatalogConfig,
            StorageConfig as PydanticStorageConfig,
            create_config as create_pydantic_config,
            create_local_config as create_pydantic_local_config,
            load_config_from_env as load_pydantic_config_from_env,
            validate_config as validate_pydantic_config,
            migrate_from_legacy_config,
        )
    except ImportError:
        # Direct execution - use absolute imports
        from config_v2 import (
            IcebergConfig as PydanticIcebergConfig,
            CatalogConfig as PydanticCatalogConfig,
            StorageConfig as PydanticStorageConfig,
            create_config as create_pydantic_config,
            create_local_config as create_pydantic_local_config,
            load_config_from_env as load_pydantic_config_from_env,
            validate_config as validate_pydantic_config,
            migrate_from_legacy_config,
        )
    PYDANTIC_AVAILABLE = True
except ImportError as e:
    warnings.warn(f"Pydantic v2 configuration system not available: {e}")
    PYDANTIC_AVAILABLE = False

# Import legacy configuration system
try:
    from .config import (
        IcebergConfig as LegacyIcebergConfig,
        create_config as create_legacy_config,
        create_local_config as create_legacy_local_config,
        load_config_from_env as load_legacy_config_from_env,
        validate_config as validate_legacy_config,
    )
except ImportError:
    # Direct execution - use absolute imports
    from config import (
        IcebergConfig as LegacyIcebergConfig,
        create_config as create_legacy_config,
        create_local_config as create_legacy_local_config,
        load_config_from_env as load_legacy_config_from_env,
        validate_config as validate_legacy_config,
    )

__all__ = [
    "detect_config_type",
    "convert_to_pydantic",
    "convert_to_legacy", 
    "create_config_smart",
    "create_local_config_smart",
    "load_config_from_env_smart",
    "validate_config_smart",
    "is_pydantic_config",
    "is_legacy_config",
]


def detect_config_type(config: Any) -> str:
    """
    Detect the type of configuration object.
    
    Args:
        config: Configuration object to detect
        
    Returns:
        str: Configuration type ("pydantic", "legacy", "unknown")
    """
    if PYDANTIC_AVAILABLE and isinstance(config, PydanticIcebergConfig):
        return "pydantic"
    elif isinstance(config, LegacyIcebergConfig):
        return "legacy"
    else:
        return "unknown"


def is_pydantic_config(config: Any) -> bool:
    """Check if configuration is a Pydantic v2 config."""
    return PYDANTIC_AVAILABLE and isinstance(config, PydanticIcebergConfig)


def is_legacy_config(config: Any) -> bool:
    """Check if configuration is a legacy dataclass config."""
    return isinstance(config, LegacyIcebergConfig)


def convert_to_pydantic(legacy_config: Any) -> Union[Any, LegacyIcebergConfig]:
    """
    Convert legacy configuration to Pydantic v2 configuration.
    
    Args:
        legacy_config: Legacy configuration object
        
    Returns:
        Union[PydanticIcebergConfig, LegacyIcebergConfig]: Converted config or original if Pydantic unavailable
    """
    if not PYDANTIC_AVAILABLE:
        warnings.warn("Pydantic v2 not available, returning legacy config")
        return legacy_config
    
    if is_pydantic_config(legacy_config):
        return legacy_config  # Already Pydantic
    
    if is_legacy_config(legacy_config):
        return migrate_from_legacy_config(legacy_config)
    
    raise ValueError(f"Unknown configuration type: {type(legacy_config)}")


def convert_to_legacy(pydantic_config: Any) -> LegacyIcebergConfig:
    """
    Convert Pydantic v2 configuration to legacy configuration.
    
    This is primarily for backward compatibility when interfacing with
    legacy systems that expect the old configuration format.
    
    Args:
        pydantic_config: Pydantic v2 configuration object
        
    Returns:
        LegacyIcebergConfig: Legacy configuration object
    """
    if is_legacy_config(pydantic_config):
        return pydantic_config  # Already legacy
    
    if not is_pydantic_config(pydantic_config):
        raise ValueError(f"Unknown configuration type: {type(pydantic_config)}")
    
    # Convert Pydantic config back to legacy format
    return create_legacy_config(
        table_name=pydantic_config.table_name,
        catalog_uri=pydantic_config.catalog_uri,
        warehouse_id=pydantic_config.warehouse_id,
        provider=str(pydantic_config.storage.provider),
        region=pydantic_config.storage.region,
        access_key_id=pydantic_config.storage.access_key_id,
        secret_access_key=pydantic_config.storage.secret_access_key.get_secret_value() if pydantic_config.storage.secret_access_key else None,
        session_token=pydantic_config.storage.session_token.get_secret_value() if pydantic_config.storage.session_token else None,
        catalog_token=pydantic_config.get_catalog_token() if hasattr(pydantic_config, 'get_catalog_token') else None,
        auth_type=pydantic_config.auth_type,
        endpoint_url=pydantic_config.storage.endpoint_url,
        account_id=pydantic_config.storage.account_id,
    )


def create_config_smart(
    table_name: str,
    catalog_uri: str,
    warehouse_id: str,
    provider: Union[str, Any],
    region: str,
    use_pydantic: bool = None,
    **kwargs
) -> Union[Any, LegacyIcebergConfig]:
    """
    Create configuration using the best available system.
    
    Automatically chooses Pydantic v2 if available, falls back to legacy.
    
    Args:
        table_name: Target Iceberg table name
        catalog_uri: REST catalog endpoint URL
        warehouse_id: Catalog warehouse identifier
        provider: Storage provider
        region: Storage region
        use_pydantic: Force Pydantic (True) or legacy (False), None for auto-detect
        **kwargs: Additional configuration options
        
    Returns:
        Union[PydanticIcebergConfig, LegacyIcebergConfig]: Configuration object
    """
    # Auto-detect best system
    if use_pydantic is None:
        use_pydantic = PYDANTIC_AVAILABLE
    
    if use_pydantic and PYDANTIC_AVAILABLE:
        return create_pydantic_config(
            table_name=table_name,
            catalog_uri=catalog_uri,
            warehouse_id=warehouse_id,
            provider=provider,
            region=region,
            **kwargs
        )
    else:
        if use_pydantic and not PYDANTIC_AVAILABLE:
            warnings.warn("Pydantic v2 requested but not available, using legacy config")
        return create_legacy_config(
            table_name=table_name,
            catalog_uri=catalog_uri,
            warehouse_id=warehouse_id,
            provider=provider,
            region=region,
            **kwargs
        )


def create_local_config_smart(
    table_name: str,
    warehouse_id: str = "local-warehouse",
    use_pydantic: bool = None,
    **kwargs
) -> Union[Any, LegacyIcebergConfig]:
    """
    Create local configuration using the best available system.
    
    Args:
        table_name: Target Iceberg table name
        warehouse_id: Local warehouse identifier
        use_pydantic: Force Pydantic (True) or legacy (False), None for auto-detect
        **kwargs: Additional configuration options
        
    Returns:
        Union[PydanticIcebergConfig, LegacyIcebergConfig]: Local configuration
    """
    # Auto-detect best system
    if use_pydantic is None:
        use_pydantic = PYDANTIC_AVAILABLE
    
    if use_pydantic and PYDANTIC_AVAILABLE:
        return create_pydantic_local_config(
            table_name=table_name,
            warehouse_id=warehouse_id,
            **kwargs
        )
    else:
        if use_pydantic and not PYDANTIC_AVAILABLE:
            warnings.warn("Pydantic v2 requested but not available, using legacy config")
        return create_legacy_local_config(
            table_name=table_name,
            warehouse_id=warehouse_id,
            **kwargs
        )


def load_config_from_env_smart(
    table_name: str = None,
    warehouse_id: str = None,
    provider: Union[str, Any] = None,
    use_pydantic: bool = None,
    **kwargs
) -> Union[Any, LegacyIcebergConfig]:
    """
    Load configuration from environment using the best available system.
    
    Args:
        table_name: Override table name from environment
        warehouse_id: Override warehouse ID from environment
        provider: Override storage provider from environment
        use_pydantic: Force Pydantic (True) or legacy (False), None for auto-detect
        **kwargs: Additional configuration options
        
    Returns:
        Union[PydanticIcebergConfig, LegacyIcebergConfig]: Environment configuration
    """
    # Auto-detect best system
    if use_pydantic is None:
        use_pydantic = PYDANTIC_AVAILABLE
    
    if use_pydantic and PYDANTIC_AVAILABLE:
        return load_pydantic_config_from_env(
            table_name=table_name,
            warehouse_id=warehouse_id,
            provider=provider,
            **kwargs
        )
    else:
        if use_pydantic and not PYDANTIC_AVAILABLE:
            warnings.warn("Pydantic v2 requested but not available, using legacy config")
        return load_legacy_config_from_env(
            table_name=table_name,
            warehouse_id=warehouse_id,
            provider=provider,
            **kwargs
        )


def validate_config_smart(config: Any) -> bool:
    """
    Validate configuration using the appropriate validation system.
    
    Args:
        config: Configuration object to validate
        
    Returns:
        bool: True if valid
        
    Raises:
        ValidationError: If configuration is invalid
    """
    config_type = detect_config_type(config)
    
    if config_type == "pydantic":
        return validate_pydantic_config(config)
    elif config_type == "legacy":
        return validate_legacy_config(config)
    else:
        raise ValueError(f"Unknown configuration type: {type(config)}")


# ================================
# Migration and Compatibility Utilities
# ================================

def get_preferred_config_system() -> str:
    """Get the preferred configuration system for new code."""
    return "pydantic" if PYDANTIC_AVAILABLE else "legacy"


def recommend_migration(config: Any) -> str:
    """
    Provide migration recommendations for configuration.
    
    Args:
        config: Configuration object to analyze
        
    Returns:
        str: Migration recommendation message
    """
    config_type = detect_config_type(config)
    
    if config_type == "legacy" and PYDANTIC_AVAILABLE:
        return (
            "💡 Consider migrating to Pydantic v2 configuration for enhanced validation, "
            "better error messages, environment loading, and production-ready features. "
            "Use convert_to_pydantic(config) for automatic migration."
        )
    elif config_type == "pydantic":
        return "✅ Using modern Pydantic v2 configuration - no migration needed!"
    else:
        return "⚠️ Unknown configuration type - please check your setup."


def demonstrate_migration():
    """Demonstrate configuration migration capabilities."""
    print("🔄 Configuration Migration Demonstration")
    print("=" * 50)
    
    # Create legacy config
    print("1. Creating legacy configuration...")
    legacy_config = create_legacy_local_config(table_name="demo_table")
    print(f"   Type: {detect_config_type(legacy_config)}")
    print(f"   Table: {legacy_config.table_name}")
    print(f"   Catalog: {legacy_config.catalog_uri}")
    
    if PYDANTIC_AVAILABLE:
        # Convert to Pydantic
        print("\n2. Converting to Pydantic v2...")
        pydantic_config = convert_to_pydantic(legacy_config)
        print(f"   Type: {detect_config_type(pydantic_config)}")
        print(f"   Table: {pydantic_config.table_name}")
        print(f"   Catalog: {pydantic_config.catalog_uri}")
        print(f"   Validation: Enhanced with descriptive errors")
        print(f"   Serialization: JSON/YAML support")
        print(f"   Environment: Automatic loading with prefixes")
        
        # Convert back to legacy (for compatibility)
        print("\n3. Converting back to legacy (for compatibility)...")
        back_to_legacy = convert_to_legacy(pydantic_config)
        print(f"   Type: {detect_config_type(back_to_legacy)}")
        print(f"   Compatible: {legacy_config.table_name == back_to_legacy.table_name}")
        
        print(f"\n✅ Migration successful!")
        print(recommend_migration(legacy_config))
    else:
        print("\n⚠️ Pydantic v2 not available - install dependencies for migration")
    
    print("\n📋 Smart Configuration API:")
    print("   - create_config_smart() - Auto-detects best system")
    print("   - create_local_config_smart() - Local development")
    print("   - load_config_from_env_smart() - Environment loading")
    print("   - validate_config_smart() - Universal validation")


if __name__ == "__main__":
    demonstrate_migration()