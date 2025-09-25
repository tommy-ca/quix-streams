#!/usr/bin/env python3
"""
Runtime configuration reloader for Iceberg REST sink.

Enables safe runtime configuration updates without requiring full restart.
Only safe parameters can be reloaded; unsafe parameters require restart.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Set
import logging


logger = logging.getLogger(__name__)


@dataclass
class ReloadResult:
    """Result of configuration reload operation."""
    success: bool
    updated_params: List[str] = field(default_factory=list)
    unsafe_params: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass 
class ValidationResult:
    """Result of configuration validation."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)


class ConfigReloader:
    """
    Handles runtime-safe configuration reloading.
    
    Safe parameters can be reloaded without restart:
    - log_level: Change logging verbosity
    - batch_timeout: Adjust batching timeout
    - metrics_enabled: Enable/disable metrics collection
    - debug_mode: Toggle debug features
    
    Unsafe parameters require restart:
    - catalog_uri: Catalog endpoint change
    - table_name: Target table change
    - storage configuration: Storage backend changes
    """
    
    # Parameters that can be safely reloaded at runtime
    SAFE_RELOAD_PARAMS: Set[str] = {
        'log_level',
        'batch_timeout', 
        'metrics_enabled',
        'debug_mode',
        'batch_size',
        'verbose_logging',
        'sql_logging',
        'trace_requests'
    }
    
    # Valid values for specific parameters
    VALID_LOG_LEVELS: Set[str] = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
    
    def __init__(self, initial_config: Dict[str, Any]):
        """
        Initialize config reloader with initial configuration.
        
        Args:
            initial_config: Initial configuration dictionary
        """
        self.current_config = initial_config.copy()
        self._validate_initial_config()
    
    def _validate_initial_config(self) -> None:
        """Validate initial configuration."""
        validation_result = self.validate_config_change(self.current_config)
        if not validation_result.is_valid:
            raise ValueError(f"Invalid initial configuration: {validation_result.errors}")
    
    def reload_safe_config(self, new_config: Dict[str, Any]) -> ReloadResult:
        """
        Reload safe configuration parameters at runtime.
        
        Args:
            new_config: New configuration to apply
            
        Returns:
            Result of reload operation
        """
        result = ReloadResult(success=False)
        
        # Identify unsafe parameters
        unsafe_params = []
        safe_params = []
        
        for param_name in new_config.keys():
            if param_name not in self.SAFE_RELOAD_PARAMS:
                unsafe_params.append(param_name)
            else:
                safe_params.append(param_name)
        
        # If any unsafe parameters, reject the entire reload
        if unsafe_params:
            result.unsafe_params = unsafe_params
            result.errors.append(f"Unsafe parameters detected: {unsafe_params}")
            logger.warning(f"Configuration reload rejected due to unsafe parameters: {unsafe_params}")
            return result
        
        # Validate safe parameters
        safe_config = {k: v for k, v in new_config.items() if k in safe_params}
        validation_result = self.validate_config_change(safe_config)
        
        if not validation_result.is_valid:
            result.errors.extend(validation_result.errors)
            logger.error(f"Configuration validation failed: {validation_result.errors}")
            return result
        
        # Apply safe changes
        updated_params = []
        for param_name, param_value in safe_config.items():
            if param_name not in self.current_config or self.current_config[param_name] != param_value:
                old_value = self.current_config.get(param_name)
                self.current_config[param_name] = param_value
                updated_params.append(param_name)
                logger.info(f"Updated config parameter {param_name}: {old_value} -> {param_value}")
        
        result.success = True
        result.updated_params = updated_params
        
        return result
    
    def validate_config_change(self, config_change: Dict[str, Any]) -> ValidationResult:
        """
        Validate configuration changes before applying.
        
        Args:
            config_change: Configuration changes to validate
            
        Returns:
            Validation result with errors if invalid
        """
        errors = []
        
        # Validate log_level
        if 'log_level' in config_change:
            log_level = config_change['log_level']
            if log_level not in self.VALID_LOG_LEVELS:
                errors.append(f"Invalid log_level '{log_level}'. Must be one of: {self.VALID_LOG_LEVELS}")
        
        # Validate batch_size 
        if 'batch_size' in config_change:
            batch_size = config_change['batch_size']
            if not isinstance(batch_size, int) or batch_size <= 0:
                errors.append(f"Invalid batch_size '{batch_size}'. Must be positive integer")
        
        # Validate batch_timeout
        if 'batch_timeout' in config_change:
            batch_timeout = config_change['batch_timeout']
            if not isinstance(batch_timeout, (int, float)) or batch_timeout <= 0:
                errors.append(f"Invalid batch_timeout '{batch_timeout}'. Must be positive number")
        
        # Validate boolean parameters
        boolean_params = ['metrics_enabled', 'debug_mode', 'verbose_logging', 'sql_logging', 'trace_requests']
        for param in boolean_params:
            if param in config_change:
                value = config_change[param]
                if not isinstance(value, bool):
                    errors.append(f"Invalid {param} '{value}'. Must be boolean")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)
    
    def get_current_config(self) -> Dict[str, Any]:
        """Get current configuration."""
        return self.current_config.copy()
    
    def get_safe_params(self) -> Set[str]:
        """Get set of parameters that can be safely reloaded."""
        return self.SAFE_RELOAD_PARAMS.copy()
    
    def is_safe_param(self, param_name: str) -> bool:
        """Check if parameter can be safely reloaded."""
        return param_name in self.SAFE_RELOAD_PARAMS