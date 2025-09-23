"""
Standardized error hierarchy for crypto sources.

This module defines custom exception classes to improve error handling
consistency across all crypto data sources.
"""

from typing import Any, Dict, Optional


class CryptoSourceError(Exception):
    """Base exception for all crypto source errors."""
    
    def __init__(self, message: str, source: Optional[str] = None, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.source = source
        self.context = context or {}


class CryptoSourceConfigError(CryptoSourceError):
    """Configuration-related errors."""
    pass


class CryptoSourceAuthError(CryptoSourceError):
    """Authentication-related errors.""" 
    pass


class CryptoSourceConnectionError(CryptoSourceError):
    """Connection and network-related errors."""
    
    def __init__(self, message: str, source: Optional[str] = None, context: Optional[Dict[str, Any]] = None, retryable: bool = True):
        super().__init__(message, source, context)
        self.retryable = retryable


class CryptoSourceDataError(CryptoSourceError):
    """Data format and parsing errors."""
    pass


class CryptoSourceRateLimitError(CryptoSourceConnectionError):
    """Rate limiting errors."""
    
    def __init__(self, message: str, source: Optional[str] = None, context: Optional[Dict[str, Any]] = None, retry_after: Optional[float] = None):
        super().__init__(message, source, context, retryable=True)
        self.retry_after = retry_after


class CryptoSourceTimeoutError(CryptoSourceConnectionError):
    """Timeout errors."""
    
    def __init__(self, message: str, source: Optional[str] = None, context: Optional[Dict[str, Any]] = None, timeout_duration: Optional[float] = None):
        super().__init__(message, source, context, retryable=True)
        self.timeout_duration = timeout_duration


class CryptoSourceDependencyError(CryptoSourceError, ImportError):
    """Missing dependency errors."""
    
    def __init__(self, message: str, package: str, install_command: Optional[str] = None):
        super().__init__(message)
        self.package = package
        self.install_command = install_command


# Convenience functions for common error scenarios
def missing_dependency_error(package: str, source_name: str, install_extra: Optional[str] = None) -> CryptoSourceDependencyError:
    """Create a standardized missing dependency error."""
    install_cmd = f"pip install quixstreams[{install_extra}]" if install_extra else f"pip install {package}"
    message = f"{source_name} requires '{package}'. Install: {install_cmd}"
    return CryptoSourceDependencyError(message, package, install_cmd)


def rate_limit_error(source: str, retry_after: Optional[float] = None, message: Optional[str] = None) -> CryptoSourceRateLimitError:
    """Create a standardized rate limit error."""
    if not message:
        message = f"Rate limit exceeded for {source}"
        if retry_after:
            message += f". Retry after {retry_after}s"
    return CryptoSourceRateLimitError(message, source=source, retry_after=retry_after)


def connection_error(source: str, original_error: Exception, retryable: bool = True) -> CryptoSourceConnectionError:
    """Create a standardized connection error."""
    message = f"Connection failed for {source}: {str(original_error)}"
    context = {"original_error": str(original_error), "error_type": type(original_error).__name__}
    return CryptoSourceConnectionError(message, source=source, context=context, retryable=retryable)


def timeout_error(source: str, timeout_duration: float, operation: Optional[str] = None) -> CryptoSourceTimeoutError:
    """Create a standardized timeout error."""
    message = f"Timeout after {timeout_duration}s for {source}"
    if operation:
        message += f" during {operation}"
    return CryptoSourceTimeoutError(message, source=source, timeout_duration=timeout_duration)


def data_error(source: str, message: str, data_context: Optional[Dict[str, Any]] = None) -> CryptoSourceDataError:
    """Create a standardized data processing error."""
    return CryptoSourceDataError(message, source=source, context=data_context)
