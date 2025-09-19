"""
Error Hierarchy for Iceberg REST Sink

This module defines a comprehensive error hierarchy for the REST-enabled
Apache Iceberg sink, providing clear error categorization and handling.

Author: TDD Sprint 3 - REFACTOR-001
Date: September 19, 2025
"""


class IcebergRESTError(Exception):
    """Base exception for all Iceberg REST sink errors."""
    
    def __init__(self, message: str, details: str = None, cause: Exception = None):
        """Initialize Iceberg REST error.
        
        Args:
            message: Human-readable error message
            details: Additional error details or context
            cause: The underlying exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.details = details
        self.cause = cause
    
    def __str__(self) -> str:
        """Return formatted error message."""
        result = self.message
        if self.details:
            result += f" - {self.details}"
        return result


class ConfigurationError(IcebergRESTError):
    """Raised when there are configuration-related issues."""
    pass


class ValidationError(ConfigurationError):
    """Raised when configuration validation fails."""
    
    def __init__(self, field: str, value, expected: str, message: str = None):
        """Initialize validation error.
        
        Args:
            field: The configuration field that failed validation
            value: The invalid value provided
            expected: Description of what was expected
            message: Optional custom message
        """
        if not message:
            message = f"Invalid value for '{field}': got {value!r}, expected {expected}"
        
        super().__init__(message)
        self.field = field
        self.value = value
        self.expected = expected


class NetworkError(IcebergRESTError):
    """Raised when network operations fail."""
    
    def __init__(self, message: str, status_code: int = None, 
                 response_text: str = None, cause: Exception = None):
        """Initialize network error.
        
        Args:
            message: Error message
            status_code: HTTP status code if applicable
            response_text: Response body text if available
            cause: Underlying network exception
        """
        details = []
        if status_code:
            details.append(f"Status: {status_code}")
        if response_text:
            # Truncate long responses
            truncated = response_text[:200] + "..." if len(response_text) > 200 else response_text
            details.append(f"Response: {truncated}")
            
        super().__init__(message, details=" | ".join(details) if details else None, cause=cause)
        self.status_code = status_code
        self.response_text = response_text


class TimeoutError(NetworkError):
    """Raised when network operations timeout."""
    
    def __init__(self, timeout_seconds: float, operation: str = "request"):
        """Initialize timeout error.
        
        Args:
            timeout_seconds: The timeout value that was exceeded
            operation: Description of the operation that timed out
        """
        message = f"{operation.capitalize()} timed out after {timeout_seconds}s"
        super().__init__(message)
        self.timeout_seconds = timeout_seconds
        self.operation = operation


class AuthenticationError(NetworkError):
    """Raised when authentication fails."""
    
    def __init__(self, message: str = "Authentication failed", 
                 auth_type: str = None, endpoint: str = None):
        """Initialize authentication error.
        
        Args:
            message: Error message
            auth_type: Type of authentication that failed
            endpoint: Endpoint that rejected authentication
        """
        details = []
        if auth_type:
            details.append(f"Auth type: {auth_type}")
        if endpoint:
            details.append(f"Endpoint: {endpoint}")
            
        super().__init__(message, details=" | ".join(details) if details else None)
        self.auth_type = auth_type
        self.endpoint = endpoint


class CatalogError(IcebergRESTError):
    """Raised when REST catalog operations fail."""
    
    def __init__(self, operation: str, table_name: str = None, 
                 catalog_uri: str = None, message: str = None):
        """Initialize catalog error.
        
        Args:
            operation: The catalog operation that failed
            table_name: Table name involved in the operation
            catalog_uri: Catalog URI where the error occurred
            message: Custom error message
        """
        if not message:
            table_part = f" on table '{table_name}'" if table_name else ""
            message = f"Catalog {operation} failed{table_part}"
            
        details = []
        if catalog_uri:
            details.append(f"Catalog: {catalog_uri}")
        if table_name:
            details.append(f"Table: {table_name}")
            
        super().__init__(message, details=" | ".join(details) if details else None)
        self.operation = operation
        self.table_name = table_name
        self.catalog_uri = catalog_uri


class SchemaError(CatalogError):
    """Raised when schema-related operations fail."""
    
    def __init__(self, schema_issue: str, table_name: str = None, 
                 expected_schema: dict = None, actual_schema: dict = None):
        """Initialize schema error.
        
        Args:
            schema_issue: Description of the schema problem
            table_name: Table name with schema issues
            expected_schema: Expected schema structure
            actual_schema: Actual schema that caused the error
        """
        message = f"Schema error: {schema_issue}"
        super().__init__("schema validation", table_name=table_name, message=message)
        self.schema_issue = schema_issue
        self.expected_schema = expected_schema
        self.actual_schema = actual_schema


class BufferError(IcebergRESTError):
    """Raised when buffer operations fail."""
    
    def __init__(self, operation: str, buffer_size: int = None, 
                 max_size: int = None, message: str = None):
        """Initialize buffer error.
        
        Args:
            operation: Buffer operation that failed
            buffer_size: Current buffer size
            max_size: Maximum allowed buffer size
            message: Custom error message
        """
        if not message:
            message = f"Buffer {operation} failed"
            if buffer_size is not None and max_size is not None:
                message += f" (size: {buffer_size}, max: {max_size})"
                
        super().__init__(message)
        self.operation = operation
        self.buffer_size = buffer_size
        self.max_size = max_size