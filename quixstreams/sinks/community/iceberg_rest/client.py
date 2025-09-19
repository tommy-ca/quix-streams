"""
REST Catalog Client for Iceberg REST Sink

This module provides the HTTP client implementation for REST catalog operations,
separated from the main sink class for better separation of concerns.

Author: TDD Sprint 3 - REFACTOR-001
Date: September 19, 2025
"""

import json
import logging
import time
import gzip
import sys
from typing import Dict, List, Any, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Try to use fast JSON libraries if available
try:
    import orjson
    _has_orjson = True
except ImportError:
    _has_orjson = False
    
try:
    import ujson
    _has_ujson = True
except ImportError:
    _has_ujson = False

from .errors import NetworkError, TimeoutError, AuthenticationError, CatalogError

logger = logging.getLogger(__name__)


class RESTCatalogClient:
    """HTTP client for REST catalog operations.
    
    This class handles all HTTP communication with REST catalog services,
    providing a clean interface for table operations while managing
    authentication, retries, and error handling.
    """
    
    def __init__(
        self,
        catalog_uri: str,
        table_name: str,
        warehouse_id: str,
        catalog_token: Optional[str] = None,
        request_timeout: float = 5.0,
        max_retries: int = 3,
        backoff_factor: float = 0.3
    ):
        """Initialize REST catalog client.
        
        Args:
            catalog_uri: Base URI of the REST catalog service
            table_name: Name of the Iceberg table
            warehouse_id: Warehouse identifier in the catalog
            catalog_token: Optional authentication token
            request_timeout: HTTP request timeout in seconds
            max_retries: Maximum retry attempts for transient failures
            backoff_factor: Exponential backoff factor for retries
        """
        self.catalog_uri = catalog_uri.rstrip('/')
        self.table_name = table_name
        self.warehouse_id = warehouse_id
        self.catalog_token = catalog_token
        self.request_timeout = request_timeout
        
        # Create HTTP session with retry configuration
        self.session = self._create_session(max_retries, backoff_factor)
        
        # Initialize optimized JSON encoder
        self._json_encoder = self._init_json_encoder()
        
        # Compression settings
        self._compression_threshold = 1024 * 1024  # 1MB
        self._enable_compression = True
        
        logger.debug(
            f"Initialized REST catalog client: {catalog_uri}, "
            f"table={table_name}, warehouse={warehouse_id}, "
            f"json_encoder={self._json_encoder.__name__ if hasattr(self._json_encoder, '__name__') else 'optimized'}"
        )
    
    def _create_session(self, max_retries: int, backoff_factor: float) -> requests.Session:
        """Create HTTP session with optimized retry and connection pooling.
        
        Args:
            max_retries: Maximum number of retry attempts
            backoff_factor: Factor for exponential backoff
            
        Returns:
            Configured requests.Session instance
        """
        session = requests.Session()
        
        # Configure retry strategy for transient failures
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "PUT"]
        )
        
        # Create optimized HTTP adapter with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,      # Number of connection pools
            pool_maxsize=100,        # Maximum connections per pool
            pool_block=False         # Don't block if pool is full
        )
        
        # Store config for testing
        adapter.config = {
            'pool_connections': 20,
            'pool_maxsize': 100,
            'pool_block': False
        }
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _init_json_encoder(self):
        """Initialize the fastest available JSON encoder.
        
        Returns:
            JSON encoder function
        """
        if _has_orjson:
            # orjson is fastest and returns bytes
            def orjson_encoder(obj):
                return orjson.dumps(obj)
            return orjson_encoder
        elif _has_ujson:
            # ujson is faster than standard json
            return ujson.dumps
        else:
            # Fall back to standard json
            return json.dumps
    
    def _serialize_payload(self, records: List[Dict[str, Any]]) -> bytes:
        """Serialize payload with optimized JSON encoder.
        
        Args:
            records: List of records to serialize
            
        Returns:
            Serialized bytes
        """
        payload = self.build_payload(records)
        
        # Use optimized encoder
        if _has_orjson:
            # orjson already returns bytes
            return self._json_encoder(payload)
        else:
            # Convert string to bytes
            json_str = self._json_encoder(payload)
            return json_str.encode('utf-8')
    
    def _should_compress(self, data: bytes) -> bool:
        """Determine if payload should be compressed.
        
        Args:
            data: Payload bytes
            
        Returns:
            True if should compress
        """
        return self._enable_compression and len(data) > self._compression_threshold
    
    def _compress_data(self, data: bytes) -> bytes:
        """Compress data using gzip.
        
        Args:
            data: Data to compress
            
        Returns:
            Compressed data
        """
        return gzip.compress(data, compresslevel=6)  # Good balance of speed/compression
    
    def build_endpoint_url(self) -> str:
        """Build the REST catalog endpoint URL for table operations.
        
        Returns:
            Complete URL for table data operations
        """
        # Build REST catalog path: /warehouses/{warehouse_id}/tables/{table_name}/data
        endpoint_path = f"warehouses/{self.warehouse_id}/tables/{self.table_name}/data"
        return urljoin(self.catalog_uri + "/", endpoint_path)
    
    def build_headers(self) -> Dict[str, str]:
        """Build HTTP headers for REST catalog requests.
        
        Returns:
            Dictionary of HTTP headers including authentication
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "QuixStreams-IcebergREST/1.0"
        }
        
        # Add authorization if token provided
        if self.catalog_token:
            headers["Authorization"] = f"Bearer {self.catalog_token}"
        
        return headers
    
    def build_payload(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build REST API payload for record batch.
        
        Args:
            records: List of record dictionaries to send
            
        Returns:
            REST API payload dictionary
        """
        return {
            "records": records,
            "format": "json",
            "table_name": self.table_name,
            "timestamp": int(time.time() * 1000),  # Unix timestamp in milliseconds
            "batch_size": len(records)
        }
    
    def post_records(self, records: List[Dict[str, Any]]) -> requests.Response:
        """Send records to the REST catalog.
        
        Args:
            records: List of record dictionaries to send
            
        Returns:
            HTTP response object
            
        Raises:
            NetworkError: On HTTP errors or network issues
            TimeoutError: On request timeouts
            AuthenticationError: On authentication failures
            CatalogError: On catalog-specific errors
        """
        if not records:
            raise ValueError("Cannot send empty record list")
        
        url = self.build_endpoint_url()
        headers = self.build_headers()
        
        # Serialize payload with optimized encoder
        serialized_data = self._serialize_payload(records)
        
        # Check if compression is needed
        if self._should_compress(serialized_data):
            serialized_data = self._compress_data(serialized_data)
            headers['Content-Encoding'] = 'gzip'
            logger.debug(f"Compressed payload from {len(self._serialize_payload(records))} to {len(serialized_data)} bytes")
        
        logger.debug(f"Posting {len(records)} records to {url} ({len(serialized_data)} bytes)")
        
        try:
            response = self.session.post(
                url=url,
                data=serialized_data,  # Use data instead of json for pre-serialized content
                headers=headers,
                timeout=self.request_timeout
            )
            
            # Handle different response scenarios
            self._handle_response(response, len(records))
            return response
            
        except requests.Timeout as e:
            raise TimeoutError(
                timeout_seconds=self.request_timeout,
                operation="REST catalog post"
            ) from e
            
        except requests.RequestException as e:
            raise NetworkError(
                message="Network error during REST catalog request",
                cause=e
            ) from e
    
    def _handle_response(self, response: requests.Response, record_count: int) -> None:
        """Handle HTTP response and raise appropriate errors.
        
        Args:
            response: HTTP response object
            record_count: Number of records that were sent
            
        Raises:
            AuthenticationError: On 401/403 responses
            CatalogError: On other 4xx/5xx responses
        """
        if 200 <= response.status_code < 300:
            # Success case
            logger.info(f"Successfully posted {record_count} records (status: {response.status_code})")
            
            # Log response details if available
            try:
                response_data = response.json()
                if "records_written" in response_data:
                    logger.info(f"Server confirmed {response_data['records_written']} records written")
            except (ValueError, KeyError):
                pass  # Response not JSON or missing expected fields
                
            return
        
        # Error cases
        error_details = f"{response.status_code} {response.reason}"
        if response.text:
            error_details += f" - {response.text[:500]}"
        
        if response.status_code == 401:
            raise AuthenticationError(
                message="Authentication failed - invalid or missing token",
                auth_type="bearer_token" if self.catalog_token else "none",
                endpoint=response.url
            )
        elif response.status_code == 403:
            raise AuthenticationError(
                message="Access forbidden - insufficient permissions",
                auth_type="bearer_token" if self.catalog_token else "none",
                endpoint=response.url
            )
        elif 400 <= response.status_code < 500:
            raise CatalogError(
                operation="data insert",
                table_name=self.table_name,
                catalog_uri=self.catalog_uri,
                message=f"Client error: {error_details}"
            )
        else:  # 5xx errors
            raise CatalogError(
                operation="data insert",
                table_name=self.table_name,
                catalog_uri=self.catalog_uri,
                message=f"Server error: {error_details}"
            )
    
    def health_check(self) -> Dict[str, Any]:
        """Perform a health check against the REST catalog.
        
        Returns:
            Dictionary with health check results
            
        Raises:
            NetworkError: On connection or HTTP errors
        """
        # Simple GET request to catalog root to check connectivity
        health_url = self.catalog_uri
        headers = {"User-Agent": "QuixStreams-IcebergREST/1.0"}
        
        try:
            response = self.session.get(
                url=health_url,
                headers=headers,
                timeout=self.request_timeout
            )
            
            return {
                "status": "healthy" if response.status_code < 400 else "unhealthy",
                "status_code": response.status_code,
                "response_time_ms": response.elapsed.total_seconds() * 1000,
                "catalog_uri": self.catalog_uri
            }
            
        except requests.RequestException as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "catalog_uri": self.catalog_uri
            }
    
    def close(self) -> None:
        """Close the HTTP session and cleanup resources."""
        if hasattr(self, 'session') and self.session:
            try:
                self.session.close()
                logger.debug("REST catalog client closed successfully")
            except Exception as e:
                logger.warning(f"Error closing REST catalog client: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()