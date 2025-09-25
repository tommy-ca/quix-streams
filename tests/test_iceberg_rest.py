"""
TDD RED Phase: Essential unit tests for IcebergRESTSink

These tests define the minimal required functionality for the REST sink.
All tests expected to FAIL initially.

Author: TDD Sprint 3 - VALIDATE-001-RED
Date: September 19, 2025
"""

import pytest
from unittest.mock import patch, MagicMock
import requests


class TestIcebergRESTSink:
    """Test the main IcebergRESTSink implementation."""
    
    @pytest.mark.iceberg_rest
    def test_import_sink_class(self):
        """Test that IcebergRESTSink can be imported."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
        assert IcebergRESTSink is not None
    
    @pytest.mark.iceberg_rest
    def test_init_requires_min_config(self):
        """Test that sink can be initialized with valid config."""
        from quixstreams.sinks.community.iceberg_rest import (
            IcebergRESTSink, 
            create_local_rest_config
        )
        
        cfg = create_local_rest_config(table_name="test_table")
        sink = IcebergRESTSink(cfg)
        assert sink is not None
    
    @pytest.mark.iceberg_rest
    def test_write_calls_rest_endpoint_with_token_header(self):
        """Test that write method calls REST endpoint with proper headers."""
        from quixstreams.sinks.community.iceberg_rest import (
            IcebergRESTSink,
            create_config,
            StorageProvider,
        )
        
        cfg = create_config(
            table_name="test_table",
            catalog_uri="http://localhost:8181/api/v1",
            warehouse_id="local-warehouse",
            provider=StorageProvider.MINIO,
            region="us-east-1",
            endpoint_url="http://localhost:9000",
            access_key_id="minioadmin",
            secret_access_key="minioadmin",
            catalog_token="test-token-123",
        )
        
        # Use small batch size to trigger immediate send
        sink = IcebergRESTSink(cfg, batch_size=2)
        
        with patch.object(sink.client.session, 'post') as mock_post:
            # Setup mock
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"status": "ok"}
            
            sink.write([{"symbol": "BTC", "price": 50000}, {"symbol": "ETH", "price": 3000}])
            
            # Verify REST call was made
            assert mock_post.call_count == 1
        args, kwargs = mock_post.call_args
        
        # Check headers
        assert "headers" in kwargs
        assert "Authorization" in kwargs["headers"]
        assert "Bearer test-token-123" in kwargs["headers"]["Authorization"]
        assert "Content-Type" in kwargs["headers"]
        
        # Check timeout
        assert "timeout" in kwargs
        assert kwargs["timeout"] <= 5.0  # Short timeout for fast tests
        
        # Check payload structure (now using optimized 'data' parameter)
        assert "data" in kwargs
        # Data is now pre-serialized bytes, so we need to decode it
        import json
        payload = json.loads(kwargs["data"].decode('utf-8'))
        assert "records" in payload
        # Commit descriptor batching: one commit with record_count
        assert len(payload["records"]) == 1
        first = payload["records"][0]
        assert first.get("record_count") == 2
    
    @pytest.mark.iceberg_rest
    def test_write_handles_non_2xx_as_error(self):
        """Test that non-2xx responses raise errors."""
        from quixstreams.sinks.community.iceberg_rest import (
            IcebergRESTSink,
            create_local_rest_config
        )
        
        cfg = create_local_rest_config(table_name="test_table")
        # Use small batch size to trigger immediate send
        sink = IcebergRESTSink(cfg, batch_size=1)
        
        with patch.object(sink.client.session, 'post') as mock_post:
            # Setup mock for server error
            mock_post.return_value.status_code = 500
            mock_post.return_value.text = "Internal Server Error"
            mock_post.return_value.reason = "Internal Server Error"
            
        # Should raise CatalogError (which is a subclass of IcebergRESTError)
        from quixstreams.sinks.community.iceberg_rest.errors import IcebergRESTError
        with pytest.raises(IcebergRESTError):
            sink.write([{"symbol": "BTC", "price": 50000}])
    
    @pytest.mark.iceberg_rest
    def test_write_empty_records_noop(self):
        """Test that writing empty records is a no-op."""
        from quixstreams.sinks.community.iceberg_rest import (
            IcebergRESTSink,
            create_local_rest_config
        )
        
        cfg = create_local_rest_config(table_name="test_table")
        sink = IcebergRESTSink(cfg)
        
        # Should not raise error
        sink.write([])
        sink.write(None)
    
    @pytest.mark.iceberg_rest
    def test_flush_and_close_noop(self):
        """Test that flush and close work without errors."""
        from quixstreams.sinks.community.iceberg_rest import (
            IcebergRESTSink,
            create_local_rest_config
        )
        
        cfg = create_local_rest_config(table_name="test_table")
        sink = IcebergRESTSink(cfg)
        
        # Should not raise errors
        sink.flush()
        sink.close()
    
    @pytest.mark.iceberg_rest 
    def test_config_validation(self):
        """Test that invalid configurations are rejected at config construction."""
        from quixstreams.sinks.community.iceberg_rest import CatalogConfig, StorageConfig, IcebergConfig, StorageProvider
        
        # Invalid config - missing required fields in CatalogConfig
        with pytest.raises(ValueError):
            invalid_config = IcebergConfig(
                table_name="test_table",
                catalog=CatalogConfig(uri="", warehouse_id="test_warehouse"),  # Empty URI should fail
                storage=StorageConfig(provider=StorageProvider.AWS, region="us-east-1"),
            )


class TestRESTSinkIntegration:
    """Integration-style tests (but with mocks for speed)."""
    
    @pytest.mark.iceberg_rest
    @pytest.mark.integration
    def test_basic_crypto_pipeline_flow(self):
        """Test basic crypto data pipeline flow."""
        from quixstreams.sinks.community.iceberg_rest import (
            IcebergRESTSink,
            create_local_rest_config
        )
        
        # Create configuration for crypto table
        cfg = create_local_rest_config(
            table_name="crypto_trades", 
            warehouse_id="local-warehouse"
        )
        
        # Use small batch size to trigger immediate send
        sink = IcebergRESTSink(cfg, batch_size=3)
        
        with patch.object(sink.client.session, 'post') as mock_post:
            # Setup successful response
            mock_post.return_value.status_code = 201
            mock_post.return_value.json.return_value = {"status": "success", "records_written": 3}
            
            # Simulate crypto trading data
            crypto_records = [
                {
                    "symbol": "BTC-USD",
                    "price": 45000.50,
                    "volume": 1.5,
                    "timestamp": "2025-09-19T00:48:43Z",
                    "exchange": "coinbase"
                },
                {
                    "symbol": "ETH-USD", 
                    "price": 2800.25,
                    "volume": 10.0,
                    "timestamp": "2025-09-19T00:48:44Z", 
                    "exchange": "binance"
                },
                {
                    "symbol": "SOL-USD",
                    "price": 145.75,
                    "volume": 25.0,
                    "timestamp": "2025-09-19T00:48:45Z",
                    "exchange": "kraken" 
                }
            ]
            
            # Write records
            sink.write(crypto_records)
            
            # Verify call was made correctly
            assert mock_post.call_count == 1
        
        # Check endpoint URL structure
        args, kwargs = mock_post.call_args
        url = args[0] if args else kwargs.get("url")
        assert "localhost:8181" in url  # Local Lakekeeper
        assert "crypto_trades" in url
        assert "local-warehouse" in url
        
        # Cleanup
        sink.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
