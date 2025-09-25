"""
TDD RED Phase: End-to-End Integration Tests for REST Sink

These tests validate the complete pipeline from configuration to data writing.
All tests expected to FAIL initially.

Author: TDD Sprint 3 - VALIDATE-001-RED
Date: September 19, 2025
"""

import pytest
from unittest.mock import patch, MagicMock


class TestEndToEndPipeline:
    """End-to-end pipeline tests with REST sink."""
    
    @pytest.mark.integration
    @pytest.mark.iceberg_rest
    def test_basic_pipeline_smoke_test(self):
        """Basic smoke test that imports and configuration work."""
        from quixstreams.sinks.community.iceberg_rest import (
            IcebergRESTSink,
            create_local_rest_config,
            check_local_stack_health
        )
        
        # Configuration should work
        config = create_local_rest_config(table_name="test_pipeline")
        assert config.catalog_uri is not None
        assert config.table_name == "test_pipeline"
        
        # Sink should be importable and initializable
        sink = IcebergRESTSink(config)
        assert sink is not None
        
        # Basic methods should exist
        assert hasattr(sink, 'write')
        assert hasattr(sink, 'flush') 
        assert hasattr(sink, 'close')
        
        # Health check should work (may use mocked fallback)
        health = check_local_stack_health()
        assert isinstance(health, dict)
    
    @pytest.mark.integration
    @pytest.mark.iceberg_rest
    def test_crypto_data_pipeline_mocked(self):
        """Test complete crypto data pipeline with mocked REST calls."""
        from quixstreams.sinks.community.iceberg_rest import (
            IcebergRESTSink,
            create_local_rest_config
        )
        
        # Create pipeline configuration
        config = create_local_rest_config(
            table_name="crypto_trades",
            warehouse_id="integration-test"
        )
        config.catalog_token = "integration-test-token"
        
        # Use batch size that will trigger immediate sending
        sink = IcebergRESTSink(config, batch_size=100)
        
        with patch.object(sink.client.session, 'post') as mock_post:
            # Setup successful responses
            mock_post.return_value.status_code = 201
            mock_post.return_value.json.return_value = {
                "status": "success", 
                "records_written": 100,
                "table_location": "s3://test-bucket/crypto_trades/"
            }
            
            # Simulate realistic crypto trading data batch
            crypto_batch = []
            exchanges = ["coinbase", "binance", "kraken", "gemini"]
            symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD"]
            
            for i in range(100):
                crypto_batch.append({
                    "symbol": symbols[i % len(symbols)],
                    "price": 45000.0 + (i * 10.5),
                    "volume": 1.0 + (i * 0.1),
                    "timestamp": f"2025-09-19T00:{50 + (i // 60)}:{(i % 60):02d}Z",
                    "exchange": exchanges[i % len(exchanges)],
                    "trade_id": f"trade_{i:06d}",
                    "side": "buy" if i % 2 == 0 else "sell"
                })
            
            # Write batch to sink
            sink.write(crypto_batch)
            
            # Verify REST API was called correctly
            assert mock_post.call_count >= 1
        
        # Check the last call
        args, kwargs = mock_post.call_args
        
        # Verify URL structure
        url = args[0] if args else kwargs.get("url")
        assert "crypto_trades" in url
        assert "integration-test" in url
        
        # Verify headers
        headers = kwargs["headers"]
        assert "Authorization" in headers
        assert "integration-test-token" in headers["Authorization"]
        assert "Content-Type" in headers
        
        # Verify payload (now using optimized 'data' parameter)
        import json
        payload = json.loads(kwargs["data"].decode('utf-8'))
        assert "records" in payload
        assert len(payload["records"]) > 0
        
        # Verify timeout is reasonable for integration tests
        assert kwargs["timeout"] <= 10.0
        
        # Cleanup
        sink.flush()
        sink.close()
    
    @pytest.mark.integration
    @pytest.mark.iceberg_rest
    def test_error_handling_in_pipeline(self):
        """Test pipeline error handling with various failure scenarios."""
        from quixstreams.sinks.community.iceberg_rest import (
            IcebergRESTSink,
            create_local_rest_config
        )
        import requests
        
        config = create_local_rest_config(table_name="error_test")
        # Use small batch size to trigger immediate sending
        sink = IcebergRESTSink(config, batch_size=1)
        
        with patch.object(sink.client.session, 'post') as mock_post:
            # Test 1: Server error (500)
            mock_post.return_value.status_code = 500
            mock_post.return_value.text = "Internal Server Error"
            mock_post.return_value.reason = "Internal Server Error"
            
            from quixstreams.sinks.community.iceberg_rest.errors import IcebergRESTError
            
            with pytest.raises(IcebergRESTError):
                sink.write([{"test": "data"}])
            
            # Test 2: Client error (400)
            mock_post.return_value.status_code = 400
            mock_post.return_value.text = "Bad Request - Invalid schema"
            mock_post.return_value.reason = "Bad Request"
            
            with pytest.raises(IcebergRESTError):
                sink.write([{"invalid": "schema"}])
            
            # Test 3: Network timeout (should be handled gracefully)
            mock_post.side_effect = requests.Timeout("Request timed out")
            
            with pytest.raises(IcebergRESTError):
                sink.write([{"timeout": "test"}])
        
        # Cleanup should still work
        sink.close()
    
    @pytest.mark.integration
    @pytest.mark.iceberg_rest
    def test_multi_provider_configuration(self):
        """Test that different storage provider configs work."""
        from quixstreams.sinks.community.iceberg_rest import (
            create_local_rest_config,
            create_r2_config,
            create_s3_rest_config,
            IcebergRESTSink
        )
        
        # Test local config
        local_config = create_local_rest_config(table_name="multi_test")
        local_sink = IcebergRESTSink(local_config)
        assert "localhost" in local_config.catalog_uri
        local_sink.close()
        
        # Test R2 config (with dummy credentials)
        r2_config = create_r2_config(
            account_id="test-account",
            access_key_id="test-key",
            secret_access_key="test-secret",
            catalog_uri="https://test-catalog.com/api/v1",
            table_name="multi_test"
        )
        r2_sink = IcebergRESTSink(r2_config)
        assert "r2.cloudflarestorage.com" in r2_config.s3_endpoint_url
        r2_sink.close()
        
        # Test S3 config (with dummy credentials)
        s3_config = create_s3_rest_config(
            catalog_uri="https://test-catalog.com/api/v1",
            warehouse_id="test-warehouse",
            aws_region="us-east-1",
            aws_access_key_id="test-key",
            aws_secret_access_key="test-secret",
            table_name="multi_test"
        )
        s3_sink = IcebergRESTSink(s3_config)
        # S3 config should have None endpoint (uses default AWS S3)
        assert s3_config.s3_endpoint_url is None or "s3.us-east-1.amazonaws.com" in s3_config.s3_endpoint_url
        s3_sink.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
