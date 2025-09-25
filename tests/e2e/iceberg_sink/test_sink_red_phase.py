"""
RED Phase Tests for Iceberg Sink TDD Implementation

These tests define the expected behavior for the Iceberg sink and will initially fail,
driving the implementation in the GREEN phase. Following TDD principles strictly.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
import os
import requests

from quixstreams.sinks.community.iceberg_rest import (
    create_local_rest_config,
    RESTIcebergConfig,
    CatalogConfig,
    StorageConfig, 
    StorageProvider,
)


@pytest.fixture(autouse=True)
def mock_http_client():
    """Automatically mock HTTP client calls to prevent network requests and JSON serialization issues.
    
    This fixture runs automatically for all tests in this module, ensuring that:
    1. No actual HTTP requests are made to external services
    2. MagicMock objects in test data don't cause JSON serialization errors
    3. Tests focus on business logic rather than network integration
    
    Mocks the RESTCatalogClient.post_records method directly to bypass JSON serialization.
    """
    # Mock successful HTTP response
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.reason = "OK"
    mock_response.text = '{"records_written": 1}'
    mock_response.json.return_value = {"records_written": 1}
    mock_response.url = "http://localhost:8181/api/v1/warehouses/local-warehouse/tables/test.table/data"
    
    # Mock the RESTCatalogClient.post_records method to bypass JSON serialization entirely
    with patch('quixstreams.sinks.community.iceberg_rest.client.RESTCatalogClient.post_records', 
               return_value=mock_response) as mock_post_records:
        yield mock_post_records


class TestSinkConfigurationValidationRED:
    """RED Phase: Configuration validation tests that should fail initially."""
    
    def test_sink_configuration_requires_table_name(self):
        """IcebergRESTSink should require table_name parameter."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
        
        config = create_local_rest_config(table_name="")  # Empty table name
        
        # Should fail without table_name
        with pytest.raises(TypeError, match="table_name"):
            IcebergRESTSink(config=config)  # Missing table_name
    
    def test_sink_validates_configuration_compatibility(self):
        """Sink should validate that configuration is properly structured."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
        
        # Should fail with invalid configuration type
        with pytest.raises(ValueError, match="Invalid configuration"):
            IcebergRESTSink(table_name="test.table", config="invalid_config")
    
    def test_environment_configuration_loading(self):
        """Should support loading configuration from environment variables."""
        env_vars = {
            "ICEBERG_TABLE_NAME": "crypto.trades",
            "ICEBERG_CATALOG_URI": "http://localhost:8181", 
            "ICEBERG_WAREHOUSE_ID": "test",
            "ICEBERG_STORAGE_PROVIDER": "minio",
            "ICEBERG_STORAGE_REGION": "us-east-1",
            "ICEBERG_STORAGE_ENDPOINT": "http://localhost:9000",
            "ICEBERG_ACCESS_KEY_ID": "minioadmin",
            "ICEBERG_SECRET_ACCESS_KEY": "minioadmin"
        }
        
        with patch.dict(os.environ, env_vars):
            # This should create a sink from environment - will fail initially
            from quixstreams.sinks.community.iceberg_rest import create_sink_from_env
            
            sink = create_sink_from_env()  # This function doesn't exist yet
            
            assert sink.table_name == "crypto.trades"
            assert "localhost" in sink._config.catalog.uri
    
    def test_configuration_validation_provides_detailed_errors(self):
        """Configuration validation should provide helpful error messages."""
        from quixstreams.sinks.community.iceberg_rest import validate_sink_config
        
        # Create a valid config first then modify it to make it invalid
        valid_config = create_local_rest_config()
        
        # Modify the config to make it invalid
        valid_config.table_name = ""  # Empty table name
        valid_config.catalog.warehouse_id = ""  # Empty warehouse  
        valid_config.storage.region = ""  # Missing region
        
        with pytest.raises(ValueError) as exc_info:
            validate_sink_config(valid_config)
        
        error_msg = str(exc_info.value)
        assert "table_name" in error_msg
        assert "warehouse_id" in error_msg
        assert "region" in error_msg


class TestSinkTableManagementRED:
    """RED Phase: Table management tests that should fail initially."""
    
    def test_sink_creates_table_with_auto_schema_detection(self):
        """Sink should create table with schema detected from first batch."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
        
        config = create_local_rest_config()
        config.table_name = "auto_schema.test"  # This attribute doesn't exist
        
        sink = IcebergRESTSink(table_name="auto_schema.test", config=config)
        
        # Mock a batch with sample data
        mock_batch = MagicMock()
        mock_batch.items = [
            MagicMock(key=b"key1", value={"id": 1, "name": "Alice", "price": 100.50}, timestamp=1609459200000),
            MagicMock(key=b"key2", value={"id": 2, "name": "Bob", "price": 200.75}, timestamp=1609459201000),
        ]
        
        # This should auto-detect schema and create table - will fail initially
        with patch('pyiceberg.catalog.rest.RestCatalog') as mock_catalog:
            sink.setup()
            sink.write(mock_batch)  # Should auto-create schema from first batch
        
        # Should have inferred schema with correct types
        expected_fields = ["id", "name", "price", "_timestamp", "_key"]
        # This assertion will fail because auto-schema detection isn't implemented
        assert hasattr(sink, '_inferred_schema')
        for field in expected_fields:
            assert field in [f.name for f in sink._inferred_schema.fields]
    
    def test_sink_handles_schema_evolution_gracefully(self):
        """Sink should handle schema evolution when new fields are added."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
        
        config = create_local_rest_config()
        sink = IcebergRESTSink(table_name="evolving_schema.test", config=config)
        
        # First batch with basic schema
        batch1 = MagicMock()
        batch1.items = [MagicMock(key=b"key1", value={"id": 1, "name": "Alice"}, timestamp=1609459200000)]
        
        # Second batch with additional fields
        batch2 = MagicMock()
        batch2.items = [MagicMock(key=b"key2", value={"id": 2, "name": "Bob", "age": 30, "active": True}, timestamp=1609459201000)]
        
        with patch('pyiceberg.catalog.rest.RestCatalog') as mock_catalog:
            sink.setup()
            sink.write(batch1)  # Create initial schema
            sink.write(batch2)  # Should evolve schema - will fail initially
            
        # Should have handled schema evolution
        # This will fail because schema evolution isn't properly implemented
        assert hasattr(sink, '_schema_evolution_count')
        assert sink._schema_evolution_count >= 1
    
    def test_sink_validates_schema_compatibility(self):
        """Sink should validate schema compatibility and reject incompatible changes."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, SchemaIncompatibilityError
        
        config = create_local_rest_config()
        sink = IcebergRESTSink(table_name="strict_schema.test", config=config)
        
        # First batch establishes schema  
        batch1 = MagicMock()
        batch1.items = [MagicMock(key=b"key1", value={"id": 1, "price": 100.50}, timestamp=1609459200000)]
        
        # Second batch with incompatible type change
        batch2 = MagicMock()
        batch2.items = [MagicMock(key=b"key2", value={"id": "string_id", "price": "not_a_number"}, timestamp=1609459201000)]
        
        with patch('pyiceberg.catalog.rest.RestCatalog') as mock_catalog:
            sink.setup()
            sink.write(batch1)
            
            # This should fail due to incompatible schema change - SchemaIncompatibilityError doesn't exist yet
            with pytest.raises(SchemaIncompatibilityError, match="incompatible.*type"):
                sink.write(batch2)


class TestSinkDataIngestionRED:
    """RED Phase: Data ingestion tests that should fail initially."""
    
    def test_sink_processes_large_batches_efficiently(self):
        """Sink should efficiently process large data batches."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
        
        config = create_local_rest_config()
        sink = IcebergRESTSink(table_name="large_batch.test", config=config)
        
        # Create large batch (10k records)
        large_batch = MagicMock()
        large_batch.items = [
            MagicMock(key=f"key_{i}".encode(), value={"id": i, "data": f"value_{i}"}, timestamp=1609459200000 + i)
            for i in range(10000)
        ]
        large_batch.size = len(large_batch.items)
        
        with patch('pyiceberg.catalog.rest.RestCatalog') as mock_catalog:
            sink.setup()
            
            # This should process efficiently without memory issues - will likely fail initially
            import time
            start_time = time.time()
            sink.write(large_batch)
            processing_time = time.time() - start_time
            
            # Should process 10k records in reasonable time
            assert processing_time < 10.0  # Less than 10 seconds
            assert hasattr(sink, '_batch_processing_stats')  # Doesn't exist yet
            assert sink._batch_processing_stats['records_per_second'] > 1000
    
    def test_sink_handles_nested_data_structures(self):
        """Sink should handle complex nested data structures."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
        
        config = create_local_rest_config()
        sink = IcebergRESTSink(table_name="nested_data.test", config=config)
        
        # Complex nested data batch
        complex_batch = MagicMock()
        complex_batch.items = [
            MagicMock(
                key=b"complex1", 
                value={
                    "user": {"id": 1, "profile": {"name": "Alice", "preferences": ["crypto", "stocks"]}},
                    "trades": [{"symbol": "BTC", "amount": 1.5}, {"symbol": "ETH", "amount": 10.0}],
                    "metadata": {"source": "api", "timestamp": "2023-01-01T00:00:00Z"}
                },
                timestamp=1609459200000
            )
        ]
        
        with patch('pyiceberg.catalog.rest.RestCatalog') as mock_catalog:
            sink.setup()
            # This should handle nested structures properly - will likely fail initially
            sink.write(complex_batch)
            
        # Should have flattened or properly structured nested data  
        # This will fail because nested data handling isn't implemented
        assert hasattr(sink, '_data_flattening_strategy')  # Doesn't exist yet
        assert sink._data_flattening_strategy in ["flatten", "json_serialize", "struct"]
    
    def test_sink_preserves_kafka_metadata(self):
        """Sink should preserve and properly handle Kafka metadata."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
        
        config = create_local_rest_config()
        sink = IcebergRESTSink(table_name="kafka_metadata.test", config=config)
        
        # Batch with rich Kafka metadata
        batch = MagicMock()
        batch.topic = "crypto.trades"
        batch.partition = 3
        batch.items = [
            MagicMock(
                key=b"BTC-USDT",
                value={"symbol": "BTC-USDT", "price": 45000.0, "volume": 1.5},
                timestamp=1609459200000,
                offset=12345,
                headers={"source": "binance", "type": "trade"}
            )
        ]
        
        with patch('pyiceberg.catalog.rest.RestCatalog') as mock_catalog:
            sink.setup()
            sink.write(batch)
            
        # Should preserve Kafka metadata in table
        # This will fail because metadata preservation isn't implemented  
        assert hasattr(sink, '_kafka_metadata_columns')  # Doesn't exist yet
        expected_columns = ["_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_headers"]
        for col in expected_columns:
            assert col in sink._kafka_metadata_columns


class TestSinkErrorHandlingRED:
    """RED Phase: Error handling tests that should fail initially."""
    
    def test_sink_handles_catalog_connection_failures(self):
        """Sink should gracefully handle catalog connection failures with retry."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, CatalogConnectionError
        
        config = create_local_rest_config()
        sink = IcebergRESTSink(table_name="connection_retry.test", config=config)
        
        # Mock connection failure then success
        with patch('pyiceberg.catalog.rest.RestCatalog') as mock_catalog:
            mock_catalog.side_effect = [
                ConnectionError("Connection failed"),
                ConnectionError("Connection failed"), 
                MagicMock()  # Success on third attempt
            ]
            
            # Should retry and eventually succeed - CatalogConnectionError doesn't exist yet
            sink.setup()  # Should handle retries internally
            
            assert mock_catalog.call_count == 3  # Should have retried twice
            assert hasattr(sink, '_connection_retry_count')  # Doesn't exist yet
    
    def test_sink_handles_commit_conflicts_with_backpressure(self):
        """Sink should handle commit conflicts by applying backpressure."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
        from quixstreams.sinks import SinkBackpressureError
        from pyiceberg.exceptions import CommitFailedException
        
        config = create_local_rest_config()
        sink = IcebergRESTSink(table_name="commit_conflict.test", config=config)
        
        batch = MagicMock()
        batch.items = [MagicMock(key=b"key1", value={"id": 1}, timestamp=1609459200000)]
        
        with patch('pyiceberg.catalog.rest.RestCatalog') as mock_catalog:
            mock_table = MagicMock()
            mock_table.append.side_effect = CommitFailedException("Commit conflict")
            mock_catalog.return_value.create_table_if_not_exists.return_value = mock_table
            
            sink.setup()
            
            # Should raise SinkBackpressureError to trigger retry - handling might not be implemented
            with pytest.raises(SinkBackpressureError) as exc_info:
                sink.write(batch)
            
            assert exc_info.value.retry_after > 0  # Should suggest retry delay
            assert hasattr(sink, '_commit_conflict_count')  # Doesn't exist yet
    
    def test_sink_provides_detailed_error_context(self):
        """Sink should provide detailed error context for debugging."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, SinkError
        
        config = create_local_rest_config()
        sink = IcebergRESTSink(table_name="error_context.test", config=config)
        
        # Batch that will cause an error
        problematic_batch = MagicMock()
        problematic_batch.items = [MagicMock(key=None, value={"invalid": float('inf')}, timestamp=1609459200000)]
        
        with patch('pyiceberg.catalog.rest.RestCatalog') as mock_catalog:
            sink.setup()
            
            # Should provide rich error context - SinkError doesn't exist yet
            with pytest.raises(SinkError) as exc_info:
                sink.write(problematic_batch)
            
            error = exc_info.value
            assert hasattr(error, 'error_context')  # Rich error context
            assert error.error_context['table_name'] == "error_context.test"
            assert error.error_context['batch_size'] == 1
            assert 'config' in error.error_context


class TestSinkPerformanceRED:
    """RED Phase: Performance tests that should fail initially."""
    
    def test_sink_meets_throughput_requirements(self):
        """Sink should meet minimum throughput requirements."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
        
        config = create_local_rest_config()
        sink = IcebergRESTSink(table_name="throughput.test", config=config)
        
        # Create performance test batch
        perf_batch = MagicMock()
        perf_batch.items = [
            MagicMock(key=f"key_{i}".encode(), value={"id": i, "value": i * 1.5}, timestamp=1609459200000 + i)
            for i in range(5000)  # 5k records
        ]
        perf_batch.size = len(perf_batch.items)
        
        with patch('pyiceberg.catalog.rest.RestCatalog') as mock_catalog:
            sink.setup()
            
            # Measure throughput
            import time
            start = time.time()
            sink.write(perf_batch)
            duration = time.time() - start
            
            throughput = perf_batch.size / duration
            
            # Should achieve >1000 records/second - likely will fail initially
            assert throughput > 1000, f"Throughput {throughput:.2f} records/sec below requirement"
            
    def test_sink_memory_usage_stays_bounded(self):
        """Sink should maintain reasonable memory usage even with large batches."""
        from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink
        import psutil
        import os
        
        config = create_local_rest_config()
        sink = IcebergRESTSink(table_name="memory.test", config=config)
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Process multiple large batches
        with patch('pyiceberg.catalog.rest.RestCatalog') as mock_catalog:
            sink.setup()
            
            for batch_num in range(5):  # 5 batches of 2k records each
                batch = MagicMock()
                batch.items = [
                    MagicMock(
                        key=f"batch{batch_num}_key_{i}".encode(), 
                        value={"batch": batch_num, "id": i, "data": "x" * 100},  # 100 char string
                        timestamp=1609459200000 + batch_num * 1000 + i
                    )
                    for i in range(2000)
                ]
                batch.size = len(batch.items)
                sink.write(batch)
        
        # Check final memory usage
        final_memory = process.memory_info().rss
        memory_growth = final_memory - initial_memory
        
        # Memory growth should be reasonable (<100MB) - may fail initially
        assert memory_growth < 100 * 1024 * 1024, f"Excessive memory growth: {memory_growth / 1024 / 1024:.2f}MB"


if __name__ == "__main__":
    print("=== RED Phase Test Suite ===")
    print("These tests define expected Iceberg sink behavior and will initially fail.")
    print("They drive the implementation during the GREEN phase.")
    print()
    print("Run with: pytest tests/e2e/iceberg_sink/test_sink_red_phase.py -v")
    print("Expected result: Many failures, establishing the TDD RED phase.")