"""
TDD RED Phase: Performance Tests for IcebergRESTSink

These tests define performance requirements and optimizations.
Expected to FAIL initially, then guide implementation.

Author: TDD Sprint 3 - REFACTOR-002-RED
Date: September 19, 2025
"""

import pytest
import time
import json
import threading
from unittest.mock import patch, MagicMock
from concurrent.futures import ThreadPoolExecutor, as_completed

from quixstreams.sinks.community.iceberg_rest import (
    IcebergRESTSink,
    create_local_rest_config
)


class TestAdaptiveBatching:
    """Test adaptive batch sizing optimization."""
    
    @pytest.mark.performance
    def test_small_records_use_larger_batches(self):
        """Small records should trigger larger batch sizes automatically."""
        config = create_local_rest_config(table_name="small_records_test")
        sink = IcebergRESTSink(config, batch_size=100)
        
        # Create small records (< 1KB each)
        small_records = [{"id": i, "value": f"small_{i}"} for i in range(50)]
        
        with patch.object(sink.client.session, 'post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"status": "ok"}
            
            # Write small records - should NOT trigger HTTP call yet (adaptive batching)
            sink.write(small_records)
            
            # Should still be buffering (adaptive batch size should be > 50)
            assert mock_post.call_count == 0, "Should buffer more small records before sending"
            
        sink.close()
    
    @pytest.mark.performance
    def test_large_records_use_smaller_batches(self):
        """Large records should trigger smaller batch sizes to avoid memory issues."""
        config = create_local_rest_config(table_name="large_records_test")
        sink = IcebergRESTSink(config, batch_size=100)
        
        # Create large records (> 100KB each)
        large_data = "x" * 100000  # 100KB string
        large_records = [{"id": i, "data": f"{large_data}_{i}"} for i in range(10)]
        
        with patch.object(sink.client.session, 'post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"status": "ok"}
            
            sink.write(large_records)
            
            # Should have triggered HTTP call with adaptive smaller batch
            assert mock_post.call_count >= 1, "Large records should trigger immediate send"
            
        sink.close()
    
    @pytest.mark.performance 
    def test_memory_based_batching(self):
        """Batching should be based on memory size, not just record count."""
        config = create_local_rest_config(table_name="memory_test")
        sink = IcebergRESTSink(config, batch_size=1000)  # High count limit
        
        # This should trigger based on memory limit (e.g., 10MB), not count
        large_data = "x" * 50000  # 50KB per record
        records = [{"id": i, "data": f"{large_data}_{i}"} for i in range(250)]  # ~12.5MB total
        
        with patch.object(sink.client.session, 'post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"status": "ok"}
            
            sink.write(records)
            
            # Should have sent multiple batches due to memory limit
            assert mock_post.call_count >= 2, "Should break into multiple batches based on memory"
            
        sink.close()


class TestJSONOptimization:
    """Test JSON serialization optimizations."""
    
    @pytest.mark.performance
    def test_uses_fast_json_library(self):
        """Should use orjson for faster serialization when available."""
        config = create_local_rest_config(table_name="json_test")
        sink = IcebergRESTSink(config, batch_size=10)
        
        # Check if client uses optimized JSON
        assert hasattr(sink.client, '_json_encoder'), "Should have optimized JSON encoder"
        
        # Test serialization performance
        large_record = {"data": {"nested": {"values": list(range(1000))}}}
        
        start_time = time.time()
        for _ in range(100):
            serialized = sink.client._serialize_payload([large_record])
        end_time = time.time()
        
        # Should be faster than standard json (this will fail initially)
        serialize_time = end_time - start_time
        assert serialize_time < 0.1, f"JSON serialization too slow: {serialize_time}s"
        
        sink.close()
    
    @pytest.mark.performance
    def test_json_compression_for_large_payloads(self):
        """Large payloads should be compressed before sending."""
        config = create_local_rest_config(table_name="compression_test")
        sink = IcebergRESTSink(config, batch_size=10)
        
        # Create large payload (> 1MB)
        large_data = "x" * 10000  # 10KB base
        large_records = [{"id": i, "data": f"{large_data}_{i}"} for i in range(200)]  # ~2MB
        
        with patch.object(sink.client.session, 'post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"status": "ok"}
            
            sink.write(large_records)
            
            # Check that compression headers were added for large payload
            assert mock_post.call_count >= 1
            call_kwargs = mock_post.call_args[1]
            headers = call_kwargs.get('headers', {})
            
            assert 'Content-Encoding' in headers, "Should add compression headers for large payloads"
            assert headers['Content-Encoding'] in ['gzip', 'deflate'], "Should use compression"
            
        sink.close()


class TestConnectionPooling:
    """Test HTTP connection pooling optimizations."""
    
    @pytest.mark.performance
    def test_reuses_connections_across_requests(self):
        """Multiple requests should reuse the same HTTP connection."""
        config = create_local_rest_config(table_name="connection_test")
        sink = IcebergRESTSink(config, batch_size=5)
        
        # Check connection pool configuration
        session = sink.client.session
        assert hasattr(session, 'get_adapter'), "Should have HTTP adapter"
        
        adapter = session.get_adapter('http://')
        assert hasattr(adapter, 'config'), "Should have connection pool config"
        
        # Pool size should be optimized (not default)
        pool_config = getattr(adapter, 'config', {})
        pool_size = pool_config.get('pool_connections', 1)
        assert pool_size >= 10, f"Connection pool too small: {pool_size}"
        
        sink.close()
    
    @pytest.mark.performance
    def test_concurrent_requests_performance(self):
        """Multiple concurrent sinks should share connection efficiently."""
        config1 = create_local_rest_config(table_name="concurrent_test_1")
        config2 = create_local_rest_config(table_name="concurrent_test_2")
        
        sinks = [
            IcebergRESTSink(config1, batch_size=10),
            IcebergRESTSink(config2, batch_size=10)
        ]
        
        def write_data(sink, data):
            records = [{"id": i, "sink_id": data} for i in range(20)]
            with patch.object(sink.client.session, 'post') as mock_post:
                mock_post.return_value.status_code = 200
                mock_post.return_value.json.return_value = {"status": "ok"}
                
                start_time = time.time()
                sink.write(records)
                return time.time() - start_time
        
        # Run concurrent writes
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(write_data, sinks[0], "sink1"),
                executor.submit(write_data, sinks[1], "sink2")
            ]
            
            times = [future.result() for future in as_completed(futures)]
        
        # Concurrent writes should be fast (shared connection pool)
        max_time = max(times)
        assert max_time < 0.05, f"Concurrent writes too slow: {max_time}s"
        
        for sink in sinks:
            sink.close()


class TestBufferingOptimization:
    """Test memory-efficient buffering."""
    
    @pytest.mark.performance
    def test_buffer_memory_limit(self):
        """Buffer should have memory limit to prevent excessive RAM usage."""
        config = create_local_rest_config(table_name="buffer_test")
        sink = IcebergRESTSink(config, batch_size=10000)  # Very high to test memory limit
        
        # Check buffer has memory tracking
        assert hasattr(sink, '_buffer_memory_bytes'), "Should track buffer memory usage"
        assert hasattr(sink, '_max_buffer_memory'), "Should have memory limit"
        
        # Memory limit should be reasonable (e.g., 100MB)
        assert sink._max_buffer_memory <= 100 * 1024 * 1024, "Memory limit too high"
        
        sink.close()
    
    @pytest.mark.performance
    def test_buffer_overflow_handling(self):
        """Buffer overflow should trigger automatic flush."""
        config = create_local_rest_config(table_name="overflow_test")
        
        # Set small memory limit for testing
        sink = IcebergRESTSink(config, batch_size=10000, max_buffer_memory_mb=1)  # 1MB limit
        
        # Create data that exceeds buffer memory limit
        large_data = "x" * 50000  # 50KB per record
        records = [{"id": i, "data": f"{large_data}_{i}"} for i in range(25)]  # ~1.25MB total
        
        with patch.object(sink.client.session, 'post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"status": "ok"}
            
            sink.write(records)
            
            # Should have triggered automatic flush due to memory limit
            assert mock_post.call_count >= 1, "Should auto-flush when memory limit exceeded"
            
        sink.close()


class TestPerformanceBenchmarks:
    """Benchmark tests for overall performance."""
    
    @pytest.mark.performance
    @pytest.mark.slow
    def test_high_throughput_writing(self):
        """Should handle high-throughput writing efficiently."""
        config = create_local_rest_config(table_name="throughput_test")
        sink = IcebergRESTSink(config, batch_size=1000)
        
        # Generate test data (10k records)
        records = [
            {
                "id": i,
                "timestamp": int(time.time() * 1000),
                "symbol": f"CRYPTO_{i % 100}",
                "price": 100.0 + (i % 1000),
                "volume": 1.5 + (i % 10),
                "metadata": {"exchange": "test", "type": "trade"}
            }
            for i in range(10000)
        ]
        
        with patch.object(sink.client.session, 'post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"status": "ok", "records_written": 1000}
            
            start_time = time.time()
            sink.write(records)
            sink.flush()
            end_time = time.time()
            
            total_time = end_time - start_time
            throughput = len(records) / total_time
            
            # Should achieve > 10k records/second
            assert throughput > 10000, f"Throughput too low: {throughput:.0f} records/sec"
            
        sink.close()
    
    @pytest.mark.performance
    def test_memory_efficiency_large_dataset(self):
        """Memory usage should stay reasonable with large datasets."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        config = create_local_rest_config(table_name="memory_test")
        sink = IcebergRESTSink(config, batch_size=500)
        
        with patch.object(sink.client.session, 'post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {"status": "ok"}
            
            # Write 50k records in batches
            for batch_num in range(100):  # 100 batches of 500 records
                batch_records = [
                    {"id": batch_num * 500 + i, "data": f"record_{i}"}
                    for i in range(500)
                ]
                sink.write(batch_records)
        
        current_memory = process.memory_info().rss
        memory_increase = current_memory - initial_memory
        
        # Memory increase should be < 50MB for 50k records
        memory_increase_mb = memory_increase / (1024 * 1024)
        assert memory_increase_mb < 50, f"Memory usage too high: {memory_increase_mb:.1f}MB"
        
        sink.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-m", "performance"])