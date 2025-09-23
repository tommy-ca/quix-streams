"""
TDD RED PHASE - Greenfield End-to-End Integration Tests

NO MOCKS, NO LEGACY, START SMALL, SOLID, KISS, DRY, CONSISTENT NAMING

These tests WILL FAIL initially - they define the expected behavior
for complete crypto data pipeline integration.

RED -> GREEN -> REFACTOR cycle:
1. RED: Write failing tests (this file)
2. GREEN: Make minimal implementation to pass
3. REFACTOR: Clean up while keeping tests green
"""

import pytest
import time
import tempfile
from typing import Dict, Any, List
from unittest.mock import patch, MagicMock

from .test_data_factory import TestDataFactory, CryptoTradeData, TestDataValidator
from .test_sinks import TestSinkFactory, MemoryTestSink, FileTestSink
from .pipeline_test_harness import PipelineTestBuilder, CommonProcessors


class TestGreenfieldEndToEndIntegration:
    """
    End-to-end integration tests for complete crypto data pipeline.
    
    Tests the flow: CryptofeedSource -> Processing -> TestSinks
    Following SOLID principles and using real objects only.
    """
    
    def test_basic_crypto_pipeline_connectivity(self):
        """
        RED PHASE: Basic pipeline connectivity test.
        
        WILL FAIL - defines that we need:
        1. CryptofeedSource that can be configured
        2. Pipeline that processes crypto data
        3. Memory sink that captures results
        4. End-to-end data flow verification
        """
        # This test defines the contract for basic pipeline connectivity
        from quixstreams.sources.community.crypto import CryptofeedSource, CryptofeedConfig
        
        # Create realistic crypto config
        config = CryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"],
            symbols=["BTC-USDT", "ETH-USDT"]
        )
        
        # Create pipeline test harness
        pipeline = (PipelineTestBuilder()
                   .with_crypto_trade_data(10)
                   .with_memory_sink()
                   .build())
        
        # Run the pipeline test
        result = pipeline.run_pipeline_test(max_items=10, timeout_seconds=5.0)
        
        # Verify pipeline execution
        assert result.success, f"Pipeline failed: {result.errors}"
        assert result.data_processed == 10, "Should process all 10 data items"
        assert result.execution_time < 5.0, "Should complete within timeout"
        
        # Verify data reached sink
        memory_sink = pipeline.test_sinks["memory"]
        assert memory_sink.get_data_count() == 10, "All data should reach sink"
        
        # Verify data structure
        received_data = memory_sink.get_all_data()
        for data in received_data:
            assert "symbol" in data, "Data should contain symbol"
            assert "price" in data, "Data should contain price"
            assert "timestamp" in data, "Data should contain timestamp"
        
        pipeline.cleanup()
    
    def test_crypto_data_transformation_pipeline(self):
        """
        RED PHASE: Data transformation pipeline test.
        
        WILL FAIL - defines that we need:
        1. Data processing functions that transform crypto data
        2. Pipeline that applies transformations in sequence
        3. Verification that transformations are applied correctly
        """
        # Create test data
        test_data = TestDataFactory.create_mixed_data_batch(5)
        
        # Create pipeline with transformations
        pipeline = (PipelineTestBuilder()
                   .with_custom_data(test_data)
                   .with_processing_function(CommonProcessors.add_processing_timestamp)
                   .with_processing_function(CommonProcessors.calculate_usd_value)
                   .with_processing_function(CommonProcessors.normalize_symbol_format)
                   .with_memory_sink()
                   .with_file_sink(format="json")
                   .build())
        
        # Run transformation pipeline
        result = pipeline.run_pipeline_test()
        
        # Verify transformations were applied
        assert result.success, f"Transformation pipeline failed: {result.errors}"
        
        # Check memory sink results
        memory_sink = pipeline.test_sinks["memory"]
        transformed_data = memory_sink.get_all_data()
        
        for data in transformed_data:
            # Verify processing timestamp was added
            assert "processing_timestamp" in data, "Processing timestamp should be added"
            assert data["processing_timestamp"] > 0, "Processing timestamp should be valid"
            
            # Verify USD value calculation (for trade data)
            if "price" in data and "volume" in data:
                expected_usd = data["price"] * data["volume"]
                assert "usd_value" in data, "USD value should be calculated"
                assert abs(data["usd_value"] - expected_usd) < 0.01, "USD value should be accurate"
            
            # Verify symbol normalization
            assert "symbol" in data, "Symbol should exist"
            assert "original_symbol" in data, "Original symbol should be preserved"
            assert "-" not in data["symbol"], "Symbol should be normalized (no dashes)"
            assert "/" not in data["symbol"], "Symbol should be normalized (no slashes)"
        
        # Verify file sink also received data
        file_sink = pipeline.test_sinks["file"]
        assert file_sink.get_data_count() == len(test_data), "File sink should have all data"
        
        pipeline.cleanup()
    
    def test_crypto_pipeline_error_handling(self):
        """
        RED PHASE: Error handling in pipeline test.
        
        WILL FAIL - defines that we need:
        1. Graceful handling of processing errors
        2. Pipeline continues with valid data after errors
        3. Error reporting and logging
        """
        # Create test data with some invalid items
        valid_data = TestDataFactory.create_trade_data().to_dict()
        invalid_data = {"invalid": "data", "missing": "required_fields"}
        
        test_data = [valid_data, invalid_data, valid_data, valid_data]
        
        # Processing function that fails on invalid data
        def strict_processor(data: Dict[str, Any]) -> Dict[str, Any]:
            if "symbol" not in data or "price" not in data:
                raise ValueError("Invalid data structure")
            return data
        
        # Create pipeline with error-prone processing
        pipeline = (PipelineTestBuilder()
                   .with_custom_data(test_data)
                   .with_processing_function(strict_processor)
                   .with_memory_sink()
                   .build())
        
        # Run pipeline (should handle errors gracefully)
        result = pipeline.run_pipeline_test()
        
        # Pipeline should continue processing despite errors
        assert result.data_processed >= 3, "Should process valid data items"
        assert len(result.errors) > 0, "Should report processing errors"
        assert "Invalid data structure" in str(result.errors), "Should capture specific error"
        
        # Valid data should still reach sink
        memory_sink = pipeline.test_sinks["memory"]
        received_data = memory_sink.get_all_data()
        assert len(received_data) >= 3, "Valid data should reach sink"
        
        # Verify all received data is valid
        for data in received_data:
            assert TestDataValidator.validate_trade_data(
                CryptoTradeData(**{k: v for k, v in data.items() 
                                   if k in ["symbol", "price", "volume", "timestamp", "exchange", "side"]})
            ), "All received data should be valid"
        
        pipeline.cleanup()
    
    def test_multi_sink_data_consistency(self):
        """
        RED PHASE: Multi-sink data consistency test.
        
        WILL FAIL - defines that we need:
        1. Data sent to multiple sinks simultaneously
        2. Consistency across all sink types
        3. No data loss between sinks
        """
        # Create consistent test data
        test_data = TestDataFactory.create_trade_batch(8)
        test_data_dicts = [trade.to_dict() for trade in test_data]
        
        # Create pipeline with multiple sinks
        pipeline = (PipelineTestBuilder()
                   .with_custom_data(test_data_dicts)
                   .with_memory_sink("memory1")
                   .with_memory_sink("memory2")
                   .with_file_sink("file_jsonl", format="jsonl")
                   .with_file_sink("file_json", format="json")
                   .build())
        
        # Run pipeline
        result = pipeline.run_pipeline_test()
        
        # Verify all sinks received data
        assert result.success, f"Multi-sink pipeline failed: {result.errors}"
        
        # Check each sink has the same data count
        expected_count = len(test_data_dicts)
        for sink_name, sink_result in result.sink_results.items():
            assert sink_result.success, f"Sink {sink_name} failed"
            assert sink_result.data_count == expected_count, f"Sink {sink_name} missing data"
        
        # Verify data consistency between memory sinks
        memory1_data = pipeline.test_sinks["memory1"].get_all_data()
        memory2_data = pipeline.test_sinks["memory2"].get_all_data()
        
        assert len(memory1_data) == len(memory2_data), "Memory sinks should have same count"
        
        # Compare data item by item
        for i, (data1, data2) in enumerate(zip(memory1_data, memory2_data)):
            assert data1["symbol"] == data2["symbol"], f"Symbol mismatch at item {i}"
            assert data1["timestamp"] == data2["timestamp"], f"Timestamp mismatch at item {i}"
            assert abs(data1["price"] - data2["price"]) < 0.01, f"Price mismatch at item {i}"
        
        # Verify file sinks wrote correctly
        jsonl_sink = pipeline.test_sinks["file_jsonl"]
        json_sink = pipeline.test_sinks["file_json"]
        
        # IMPORTANT: Ensure files are written by calling close() explicitly
        jsonl_sink.close()
        json_sink.close()
        
        jsonl_file_data = jsonl_sink.read_file_contents()
        json_file_data = json_sink.read_file_contents()
        
        assert len(jsonl_file_data) == expected_count, "JSONL file should have all data"
        assert len(json_file_data) == expected_count, "JSON file should have all data"
        
        pipeline.cleanup()
    
    def test_pipeline_performance_characteristics(self):
        """
        RED PHASE: Pipeline performance test.
        
        WILL FAIL - defines that we need:
        1. Pipeline handles reasonable data volumes efficiently
        2. Performance metrics are tracked
        3. Memory usage is reasonable
        """
        # Create larger dataset for performance testing
        large_dataset = TestDataFactory.create_trade_batch(100)
        test_data = [trade.to_dict() for trade in large_dataset]
        
        # Create pipeline with processing
        pipeline = (PipelineTestBuilder()
                   .with_custom_data(test_data)
                   .with_processing_function(CommonProcessors.add_processing_timestamp)
                   .with_processing_function(CommonProcessors.calculate_usd_value)
                   .with_memory_sink()
                   .build())
        
        # Run performance test
        start_time = time.time()
        result = pipeline.run_pipeline_test(max_items=100, timeout_seconds=30.0)
        end_time = time.time()
        
        # Verify performance requirements
        assert result.success, f"Performance test failed: {result.errors}"
        assert result.data_processed == 100, "Should process all data items"
        
        # Performance assertions
        total_time = end_time - start_time
        items_per_second = result.data_processed / result.execution_time
        
        # Should process at least 10 items per second (very conservative)
        assert items_per_second >= 10, f"Too slow: {items_per_second} items/sec"
        
        # Should complete within reasonable time
        assert result.execution_time < 20.0, f"Too slow: {result.execution_time}s"
        
        # Memory usage should be reasonable (sink should handle 100 items)
        memory_sink = pipeline.test_sinks["memory"]
        assert memory_sink.get_data_count() == 100, "All data should fit in memory sink"
        
        pipeline.cleanup()
    
    def test_data_filtering_and_routing(self):
        """
        RED PHASE: Data filtering and routing test.
        
        WILL FAIL - defines that we need:
        1. Processing functions can filter out data (return None)
        2. Pipeline handles filtered data correctly
        3. Only matching data reaches sinks
        """
        # Create mixed crypto data (BTC and non-BTC)
        btc_data = TestDataFactory.create_trade_data(symbol="BTC-USDT").to_dict()
        eth_data = TestDataFactory.create_trade_data(symbol="ETH-USDT").to_dict()
        sol_data = TestDataFactory.create_trade_data(symbol="SOL-USDT").to_dict()
        
        test_data = [btc_data, eth_data, btc_data, sol_data, btc_data]
        
        # Create pipeline with BTC filter
        pipeline = (PipelineTestBuilder()
                   .with_custom_data(test_data)
                   .with_processing_function(CommonProcessors.filter_btc_only)
                   .with_memory_sink()
                   .build())
        
        # Run filtering pipeline
        result = pipeline.run_pipeline_test()
        
        # Should process all items but only BTC should reach sink
        assert result.success, f"Filtering pipeline failed: {result.errors}"
        
        # Only 3 BTC items should reach the sink
        memory_sink = pipeline.test_sinks["memory"]
        filtered_data = memory_sink.get_all_data()
        
        assert len(filtered_data) == 3, "Should only have BTC data"
        
        # Verify all data is BTC-related
        for data in filtered_data:
            assert "BTC" in data["symbol"], "All filtered data should be BTC-related"
        
        pipeline.cleanup()
    
    def test_cryptofeed_source_integration_with_quixstreams(self):
        """
        RED PHASE: Real CryptofeedSource integration test.
        
        WILL FAIL - defines that we need:
        1. CryptofeedSource works with QuixStreams Application
        2. Real crypto data flows through processing pipeline
        3. Integration with QuixStreams topics and serialization
        
        This test will mock the external cryptofeed dependency only.
        """
        from quixstreams.sources.community.crypto import CryptofeedSource, CryptofeedConfig
        from quixstreams import Application
        
        # Create real crypto config
        config = CryptofeedConfig(
            exchanges=["binance"],
            channels=["trades"],
            symbols=["BTC-USDT"]
        )
        
        # Mock external cryptofeed dependency only
        with patch.dict('sys.modules', {'cryptofeed': MagicMock()}):
            # Create real CryptofeedSource
            source = CryptofeedSource(config)
            
            # Verify source was created correctly
            assert source._config == config
            assert source._config.exchanges == ["binance"]
            assert source._config.channels == ["trades"]
            
            # This test defines the contract for QuixStreams integration
            # In GREEN phase, we'll implement the minimal viable integration
            
            # For now, just verify the source can be instantiated
            # Future: integrate with QuixStreams Application
            assert source is not None
            assert hasattr(source, 'run')
            assert hasattr(source, 'stop')


class TestDataIntegrityValidation:
    """Tests for data integrity throughout the pipeline."""
    
    def test_timestamp_ordering_preservation(self):
        """
        RED PHASE: Timestamp ordering test.
        
        WILL FAIL - defines that timestamps should be preserved in order.
        """
        # Create ordered test data
        base_time = int(time.time() * 1000)
        ordered_data = []
        
        for i in range(5):
            trade = TestDataFactory.create_trade_data(timestamp=base_time + (i * 1000))
            ordered_data.append(trade.to_dict())
        
        # Create pipeline
        pipeline = (PipelineTestBuilder()
                   .with_custom_data(ordered_data)
                   .with_memory_sink()
                   .build())
        
        # Run pipeline
        result = pipeline.run_pipeline_test()
        assert result.success
        
        # Verify timestamp ordering is preserved
        memory_sink = pipeline.test_sinks["memory"]
        received_data = memory_sink.get_all_data()
        
        timestamps = [data["timestamp"] for data in received_data]
        assert timestamps == sorted(timestamps), "Timestamps should remain ordered"
        
        pipeline.cleanup()
    
    def test_data_completeness_validation(self):
        """
        RED PHASE: Data completeness test.
        
        WILL FAIL - defines that no data should be lost in pipeline.
        """
        # Create test data with unique identifiers
        test_data = []
        for i in range(10):
            trade = TestDataFactory.create_trade_data()
            trade_dict = trade.to_dict()
            trade_dict["test_id"] = f"test_{i:03d}"
            test_data.append(trade_dict)
        
        # Create pipeline
        pipeline = (PipelineTestBuilder()
                   .with_custom_data(test_data)
                   .with_memory_sink()
                   .build())
        
        # Run pipeline
        result = pipeline.run_pipeline_test()
        assert result.success
        
        # Verify all test IDs are present
        memory_sink = pipeline.test_sinks["memory"]
        received_data = memory_sink.get_all_data()
        
        received_ids = {data["test_id"] for data in received_data}
        expected_ids = {f"test_{i:03d}" for i in range(10)}
        
        assert received_ids == expected_ids, "All data items should be present"
        
        pipeline.cleanup()