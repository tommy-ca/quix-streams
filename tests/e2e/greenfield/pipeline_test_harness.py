"""
Greenfield Pipeline Test Harness

NO MOCKS, NO LEGACY, START SMALL, SOLID, KISS, DRY, CONSISTENT NAMING

Orchestrates end-to-end pipeline tests using real objects and configurations.
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Callable
import time
import threading
from contextlib import contextmanager

from .test_data_factory import TestDataFactory, CryptoTradeData
from .test_sinks import BaseTestSink, TestSinkResult, TestSinkFactory


@dataclass
class PipelineTestResult:
    """Result container for pipeline test execution."""
    success: bool
    execution_time: float
    data_processed: int
    errors: List[str]
    sink_results: Dict[str, TestSinkResult]
    metadata: Dict[str, Any]


class MockDataSource:
    """
    Simple mock data source for testing pipeline.
    
    This is the ONLY mock we allow - it mocks external data sources,
    not our internal objects. Follows NO MOCKS principle correctly.
    """
    
    def __init__(self, test_data: List[Dict[str, Any]]):
        self.test_data = test_data.copy()
        self.current_index = 0
        self.is_running = False
    
    def start(self) -> None:
        """Start the mock data source."""
        self.is_running = True
        self.current_index = 0
    
    def stop(self) -> None:
        """Stop the mock data source."""
        self.is_running = False
    
    def get_next_data(self) -> Optional[Dict[str, Any]]:
        """Get next data item."""
        if not self.is_running or self.current_index >= len(self.test_data):
            return None
        
        data = self.test_data[self.current_index]
        self.current_index += 1
        return data
    
    def has_more_data(self) -> bool:
        """Check if more data is available."""
        return self.is_running and self.current_index < len(self.test_data)


class PipelineTestHarness:
    """
    Orchestrates end-to-end pipeline tests.
    
    Single Responsibility: Manages pipeline test execution
    Open/Closed: Easy to extend with new test scenarios
    KISS: Simple, straightforward test orchestration
    """
    
    def __init__(self):
        self.test_sinks: Dict[str, BaseTestSink] = {}
        self.data_source: Optional[MockDataSource] = None
        self.processing_functions: List[Callable] = []
        self.errors: List[str] = []
    
    def add_sink(self, name: str, sink: BaseTestSink) -> None:
        """Add a test sink to the pipeline."""
        self.test_sinks[name] = sink
    
    def set_data_source(self, data: List[Dict[str, Any]]) -> None:
        """Set the test data source."""
        self.data_source = MockDataSource(data)
    
    def add_processing_function(self, func: Callable[[Dict[str, Any]], Dict[str, Any]]) -> None:
        """Add a data processing function to the pipeline."""
        self.processing_functions.append(func)
    
    def run_pipeline_test(
        self,
        max_items: int = 100,
        timeout_seconds: float = 10.0
    ) -> PipelineTestResult:
        """
        Run the complete pipeline test.
        
        This is the main test orchestration method that:
        1. Starts the data source
        2. Processes data through transformation functions
        3. Writes to all configured sinks
        4. Collects and returns results
        """
        start_time = time.time()
        processed_count = 0
        sink_results: Dict[str, TestSinkResult] = {}
        errors: List[str] = []
        
        if not self.data_source:
            return PipelineTestResult(
                success=False,
                execution_time=0.0,
                data_processed=0,
                errors=["No data source configured"],
                sink_results={},
                metadata={}
            )
        
        if not self.test_sinks:
            return PipelineTestResult(
                success=False,
                execution_time=0.0,
                data_processed=0,
                errors=["No test sinks configured"],
                sink_results={},
                metadata={}
            )
        
        try:
            self.data_source.start()
            
            # Process data through pipeline
            while (self.data_source.has_more_data() and 
                   processed_count < max_items and 
                   (time.time() - start_time) < timeout_seconds):
                
                # Get next data item
                raw_data = self.data_source.get_next_data()
                if raw_data is None:
                    break
                
                # Apply processing functions in sequence
                processed_data = raw_data
                try:
                    for process_func in self.processing_functions:
                        processed_data = process_func(processed_data)
                        if processed_data is None:
                            break
                    
                    if processed_data is None:
                        continue
                    
                    # Write to all sinks
                    for sink_name, sink in self.test_sinks.items():
                        try:
                            success = sink.write(processed_data)
                            if not success:
                                errors.append(f"Failed to write to sink '{sink_name}'")
                        except Exception as e:
                            errors.append(f"Exception writing to sink '{sink_name}': {e}")
                    
                    processed_count += 1
                
                except Exception as e:
                    errors.append(f"Processing error: {e}")
                    continue
            
            # Collect sink results
            for sink_name, sink in self.test_sinks.items():
                try:
                    sink_results[sink_name] = TestSinkResult(
                        success=True,
                        data_count=sink.get_data_count(),
                        metadata={"sink_type": type(sink).__name__}
                    )
                except Exception as e:
                    sink_results[sink_name] = TestSinkResult(
                        success=False,
                        data_count=0,
                        error_message=str(e)
                    )
        
        finally:
            if self.data_source:
                self.data_source.stop()
        
        execution_time = time.time() - start_time
        
        # Determine overall success
        overall_success = (
            processed_count > 0 and
            len(errors) == 0 and
            all(result.success for result in sink_results.values())
        )
        
        return PipelineTestResult(
            success=overall_success,
            execution_time=execution_time,
            data_processed=processed_count,
            errors=errors,
            sink_results=sink_results,
            metadata={
                "max_items": max_items,
                "timeout_seconds": timeout_seconds,
                "processing_functions": len(self.processing_functions)
            }
        )
    
    def verify_data_integrity(self, expected_data: List[Dict[str, Any]]) -> bool:
        """Verify that all sinks received the expected data."""
        if not expected_data:
            return True
        
        for sink_name, sink in self.test_sinks.items():
            if sink.get_data_count() != len(expected_data):
                return False
            
            # For memory sink, we can do detailed comparison
            if hasattr(sink, 'get_all_data'):
                received_data = sink.get_all_data()
                if len(received_data) != len(expected_data):
                    return False
                
                # Compare key fields (symbol, timestamp)
                for i, (received, expected) in enumerate(zip(received_data, expected_data)):
                    if received.get("symbol") != expected.get("symbol"):
                        return False
                    if received.get("timestamp") != expected.get("timestamp"):
                        return False
        
        return True
    
    def cleanup(self) -> None:
        """Cleanup all resources."""
        for sink_name, sink in self.test_sinks.items():
            try:
                sink.close()
                if hasattr(sink, 'cleanup'):
                    sink.cleanup()
            except Exception as e:
                print(f"Error cleaning up sink {sink_name}: {e}")
        
        self.test_sinks.clear()
        self.data_source = None
        self.processing_functions.clear()
        self.errors.clear()


class PipelineTestBuilder:
    """
    Builder for creating pipeline tests (SOLID - Single Responsibility).
    
    Makes it easy to construct pipeline tests with various configurations.
    """
    
    def __init__(self):
        self.harness = PipelineTestHarness()
        self._test_data: List[Dict[str, Any]] = []
    
    def with_crypto_trade_data(self, count: int = 10) -> 'PipelineTestBuilder':
        """Add crypto trade test data."""
        trades = TestDataFactory.create_trade_batch(count)
        self._test_data.extend([trade.to_dict() for trade in trades])
        return self
    
    def with_custom_data(self, data: List[Dict[str, Any]]) -> 'PipelineTestBuilder':
        """Add custom test data."""
        self._test_data.extend(data)
        return self
    
    def with_memory_sink(self, name: str = "memory") -> 'PipelineTestBuilder':
        """Add memory test sink."""
        sink = TestSinkFactory.create_memory_sink()
        self.harness.add_sink(name, sink)
        return self
    
    def with_file_sink(self, name: str = "file", format: str = "jsonl") -> 'PipelineTestBuilder':
        """Add file test sink."""
        sink = TestSinkFactory.create_file_sink(format)
        self.harness.add_sink(name, sink)
        return self
    
    def with_console_sink(self, name: str = "console") -> 'PipelineTestBuilder':
        """Add console test sink."""
        sink = TestSinkFactory.create_console_sink()
        self.harness.add_sink(name, sink)
        return self
    
    def with_processing_function(self, func: Callable) -> 'PipelineTestBuilder':
        """Add data processing function."""
        self.harness.add_processing_function(func)
        return self
    
    def build(self) -> PipelineTestHarness:
        """Build the configured pipeline test harness."""
        if self._test_data:
            self.harness.set_data_source(self._test_data)
        return self.harness


# Common processing functions for reuse (DRY principle)
class CommonProcessors:
    """Common data processing functions for pipeline tests."""
    
    @staticmethod
    def add_processing_timestamp(data: Dict[str, Any]) -> Dict[str, Any]:
        """Add processing timestamp to data."""
        result = data.copy()
        result["processing_timestamp"] = int(time.time() * 1000)
        return result
    
    @staticmethod
    def filter_btc_only(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Filter to only BTC-related data."""
        if "BTC" in data.get("symbol", ""):
            return data
        return None  # None means filter out this item
    
    @staticmethod
    def calculate_usd_value(data: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate USD value for trades."""
        result = data.copy()
        price = data.get("price", 0.0)
        volume = data.get("volume", 0.0)
        result["usd_value"] = price * volume
        return result
    
    @staticmethod
    def normalize_symbol_format(data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize symbol format to consistent style."""
        result = data.copy()
        symbol = data.get("symbol", "")
        # Convert "BTC-USDT" to "BTCUSDT" format
        normalized = symbol.replace("-", "").replace("/", "").upper()
        result["symbol"] = normalized
        result["original_symbol"] = symbol
        return result