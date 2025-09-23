"""
Greenfield Real Object Test Sinks

NO MOCKS, NO LEGACY, START SMALL, SOLID, KISS, DRY, CONSISTENT NAMING

Real sink implementations that capture data for end-to-end testing verification.
These are NOT mocks - they are real sink implementations for testing.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import json
import tempfile
import os
from pathlib import Path


@dataclass
class TestSinkResult:
    """Result container for test sink operations."""
    success: bool
    data_count: int
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseTestSink(ABC):
    """
    Base class for test sinks.
    
    Follows SOLID principles:
    - Single Responsibility: Each sink handles one type of output
    - Open/Closed: Easy to extend with new sink types
    - Liskov Substitution: All sinks can be used interchangeably
    """
    
    def __init__(self):
        self.data_received: List[Dict[str, Any]] = []
        self.is_closed = False
    
    @abstractmethod
    def write(self, data: Dict[str, Any]) -> bool:
        """Write single data item to sink."""
        pass
    
    @abstractmethod
    def write_batch(self, batch: List[Dict[str, Any]]) -> TestSinkResult:
        """Write batch of data to sink."""
        pass
    
    def close(self) -> None:
        """Close the sink and cleanup resources."""
        self.is_closed = True
    
    def get_data_count(self) -> int:
        """Get count of data items received."""
        return len(self.data_received)
    
    def clear(self) -> None:
        """Clear all received data."""
        self.data_received.clear()


class MemoryTestSink(BaseTestSink):
    """
    Real sink that captures data in memory for verification.
    
    This is NOT a mock - it's a real implementation that stores data
    in memory for testing purposes.
    """
    
    def __init__(self, max_capacity: int = 10000):
        super().__init__()
        self.max_capacity = max_capacity
        self.write_count = 0
    
    def write(self, data: Dict[str, Any]) -> bool:
        """Write single data item to memory."""
        if self.is_closed:
            return False
        
        if len(self.data_received) >= self.max_capacity:
            return False
        
        self.data_received.append(data.copy())
        self.write_count += 1
        return True
    
    def write_batch(self, batch: List[Dict[str, Any]]) -> TestSinkResult:
        """Write batch of data to memory."""
        if self.is_closed:
            return TestSinkResult(False, 0, "Sink is closed")
        
        if len(self.data_received) + len(batch) > self.max_capacity:
            return TestSinkResult(False, 0, f"Exceeds capacity {self.max_capacity}")
        
        success_count = 0
        for item in batch:
            if self.write(item):
                success_count += 1
            else:
                break
        
        return TestSinkResult(
            success=success_count == len(batch),
            data_count=success_count,
            metadata={"total_writes": self.write_count}
        )
    
    def get_all_data(self) -> List[Dict[str, Any]]:
        """Get all received data."""
        return self.data_received.copy()
    
    def get_latest_data(self, count: int = 1) -> List[Dict[str, Any]]:
        """Get latest N data items."""
        return self.data_received[-count:]
    
    def find_data_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """Find all data for specific symbol."""
        return [item for item in self.data_received if item.get("symbol") == symbol]


class FileTestSink(BaseTestSink):
    """
    Real sink that writes to temporary files for verification.
    
    This is NOT a mock - it's a real file-writing implementation
    for testing purposes.
    """
    
    def __init__(self, file_format: str = "jsonl"):
        super().__init__()
        self.file_format = file_format.lower()
        self.temp_dir = tempfile.mkdtemp(prefix="test_sink_")
        self.file_path = Path(self.temp_dir) / f"test_output.{self.file_format}"
        self._file_handle = None
    
    def _get_file_handle(self):
        """Get file handle, opening if needed."""
        if self._file_handle is None or self._file_handle.closed:
            self._file_handle = open(self.file_path, 'w', encoding='utf-8')
        return self._file_handle
    
    def write(self, data: Dict[str, Any]) -> bool:
        """Write single data item to file."""
        if self.is_closed:
            return False
        
        try:
            handle = self._get_file_handle()
            
            if self.file_format == "jsonl":
                json.dump(data, handle)
                handle.write('\n')
                # Track data for JSONL format too
                self.data_received.append(data)
            elif self.file_format == "json":
                # For JSON format, we'll append to a list structure
                self.data_received.append(data)
            else:
                return False
            
            handle.flush()
            return True
            
        except Exception:
            return False
    
    def write_batch(self, batch: List[Dict[str, Any]]) -> TestSinkResult:
        """Write batch of data to file."""
        if self.is_closed:
            return TestSinkResult(False, 0, "Sink is closed")
        
        success_count = 0
        error_message = None
        
        try:
            if self.file_format == "json":
                # Write entire batch as JSON array
                with open(self.file_path, 'w', encoding='utf-8') as f:
                    json.dump(batch, f, indent=2)
                success_count = len(batch)
                self.data_received.extend(batch)
            else:
                # Write JSONL format
                for item in batch:
                    if self.write(item):
                        success_count += 1
                    else:
                        error_message = f"Failed to write item {success_count}"
                        break
        
        except Exception as e:
            error_message = str(e)
        
        return TestSinkResult(
            success=success_count == len(batch),
            data_count=success_count,
            error_message=error_message,
            metadata={"file_path": str(self.file_path), "format": self.file_format}
        )
    
    def read_file_contents(self) -> List[Dict[str, Any]]:
        """Read back the file contents for verification."""
        if not self.file_path.exists():
            return []
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                if self.file_format == "json":
                    return json.load(f)
                elif self.file_format == "jsonl":
                    return [json.loads(line.strip()) for line in f if line.strip()]
                else:
                    return []
        except Exception:
            return []
    
    def close(self) -> None:
        """Close file handle and cleanup."""
        # For JSON format, write accumulated data before closing
        if self.file_format == "json" and self.data_received:
            try:
                with open(self.file_path, 'w', encoding='utf-8') as f:
                    json.dump(self.data_received, f, indent=2)
            except Exception:
                pass  # Ignore errors during cleanup
                
        if self._file_handle and not self._file_handle.closed:
            self._file_handle.close()
        super().close()
    
    def cleanup(self) -> None:
        """Remove temporary files and directories."""
        self.close()
        if self.file_path.exists():
            os.remove(self.file_path)
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)


class ConsoleTestSink(BaseTestSink):
    """
    Real sink that captures console output for verification.
    
    This is NOT a mock - it's a real implementation that captures
    stdout/stderr for testing purposes.
    """
    
    def __init__(self, output_format: str = "json"):
        super().__init__()
        self.output_format = output_format
        self.console_output: List[str] = []
    
    def write(self, data: Dict[str, Any]) -> bool:
        """Write single data item to console output."""
        if self.is_closed:
            return False
        
        try:
            if self.output_format == "json":
                output = json.dumps(data)
            elif self.output_format == "text":
                # Simple text format for readability
                symbol = data.get("symbol", "UNKNOWN")
                price = data.get("price", 0.0)
                timestamp = data.get("timestamp", 0)
                output = f"[{timestamp}] {symbol}: ${price}"
            else:
                output = str(data)
            
            self.console_output.append(output)
            self.data_received.append(data)
            
            # Actually print to stdout for real console output
            print(output)
            return True
            
        except Exception:
            return False
    
    def write_batch(self, batch: List[Dict[str, Any]]) -> TestSinkResult:
        """Write batch of data to console."""
        if self.is_closed:
            return TestSinkResult(False, 0, "Sink is closed")
        
        success_count = 0
        for item in batch:
            if self.write(item):
                success_count += 1
            else:
                break
        
        return TestSinkResult(
            success=success_count == len(batch),
            data_count=success_count,
            metadata={"output_lines": len(self.console_output)}
        )
    
    def get_console_output(self) -> List[str]:
        """Get all console output lines."""
        return self.console_output.copy()
    
    def get_latest_output(self, count: int = 1) -> List[str]:
        """Get latest N console output lines."""
        return self.console_output[-count:]


class TestSinkFactory:
    """Factory for creating test sinks (DRY principle)."""
    
    @staticmethod
    def create_memory_sink(capacity: int = 1000) -> MemoryTestSink:
        """Create memory test sink."""
        return MemoryTestSink(max_capacity=capacity)
    
    @staticmethod
    def create_file_sink(format: str = "jsonl") -> FileTestSink:
        """Create file test sink."""
        return FileTestSink(file_format=format)
    
    @staticmethod
    def create_console_sink(format: str = "json") -> ConsoleTestSink:
        """Create console test sink."""
        return ConsoleTestSink(output_format=format)
    
    @staticmethod
    def create_all_sinks() -> Dict[str, BaseTestSink]:
        """Create all available test sink types."""
        return {
            "memory": TestSinkFactory.create_memory_sink(),
            "file": TestSinkFactory.create_file_sink(),
            "console": TestSinkFactory.create_console_sink()
        }