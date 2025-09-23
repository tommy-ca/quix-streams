# QuixStreams Sinks & Sources Refactor Action Plan

**Priority**: HIGH  
**Impact**: Architecture improvement, Developer Experience, Code Quality  
**Timeline**: 4-6 weeks  
**Status**: Ready for Implementation

## 🎯 Executive Summary

This action plan outlines a strategic refactoring of QuixStreams sinks and sources architecture, applying the proven SOLID principles successfully implemented in the Iceberg REST sink. The plan focuses on immediate high-impact improvements while maintaining 100% backward compatibility.

## 🚀 Phase 1: Foundation Infrastructure (Week 1-2)

### 1.1 Create Base Configuration System

**File**: `quixstreams/sinks/base/config.py`

```python
"""
Unified configuration system for all sinks following SOLID principles.
Based on proven patterns from IcebergRESTSink refactor.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, Optional, Union
from enum import Enum

class SinkType(Enum):
    DATABASE = "database"
    DOCUMENT_STORE = "document_store"  
    KEY_VALUE = "key_value"
    SEARCH_ENGINE = "search_engine"
    FILE_STORAGE = "file_storage"
    TIME_SERIES = "time_series"

@dataclass
class ConnectionConfig(ABC):
    """Base configuration for sink connections."""
    timeout: float = 30.0
    max_retries: int = 3
    
    @abstractmethod
    def get_client_kwargs(self) -> Dict[str, Any]:
        """Return client initialization arguments."""

@dataclass 
class SinkConfig(ABC):
    """Base configuration for all sinks."""
    sink_type: SinkType
    connection: ConnectionConfig
    
    def validate(self) -> None:
        """Validate configuration."""
        if not self.connection:
            raise ValueError("Connection configuration is required")
```

### 1.2 Create Authentication System

**File**: `quixstreams/sinks/base/auth.py`

```python
"""
Unified authentication system for all sinks.
Eliminates duplication across BigQuery, MongoDB, Redis, etc.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, Optional, List

class AuthProvider(ABC):
    @abstractmethod
    def authenticate_client(self, client_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Apply authentication to client configuration."""

@dataclass
class NoAuth(AuthProvider):
    def authenticate_client(self, client_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return client_kwargs

@dataclass
class BasicAuth(AuthProvider):
    username: str
    password: str
    
@dataclass
class TokenAuth(AuthProvider):  
    token: str
    token_type: str = "Bearer"
    
@dataclass
class ServiceAccountAuth(AuthProvider):
    service_account_json: str
    scopes: Optional[List[str]] = None
```

### 1.3 Create Error Hierarchy

**File**: `quixstreams/sinks/base/errors.py`

```python
"""
Standardized error hierarchy for consistent error handling across all sinks.
"""

class SinkError(Exception):
    """Base exception for all sink operations."""
    def __init__(self, message: str, sink_type: str = None, original_error: Exception = None):
        super().__init__(message)
        self.sink_type = sink_type
        self.original_error = original_error

class SinkConfigurationError(SinkError):
    """Configuration validation and setup errors."""
    
class SinkConnectionError(SinkError):
    """Connection and authentication errors."""
    
class SinkTimeoutError(SinkConnectionError):
    """Request timeout errors."""
    
class SinkAuthenticationError(SinkConnectionError):
    """Authentication-specific errors."""
```

## 🎯 Phase 2: High-Impact Sink Refactors (Week 2-4)

### 2.1 BigQuery Sink Refactor (Week 2)

**Priority**: CRITICAL (Most complex, highest usage)

**Current Issues**:
- 12+ constructor parameters
- Mixed responsibilities (auth + config + client management)
- Complex error handling
- No configuration validation

**Target Architecture**:

```python
# quixstreams/sinks/community/bigquery/config.py
@dataclass
class BigQueryConnection(ConnectionConfig):
    project_id: str
    location: str
    service_account_json: Optional[str] = None
    
    def get_client_kwargs(self) -> Dict[str, Any]:
        kwargs = {"project": self.project_id}
        if self.service_account_json:
            credentials = service_account.Credentials.from_service_account_info(
                json.loads(self.service_account_json)
            )
            kwargs["credentials"] = credentials
        return kwargs

@dataclass
class BigQueryConfig(SinkConfig):
    connection: BigQueryConnection
    dataset_id: str
    table_name: str
    schema_auto_update: bool = True
    ddl_timeout: float = 10.0
    insert_timeout: float = 10.0
    sink_type: SinkType = SinkType.DATABASE

def create_bigquery_config(
    project_id: str,
    location: str,
    dataset_id: str, 
    table_name: str,
    service_account_json: Optional[str] = None,
    **kwargs
) -> BigQueryConfig:
    """Factory function for BigQuery configuration."""
    connection = BigQueryConnection(
        project_id=project_id,
        location=location,
        service_account_json=service_account_json
    )
    return BigQueryConfig(
        connection=connection,
        dataset_id=dataset_id,
        table_name=table_name,
        **kwargs
    )

# Usage (NEW - preferred)
config = create_bigquery_config(
    project_id="my-project",
    location="us-central1",
    dataset_id="analytics", 
    table_name="events"
)
sink = BigQuerySink(config=config)

# Usage (OLD - deprecated but working) 
sink = BigQuerySink(
    project_id="my-project",  # Still works with deprecation warning
    location="us-central1",
    dataset_id="analytics",
    table_name="events"
)
```

### 2.2 Redis Sink Refactor (Week 2)

**Priority**: HIGH (Simple validation case)

```python
# quixstreams/sinks/community/redis/config.py
@dataclass
class RedisConnection(ConnectionConfig):
    host: str
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    
    def get_client_kwargs(self) -> Dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port, 
            "db": self.db,
            "password": self.password,
            "socket_timeout": self.timeout
        }

@dataclass        
class RedisConfig(SinkConfig):
    connection: RedisConnection
    key_serializer: Optional[Callable] = None
    value_serializer: Callable = json.dumps
    sink_type: SinkType = SinkType.KEY_VALUE

def create_redis_config(
    host: str,
    port: int = 6379,
    db: int = 0,
    password: Optional[str] = None,
    **kwargs
) -> RedisConfig:
    """Factory function for Redis configuration."""
    connection = RedisConnection(host=host, port=port, db=db, password=password)
    return RedisConfig(connection=connection, **kwargs)
```

### 2.3 MongoDB Sink Refactor (Week 3) 

**Priority**: HIGH (Complex configuration, good document store example)

```python
# quixstreams/sinks/community/mongodb/config.py
@dataclass
class MongoConnection(ConnectionConfig):
    host: str
    port: int = 27017
    username: Optional[str] = None
    password: Optional[str] = None
    database: str
    
    def get_connection_string(self) -> str:
        auth_part = ""
        if self.username and self.password:
            from urllib.parse import quote_plus
            auth_part = f"{quote_plus(self.username)}:{quote_plus(self.password)}@"
        return f"mongodb://{auth_part}{self.host}:{self.port}"
        
    def get_client_kwargs(self) -> Dict[str, Any]:
        return {
            "host": self.get_connection_string(),
            "serverSelectionTimeoutMS": int(self.timeout * 1000)
        }

@dataclass
class MongoConfig(SinkConfig):
    connection: MongoConnection
    collection: str
    upsert: bool = True
    update_method: Literal["UpdateOne", "UpdateMany", "ReplaceOne"] = "UpdateOne"
    sink_type: SinkType = SinkType.DOCUMENT_STORE
```

## 🔧 Phase 3: File Sinks Consolidation (Week 4-5)

### 3.1 Unified File Storage Backend

**Target**: Eliminate 90% code duplication between local, S3, and Azure file sinks

```python
# quixstreams/sinks/community/file/backends.py
from abc import ABC, abstractmethod
from pathlib import Path

class FileStorageBackend(ABC):
    @abstractmethod
    def write_file(self, path: str, data: bytes) -> None:
        """Write data to storage backend."""
        
    @abstractmethod
    def ensure_directory(self, directory: str) -> None:
        """Ensure directory exists in storage backend."""

class LocalStorageBackend(FileStorageBackend):
    def write_file(self, path: str, data: bytes) -> None:
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'wb') as f:
            f.write(data)
            
    def ensure_directory(self, directory: str) -> None:
        Path(directory).mkdir(parents=True, exist_ok=True)

class S3StorageBackend(FileStorageBackend):
    def __init__(self, bucket: str, **s3_kwargs):
        self.bucket = bucket
        self.s3_client = boto3.client('s3', **s3_kwargs)
        
    def write_file(self, path: str, data: bytes) -> None:
        self.s3_client.put_object(
            Bucket=self.bucket, 
            Key=path, 
            Body=data
        )

# Unified FileSink
class FileSink(BatchingSink):
    def __init__(self, storage: FileStorageBackend, format: Format):
        super().__init__()
        self.storage = storage
        self.format = format
        
    def write(self, batch: SinkBatch) -> None:
        data = self.format.serialize(batch)
        path = self._generate_path(batch)
        self.storage.write_file(path, data)
```

## 📊 Phase 4: Success Validation (Week 5-6)

### 4.1 Automated Testing Suite

Create comprehensive tests for all refactored sinks:

```python
# tests/sinks/test_config_standardization.py
import pytest
from quixstreams.sinks.base.config import SinkConfig
from quixstreams.sinks.community.bigquery import create_bigquery_config
from quixstreams.sinks.community.redis import create_redis_config

class TestConfigStandardization:
    def test_all_sinks_use_config_pattern(self):
        """Verify all refactored sinks use the unified config pattern."""
        
    def test_backward_compatibility(self):
        """Verify old constructor patterns still work with deprecation warnings."""
        
    def test_error_consistency(self):
        """Verify all sinks use standardized error hierarchy."""
```

### 4.2 Performance Benchmarking

Ensure no performance regression:

```python
# benchmarks/sinks_performance.py
import time
from quixstreams.sinks.community.bigquery import BigQuerySink

def benchmark_sink_performance():
    """Compare old vs new sink performance."""
    # Measure batch write performance
    # Measure memory usage  
    # Measure connection overhead
```

### 4.3 Migration Documentation

Create comprehensive migration guides:

```markdown
# Sink Migration Guide

## BigQuery Sink Migration

### Before (v1.x)
```python
sink = BigQuerySink(
    project_id="my-project",
    location="us-central1", 
    dataset_id="analytics",
    table_name="events",
    service_account_json="...",
    schema_auto_update=True
)
```

### After (v2.x - Recommended)
```python
config = create_bigquery_config(
    project_id="my-project",
    location="us-central1",
    dataset_id="analytics", 
    table_name="events",
    service_account_json="...",
    schema_auto_update=True
)
sink = BigQuerySink(config=config)
```

### Benefits
- ✅ Type safety and validation
- ✅ Consistent API across all sinks  
- ✅ Better error messages
- ✅ Easier testing and mocking
```

## 📈 Success Metrics & Validation

### Code Quality Metrics
- [ ] **40% reduction** in total lines of code across refactored sinks
- [ ] **≤3 constructor parameters** for all new sink APIs
- [ ] **100% backward compatibility** with deprecation warnings
- [ ] **90%+ test coverage** for all refactored sinks

### Developer Experience Metrics
- [ ] **Consistent configuration** patterns across all sinks
- [ ] **Standardized error handling** with detailed context
- [ ] **Comprehensive documentation** with migration examples  
- [ ] **Zero breaking changes** in existing APIs

### Performance Metrics
- [ ] **No performance regression** in sink throughput
- [ ] **Reduced memory usage** through better connection management
- [ ] **Faster test execution** through dependency injection patterns

## 🚨 Risk Mitigation

### Risk: Breaking Existing Code
**Mitigation**: 
- Maintain 100% backward compatibility
- Add deprecation warnings with clear migration paths
- Provide automated migration tools where possible

### Risk: Performance Regression
**Mitigation**:
- Comprehensive performance testing before/after
- Benchmark suite for continuous monitoring  
- Gradual rollout with performance monitoring

### Risk: Community Adoption Resistance  
**Mitigation**:
- Clear communication of benefits
- Comprehensive migration documentation
- Gradual deprecation timeline (3+ releases)
- Community feedback integration

## 💼 Resource Requirements

### Development Team
- **2 senior developers** (architecture + implementation)
- **1 developer** (testing + documentation)
- **Part-time technical writer** (documentation polish)

### Timeline
- **Week 1-2**: Foundation infrastructure  
- **Week 3-4**: High-impact sink refactors
- **Week 5**: File sink consolidation
- **Week 6**: Testing, documentation, validation

### Dependencies
- No external dependencies
- Requires coordination with existing sink maintainers
- May need minor updates to QuixStreams core for optimal integration

## 🎯 Next Actions

### Immediate (This Week)
1. [ ] **Approve this action plan** and allocate resources
2. [ ] **Create GitHub issues** for each phase
3. [ ] **Set up tracking** and success metrics monitoring
4. [ ] **Begin Phase 1 implementation** with base infrastructure

### Week 1
1. [ ] **Implement base configuration system** (`quixstreams/sinks/base/config.py`)
2. [ ] **Implement authentication system** (`quixstreams/sinks/base/auth.py`)  
3. [ ] **Implement error hierarchy** (`quixstreams/sinks/base/errors.py`)
4. [ ] **Create testing framework** for validation

### Week 2
1. [ ] **Refactor BigQuery sink** with new patterns
2. [ ] **Refactor Redis sink** as validation case
3. [ ] **Add comprehensive tests** for refactored sinks  
4. [ ] **Create migration documentation**

## 🏆 Expected Outcomes

After completing this refactor plan:

1. **Developer Productivity**: 2-3x faster sink development and maintenance
2. **Code Quality**: Significantly improved maintainability and extensibility
3. **User Experience**: Consistent, intuitive APIs across all sinks
4. **Error Handling**: Better debugging experience with detailed error context
5. **Testing**: Easier testing through dependency injection and mocking
6. **Documentation**: Comprehensive, consistent documentation across all sinks

This refactor represents a strategic investment in the QuixStreams architecture that will pay dividends in development velocity, code quality, and user satisfaction for years to come.

---

**Status**: Ready for Implementation  
**Approval Needed**: Architecture team sign-off  
**Timeline**: 6 weeks to completion  
**Success Probability**: HIGH (based on proven Iceberg REST sink refactor patterns)