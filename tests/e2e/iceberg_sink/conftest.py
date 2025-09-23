"""
Common fixtures and test configuration for Iceberg sink E2E tests.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from typing import Dict, List, Any

from quixstreams.sinks.community.iceberg_rest import (
    RESTIcebergConfig,
    create_local_rest_config,
)


@pytest.fixture
def local_iceberg_config() -> RESTIcebergConfig:
    """REST config for local development stack."""
    return create_local_rest_config()


# AWS config not available in current implementation


@pytest.fixture
def rest_iceberg_config() -> RESTIcebergConfig:
    """REST catalog Iceberg configuration for testing."""
    return RESTIcebergConfig(
        rest_uri="http://localhost:8181",
        warehouse_id="test",
        endpoint_url="http://localhost:9000",
        aws_region="us-east-1", 
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        auth_type="none"
    )


@pytest.fixture
def r2_iceberg_config() -> RESTIcebergConfig:
    """Cloudflare R2 with REST catalog configuration."""
    return RESTIcebergConfig(
        rest_uri="https://catalog.example.com/api/v1",
        warehouse_id="main",
        endpoint_url="https://account-id.r2.cloudflarestorage.com",
        aws_region="auto",
        aws_access_key_id="r2-access-key",
        aws_secret_access_key="r2-secret-key",
        auth_type="bearer_token",
        auth_token="test-catalog-token"
    )


@pytest.fixture
def mock_catalog():
    """Mock Iceberg catalog for isolated testing."""
    catalog = MagicMock()
    catalog.create_table_if_not_exists.return_value = MagicMock()
    catalog.load_table.return_value = MagicMock()
    return catalog


@pytest.fixture
def mock_table():
    """Mock Iceberg table for testing."""
    table = MagicMock()
    table.name.return_value = "test.table"
    table.append.return_value = None
    
    # Mock schema update context manager
    schema_update = MagicMock()
    schema_update.__enter__.return_value = schema_update
    schema_update.__exit__.return_value = None
    table.update_schema.return_value = schema_update
    
    return table


@pytest.fixture
def sample_kafka_batch():
    """Sample Kafka batch for testing data ingestion."""
    from quixstreams.sinks import SinkBatch
    
    # Create mock batch items
    batch_items = []
    for i in range(5):
        item = MagicMock()
        item.key = f"key-{i}".encode()
        item.value = {
            "id": i,
            "symbol": "BTC/USDT",
            "price": 45000 + i * 100,
            "volume": 1.5 + i * 0.1,
            "timestamp_ms": 1609459200000 + i * 1000
        }
        item.timestamp = 1609459200000 + i * 1000
        batch_items.append(item)
    
    return SinkBatch(topic="crypto.trades", partition=0, items=batch_items)


@pytest.fixture
def sample_data_batches():
    """Various data batch scenarios for testing."""
    return {
        "small": {"size": 10, "data_types": ["int", "str", "float"]},
        "medium": {"size": 1000, "data_types": ["int", "str", "float", "bool"]},
        "large": {"size": 10000, "data_types": ["int", "str", "float", "bool", "datetime"]},
        "complex": {"size": 100, "data_types": ["nested_dict", "list", "null_values"]}
    }


@pytest.fixture
def sample_schemas():
    """Sample Iceberg schemas for testing."""
    from pyiceberg.schema import Schema, NestedField
    from pyiceberg.types import StringType, IntegerType, DoubleType, TimestampType
    
    return {
        "basic": Schema(
            fields=[
                NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
                NestedField(field_id=2, name="name", field_type=StringType(), required=False),
            ]
        ),
        "crypto_trades": Schema(
            fields=[
                NestedField(field_id=1, name="_timestamp", field_type=TimestampType(), required=False),
                NestedField(field_id=2, name="_key", field_type=StringType(), required=False),
                NestedField(field_id=3, name="symbol", field_type=StringType(), required=True),
                NestedField(field_id=4, name="price", field_type=DoubleType(), required=True),
                NestedField(field_id=5, name="volume", field_type=DoubleType(), required=False),
            ]
        )
    }


@pytest.fixture
def environment_variables():
    """Environment variables for configuration testing."""
    return {
        "ICEBERG_REST_URI": "http://localhost:8181",
        "ICEBERG_WAREHOUSE_ID": "test",
        "ICEBERG_ENDPOINT_URL": "http://localhost:9000",
        "ICEBERG_AWS_REGION": "us-east-1",
        "ICEBERG_ACCESS_KEY_ID": "testkey",
        "ICEBERG_SECRET_ACCESS_KEY": "testsecret",
        "ICEBERG_AUTH_TYPE": "none"
    }


@pytest.fixture
def performance_test_data():
    """Large datasets for performance testing."""
    return {
        "records_1k": 1000,
        "records_10k": 10000,
        "records_100k": 100000,
        "batch_sizes": [10, 100, 1000, 5000],
        "concurrent_writers": [1, 2, 5, 10]
    }


@pytest.fixture(autouse=True)
def setup_test_logging(caplog):
    """Configure logging for tests."""
    import logging
    caplog.set_level(logging.DEBUG, logger="quixstreams.sinks.community.iceberg_rest")
    caplog.set_level(logging.INFO, logger="tests")


# Error simulation fixtures
@pytest.fixture
def mock_connection_error():
    """Mock connection errors for testing."""
    from requests.exceptions import ConnectionError
    return ConnectionError("Connection failed")


@pytest.fixture
def mock_commit_conflict():
    """Mock commit conflict errors for testing."""
    from pyiceberg.exceptions import CommitFailedException
    return CommitFailedException("Commit failed due to conflict")


@pytest.fixture
def mock_auth_error():
    """Mock authentication errors for testing."""
    return Exception("Authentication failed")


# Integration test helpers
@pytest.fixture
def integration_test_table_name():
    """Unique table name for integration tests."""
    timestamp = int(datetime.now().timestamp())
    return f"test_table_{timestamp}"


@pytest.fixture
def cleanup_tables():
    """Clean up test tables after integration tests."""
    created_tables = []
    
    def register_table(table_name: str):
        created_tables.append(table_name)
    
    yield register_table
    
    # Cleanup logic would go here if we had real catalog access
    # For now, just log what would be cleaned up
    if created_tables:
        print(f"Would cleanup tables: {created_tables}")


@pytest.fixture
def mock_pyiceberg():
    """Mock PyIceberg components for isolated testing."""
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_rest_catalog, \
         patch("pyiceberg.catalog.glue.GlueCatalog") as mock_glue_catalog, \
         patch("pyarrow.Table.from_pydict") as mock_arrow_table, \
         patch("pyarrow.parquet.write_table") as mock_write_table:
        
        yield {
            "rest_catalog": mock_rest_catalog,
            "glue_catalog": mock_glue_catalog,
            "arrow_table": mock_arrow_table,
            "write_table": mock_write_table
        }