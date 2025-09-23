"""End-to-end smoke tests for the Iceberg REST configuration layer."""

import os
import pytest

from quixstreams.sinks.community.iceberg_rest import (
    IcebergRESTSink,
    create_config,
    create_local_config,
    create_sink_from_env,
)
from quixstreams.sinks.community.iceberg_rest.config import (
    CatalogConfig,
    IcebergConfig,
    StorageConfig,
    StorageProvider,
    load_config_from_env,
    validate_config,
    validate_sink_config,
)


class TestCreateConfig:
    def test_create_config_cloudflare_r2(self):
        config = create_config(
            table_name="events",
            catalog_uri="https://catalog.example.com/api/v1",
            warehouse_id="analytics",
            provider="cloudflare_r2",
            region="auto",
            account_id="account",
            access_key_id="r2-key",
            secret_access_key="r2-secret",
            catalog_token="token",
        )

        assert isinstance(config, IcebergConfig)
        assert config.catalog.uri == "https://catalog.example.com/api/v1"
        assert config.catalog.token == "token"
        assert config.storage.provider is StorageProvider.CLOUDFLARE_R2
        assert config.storage.endpoint_url == "https://account.r2.cloudflarestorage.com"
        assert config.storage.access_key_id == "r2-key"
        assert config.storage.secret_access_key == "r2-secret"
        assert validate_config(config) is True

    def test_create_local_config_suitable_for_sink(self):
        config = create_local_config(table_name="events")
        sink = IcebergRESTSink(config=config)
        # setup should complete without raising
        sink.setup()
        sink.close()


class TestEnvironmentLoading:
    def test_load_config_from_env_requires_required_vars(self, monkeypatch):
        monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
        monkeypatch.delenv("ICEBERG_WAREHOUSE_ID", raising=False)

        with pytest.raises(ValueError):
            load_config_from_env(table_name="events")

    def test_load_config_from_env_success(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "https://catalog.local/api/v1")
        monkeypatch.setenv("ICEBERG_WAREHOUSE_ID", "local")
        monkeypatch.setenv("ICEBERG_STORAGE_PROVIDER", "aws")
        monkeypatch.setenv("ICEBERG_REGION", "us-east-1")

        config = load_config_from_env(table_name="events")

        assert config.catalog.uri == "https://catalog.local/api/v1"
        assert config.catalog.warehouse_id == "local"
        assert config.storage.provider is StorageProvider.AWS

    def test_create_sink_from_env_round_trip(self, monkeypatch):
        monkeypatch.setenv("ICEBERG_TABLE_NAME", "analytics.events")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "https://catalog.local/api/v1")
        monkeypatch.setenv("ICEBERG_WAREHOUSE_ID", "local")
        monkeypatch.setenv("ICEBERG_STORAGE_PROVIDER", "aws")
        monkeypatch.setenv("ICEBERG_REGION", "us-east-1")

        sink = create_sink_from_env()
        assert isinstance(sink, IcebergRESTSink)
        assert sink.config.catalog.uri == "https://catalog.local/api/v1"
        sink.close()


class TestValidation:
    def test_validate_config_requires_table_name(self):
        config = IcebergConfig(
            table_name="",
            catalog=CatalogConfig(uri="https://catalog", warehouse_id="warehouse"),
            storage=StorageConfig(provider=StorageProvider.AWS, region="us-east-1"),
        )

        with pytest.raises(ValueError):
            validate_config(config)

    def test_validate_sink_config_collects_errors(self):
        config = IcebergConfig(
            table_name="",
            catalog=CatalogConfig(uri="https://catalog", warehouse_id="warehouse"),
            storage=StorageConfig(provider=StorageProvider.AWS, region="us-east-1"),
        )

        with pytest.raises(ValueError) as excinfo:
            validate_sink_config(config)

        message = str(excinfo.value)
        assert "table_name" in message


class TestIntegrationBehaviour:
    def test_create_config_and_sink_with_custom_endpoint(self):
        config = create_config(
            table_name="events",
            catalog_uri="https://catalog.example.com/api/v1",
            warehouse_id="analytics",
            provider="custom",
            region="us-east-1",
            endpoint_url="https://storage.custom",
            access_key_id="key",
            secret_access_key="secret",
        )

        sink = IcebergRESTSink(config=config)
        # Ensure setup/close lifecycle works without hitting external services
        sink.setup()
        sink.close()
