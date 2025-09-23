import os

import pytest

from quixstreams.sinks.community.iceberg_rest.config import (
    CatalogConfig,
    IcebergConfig,
    StorageConfig,
    StorageProvider,
    create_config,
    create_local_config,
    load_config_from_env,
    validate_config,
    validate_sink_config,
)


class TestCreateConfig:
    def test_create_local_config_defaults(self):
        config = create_local_config(table_name="events")

        assert isinstance(config, IcebergConfig)
        assert config.catalog.uri == "http://localhost:8181/api/v1"
        assert config.catalog.warehouse_id == "local"
        assert config.storage.provider is StorageProvider.MINIO
        assert config.storage.endpoint_url == "http://localhost:9000"
        assert config.storage.access_key_id == "minioadmin"
        assert config.storage.secret_access_key == "minioadmin"

    def test_create_config_cloudflare_r2(self):
        config = create_config(
            table_name="events",
            catalog_uri="https://catalog.example.com/api/v1",
            warehouse_id="analytics",
            provider="cloudflare_r2",
            region="auto",
            account_id="acct",
            access_key_id="r2-key",
            secret_access_key="r2-secret",
            catalog_token="token",
        )

        assert config.catalog.uri == "https://catalog.example.com/api/v1"
        assert config.catalog.token == "token"
        assert config.storage.provider is StorageProvider.CLOUDFLARE_R2
        assert config.storage.endpoint_url == "https://acct.r2.cloudflarestorage.com"
        assert config.storage.access_key_id == "r2-key"
        assert config.storage.secret_access_key == "r2-secret"


class TestLoadConfigFromEnv:
    def test_load_config_from_env_requires_variables(self, monkeypatch):
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


class TestValidation:
    def test_validate_config_rejects_empty_table(self):
        config = IcebergConfig(
            table_name="",
            catalog=CatalogConfig(uri="https://catalog", warehouse_id="wh"),
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
