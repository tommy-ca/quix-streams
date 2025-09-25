"""Deployment readiness checks for Iceberg REST sink."""

from __future__ import annotations

import pytest

from quixstreams.sinks.community.iceberg_rest import create_local_config
from quixstreams.sinks.community.iceberg_rest.config import (
    CatalogConfig,
    IcebergConfig,
    StorageConfig,
    StorageProvider,
)
from quixstreams.sinks.community.iceberg_rest.deployment import DeploymentReadiness


RUNBOOK_PATH = "docs/specs/iceberg-rest-sink-testing-plan.md"


@pytest.mark.iceberg_rest
@pytest.mark.infrastructure
def test_deployment_readiness_report_passes_for_local_stack():
    """TSK-6.2: Local deployment surface should report ready status with runbook linkage."""

    config = create_local_config(table_name="deployment_checks")
    readiness = DeploymentReadiness(config=config, runbook_path=RUNBOOK_PATH)

    report = readiness.generate()

    assert report["status"] == "ready"
    assert all(check["ok"] for check in report["checks"])
    assert report["runbook_path"].endswith("iceberg-rest-sink-testing-plan.md")


@pytest.mark.iceberg_rest
@pytest.mark.infrastructure
def test_deployment_readiness_flags_missing_credentials():
    """TSK-6.2: Missing storage credentials must trigger attention status."""

    insecure_config = IcebergConfig(
        table_name="insecure",
        catalog=CatalogConfig(uri="http://localhost:8181/api/v1", warehouse_id="missing-creds"),
        storage=StorageConfig(
            provider=StorageProvider.MINIO,
            region="us-east-1",
            endpoint_url="http://localhost:9000",
        ),
    )

    readiness = DeploymentReadiness(config=insecure_config, runbook_path=RUNBOOK_PATH)
    report = readiness.generate()

    assert report["status"] == "needs_attention"
    failures = {item["name"]: item["ok"] for item in report["checks"]}
    assert failures.get("storage_credentials") is False
    assert failures.get("catalog_authentication") is True
