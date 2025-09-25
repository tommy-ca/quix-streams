"""Deployment readiness utilities for Iceberg REST sink."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from .config import IcebergConfig, StorageProvider


@dataclass
class DeploymentReadiness:
    """Generate deployment readiness reports for production validation."""

    config: IcebergConfig
    runbook_path: str

    def generate(self) -> Dict[str, object]:
        """Produce a readiness report with individual check results."""

        checks = [
            self._check_catalog_endpoint(),
            self._check_authentication(),
            self._check_storage_credentials(),
            self._check_runbook(),
        ]

        status = "ready" if all(item["ok"] for item in checks) else "needs_attention"

        return {
            "status": status,
            "checks": checks,
            "runbook_path": self.runbook_path,
            "config": {
                "catalog_uri": self.config.catalog_uri,
                "warehouse_id": self.config.warehouse_id,
                "storage_provider": self.config.storage.provider.value,
            },
        }

    def _check_catalog_endpoint(self) -> Dict[str, object]:
        uri = self.config.catalog_uri
        ok = uri.startswith("http://") or uri.startswith("https://")
        return {
            "name": "catalog_endpoint",
            "ok": ok,
            "details": uri,
        }

    def _check_authentication(self) -> Dict[str, object]:
        auth_type = self.config.catalog.auth_type
        token_present = bool(getattr(self.config.catalog, "token", None))
        ok = auth_type == "none" or token_present
        return {
            "name": "catalog_authentication",
            "ok": ok,
            "details": {"auth_type": auth_type, "token_present": token_present},
        }

    def _check_storage_credentials(self) -> Dict[str, object]:
        storage = self.config.storage
        credentials_required = storage.provider in {StorageProvider.AWS, StorageProvider.MINIO}
        has_access_key = bool(storage.access_key_id)
        has_secret = bool(storage.secret_access_key)

        ok = True
        if credentials_required:
            ok = has_access_key and has_secret

        return {
            "name": "storage_credentials",
            "ok": ok,
            "details": {
                "provider": storage.provider.value,
                "access_key": bool(storage.access_key_id),
                "secret_key": bool(storage.secret_access_key),
            },
        }

    def _check_runbook(self) -> Dict[str, object]:
        exists = Path(self.runbook_path).is_file()
        return {
            "name": "runbook_available",
            "ok": exists,
            "details": self.runbook_path,
        }


__all__ = ["DeploymentReadiness"]
