"""Storage writer for Iceberg REST sink."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Iterable, List

import pyarrow as pa
import pyarrow.parquet as pq


class StorageWriter:
    """Persist batches prior to REST catalog commit.

    This initial implementation writes JSON payloads to a temporary directory to
    support TDD scaffolding. Future iterations will emit Parquet/Avro artifacts
    once pyiceberg integration is complete.
    """

    def __init__(self, base_path: str | None = None) -> None:
        self._base_path = Path(base_path or tempfile.mkdtemp(prefix="iceberg-rest-writer-"))
        self._base_path.mkdir(parents=True, exist_ok=True)
        self._sequence = 0
        self._artifacts: List[Path] = []

    def write(self, records: Iterable[dict]) -> List[dict]:
        batch = list(records)
        if not batch:
            raise ValueError("StorageWriter.write requires at least one record")

        self._sequence += 1
        file_path = self._base_path / f"batch-{self._sequence}.parquet"

        table = pa.Table.from_pylist(batch)
        pq.write_table(table, file_path)

        self._artifacts.append(file_path)
        return [
            {
                "path": str(file_path),
                "format": "parquet",
                "record_count": len(batch),
            }
        ]

    def rollback(self, artifacts: Iterable[dict]) -> None:
        """Delete staged artifacts for the last batch when a commit fails."""

        for descriptor in artifacts:
            path_value = descriptor.get("path") if isinstance(descriptor, dict) else None
            if not path_value:
                continue

            path = Path(path_value)
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass

            try:
                self._artifacts.remove(path)
            except ValueError:
                # Artifact might already be removed or was not tracked
                continue

    def close(self) -> None:
        for artifact in self._artifacts:
            try:
                artifact.unlink(missing_ok=True)
            except OSError:
                pass
        try:
            os.rmdir(self._base_path)
        except OSError:
            pass
