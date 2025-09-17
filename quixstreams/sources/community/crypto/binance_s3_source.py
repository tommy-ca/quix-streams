from __future__ import annotations

import logging
from io import BytesIO
from pathlib import Path
from typing import Callable, Iterable, Optional, Union

try:
    import boto3
    import botocore
    from botocore.config import Config as BotoConfig
except ImportError as exc:
    # Import guard; this module will still import, but constructing the source will raise a helpful error
    boto3 = None  # type: ignore
    botocore = None  # type: ignore
    BotoConfig = None  # type: ignore

from quixstreams.sources.base import ClientConnectFailureCallback, ClientConnectSuccessCallback, Source
from quixstreams.sources.community.crypto.utils import TopicBuilder, default_key_fn, default_ts_fn

logger = logging.getLogger(__name__)

# module-level aliases for test monkeypatching
_time = None
_backoff_sequence: list[float] | None = None

__all__ = ("BinanceS3Source",)


class BinanceS3Source(Source):
    """
    Source for replaying Binance public archive objects stored in S3 into Kafka.

    v1: supports JSONL (optionally gz) with unsigned/signed access, deterministic traversal,
        and basic replay pacing using event timestamps.

    Optional dependency: boto3
    Install: pip install quixstreams[aws]
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        unsigned: bool = False,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        file_format: str = "infer",
        compression: Optional[str] = "infer",
        datatype: str = "trades",
        replay_speed: float = 0.0,
        key_setter: Optional[Callable[[dict], object]] = None,
        timestamp_setter: Optional[Callable[[dict], Optional[int]]] = None,
        has_partition_folders: bool = False,
        checksum_mode: str = "skip",
        extract_metadata: bool = True,
        name: Optional[str] = None,
        shutdown_timeout: float = 30.0,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        # Name defaults to our desired topic convention so Source.default_topic() aligns with docs
        default_name = TopicBuilder("binance.s3", datatype=datatype)
        super().__init__(
            name=name or default_name,
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        if boto3 is None:
            raise ImportError(
                "BinanceS3Source requires 'boto3'. Install: pip install quixstreams[aws]"
            )

        self._bucket = bucket
        self._prefix = prefix.rstrip("/") + "/" if not prefix.endswith("/") else prefix
        self._unsigned = unsigned
        self._credentials = {
            "region_name": region_name,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "endpoint_url": endpoint_url,
        }
        self._file_format = file_format
        self._compression = compression
        self._datatype = datatype
        self._replay_speed = replay_speed
        self._key_setter = key_setter or default_key_fn
        self._timestamp_setter = timestamp_setter or default_ts_fn
        self._has_partition_folders = has_partition_folders
        self._checksum_mode = checksum_mode
        self._extract_metadata = extract_metadata

        self._s3 = None

    def setup(self):
        cfg = None
        if self._unsigned:
            # Use unsigned requests for public archives
            cfg = BotoConfig(signature_version=botocore.UNSIGNED)  # type: ignore[attr-defined]
        self._s3 = boto3.client("s3", config=cfg, **{k: v for k, v in self._credentials.items() if v})
        # validate access by listing a single key under prefix
        self._s3.list_objects_v2(Bucket=self._bucket, Prefix=self._prefix, MaxKeys=1)

    def _iter_object_keys(self) -> Iterable[str]:
        token = None
        while True:
            kwargs = {"Bucket": self._bucket, "Prefix": self._prefix, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self._s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []) or []:
                yield obj["Key"]
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break

    def _read_object(self, key: str) -> bytes:
        # retry with simple backoff sequence injected for tests
        from typing import cast
        import hashlib
        seq = cast(list[float], globals().get("_backoff_sequence") or [0.5, 1.0, 2.0])
        for i, backoff in enumerate(seq + [None]):
            try:
                obj = self._s3.get_object(Bucket=self._bucket, Key=key)
                body = obj["Body"].read()
                # Optional checksum verification
                if self._checksum_mode != "skip":
                    try:
                        chk_obj = self._s3.get_object(Bucket=self._bucket, Key=f"{key}.CHECKSUM")
                        expected = chk_obj["Body"].read().decode().strip().split()[0]
                        actual = hashlib.md5(body).hexdigest()
                        if expected != actual:
                            msg = f"Checksum mismatch for {key}: expected {expected}, got {actual}"
                            if self._checksum_mode == "strict":
                                raise ValueError(msg)
                            else:
                                logger.warning(msg)
                    except Exception:
                        # ignore missing checksum file or errors in warn/skip
                        if self._checksum_mode == "strict":
                            raise
                return body
            except Exception as e:  # noqa: BLE001
                if backoff is None:
                    raise
                logger.warning(f"get_object failed for {key}, retrying in {backoff}s: {e}")
                global _time
                import time as _t
                (_time or _t).sleep(backoff)
        raise RuntimeError("unreachable")

    def _iter_records_from_body(self, key: str, body: bytes) -> Iterable[dict]:
        # Handle gzip/zip/csv/jsonl
        import gzip as _gzip
        import io as _io
        import zipfile as _zip
        import csv as _csv
        import json

        raw = body
        if key.endswith(".gz"):
            raw = _gzip.decompress(body)
        elif key.endswith(".zip"):
            with _io.BytesIO(body) as bio, _zip.ZipFile(bio) as zf:
                # take first file
                first = zf.namelist()[0]
                raw = zf.read(first)

        # CSV klines (simple mapping) when .csv
        if key.endswith(".csv"):
            text = raw.decode()
            reader = _csv.DictReader(text.splitlines())
            for row in reader:
                try:
                    ev = {
                        "exchange": "binance",
                        "symbol": "unknown",
                        "open_time": int(row["open_time"]),
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": float(row["volume"]),
                        "close_time": int(row["close_time"]),
                    }
                except Exception:
                    logger.warning("Skipping malformed csv row in %s", key)
                    continue
                yield ev
            return

        # Default: JSONL
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:  # noqa: BLE001
                logger.warning("Skipping malformed line in %s", key)
                continue


    def _extract_meta(self, key: str) -> dict:
        parts = key.split("/")
        meta = {"s3_key": key}
        # naive heuristics: market at index 1, segment at 2, datatype at 3, symbol at 4, date at 5
        # Adjust if prefix includes a leading folder
        for i, p in enumerate(parts):
            if p in ("spot", "um_futures", "cm_futures"):
                meta["market"] = p
                if i + 1 < len(parts):
                    meta["segment"] = parts[i + 1]
                if i + 2 < len(parts):
                    meta["datatype"] = parts[i + 2]
                if i + 3 < len(parts):
                    meta["symbol"] = parts[i + 3]
                if i + 4 < len(parts):
                    meta["date"] = parts[i + 4]
                break
        return meta

    def run(self):
        import time as _t
        # expose alias on module for tests
        global _time
        if _time is None:
            _time = _t
        keys = sorted(list(self._iter_object_keys()))
        prev_ts = None
        for key in keys:
            body = self._read_object(key)
            for record in self._iter_records_from_body(key, body):
                # For CSV klines, prefer close_time as ts if timestamp not set
                ts = self._timestamp_setter(record)
                if ts is None and "close_time" in record:
                    ts = int(record["close_time"])
                if prev_ts is not None and self._replay_speed and self._replay_speed > 0:
                    delta = max(0.0, (ts - prev_ts) / 1000.0) * self._replay_speed
                    if delta > 0:
                        if _time is None:
                            import time as _t
                            _t.sleep(delta)
                        else:
                            _time.sleep(delta)
                prev_ts = ts
                # attach metadata if enabled
                value = record
                if self._extract_metadata:
                    meta = self._extract_meta(key)
                    if isinstance(value, dict):
                        value = {**value, "meta": meta}
                msg = self.producer_topic.serialize(
                    key=self._key_setter(record), value=value, timestamp_ms=ts
                )
                self.produce(key=msg.key, value=msg.value, timestamp=msg.timestamp)
        self.producer.flush()
