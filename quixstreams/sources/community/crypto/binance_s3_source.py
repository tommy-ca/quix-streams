from __future__ import annotations

import logging
import warnings
from io import BytesIO
from pathlib import Path
from typing import Callable, Iterable, Optional, Union

try:
    import boto3
    import botocore
    from botocore.config import Config as BotoConfig
    _BOTO3_AVAILABLE = True
except ImportError as exc:
    # Import guard; this module will still import, but constructing the source will raise a helpful error
    boto3 = None  # type: ignore
    botocore = None  # type: ignore
    BotoConfig = None  # type: ignore
    _BOTO3_AVAILABLE = False
    _BOTO3_IMPORT_ERROR = exc

from quixstreams.sources.base import ClientConnectFailureCallback, ClientConnectSuccessCallback, Source
from quixstreams.sources.community.crypto.config import (
    BinanceS3Config,
    AWSAuth,
    load_from_env,
)
from quixstreams.sources.community.crypto.errors import (
    missing_dependency_error,
    connection_error,
)
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
        config: Optional[BinanceS3Config] = None,
        *,
        # Backward compatibility parameters
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        unsigned: Optional[bool] = None,
        region_name: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        file_format: Optional[str] = None,
        compression: Optional[str] = None,
        datatype: Optional[str] = None,
        replay_speed: Optional[float] = None,
        key_setter: Optional[Callable[[dict], object]] = None,
        timestamp_setter: Optional[Callable[[dict], Optional[int]]] = None,
        has_partition_folders: Optional[bool] = None,
        checksum_mode: Optional[str] = None,
        extract_metadata: Optional[bool] = None,
        # dataloader (templated_prefixes) options
        access_mode: Optional[str] = None,
        prefix_template: Optional[str] = None,
        root: Optional[str] = None,
        market: Optional[str] = None,
        segments: Optional[list[str]] = None,
        datatypes_list: Optional[list[str]] = None,
        symbols: Optional[list[str]] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        interval: Optional[str] = None,
        dry_run: Optional[bool] = None,
        name: Optional[str] = None,
        shutdown_timeout: Optional[float] = None,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        # Handle configuration - support both new config object and old parameters
        if config is None:
            # Backward compatibility: create config from individual parameters
            if bucket is not None or prefix is not None:
                warnings.warn(
                    "Using individual parameters is deprecated. Use BinanceS3Config instead.",
                    DeprecationWarning,
                    stacklevel=2
                )
                auth_provider = None
                if aws_access_key_id or aws_secret_access_key:
                    auth_provider = AWSAuth(
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name=region_name,
                        endpoint_url=endpoint_url
                    )
                
                config_kwargs = dict(
                    bucket=bucket or "",
                    prefix=prefix or "",
                    unsigned=unsigned if unsigned is not None else False,
                    file_format=file_format or "infer",
                    compression=compression if compression is not None else "infer",
                    datatype=datatype or "trades",
                    replay_speed=replay_speed if replay_speed is not None else 0.0,
                    has_partition_folders=has_partition_folders if has_partition_folders is not None else False,
                    checksum_mode=(checksum_mode or "skip"),
                    extract_metadata=extract_metadata if extract_metadata is not None else True,
                    access_mode=access_mode or "direct_prefix",
                    prefix_template=prefix_template,
                    root=root,
                    market=market,
                    segments=segments or ["daily"],
                    datatypes_list=datatypes_list or [datatype or "trades"],
                    symbols=symbols or [],
                    date_from=date_from,
                    date_to=date_to,
                    interval=interval,
                    dry_run=dry_run if dry_run is not None else False,
                    name=name,
                    shutdown_timeout=shutdown_timeout if shutdown_timeout is not None else 30.0,
                    validate_prefix_template=False,
                )
                if auth_provider is not None:
                    config_kwargs["auth_provider"] = auth_provider

                config = BinanceS3Config(**config_kwargs)
            else:
                # Try to load from environment
                try:
                    config = load_from_env(BinanceS3Config)
                except Exception:
                    raise ValueError("Either config or bucket/prefix parameters must be provided")
        
        # Import guard with better error handling
        if not _BOTO3_AVAILABLE or boto3 is None:
            raise missing_dependency_error("boto3", "BinanceS3Source", "aws")

        self._config = config
        
        # Name defaults to our desired topic convention so Source.default_topic() aligns with docs
        default_name = TopicBuilder("binance.s3", datatype=config.datatype)
        super().__init__(
            name=config.name or default_name,
            shutdown_timeout=config.shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        
        self._key_setter = key_setter or default_key_fn
        self._timestamp_setter = timestamp_setter or default_ts_fn
        self._s3 = None
        self._dry_run = getattr(self._config, "dry_run", False)
        self._replay_speed = getattr(self._config, "replay_speed", 0.0)
        self._extract_metadata = getattr(self._config, "extract_metadata", True)
        self._checksum_mode = getattr(self._config, "checksum_mode", "skip").lower()

    def setup(self):
        # Configuration is already validated in __post_init__, but check again for safety
        if self._config.access_mode == "templated_prefixes" and not self._config.prefix_template:
            raise ValueError("'prefix_template' is required when access_mode='templated_prefixes'")
        
        cfg = None
        if self._config.unsigned:
            # Use unsigned requests for public archives
            cfg = BotoConfig(signature_version=botocore.UNSIGNED)  # type: ignore[attr-defined]
        
        # Get credentials from auth provider
        credentials = self._config.auth_provider.get_credentials()
        s3_credentials = {k: v for k, v in credentials.items() if v and k in [
            "aws_access_key_id", "aws_secret_access_key", "region_name", "endpoint_url"
        ]}
        
        try:
            self._s3 = boto3.client("s3", config=cfg, **s3_credentials)
            # validate access by listing a single key under prefix
            # In templated mode, we cannot validate every prefix cheaply; validate root prefix only
            self._s3.list_objects_v2(Bucket=self._config.bucket, Prefix=self._config.prefix, MaxKeys=1)
        except Exception as e:
            # Include bucket information in error message for better debugging
            error_msg = f"Connection failed for BinanceS3Source"
            if "credentials" in str(e).lower():
                error_msg += f": Unable to locate credentials"
            elif "nosuchbucket" in str(e).lower() or "not exist" in str(e).lower():
                error_msg += f": Bucket '{self._config.bucket}' does not exist or is not accessible"
            else:
                error_msg += f": {str(e)}"
            
            from .errors import CryptoSourceConnectionError
            raise CryptoSourceConnectionError(
                error_msg,
                source="BinanceS3Source",
                context={"bucket": self._config.bucket, "prefix": self._config.prefix, "original_error": str(e)},
                retryable=False
            )

    def _iter_prefixes(self) -> Iterable[str]:
        if self._config.access_mode == "direct_prefix" or not self._config.prefix_template:
            yield self._config.prefix
            return
        # templated dataloader expansion
        from datetime import datetime, timedelta
        dates: list[str] = []
        if self._config.date_from and self._config.date_to:
            start = datetime.strptime(self._config.date_from, "%Y-%m-%d")
            end = datetime.strptime(self._config.date_to, "%Y-%m-%d")
            cur = start
            while cur <= end:
                dates.append(cur.strftime("%Y-%m-%d"))
                cur += timedelta(days=1)
        else:
            dates = []
        for seg in self._config.segments:
            for dt in self._config.datatypes_list:
                for sym in (self._config.symbols or [""]):
                    if seg == "daily" and dates:
                        for d in dates:
                            yield self._config.prefix_template.format(
                                root=self._config.root or "",
                                market=self._config.market or "",
                                segment=seg,
                                datatype=dt,
                                symbol=sym,
                                date=d,
                                interval=self._config.interval or "",
                            )
                    else:
                        # monthly or unspecified date
                        yield self._config.prefix_template.format(
                            root=self._config.root or "",
                            market=self._config.market or "",
                            segment=seg,
                            datatype=dt,
                            symbol=sym,
                            date="",
                            interval=self._config.interval or "",
                        )

    def _iter_object_keys(self) -> Iterable[str]:
        for pref in self._iter_prefixes():
            # normalize double slashes but do not force a trailing slash
            # This allows using file-stem prefixes like "...-YYYY-MM-DD" without extensions.
            npref = pref.replace("//", "/")
            token = None
            while True:
                kwargs = {"Bucket": self._config.bucket, "Prefix": npref, "MaxKeys": 1000}
                if token:
                    kwargs["ContinuationToken"] = token
                resp = self._s3.list_objects_v2(**kwargs)
                for obj in resp.get("Contents", []) or []:
                    key = obj.get("Key")
                    if not key:
                        continue
                    # Skip checksum sidecar files
                    if key.endswith(".CHECKSUM"):
                        continue
                    yield key
                if resp.get("IsTruncated"):
                    token = resp.get("NextContinuationToken")
                else:
                    break

    def _read_object(self, key: str) -> bytes:
        # retry with simple backoff sequence injected for tests
        from typing import cast
        import hashlib
        seq = cast(list[float], globals().get("_backoff_sequence") or [0.5, 1.0, 2.0])
        # Step 1: fetch body with retry/backoff
        body = None
        for i, backoff in enumerate(seq + [None]):
            try:
                obj = self._s3.get_object(Bucket=self._config.bucket, Key=key)
                body = obj["Body"].read()
                break
            except Exception as e:  # noqa: BLE001
                if backoff is None:
                    raise
                logger.warning(f"get_object failed for {key}, retrying in {backoff}s: {e}")
                global _time
                import time as _t
                (_time or _t).sleep(backoff)
        if body is None:
            raise RuntimeError("unreachable")
        # Step 2: checksum verification without backoff looping (deterministic)
        if self._checksum_mode != "skip":
            try:
                chk_obj = self._s3.get_object(Bucket=self._config.bucket, Key=f"{key}.CHECKSUM")
                expected = chk_obj["Body"].read().decode().strip().split()[0]
                actual = hashlib.md5(body).hexdigest()
                if expected != actual:
                    msg = f"Checksum mismatch for {key}: expected {expected}, got {actual}"
                    if self._checksum_mode == "strict":
                        # do not backoff/retry on deterministic mismatch
                        raise ValueError(msg)
                    else:
                        logger.warning(msg)
            except Exception as e:
                # Missing checksum or errors: only raise in strict mode
                if self._checksum_mode == "strict":
                    raise
        return body

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
            inner_key = key[:-3]
        elif key.endswith(".zip"):
            with _io.BytesIO(body) as bio, _zip.ZipFile(bio) as zf:
                # take first file
                names = zf.namelist()
                if not names:
                    return
                first = names[0]
                raw = zf.read(first)
                inner_key = first
                # handle nested gzip inside zip
                if inner_key.endswith(".gz"):
                    try:
                        raw = _gzip.decompress(raw)
                        inner_key = inner_key[:-3]
                    except Exception:
                        logger.warning("Failed to decompress nested gz in %s", key)
        else:
            inner_key = key

        def _symbol_from_path(k: str) -> str:
            parts = k.split("/")
            # try to locate after 'trades' or 'klines'
            for tag in ("trades", "klines"):
                if tag in parts:
                    idx = parts.index(tag)
                    if idx + 1 < len(parts):
                        return parts[idx + 1]
            return "unknown"

        # CSV parsing: handle klines and trades CSV
        if inner_key.endswith(".csv"):
            text = raw.decode(errors="ignore")
            lines = text.splitlines()
            header = (lines[0].strip().lower() if lines else "")
            is_klines_header = ("open time" in header) or ("open_time" in header)

            # Case 1: Klines with header row
            if is_klines_header:
                reader = _csv.DictReader(lines, delimiter=",")
                for row in reader:
                    try:
                        ev = {
                            "exchange": "binance",
                            "symbol": row.get("symbol") or _symbol_from_path(key),
                            "open_time": int(row.get("open_time") or row.get("Open time") or 0),
                            "open": float(row.get("open") or row.get("Open") or 0.0),
                            "high": float(row.get("high") or row.get("High") or 0.0),
                            "low": float(row.get("low") or row.get("Low") or 0.0),
                            "close": float(row.get("close") or row.get("Close") or 0.0),
                            "volume": float(row.get("volume") or row.get("Volume") or 0.0),
                            "close_time": int(row.get("close_time") or row.get("Close time") or 0),
                        }
                        yield ev
                    except Exception:
                        logger.warning("Skipping malformed kline csv row in %s", key)
                        continue
                return

            # Case 2: Headerless klines CSV (detect by path)
            if ("/klines/" in key) or ("/klines/" in inner_key):
                raw_rows = _csv.reader(lines)
                for row in raw_rows:
                    try:
                        # Expect at least [open_time, open, high, low, close, volume, close_time]
                        if len(row) < 7:
                            continue
                        ev = {
                            "exchange": "binance",
                            "symbol": _symbol_from_path(key),
                            "open_time": int(row[0]),
                            "open": float(row[1]),
                            "high": float(row[2]),
                            "low": float(row[3]),
                            "close": float(row[4]),
                            "volume": float(row[5]),
                            "close_time": int(row[6]),
                        }
                        yield ev
                    except Exception:
                        # Skip silently to avoid log spam for large files
                        continue
                return

            # Case 3: Trades CSV with header (price,qty,time present)
            reader = _csv.DictReader(lines, delimiter=",")
            if reader.fieldnames and all(h in (reader.fieldnames or []) for h in ("price", "qty", "time")):
                for row in reader:
                    try:
                        ev = {
                            "exchange": "binance",
                            "symbol": row.get("symbol") or _symbol_from_path(key),
                            "price": float(row.get("price") or 0.0),
                            "qty": float(row.get("qty") or 0.0),
                            "ts_event": int(row.get("time") or 0),
                        }
                        yield ev
                    except Exception:
                        logger.warning("Skipping malformed trade csv row in %s", key)
                        continue
                return

            # Case 4: Headerless trades CSV (id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch)
            raw_rows = _csv.reader(lines)
            for row in raw_rows:
                try:
                    if len(row) < 5:
                        continue
                    ev = {
                        "exchange": "binance",
                        "symbol": _symbol_from_path(key),
                        "price": float(row[1]),
                        "qty": float(row[2]),
                        "ts_event": int(row[4]),
                    }
                    yield ev
                except Exception:
                    # Skip silently to avoid log spam
                    continue
            return

        # Default: JSONL (direct .jsonl or uncompressed)
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
        import os as _os
        # expose alias on module for tests
        global _time
        if _time is None:
            _time = _t
        keys = sorted(list(self._iter_object_keys()))
        debug = _os.environ.get("BINANCE_S3_DEBUG")
        if debug:
            logger.info("BinanceS3Source: will process %d keys", len(keys))
        prev_ts = None
        total_records = 0
        for key in keys:
            if self._dry_run:
                if debug:
                    logger.info("[dry-run] key=%s", key)
                continue
            body = self._read_object(key)
            per_key = 0
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
                per_key += 1
                total_records += 1
            if debug:
                logger.info("Parsed %d records from %s", per_key, key)
        if debug:
            logger.info("BinanceS3Source: total produced records=%d", total_records)
        self.producer.flush()
