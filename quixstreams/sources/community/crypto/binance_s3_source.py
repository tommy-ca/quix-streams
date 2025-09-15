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

        self._s3 = None

    def setup(self):
        cfg = None
        if self._unsigned:
            # Use unsigned requests for public archives
            cfg = BotoConfig(signature_version=botocore.UNSIGNED)  # type: ignore[attr-defined]
        self._s3 = boto3.client("s3", config=cfg, **{k: v for k, v in self._credentials.items() if v})
        # validate access by listing a single key under prefix
        self._s3.list_objects_v2(Bucket=self._bucket, Prefix=self._prefix, MaxKeys=1)

    # Placeholder run to be implemented in follow-up TDD steps
    def run(self):
        logger.info("BinanceS3Source run() not yet implemented; exiting.")
