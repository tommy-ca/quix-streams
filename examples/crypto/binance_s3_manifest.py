#!/usr/bin/env python3
"""Generate a manifest of S3 object keys for given dataloader configuration."""
import json
import os
import sys
from typing import Any

# Ensure local package is importable when running from repo root
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

try:
    import boto3  # type: ignore
    import botocore  # type: ignore
    from botocore.config import Config as BotoConfig  # type: ignore
except Exception as e:
    raise SystemExit("Install boto3 to use this script: pip install boto3")

from quixstreams.sources.community.crypto.utils import expand_dataloader_prefixes


def main() -> None:
    bucket = os.environ.get("BUCKET", "your-bucket")
    root = os.environ.get("ROOT", "p")
    market = os.environ.get("MARKET", "spot")
    segments = os.environ.get("SEGMENTS", "daily").split(",")
    datatypes = os.environ.get("DATATYPES", "trades").split(",")
    symbols = os.environ.get("SYMBOLS", "BTCUSDT").split(",")
    date_from = os.environ.get("DATE_FROM")
    date_to = os.environ.get("DATE_TO")
    interval = os.environ.get("INTERVAL", "")
    prefix_template = os.environ.get(
        "PREFIX_TEMPLATE", f"{root}/{{market}}/{{segment}}/{{datatype}}/{{symbol}}/{{date}}/"
    )

    prefixes = expand_dataloader_prefixes(
        prefix_template,
        market=market,
        segments=segments,
        datatypes=datatypes,
        symbols=symbols,
        date_from=date_from,
        date_to=date_to,
        interval=interval,
        root=root,
    )

    # Configure S3 client: default to unsigned for public archives
    unsigned = os.environ.get("UNSIGNED", "true").lower() in {"1","true","yes","y"}
    region = os.environ.get("REGION_NAME")
    endpoint_url = os.environ.get("ENDPOINT_URL")
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    cfg = BotoConfig(signature_version=botocore.UNSIGNED) if unsigned else None
    client_kwargs = {k:v for k,v in dict(region_name=region, endpoint_url=endpoint_url, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key).items() if v}
    s3 = boto3.client("s3", config=cfg, **client_kwargs)

    manifest: list[dict[str, Any]] = []
    for pref in prefixes:
        token = None
        while True:
            kwargs = {"Bucket": bucket, "Prefix": pref, "MaxKeys": 1000}
            if token:
                kwargs["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []) or []:
                manifest.append({"key": obj["Key"]})
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
            else:
                break

    with open("manifest.json", "w") as f:
        json.dump({"bucket": bucket, "objects": manifest}, f, indent=2)
    print(f"Wrote manifest.json with {len(manifest)} objects")


if __name__ == "__main__":
    main()