#!/usr/bin/env python3
"""Generate a manifest of S3 object keys for given dataloader configuration."""
import json
import os
from typing import Any

try:
    import boto3  # type: ignore
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

    s3 = boto3.client("s3")
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