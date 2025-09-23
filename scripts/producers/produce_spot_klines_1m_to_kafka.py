#!/usr/bin/env python3
import os, sys

# Ensure repo root is importable
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from quixstreams import Application
from quixstreams.sources.community.crypto import BinanceS3Source

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = os.getenv("TOPIC", "crypto.binance.spot.klines.1m")
TIMEOUT_SECS = float(os.getenv("TIMEOUT_SECS", "15"))
GROUP = os.getenv("GROUP", "producer-klines-1m-1")

# Defaults target BTCUSDT 1m klines for a single day
BUCKET = os.getenv("BINANCE_S3_BUCKET", "data.binance.vision")
PREFIX = os.getenv(
    "BINANCE_S3_PREFIX",
    "data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2017-08-17",
)


def main():
    app = Application(
        broker_address=KAFKA_BOOTSTRAP,
        consumer_group=GROUP,
        auto_offset_reset="earliest",
    )

    source = BinanceS3Source(
        bucket=BUCKET,
        prefix=PREFIX,
        unsigned=True,
        access_mode="direct_prefix",
        datatype="klines",
        checksum_mode="skip",
    )

    topic = app.topic(TOPIC)

    sdf = app.dataframe(source=source)
    sdf.to_topic(topic)

    app.run(timeout=TIMEOUT_SECS)


if __name__ == "__main__":
    main()
