#!/usr/bin/env python3
import os, sys
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from quixstreams import Application
from quixstreams.sources.community.crypto import BinanceS3Source

def main():
    app = Application(broker_address="localhost:19092")

    source = BinanceS3Source(
        bucket="data.binance.vision",
        prefix="data/spot/daily/trades/ETHUSDT/ETHUSDT-trades-2017-08-17",
        unsigned=True,
        access_mode="direct_prefix",
        datatype="trades",
        checksum_mode="skip",
    )

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)
    sdf = sdf.apply(lambda value: value)

    collected = app.run(timeout=15, collect=True, metadata=True)
    print(f"produced records: {len(collected)}")

if __name__ == "__main__":
    main()