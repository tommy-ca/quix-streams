#!/usr/bin/env python3
from quixstreams import Application
from quixstreams.sources.community.crypto import BinanceS3Source

def main():
    app = Application(broker_address="localhost:19092")

    source = BinanceS3Source(
        bucket="your-bucket",
        prefix="p/",
        access_mode="templated_prefixes",
        prefix_template="p/{market}/{segment}/{datatype}/{symbol}/{date}/",
        market="spot",
        segments=["daily"],
        datatypes_list=["trades","klines"],
        symbols=["BTCUSDT","ETHUSDT"],
        date_from="2025-01-01",
        date_to="2025-01-02",
        checksum_mode="warn",
    )

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run()

if __name__ == "__main__":
    main()