# pipelines/iceberg/sdf_klines1m_to_iceberg.py
import os
from quixstreams import Application
import json
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
KLINES_1M_TOPIC = os.getenv("KLINES_1M_TOPIC", "crypto.binance.spot.klines.1m")

CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "local")
CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "sql")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "/home/tommyk/data/iceberg/warehouse")
CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", f"sqlite:////{os.path.expanduser('~')}/data/iceberg/catalog.db")

ICEBERG_CATALOG = {"name": CATALOG_NAME, "type": CATALOG_TYPE, "warehouse": WAREHOUSE, "uri": CATALOG_URI}

KLINES_TABLE = os.getenv("ICEBERG_KLINES_TABLE", "local.crypto.binance.klines_spot_1m")


def main():
    app = Application(broker_address=KAFKA_BOOTSTRAP)

    topic = app.topic(KLINES_1M_TOPIC)

    def _project(record):
        try:
            if isinstance(record, (bytes, bytearray)):
                m = json.loads(record.decode("utf-8"))
            elif isinstance(record, str):
                m = json.loads(record)
            elif isinstance(record, dict):
                m = record
            else:
                return None
            def to_ts_ms(v):
                return datetime.fromtimestamp(float(v)/1000.0, tz=timezone.utc) if v is not None else None
            return {
                "exchange": m.get("exchange"),
                "market": m.get("market") or "spot",
                "symbol": m.get("symbol"),
                "interval": m.get("interval") or "1m",
                "open_time": to_ts_ms(m.get("open_time")),
                "open": float(m.get("open")) if m.get("open") is not None else None,
                "high": float(m.get("high")) if m.get("high") is not None else None,
                "low": float(m.get("low")) if m.get("low") is not None else None,
                "close": float(m.get("close")) if m.get("close") is not None else None,
                "volume": float(m.get("volume")) if m.get("volume") is not None else None,
                "close_time": to_ts_ms(m.get("close_time")),
            }
        except Exception:
            return None

    sdf = (
        app.dataframe(topic=topic)
           .apply(_project)
           .filter(lambda r: r is not None)
    )

    from quixstreams.connectors.sinks import IcebergSink
    iceberg_sink = IcebergSink(
        catalog=ICEBERG_CATALOG,
        table=KLINES_TABLE,
    )

    sdf.sink(iceberg_sink)
    app.run(timeout=60)


if __name__ == "__main__":
    main()
