# pipelines/iceberg/sdf_trades_to_iceberg.py
import os
from quixstreams import Application
import json
from datetime import datetime, timezone

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TRADES_TOPIC = os.getenv("TRADES_TOPIC", "crypto.binance.spot.trades")

CATALOG_NAME = os.getenv("ICEBERG_CATALOG_NAME", "local")
CATALOG_TYPE = os.getenv("ICEBERG_CATALOG_TYPE", "sql")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "/home/tommyk/data/iceberg/warehouse")
CATALOG_URI = os.getenv("ICEBERG_CATALOG_URI", f"sqlite:////{os.path.expanduser('~')}/data/iceberg/catalog.db")

ICEBERG_CATALOG = {"name": CATALOG_NAME, "type": CATALOG_TYPE, "warehouse": WAREHOUSE, "uri": CATALOG_URI}

TRADES_TABLE = os.getenv("ICEBERG_TRADES_TABLE", "local.crypto.binance.trades_spot")


def main():
    app = Application(broker_address=KAFKA_BOOTSTRAP)

    topic = app.topic(TRADES_TOPIC)

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
                "ts_event": to_ts_ms(m.get("ts_event")),
                "price": float(m.get("price")) if m.get("price") is not None else None,
                "qty": float(m.get("qty")) if m.get("qty") is not None else None,
                "trade_id": int(m.get("trade_id")) if m.get("trade_id") is not None else None,
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
        table=TRADES_TABLE,
    )

    sdf.sink(iceberg_sink)
    app.run(timeout=60)


if __name__ == "__main__":
    main()
