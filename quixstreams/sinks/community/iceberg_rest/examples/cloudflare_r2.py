"""
Cloudflare R2 Production Example: Iceberg REST Sink

This example demonstrates production deployment with Cloudflare R2 storage
and a managed REST catalog service.

Example:
    python cloudflare_r2.py

Requirements:
    - Cloudflare R2 credentials
    - REST catalog service (Tabular, Lakekeeper, etc.)

Author: TDD Sprint 3 - GREEN Phase
Date: September 19, 2025  
"""

from quixstreams import Application
from quixstreams.sinks.community.iceberg_rest import create_r2_config


def main():
    """
    Example: Production deployment with Cloudflare R2 and REST catalog.
    
    This example demonstrates:
    1. Configuring Cloudflare R2 storage
    2. Connecting to a managed REST catalog
    3. High-volume crypto data processing
    """
    
    print("🌍 Starting Cloudflare R2 Production Example")
    
    # 1. Create R2 configuration
    config = create_r2_config(
        account_id="your-cloudflare-account-id",
        access_key_id="your-r2-token", 
        secret_access_key="your-r2-secret",
        catalog_uri="https://api.tabular.io/ws/your-workspace/v1",
        catalog_token="your-catalog-token",
        table_name="crypto_trades_prod",
        warehouse_id="production"
    )
    print(f"⚙️ R2 Configuration ready: {config.s3_endpoint_url}")
    
    # 2. Initialize production application
    app = Application(
        broker_address="your-kafka-cluster:9092",
        consumer_group="crypto-iceberg-production"  
    )
    
    topic = app.topic("crypto-trades", value_deserializer="json")
    sdf = app.dataframe(topic)
    
    # 3. Production data transformation
    sdf = sdf.apply(lambda value: {
        "symbol": value["symbol"],
        "price": float(value["price"]), 
        "volume": float(value["volume"]),
        "timestamp": value["timestamp"],
        "exchange": value["exchange"],
        "trade_id": value["trade_id"]
    })
    
    print("🚧 Production sink ready - implementation pending in VALIDATE phase")
    print(f"📊 Target table: {config.table_name}")
    print(f"☁️ Storage: Cloudflare R2")
    
    # Placeholder: sdf = sdf.to_sink(IcebergRESTSink(config))


if __name__ == "__main__":
    main()