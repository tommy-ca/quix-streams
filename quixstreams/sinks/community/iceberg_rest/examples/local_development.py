"""
Local Development Example: Iceberg REST Sink with Local Stack

This example demonstrates how to use the Iceberg REST sink with the local
development stack (Redpanda + MinIO + Lakekeeper + PostgreSQL).

Example:
    python local_development.py

Requirements:
    - Docker and docker-compose installed
    - Local development stack running (use init_local_stack())

Author: TDD Sprint 3 - GREEN Phase  
Date: September 19, 2025
"""

import asyncio
from quixstreams import Application
from quixstreams.sinks.community.iceberg_rest import (
    create_local_rest_config,
    init_local_stack,
    check_local_stack_health
)


def main():
    """
    Example: Local development with REST catalog and MinIO storage.
    
    This example shows how to:
    1. Start the local development stack
    2. Configure the REST Iceberg sink for local development
    3. Process crypto data and write to Iceberg tables
    """
    
    print("🚀 Starting Local Development Example")
    
    # 1. Initialize local development stack
    print("📦 Initializing local development stack...")
    if not init_local_stack():
        print("❌ Failed to start local stack")
        return
    
    # 2. Check stack health
    health = check_local_stack_health()
    print(f"📊 Stack health: {health}")
    
    if not all(health.values()):
        print("⚠️ Some services are unhealthy - continuing anyway")
    
    # 3. Create local REST configuration
    config = create_local_rest_config(
        table_name="crypto_trades",
        warehouse_id="local-warehouse"
    )
    print(f"⚙️ Configuration: {config}")
    
    # 4. Initialize QuixStreams application
    app = Application(
        broker_address="localhost:19092",  # Local Redpanda
        consumer_group="crypto-iceberg-local"
    )
    
    # 5. Create input topic
    topic = app.topic("crypto-trades", value_deserializer="json")
    
    # 6. Process and sink data
    sdf = app.dataframe(topic)
    
    # Transform data for Iceberg
    sdf = sdf.apply(lambda value: {
        "symbol": value["symbol"],
        "price": float(value["price"]),
        "volume": float(value["volume"]),
        "timestamp": value["timestamp"],
        "exchange": value.get("exchange", "unknown")
    })
    
    # Note: Actual sink will be implemented in VALIDATE-001-GREEN phase
    # For now, just print the configuration
    print("🏗️ Sink configuration ready - implementation pending in VALIDATE phase")
    print(f"📝 Would write to table: {config.table_name}")
    print(f"🗂️ Catalog URI: {config.catalog_uri}")
    print(f"💾 Storage: {config.s3_endpoint_url}")
    
    # Placeholder for actual sink usage
    # sdf = sdf.to_sink(IcebergRESTSink(config))
    
    print("✅ Local development example configured successfully!")
    print("🚧 Run this again after VALIDATE-001-GREEN implementation")


if __name__ == "__main__":
    main()