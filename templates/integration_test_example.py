#!/usr/bin/env python3
"""
Example Integration Test for Crypto Lakehouse Templates

This script demonstrates how to test crypto source → Iceberg sink integration
following the no-mocks policy with real services.
"""

import yaml
from quixstreams import Application
from quixstreams.sources.community.crypto import CryptofeedSource, create_cryptofeed_config
from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_config

def test_real_time_trading_integration():
    """Test real-time trading template with paper trading mode."""
    
    # Load template
    with open('templates/real-time-trading.yaml') as f:
        template = yaml.safe_load(f)
    
    # Create application
    app = Application(
        broker_address=template['kafka']['bootstrap_servers'],
        app_id=template['application']['app_id']
    )
    
    # Configure crypto source (paper trading mode)
    source_config = create_cryptofeed_config(
        exchanges=template['crypto_source']['config']['exchanges'][:1],  # Test with one exchange
        channels=template['crypto_source']['config']['channels'],
        symbols=template['crypto_source']['config']['symbols'][:2]  # Test with fewer symbols
    )
    
    # Configure Iceberg sink
    sink_config = create_local_config(
        table_name=template['iceberg_sink']['table_name']
    )
    
    # Setup pipeline
    source = CryptofeedSource(source_config)
    sink = IcebergRESTSink(sink_config)
    
    topic = app.topic(template['topics']['crypto_trades']['name'])
    sdf = app.dataframe(topic)
    sdf.sink(sink)
    
    # Run for short duration to test integration
    print("🚀 Testing integration for 30 seconds...")
    app.run(source, duration=30)
    
    print("✅ Integration test completed successfully")

if __name__ == "__main__":
    test_real_time_trading_integration()
