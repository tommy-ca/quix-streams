#!/usr/bin/env python3
"""
Crypto Lakehouse Configuration Helpers

Simple helpers for loading crypto lakehouse configuration from environment
variables and validating complete pipeline configurations.

Usage:
    from examples.crypto_lakehouse.config_helpers import (
        load_crypto_source_config,
        load_iceberg_sink_config,
        validate_pipeline_config,
        create_local_dev_config
    )
"""

import os
import yaml
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""
    crypto_source: Dict[str, Any]
    iceberg_sink: Dict[str, Any]
    kafka_config: Dict[str, Any]
    topic_config: Dict[str, Any]

def load_crypto_source_config(source_type: str = "cryptofeed") -> Dict[str, Any]:
    """
    Load crypto source configuration from environment variables.
    
    Args:
        source_type: Type of crypto source (cryptofeed, binance_s3, ccxt)
        
    Returns:
        Dictionary with crypto source configuration
        
    Environment Variables:
        CRYPTO_EXCHANGES: Comma-separated list of exchanges
        CRYPTO_CHANNELS: Comma-separated list of channels
        CRYPTO_SYMBOLS: Comma-separated list of symbols
        BINANCE_API_KEY: Binance API key (if needed)
        BINANCE_API_SECRET: Binance API secret (if needed)
        S3_BUCKET: S3 bucket for historical data
        S3_PREFIX: S3 prefix template
        REPLAY_SPEED: Replay speed for historical data
    """
    
    if source_type == "cryptofeed":
        return {
            "type": "cryptofeed",
            "exchanges": _parse_csv_env("CRYPTO_EXCHANGES", ["binance"]),
            "channels": _parse_csv_env("CRYPTO_CHANNELS", ["trades"]),
            "symbols": _parse_csv_env("CRYPTO_SYMBOLS", ["BTCUSDT"]),
            # Optional authentication
            "auth_provider": _get_auth_provider() if _has_api_credentials() else None
        }
        
    elif source_type == "binance_s3":
        return {
            "type": "binance_s3",
            "bucket": os.getenv("S3_BUCKET", "binance-public-data"),
            "prefix_template": os.getenv("S3_PREFIX", "data/spot/daily/trades/{symbol}/"),
            "data_type": os.getenv("DATA_TYPE", "trades"),
            "symbols": _parse_csv_env("CRYPTO_SYMBOLS", ["BTCUSDT"]),
            "date_from": os.getenv("DATE_FROM", "2024-01-01"),
            "date_to": os.getenv("DATE_TO", "2024-01-31"),
            "replay_speed": float(os.getenv("REPLAY_SPEED", "1.0")),
            "enable_checksum": os.getenv("ENABLE_CHECKSUM", "true").lower() == "true"
        }
        
    elif source_type == "ccxt":
        return {
            "type": "ccxt",
            "exchange": os.getenv("CCXT_EXCHANGE", "binance"),
            "symbols": _parse_csv_env("CRYPTO_SYMBOLS", ["BTC/USDT"]),
            "mode": os.getenv("CCXT_MODE", "rest"),
            "poll_interval": int(os.getenv("POLL_INTERVAL", "5")),
            "auth_provider": _get_auth_provider() if _has_api_credentials() else None
        }
        
    else:
        raise ValueError(f"Unknown source type: {source_type}")

def load_iceberg_sink_config() -> Dict[str, Any]:
    """
    Load Iceberg REST sink configuration from environment variables.
    
    Returns:
        Dictionary with Iceberg sink configuration
        
    Environment Variables:
        ICEBERG_CATALOG_URI: Iceberg REST catalog endpoint
        ICEBERG_TABLE_NAME: Target table name
        STORAGE_ENDPOINT: S3-compatible storage endpoint
        STORAGE_BUCKET: Storage bucket name
        STORAGE_PREFIX: Storage prefix
        AWS_ACCESS_KEY_ID: Storage access key
        AWS_SECRET_ACCESS_KEY: Storage secret key
        AWS_REGION: AWS region (for S3)
    """
    
    return {
        "catalog_uri": os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181"),
        "table_name": os.getenv("ICEBERG_TABLE_NAME", "crypto.lakehouse_data"),
        "storage": {
            "endpoint": os.getenv("STORAGE_ENDPOINT", "http://localhost:9000"),
            "bucket": os.getenv("STORAGE_BUCKET", "lakehouse"),
            "prefix": os.getenv("STORAGE_PREFIX", "crypto/"),
            "credentials": {
                "access_key": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
                "region": os.getenv("AWS_REGION", "us-east-1")
            }
        }
    }

def load_kafka_config() -> Dict[str, Any]:
    """
    Load Kafka configuration from environment variables.
    
    Environment Variables:
        KAFKA_BOOTSTRAP_SERVERS: Kafka cluster address
        KAFKA_AUTO_OFFSET_RESET: Auto offset reset policy
    """
    
    return {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "auto_offset_reset": os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    }

def validate_pipeline_config(config: PipelineConfig) -> List[str]:
    """
    Validate complete pipeline configuration.
    
    Args:
        config: Pipeline configuration to validate
        
    Returns:
        List of validation errors (empty if valid)
    """
    
    errors = []
    
    # Validate crypto source
    if not config.crypto_source.get("type"):
        errors.append("Crypto source missing type")
        
    source_type = config.crypto_source.get("type")
    if source_type == "cryptofeed":
        if not config.crypto_source.get("exchanges"):
            errors.append("Cryptofeed source missing exchanges")
        if not config.crypto_source.get("channels"):
            errors.append("Cryptofeed source missing channels")
    elif source_type == "binance_s3":
        if not config.crypto_source.get("bucket"):
            errors.append("Binance S3 source missing bucket")
        if not config.crypto_source.get("symbols"):
            errors.append("Binance S3 source missing symbols")
    
    # Validate Iceberg sink
    if not config.iceberg_sink.get("catalog_uri"):
        errors.append("Iceberg sink missing catalog_uri")
    if not config.iceberg_sink.get("table_name"):
        errors.append("Iceberg sink missing table_name")
        
    storage = config.iceberg_sink.get("storage", {})
    if not storage.get("bucket"):
        errors.append("Iceberg sink storage missing bucket")
    if not storage.get("endpoint"):
        errors.append("Iceberg sink storage missing endpoint")
        
    # Validate Kafka
    if not config.kafka_config.get("bootstrap_servers"):
        errors.append("Kafka config missing bootstrap_servers")
        
    return errors

def create_local_dev_config(source_type: str = "cryptofeed") -> PipelineConfig:
    """
    Create configuration optimized for local development.
    
    Args:
        source_type: Type of crypto source to configure
        
    Returns:
        Complete pipeline configuration for local development
    """
    
    # Local development defaults
    crypto_source = {
        "type": source_type,
        "exchanges": ["binance"] if source_type == "cryptofeed" else None,
        "channels": ["trades"] if source_type == "cryptofeed" else None,
        "symbols": ["BTCUSDT"],
        # Local development with limited data
        "replay_speed": 10.0 if source_type == "binance_s3" else None,
        "date_from": "2024-01-01" if source_type == "binance_s3" else None,
        "date_to": "2024-01-02" if source_type == "binance_s3" else None,  # Single day
    }
    
    # Remove None values
    crypto_source = {k: v for k, v in crypto_source.items() if v is not None}
    
    iceberg_sink = {
        "catalog_uri": "http://localhost:8181",
        "table_name": f"crypto.dev_{source_type}",
        "storage": {
            "endpoint": "http://localhost:9000",  # Local MinIO
            "bucket": "dev-lakehouse",
            "prefix": "dev/crypto/",
            "credentials": {
                "access_key": "minioadmin",
                "secret_key": "minioadmin"
            }
        }
    }
    
    kafka_config = {
        "bootstrap_servers": "localhost:9092",
        "auto_offset_reset": "latest"
    }
    
    topic_config = {
        "name": f"crypto.dev.{source_type}",
        "partitions": 1,  # Single partition for development
        "replication_factor": 1
    }
    
    return PipelineConfig(
        crypto_source=crypto_source,
        iceberg_sink=iceberg_sink,
        kafka_config=kafka_config,
        topic_config=topic_config
    )

def load_template_config(template_path: str) -> PipelineConfig:
    """
    Load configuration from a YAML template file.
    
    Args:
        template_path: Path to YAML template file
        
    Returns:
        Pipeline configuration loaded from template
    """
    
    with open(template_path) as f:
        template = yaml.safe_load(f)
    
    return PipelineConfig(
        crypto_source=template.get("crypto_source", {}),
        iceberg_sink=template.get("iceberg_sink", {}),
        kafka_config=template.get("kafka", {}),
        topic_config=template.get("topic", {})
    )

def print_config_guide():
    """Print configuration guide with environment variable documentation."""
    
    guide = """
# Crypto Lakehouse Configuration Guide

## Environment Variables

### Crypto Source Configuration
CRYPTO_EXCHANGES=binance,kraken          # Exchanges for cryptofeed
CRYPTO_CHANNELS=trades,ticker            # Data channels
CRYPTO_SYMBOLS=BTCUSDT,ETHUSDT          # Trading symbols

### Binance S3 Historical Data
S3_BUCKET=binance-public-data           # S3 bucket
S3_PREFIX=data/spot/daily/trades/{symbol}/  # Prefix template
DATE_FROM=2024-01-01                    # Start date
DATE_TO=2024-01-31                      # End date
REPLAY_SPEED=1.0                        # Replay speed (1.0=real-time)

### Authentication (Optional)
BINANCE_API_KEY=your_api_key            # Exchange API key
BINANCE_API_SECRET=your_api_secret      # Exchange API secret

### Iceberg Sink Configuration
ICEBERG_CATALOG_URI=http://localhost:8181    # REST catalog endpoint
ICEBERG_TABLE_NAME=crypto.my_table           # Target table name

### Storage Configuration
STORAGE_ENDPOINT=http://localhost:9000       # S3-compatible endpoint
STORAGE_BUCKET=lakehouse                     # Storage bucket
STORAGE_PREFIX=crypto/                       # Storage prefix
AWS_ACCESS_KEY_ID=minioadmin                 # Access key
AWS_SECRET_ACCESS_KEY=minioadmin             # Secret key
AWS_REGION=us-east-1                         # AWS region

### Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092      # Kafka cluster
KAFKA_AUTO_OFFSET_RESET=latest              # Offset reset policy

## Example Usage

```python
from examples.crypto_lakehouse.config_helpers import (
    load_crypto_source_config,
    load_iceberg_sink_config,
    validate_pipeline_config,
    create_local_dev_config
)

# Load from environment variables
source_config = load_crypto_source_config("cryptofeed")
sink_config = load_iceberg_sink_config()

# Create local development configuration
dev_config = create_local_dev_config("cryptofeed")

# Validate configuration
errors = validate_pipeline_config(dev_config)
if errors:
    print("Configuration errors:", errors)
else:
    print("Configuration is valid!")
```
    """
    
    print(guide)

# Helper functions

def _parse_csv_env(env_var: str, default: List[str]) -> List[str]:
    """Parse comma-separated environment variable."""
    value = os.getenv(env_var)
    if value:
        return [item.strip() for item in value.split(",")]
    return default

def _has_api_credentials() -> bool:
    """Check if API credentials are available."""
    return bool(os.getenv("BINANCE_API_KEY") and os.getenv("BINANCE_API_SECRET"))

def _get_auth_provider() -> Dict[str, str]:
    """Get authentication provider configuration."""
    return {
        "type": "api_key",
        "api_key": os.getenv("BINANCE_API_KEY"),
        "api_secret": os.getenv("BINANCE_API_SECRET")
    }

if __name__ == "__main__":
    print_config_guide()