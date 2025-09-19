# Iceberg REST Sink

A REST-enabled Apache Iceberg sink for QuixStreams that supports multiple S3-compatible storage providers and REST catalog backends.

## Installation

```bash
pip install quixstreams[iceberg_rest,crypto]
```

## Quick Start

### Local Development

```python
from quixstreams.sinks.community.iceberg_rest import (
    create_local_rest_config, 
    init_local_stack
)

# Start local development stack
init_local_stack()

# Create configuration
config = create_local_rest_config(table_name="my_table")

# Use with QuixStreams (implementation pending)
# sdf.to_sink(IcebergRESTSink(config))
```

### Production with Cloudflare R2

```python
from quixstreams.sinks.community.iceberg_rest import create_r2_config

config = create_r2_config(
    account_id="your-account",
    access_key_id="your-token",
    secret_access_key="your-secret",
    catalog_uri="https://catalog.yourdomain.com/api/v1"
)
```

## Configuration

The REST sink supports multiple storage providers:

- **Local Development**: MinIO + Lakekeeper + PostgreSQL
- **Cloudflare R2**: Cost-effective object storage with REST catalog
- **AWS S3**: Traditional S3 storage with REST catalog (not Glue)
- **Generic S3**: Any S3-compatible storage provider

## Examples

See the `examples/` directory for complete usage examples:

- `local_development.py` - Local development with Docker stack
- `cloudflare_r2.py` - Production deployment with R2
- `aws_s3_rest.py` - AWS S3 with REST catalog

## API Reference

### Configuration Helpers

- `create_local_rest_config()` - Local development configuration
- `create_r2_config()` - Cloudflare R2 configuration  
- `create_s3_rest_config()` - AWS S3 with REST catalog
- `validate_rest_config()` - Configuration validation

### Local Stack Management

- `init_local_stack()` - Initialize local development environment
- `start_local_stack()` - Start Docker services
- `stop_local_stack()` - Stop Docker services
- `check_local_stack_health()` - Health monitoring

### Main Classes

- `IcebergRESTSink` - Main sink implementation *(implementation pending)*
- `RESTIcebergConfig` - Configuration data class

## Status

🚧 **Implementation Status**: 
- ✅ Configuration helpers and validation
- ✅ Local development stack management  
- ✅ Example applications and documentation
- 🔄 **Main sink implementation** (pending VALIDATE-001-GREEN phase)

## Development

This module was developed using Test-Driven Development (TDD) methodology during Sprint 3.

**Author**: TDD Sprint 3 - GREEN Phase  
**Date**: September 19, 2025