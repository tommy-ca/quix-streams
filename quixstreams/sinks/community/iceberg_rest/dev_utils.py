#!/usr/bin/env python3
"""
Development utilities for Iceberg REST sink local development stack.

This module provides enhanced Docker Compose configurations, debugging tools, 
sample data generation, and development stack orchestration following TDD principles.

Key features:
- Enhanced Docker Compose with health checks
- Sample data generation for multiple domains (trading, IoT, logs)
- Debug configuration and logging utilities
- Complete development stack setup and validation

Example usage:
    >>> from quixstreams.sinks.community.iceberg_rest.dev_utils import setup_dev_stack
    >>> stack_path = setup_dev_stack('/tmp/dev', include_sample_data=True)
    >>> print(f"Development stack created at: {stack_path}")
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


# Constants for development stack configuration
DEFAULT_HEALTH_CHECK_INTERVAL = '30s'
DEFAULT_HEALTH_CHECK_TIMEOUT = '10s'
DEFAULT_HEALTH_CHECK_RETRIES = 3
DEFAULT_HEALTH_CHECK_START_PERIOD = '40s'

# Sample data generation constants
TRADING_EXCHANGES = ['binance', 'kraken', 'coinbase', 'bitfinex']
TRADING_SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'ADAUSDT', 'DOTUSDT', 'LINKUSDT']
IOT_DEVICE_TYPES = ['temperature', 'humidity', 'pressure', 'motion', 'light']
IOT_LOCATIONS = ['warehouse_a', 'warehouse_b', 'office_1', 'office_2', 'factory_floor']
LOG_SERVICES = ['api-gateway', 'user-service', 'payment-service', 'notification-service']
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR']


def generate_docker_compose() -> Dict[str, Any]:
    """
    Generate Docker Compose configuration with enhanced health checks.
    
    Returns:
        Dictionary containing Docker Compose configuration
    """
    
    compose_config = {
        'version': '3.8',
        'services': {
            'zookeeper': {
                'image': 'confluentinc/cp-zookeeper:latest',
                'environment': {
                    'ZOOKEEPER_CLIENT_PORT': '2181',
                    'ZOOKEEPER_TICK_TIME': '2000'
                },
                'ports': ['2181:2181'],
                'healthcheck': {
                    'test': ['CMD', 'nc', '-z', 'localhost', '2181'],
                    'interval': '30s',
                    'timeout': '10s',
                    'retries': 3,
                    'start_period': '40s'
                }
            },
            'kafka': {
                'image': 'confluentinc/cp-kafka:latest',
                'depends_on': ['zookeeper'],
                'ports': ['9092:9092'],
                'environment': {
                    'KAFKA_BROKER_ID': '1',
                    'KAFKA_ZOOKEEPER_CONNECT': 'zookeeper:2181',
                    'KAFKA_ADVERTISED_LISTENERS': 'PLAINTEXT://localhost:9092',
                    'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1',
                    'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'true'
                },
                'healthcheck': {
                    'test': ['CMD', 'kafka-broker-api-versions', '--bootstrap-server', 'localhost:9092'],
                    'interval': '30s',
                    'timeout': '10s',
                    'retries': 3,
                    'start_period': '40s'
                }
            },
            'minio': {
                'image': 'minio/minio:latest',
                'ports': ['9000:9000', '9001:9001'],
                'environment': {
                    'MINIO_ACCESS_KEY': 'minioadmin',
                    'MINIO_SECRET_KEY': 'minioadmin'
                },
                'command': 'server /data --console-address ":9001"',
                'volumes': ['minio_data:/data'],
                'healthcheck': {
                    'test': ['CMD', 'curl', '-f', 'http://localhost:9000/minio/health/live'],
                    'interval': '30s',
                    'timeout': '20s',
                    'retries': 3,
                    'start_period': '40s'
                }
            },
            'iceberg-rest': {
                'image': 'tabulario/iceberg-rest:latest',
                'ports': ['8181:8181'],
                'environment': {
                    'CATALOG_WAREHOUSE': 's3://lakehouse/',
                    'CATALOG_IO__IMPL': 'org.apache.iceberg.aws.s3.S3FileIO',
                    'CATALOG_S3_ENDPOINT': 'http://minio:9000',
                    'CATALOG_S3_ACCESS_KEY_ID': 'minioadmin',
                    'CATALOG_S3_SECRET_ACCESS_KEY': 'minioadmin',
                    'CATALOG_S3_PATH_STYLE_ACCESS': 'true'
                },
                'depends_on': ['minio'],
                'healthcheck': {
                    'test': ['CMD', 'curl', '-f', 'http://localhost:8181/v1/config'],
                    'interval': '30s',
                    'timeout': '10s',
                    'retries': 3,
                    'start_period': '60s'
                }
            }
        },
        'volumes': {
            'minio_data': None
        }
    }
    
    return compose_config


def generate_init_scripts() -> Dict[str, str]:
    """
    Generate initialization scripts for services.
    
    Returns:
        Dictionary mapping script names to script content
    """
    
    scripts = {}
    
    # MinIO initialization script
    scripts['minio-init.sh'] = '''#!/bin/bash
set -e

echo "Waiting for MinIO to be ready..."
until curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; do
  echo "MinIO not ready, waiting..."
  sleep 2
done

echo "Configuring MinIO client..."
mc alias set myminio http://localhost:9000 minioadmin minioadmin

echo "Creating lakehouse bucket..."
mc mb myminio/lakehouse --ignore-existing

echo "Setting bucket policy..."
mc anonymous set public myminio/lakehouse

echo "MinIO initialization complete"
'''

    # Iceberg catalog initialization script
    scripts['iceberg-init.sh'] = '''#!/bin/bash
set -e

echo "Waiting for Iceberg REST catalog to be ready..."
until curl -f http://localhost:8181/v1/config > /dev/null 2>&1; do
  echo "Iceberg catalog not ready, waiting..."
  sleep 2
done

echo "Testing catalog connectivity..."
curl -X GET http://localhost:8181/v1/config

echo "Iceberg catalog initialization complete"
'''

    return scripts


def create_debug_config(enable_verbose: bool = False, 
                       log_level: str = 'INFO',
                       enable_sql_logging: bool = False) -> Dict[str, Any]:
    """
    Create debug configuration for enhanced logging and debugging.
    
    Args:
        enable_verbose: Enable verbose HTTP logging
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        enable_sql_logging: Enable SQL query logging
        
    Returns:
        Debug configuration dictionary
    """
    
    debug_config = {
        'log_level': log_level,
        'verbose_http': enable_verbose,
        'sql_logging': enable_sql_logging,
        'log_filters': {
            'iceberg': log_level,
            'pyarrow': log_level,
            'requests': 'WARNING' if not enable_verbose else 'DEBUG',
            'urllib3': 'WARNING' if not enable_verbose else 'DEBUG'
        },
        'debug_features': {
            'trace_requests': enable_verbose,
            'dump_schemas': log_level == 'DEBUG',
            'profile_performance': log_level == 'DEBUG'
        }
    }
    
    return debug_config


def configure_log_filters(include_patterns: List[str],
                         exclude_patterns: List[str],
                         min_level: str = 'INFO') -> Dict[str, Any]:
    """
    Configure log filters for debugging.
    
    Args:
        include_patterns: Logger name patterns to include
        exclude_patterns: Logger name patterns to exclude
        min_level: Minimum log level
        
    Returns:
        Log filter configuration
    """
    
    filters = {
        'include': include_patterns,
        'exclude': exclude_patterns,
        'min_level': min_level,
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': min_level,
                'formatter': 'detailed'
            },
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': 'iceberg_debug.log',
                'maxBytes': 10485760,  # 10MB
                'backupCount': 5,
                'level': min_level,
                'formatter': 'detailed'
            }
        },
        'formatters': {
            'detailed': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        }
    }
    
    return filters


def generate_sample_data(domain: str, 
                        record_count: int,
                        schema_preset: str) -> List[Dict[str, Any]]:
    """
    Generate sample data for testing different domains.
    
    Args:
        domain: Data domain (trading, iot, logs)
        record_count: Number of records to generate
        schema_preset: Schema preset to use
        
    Returns:
        List of sample records
    """
    
    if domain == 'trading':
        return _generate_trading_data(record_count, schema_preset)
    elif domain == 'iot':
        return _generate_iot_data(record_count, schema_preset)
    elif domain == 'logs':
        return _generate_logs_data(record_count, schema_preset)
    else:
        raise ValueError(f"Unknown domain: {domain}")


def _generate_trading_data(record_count: int, schema_preset: str) -> List[Dict[str, Any]]:
    """Generate trading domain sample data."""
    
    sides = ['buy', 'sell']
    
    data = []
    base_time = int(time.time())
    
    for i in range(record_count):
        record = {
            'exchange': random.choice(TRADING_EXCHANGES),
            'symbol': random.choice(TRADING_SYMBOLS),
            'price': round(random.uniform(100, 50000), 2),
            'quantity': round(random.uniform(0.001, 100), 6),
            'side': random.choice(sides),
            'timestamp': base_time + i,
            'trade_id': str(uuid.uuid4()),
            'channel': 'trades'
        }
        data.append(record)
    
    return data


def _generate_iot_data(record_count: int, schema_preset: str) -> List[Dict[str, Any]]:
    """Generate IoT domain sample data."""
    
    data = []
    base_time = int(time.time())
    
    for i in range(record_count):
        sensor_type = random.choice(IOT_DEVICE_TYPES)
        record = {
            'device_id': f"sensor_{random.randint(1000, 9999)}",
            'timestamp': base_time + i * 60,  # Every minute
            'sensor_type': sensor_type,
            'value': _generate_sensor_value(sensor_type),
            'location': random.choice(IOT_LOCATIONS),
            'battery_level': random.randint(20, 100),
            'status': random.choice(['active', 'idle', 'maintenance'])
        }
        data.append(record)
    
    return data


def _generate_logs_data(record_count: int, schema_preset: str) -> List[Dict[str, Any]]:
    """Generate logs domain sample data."""
    
    messages = [
        'Request processed successfully',
        'User authenticated',
        'Payment processed',
        'Database connection established',
        'Cache hit for key',
        'Rate limit exceeded',
        'Validation failed',
        'Service temporarily unavailable'
    ]
    
    data = []
    base_time = datetime.now()
    
    for i in range(record_count):
        timestamp = base_time + timedelta(seconds=i)
        record = {
            'timestamp': timestamp.isoformat(),
            'level': random.choice(LOG_LEVELS),
            'message': random.choice(messages),
            'service': random.choice(LOG_SERVICES),
            'request_id': str(uuid.uuid4()),
            'user_id': f"user_{random.randint(1000, 9999)}",
            'response_time_ms': random.randint(10, 2000)
        }
        data.append(record)
    
    return data


def _generate_sensor_value(sensor_type: str) -> float:
    """Generate realistic sensor values based on type."""
    
    ranges = {
        'temperature': (15.0, 35.0),
        'humidity': (30.0, 80.0),
        'pressure': (980.0, 1050.0),
        'motion': (0.0, 1.0),
        'light': (0.0, 1000.0)
    }
    
    min_val, max_val = ranges.get(sensor_type, (0.0, 100.0))
    return round(random.uniform(min_val, max_val), 2)


def setup_dev_stack(output_dir: str,
                   include_sample_data: bool = False,
                   enable_debugging: bool = False) -> str:
    """
    Set up complete development stack.
    
    Args:
        output_dir: Directory to create stack files
        include_sample_data: Whether to generate sample data
        enable_debugging: Whether to enable debug configuration
        
    Returns:
        Path to created stack directory
    """
    
    stack_path = Path(output_dir) / 'iceberg-dev-stack'
    stack_path.mkdir(exist_ok=True)
    
    # Generate Docker Compose
    compose_config = generate_docker_compose()
    with open(stack_path / 'docker-compose.yml', 'w') as f:
        yaml.dump(compose_config, f, default_flow_style=False, indent=2)
    
    # Generate init scripts
    scripts_dir = stack_path / 'scripts'
    scripts_dir.mkdir(exist_ok=True)
    
    init_scripts = generate_init_scripts()
    for script_name, script_content in init_scripts.items():
        script_path = scripts_dir / script_name
        with open(script_path, 'w') as f:
            f.write(script_content)
        script_path.chmod(0o755)  # Make executable
    
    # Generate sample data if requested
    if include_sample_data:
        sample_data_dir = stack_path / 'sample-data'
        sample_data_dir.mkdir(exist_ok=True)
        
        # Generate data for each domain
        domains = [
            ('trading', 'crypto_trades'),
            ('iot', 'sensor_readings'),
            ('logs', 'application_logs')
        ]
        
        for domain, preset in domains:
            data = generate_sample_data(domain, 1000, preset)
            with open(sample_data_dir / f'{domain}_sample.json', 'w') as f:
                json.dump(data, f, indent=2)
    
    # Generate environment file
    env_content = '''# Iceberg REST Sink Development Environment

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_AUTO_OFFSET_RESET=latest

# Iceberg Configuration
ICEBERG_CATALOG_URI=http://localhost:8181
ICEBERG_TABLE_NAME=dev.test_table

# MinIO Configuration
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=lakehouse

# Debug Configuration
LOG_LEVEL=INFO
VERBOSE_LOGGING=false
'''
    
    if enable_debugging:
        env_content += '''
# Enhanced Debugging
LOG_LEVEL=DEBUG
VERBOSE_LOGGING=true
TRACE_REQUESTS=true
SQL_LOGGING=true
'''
    
    with open(stack_path / '.env', 'w') as f:
        f.write(env_content)
    
    return str(stack_path)


def validate_dev_stack(check_docker: bool = True,
                      check_ports: List[int] = None,
                      check_health: bool = False) -> Dict[str, Any]:
    """
    Validate development stack readiness.
    
    Args:
        check_docker: Whether to check Docker availability
        check_ports: List of ports to check availability
        check_health: Whether to check service health
        
    Returns:
        Validation results
    """
    
    results = {
        'docker_available': False,
        'ports_available': {},
        'services_healthy': {},
        'overall_status': 'not_ready'
    }
    
    # Check Docker availability
    if check_docker:
        try:
            import subprocess
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            results['docker_available'] = result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            results['docker_available'] = False
    
    # Check port availability
    if check_ports:
        import socket
        for port in check_ports:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    result = s.connect_ex(('localhost', port))
                    results['ports_available'][port] = result != 0  # Available if connection fails
            except Exception:
                results['ports_available'][port] = True  # Assume available on error
    
    # Check service health
    if check_health:
        health_endpoints = {
            'minio': 'http://localhost:9000/minio/health/live',
            'iceberg': 'http://localhost:8181/v1/config'
        }
        
        try:
            import requests
            for service, endpoint in health_endpoints.items():
                try:
                    response = requests.get(endpoint, timeout=5)
                    results['services_healthy'][service] = response.status_code == 200
                except requests.RequestException:
                    results['services_healthy'][service] = False
        except ImportError:
            # requests not available, skip health checks
            pass
    
    # Determine overall status
    docker_ok = not check_docker or results['docker_available']
    ports_ok = not check_ports or all(results['ports_available'].values())
    health_ok = not check_health or all(results['services_healthy'].values())
    
    results['overall_status'] = 'ready' if (docker_ok and ports_ok and health_ok) else 'not_ready'
    
    return results