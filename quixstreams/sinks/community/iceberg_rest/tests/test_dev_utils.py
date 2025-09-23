#!/usr/bin/env python3
"""
Tests for development utilities and local development stack improvements.

Following TDD methodology - tests written first, then implementation.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, patch
import yaml


class TestDockerComposeHealthChecks:
    """Test Docker Compose health check enhancements."""
    
    def test_health_check_config_generation(self):
        """Test generation of Docker Compose with enhanced health checks."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.dev_utils import generate_docker_compose
        
        compose_config = generate_docker_compose()
        
        # Verify health checks are present for all services
        assert 'services' in compose_config
        assert 'iceberg-rest' in compose_config['services']
        assert 'minio' in compose_config['services']
        
        # Check health check configuration
        iceberg_service = compose_config['services']['iceberg-rest']
        assert 'healthcheck' in iceberg_service
        assert 'test' in iceberg_service['healthcheck']
        assert 'interval' in iceberg_service['healthcheck']
        assert 'timeout' in iceberg_service['healthcheck']
        assert 'retries' in iceberg_service['healthcheck']
        
    def test_init_scripts_generation(self):
        """Test generation of initialization scripts for services."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.dev_utils import generate_init_scripts
        
        scripts = generate_init_scripts()
        
        # Verify init scripts for each service
        assert 'minio-init.sh' in scripts
        assert 'iceberg-init.sh' in scripts
        
        # Check script content
        minio_script = scripts['minio-init.sh']
        assert 'mc alias set' in minio_script
        assert 'mc mb' in minio_script  # Create bucket
        
        iceberg_script = scripts['iceberg-init.sh']
        assert 'wait-for-it' in iceberg_script or 'curl' in iceberg_script


class TestVerboseDebugging:
    """Test verbose debugging flags and log filters."""
    
    def test_debug_config_creation(self):
        """Test creation of debug configuration."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.dev_utils import create_debug_config
        
        debug_config = create_debug_config(
            enable_verbose=True,
            log_level='DEBUG',
            enable_sql_logging=True
        )
        
        assert debug_config['log_level'] == 'DEBUG'
        assert debug_config['verbose_http'] is True
        assert debug_config['sql_logging'] is True
        assert 'log_filters' in debug_config
        
    def test_log_filter_configuration(self):
        """Test log filter configuration for debugging."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.dev_utils import configure_log_filters
        
        filters = configure_log_filters(
            include_patterns=['iceberg.*', 'pyarrow.*'],
            exclude_patterns=['urllib3.*'],
            min_level='DEBUG'
        )
        
        assert len(filters['include']) == 2
        assert len(filters['exclude']) == 1
        assert filters['min_level'] == 'DEBUG'


class TestSampleDataGeneration:
    """Test sample dataset generation for multiple domains."""
    
    def test_trading_sample_data_generation(self):
        """Test generation of trading domain sample data."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.dev_utils import generate_sample_data
        
        trading_data = generate_sample_data(
            domain='trading',
            record_count=1000,
            schema_preset='crypto_trades'
        )
        
        assert len(trading_data) == 1000
        assert all('exchange' in record for record in trading_data)
        assert all('symbol' in record for record in trading_data)
        assert all('price' in record for record in trading_data)
        assert all('quantity' in record for record in trading_data)
        
    def test_iot_sample_data_generation(self):
        """Test generation of IoT domain sample data."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.dev_utils import generate_sample_data
        
        iot_data = generate_sample_data(
            domain='iot',
            record_count=500,
            schema_preset='sensor_readings'
        )
        
        assert len(iot_data) == 500
        assert all('device_id' in record for record in iot_data)
        assert all('timestamp' in record for record in iot_data)
        assert all('sensor_type' in record for record in iot_data)
        assert all('value' in record for record in iot_data)
        
    def test_logs_sample_data_generation(self):
        """Test generation of logs domain sample data."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.dev_utils import generate_sample_data
        
        logs_data = generate_sample_data(
            domain='logs',
            record_count=750,
            schema_preset='application_logs'
        )
        
        assert len(logs_data) == 750
        assert all('timestamp' in record for record in logs_data)
        assert all('level' in record for record in logs_data)
        assert all('message' in record for record in logs_data)
        assert all('service' in record for record in logs_data)


class TestDevStackOrchestration:
    """Test complete development stack orchestration."""
    
    def test_full_stack_setup(self):
        """Test complete development stack setup."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.dev_utils import setup_dev_stack
        
        with tempfile.TemporaryDirectory() as temp_dir:
            stack_path = setup_dev_stack(
                output_dir=temp_dir,
                include_sample_data=True,
                enable_debugging=True
            )
            
            # Verify generated files
            assert (Path(stack_path) / 'docker-compose.yml').exists()
            assert (Path(stack_path) / 'scripts' / 'minio-init.sh').exists()
            assert (Path(stack_path) / 'scripts' / 'iceberg-init.sh').exists()
            assert (Path(stack_path) / 'sample-data').exists()
            assert (Path(stack_path) / '.env').exists()
            
    def test_stack_validation(self):
        """Test development stack validation."""
        # RED: This test will fail initially
        from quixstreams.sinks.community.iceberg_rest.dev_utils import validate_dev_stack
        
        validation_result = validate_dev_stack(
            check_docker=True,
            check_ports=[8181, 9000, 9001],
            check_health=True
        )
        
        assert 'docker_available' in validation_result
        assert 'ports_available' in validation_result
        assert 'services_healthy' in validation_result
        assert validation_result['overall_status'] in ['ready', 'not_ready']


if __name__ == '__main__':
    pytest.main([__file__])