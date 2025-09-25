"""
Test-Driven Development tests for local development stack infrastructure.

This module tests the Docker Compose stack setup and configuration
for local development with Redpanda, Lakekeeper, and MinIO.
"""

import pytest
import os
import yaml
from pathlib import Path
from unittest.mock import patch, MagicMock


class TestLocalDevStackFiles:
    """Test that local development stack files exist and are properly configured."""
    
    def test_docker_compose_file_exists(self):
        """Test that docker-compose.yml exists in the correct location."""
        compose_file = Path("/home/tommyk/projects/devops/quix-streams/infra/local-dev-stack/docker-compose.yml")
        assert compose_file.exists(), f"docker-compose.yml should exist at {compose_file}"
    
    def test_docker_compose_file_is_valid_yaml(self):
        """Test that docker-compose.yml is valid YAML."""
        compose_file = Path("/home/tommyk/projects/devops/quix-streams/infra/local-dev-stack/docker-compose.yml")
        
        with open(compose_file, 'r') as f:
            compose_config = yaml.safe_load(f)
        
        assert isinstance(compose_config, dict), "docker-compose.yml should contain a valid YAML dict"
        assert "services" in compose_config, "docker-compose.yml should have a 'services' section"
    
    def test_docker_compose_has_required_services(self):
        """Test that docker-compose.yml includes all required services."""
        compose_file = Path("/home/tommyk/projects/devops/quix-streams/infra/local-dev-stack/docker-compose.yml")
        
        with open(compose_file, 'r') as f:
            compose_config = yaml.safe_load(f)
        
        required_services = ["redpanda", "minio", "lakekeeper", "postgres"]
        services = compose_config.get("services", {})
        
        for service in required_services:
            assert service in services, f"Service '{service}' should be defined in docker-compose.yml"
    
    def test_redpanda_service_configuration(self):
        """Test that Redpanda service is properly configured."""
        compose_file = Path("/home/tommyk/projects/devops/quix-streams/infra/local-dev-stack/docker-compose.yml")
        
        with open(compose_file, 'r') as f:
            compose_config = yaml.safe_load(f)
        
        redpanda = compose_config["services"]["redpanda"]
        
        # Check image
        assert "image" in redpanda, "Redpanda service should specify an image"
        assert "redpanda" in redpanda["image"].lower(), "Redpanda service should use a Redpanda image"
        
        # Check ports
        assert "ports" in redpanda, "Redpanda service should expose ports"
        ports = redpanda["ports"]
        
        # Check that Kafka port (9092) is exposed
        kafka_port_exposed = any("9092" in str(port) for port in ports)
        assert kafka_port_exposed, "Redpanda should expose Kafka port 9092"
    
    def test_minio_service_configuration(self):
        """Test that MinIO service is properly configured."""
        compose_file = Path("/home/tommyk/projects/devops/quix-streams/infra/local-dev-stack/docker-compose.yml")
        
        with open(compose_file, 'r') as f:
            compose_config = yaml.safe_load(f)
        
        minio = compose_config["services"]["minio"]
        
        # Check image
        assert "image" in minio, "MinIO service should specify an image"
        assert "minio" in minio["image"].lower(), "MinIO service should use a MinIO image"
        
        # Check ports
        assert "ports" in minio, "MinIO service should expose ports"
        ports = minio["ports"]
        
        # Check that MinIO API port (9000) is exposed
        api_port_exposed = any("9000" in str(port) for port in ports)
        assert api_port_exposed, "MinIO should expose API port 9000"
        
        # Check environment variables
        assert "environment" in minio, "MinIO service should have environment variables"
        env = minio["environment"]
        
        # Check default credentials
        root_user_set = any("MINIO_ROOT_USER" in str(var) for var in env)
        root_password_set = any("MINIO_ROOT_PASSWORD" in str(var) for var in env)
        assert root_user_set, "MinIO should have MINIO_ROOT_USER set"
        assert root_password_set, "MinIO should have MINIO_ROOT_PASSWORD set"
    
    def test_lakekeeper_service_configuration(self):
        """Test that Lakekeeper service is properly configured."""
        compose_file = Path("/home/tommyk/projects/devops/quix-streams/infra/local-dev-stack/docker-compose.yml")
        
        with open(compose_file, 'r') as f:
            compose_config = yaml.safe_load(f)
        
        lakekeeper = compose_config["services"]["lakekeeper"]
        
        # Check image
        assert "image" in lakekeeper, "Lakekeeper service should specify an image"
        assert "lakekeeper" in lakekeeper["image"].lower(), "Lakekeeper service should use a Lakekeeper image"
        
        # Check ports
        assert "ports" in lakekeeper, "Lakekeeper service should expose ports"
        ports = lakekeeper["ports"]
        
        # Check that REST API port (8181) is exposed
        rest_port_exposed = any("8181" in str(port) for port in ports)
        assert rest_port_exposed, "Lakekeeper should expose REST API port 8181"
        
        # Check dependencies
        assert "depends_on" in lakekeeper, "Lakekeeper service should have dependencies"
        deps = lakekeeper["depends_on"]
        assert "postgres" in deps, "Lakekeeper should depend on postgres"
        assert "minio" in deps, "Lakekeeper should depend on minio"
    
    def test_postgres_service_configuration(self):
        """Test that PostgreSQL service is properly configured."""
        compose_file = Path("/home/tommyk/projects/devops/quix-streams/infra/local-dev-stack/docker-compose.yml")
        
        with open(compose_file, 'r') as f:
            compose_config = yaml.safe_load(f)
        
        postgres = compose_config["services"]["postgres"]
        
        # Check image
        assert "image" in postgres, "PostgreSQL service should specify an image"
        assert "postgres" in postgres["image"].lower(), "PostgreSQL service should use a PostgreSQL image"
        
        # Check environment variables
        assert "environment" in postgres, "PostgreSQL service should have environment variables"
        env = postgres["environment"]
        
        # Check required environment variables
        required_env_vars = ["POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
        for var in required_env_vars:
            var_set = any(var in str(env_var) for env_var in env)
            assert var_set, f"PostgreSQL should have {var} environment variable set"


class TestLocalStackHelpers:
    """Test helper functions for local stack management."""
    
    def test_start_local_stack_function_exists(self):
        """Test that start_local_stack function exists."""
        # This test will initially fail - TDD Red phase
        try:
            from quixstreams.sinks.community.iceberg_rest import start_local_stack
            assert callable(start_local_stack)
        except ImportError:
            pytest.fail("start_local_stack function should be importable")
    
    def test_stop_local_stack_function_exists(self):
        """Test that stop_local_stack function exists."""
        try:
            from quixstreams.sinks.community.iceberg_rest import stop_local_stack
            assert callable(stop_local_stack)
        except ImportError:
            pytest.fail("stop_local_stack function should be importable")
    
    def test_check_local_stack_health_function_exists(self):
        """Test that check_local_stack_health function exists."""
        try:
            from quixstreams.sinks.community.iceberg_rest import check_local_stack_health
            assert callable(check_local_stack_health)
        except ImportError:
            pytest.fail("check_local_stack_health function should be importable")
    
    @patch('subprocess.run')
    def test_start_local_stack_calls_docker_compose(self, mock_run):
        """Test that start_local_stack calls docker compose up."""
        from quixstreams.sinks.community.iceberg_rest import start_local_stack
        
        mock_run.return_value = MagicMock(returncode=0)
        
        result = start_local_stack()
        
        # Verify docker compose was called
        mock_run.assert_called()
        call_args = mock_run.call_args[0][0]  # Get the command that was called
        
        # Should include docker compose up
        assert "docker" in " ".join(call_args).lower()
        assert "compose" in " ".join(call_args).lower() or "docker-compose" in " ".join(call_args).lower()
        assert "up" in " ".join(call_args).lower()
        
        assert result is True, "start_local_stack should return True on success"
    
    @patch('subprocess.run')
    def test_stop_local_stack_calls_docker_compose_down(self, mock_run):
        """Test that stop_local_stack calls docker compose down."""
        from quixstreams.sinks.community.iceberg_rest import stop_local_stack
        
        mock_run.return_value = MagicMock(returncode=0)
        
        result = stop_local_stack()
        
        # Verify docker compose down was called
        mock_run.assert_called()
        call_args = mock_run.call_args[0][0]
        
        assert "docker" in " ".join(call_args).lower()
        assert "compose" in " ".join(call_args).lower() or "docker-compose" in " ".join(call_args).lower()
        assert "down" in " ".join(call_args).lower()
        
        assert result is True, "stop_local_stack should return True on success"
    
    @patch('requests.get')
    def test_check_local_stack_health_checks_all_services(self, mock_get):
        """Test that health check verifies all services are running."""
        from quixstreams.sinks.community.iceberg_rest import check_local_stack_health
        
        # Mock successful responses from all services
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        health_status = check_local_stack_health()
        
        # Should return a dict with health status for each service
        assert isinstance(health_status, dict)
        
        expected_services = ["redpanda", "minio", "lakekeeper", "postgres"]
        for service in expected_services:
            assert service in health_status
            # All should be healthy due to mocked responses
            assert health_status[service] is True
    
    @patch('requests.get')
    def test_check_local_stack_health_handles_service_down(self, mock_get):
        """Test that health check handles when services are down."""
        from quixstreams.sinks.community.iceberg_rest import check_local_stack_health
        
        # Mock failed responses (connection error)
        mock_get.side_effect = Exception("Connection failed")
        
        health_status = check_local_stack_health()
        
        # All services should be marked as unhealthy
        for service_health in health_status.values():
            assert service_health is False


class TestLocalStackInitialization:
    """Test initialization scripts and setup helpers."""
    
    def test_init_local_stack_function_exists(self):
        """Test that init_local_stack function exists."""
        try:
            from quixstreams.sinks.community.iceberg_rest import init_local_stack
            assert callable(init_local_stack)
        except ImportError:
            pytest.fail("init_local_stack function should be importable")
    
    def test_wait_for_services_function_exists(self):
        """Test that wait_for_services function exists."""
        try:
            from quixstreams.sinks.community.iceberg_rest import wait_for_services
            assert callable(wait_for_services)
        except ImportError:
            pytest.fail("wait_for_services function should be importable")
    
    @patch('quixstreams.sinks.community.iceberg_rest.check_local_stack_health')
    @patch('time.sleep')
    def test_wait_for_services_waits_until_healthy(self, mock_sleep, mock_health_check):
        """Test that wait_for_services waits until all services are healthy."""
        from quixstreams.sinks.community.iceberg_rest import wait_for_services
        
        # Mock health check to fail twice, then succeed
        mock_health_check.side_effect = [
            {"redpanda": False, "minio": True, "lakekeeper": False, "postgres": True},  # Some failing
            {"redpanda": True, "minio": True, "lakekeeper": False, "postgres": True},   # One failing
            {"redpanda": True, "minio": True, "lakekeeper": True, "postgres": True},    # All healthy
        ]
        
        result = wait_for_services(timeout=30)
        
        # Should have called health check multiple times
        assert mock_health_check.call_count == 3
        # Should have slept between checks
        assert mock_sleep.call_count >= 2
        # Should return True when all services are healthy
        assert result is True
    
    @patch('quixstreams.sinks.community.iceberg_rest.check_local_stack_health')
    @patch('time.sleep')
    def test_wait_for_services_times_out_on_unhealthy_services(self, mock_sleep, mock_health_check):
        """Test that wait_for_services times out if services don't become healthy."""
        from quixstreams.sinks.community.iceberg_rest import wait_for_services
        
        # Mock health check to always have failures
        mock_health_check.return_value = {
            "redpanda": True, "minio": True, "lakekeeper": False, "postgres": True
        }
        
        result = wait_for_services(timeout=1)  # Short timeout for testing
        
        # Should return False on timeout
        assert result is False


# Integration test markers for pytest
pytestmark = [
    pytest.mark.integration,
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),
]