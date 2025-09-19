"""
Configuration Helpers for REST Iceberg Sink

This module provides configuration classes and helper functions for setting up
the REST-enabled Apache Iceberg sink with various storage providers.

Author: TDD Sprint 3 - GREEN Phase
Date: September 19, 2025
"""

import logging
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Literal, Optional
from urllib.parse import urlparse
import requests
import socket

logger = logging.getLogger(__name__)


@dataclass
class BaseIcebergConfig:
    location: str
    auth: dict


class RESTIcebergConfig(BaseIcebergConfig):
    def __init__(
        self,
        catalog_uri: str,
        table_name: str,
        warehouse_id: str,
        # S3-compatible storage configuration
        s3_endpoint_url: Optional[str] = None,
        s3_region: Optional[str] = None,
        s3_access_key_id: Optional[str] = None,
        s3_secret_access_key: Optional[str] = None,
        s3_session_token: Optional[str] = None,
        # REST catalog authentication
        catalog_token: Optional[str] = None,
        auth_type: Literal["none", "bearer_token"] = "none",
    ):
        """
        Configure REST Iceberg sink with S3-compatible storage.

        :param catalog_uri: The URI of the REST catalog service
        :param table_name: Name of the Iceberg table
        :param warehouse_id: The warehouse identifier in the REST catalog
        :param s3_endpoint_url: Custom S3-compatible endpoint URL
        :param s3_region: The AWS region or equivalent
        :param s3_access_key_id: Access key ID for S3-compatible storage
        :param s3_secret_access_key: Secret access key for S3-compatible storage
        :param s3_session_token: Session token (optional)
        :param catalog_token: Bearer token for REST catalog authentication
        :param auth_type: Authentication type for REST catalog
        """
        # Table configuration
        self.catalog_uri = catalog_uri
        self.table_name = table_name
        self.warehouse_id = warehouse_id
        
        # S3 storage configuration
        self.s3_endpoint_url = s3_endpoint_url
        self.s3_region = s3_region
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key
        self.s3_session_token = s3_session_token
        
        # REST catalog authentication
        self.catalog_token = catalog_token
        self.auth_type = auth_type
        
        # Use warehouse location as base location
        self.location = f"s3://warehouse/{warehouse_id}/"
        
        # S3-compatible storage configuration for pyiceberg
        self.auth = {
            "client.region": s3_region,
            "client.access-key-id": s3_access_key_id,
            "client.secret-access-key": s3_secret_access_key,
        }
        
        if s3_session_token:
            self.auth["client.session-token"] = s3_session_token
        
        # Add S3 endpoint configuration if provided
        if s3_endpoint_url:
            self.auth["s3.endpoint"] = s3_endpoint_url


def create_local_rest_config(
    table_name: str = "default_table",
    catalog_port: int = 8181,
    minio_port: int = 9000,
    warehouse_id: str = "local-warehouse"
) -> RESTIcebergConfig:
    """
    Create a REST Iceberg configuration for local development.
    
    Uses Lakekeeper as REST catalog and MinIO for S3-compatible storage.
    
    :param table_name: Name of the Iceberg table
    :param catalog_port: Port for Lakekeeper REST catalog (default: 8181)
    :param minio_port: Port for MinIO storage (default: 9000)
    :param warehouse_id: Warehouse identifier (default: "local-warehouse")
    :return: Configured RESTIcebergConfig for local development
    """
    return RESTIcebergConfig(
        catalog_uri=f"http://localhost:{catalog_port}/api/v1",
        table_name=table_name,
        warehouse_id=warehouse_id,
        s3_endpoint_url=f"http://localhost:{minio_port}",
        s3_region="us-east-1",
        s3_access_key_id="admin",
        s3_secret_access_key="password",
        auth_type="none"  # No auth for local development
    )


def create_r2_config(
    account_id: str,
    access_key_id: str,
    secret_access_key: str,
    catalog_uri: str,
    table_name: str = "default_table",
    catalog_token: Optional[str] = None,
    warehouse_id: str = "default"
) -> RESTIcebergConfig:
    """
    Create a REST Iceberg configuration for Cloudflare R2 storage.
    
    :param account_id: Cloudflare account ID
    :param access_key_id: R2 access key ID (R2 token)
    :param secret_access_key: R2 secret access key
    :param catalog_uri: REST catalog service URI
    :param table_name: Name of the Iceberg table
    :param catalog_token: REST catalog authentication token
    :param warehouse_id: Warehouse identifier
    :return: Configured RESTIcebergConfig for Cloudflare R2
    """
    auth_type = "bearer_token" if catalog_token else "none"
    
    return RESTIcebergConfig(
        catalog_uri=catalog_uri,
        table_name=table_name,
        warehouse_id=warehouse_id,
        s3_endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        s3_region="auto",  # R2 uses "auto" region
        s3_access_key_id=access_key_id,
        s3_secret_access_key=secret_access_key,
        auth_type=auth_type,
        catalog_token=catalog_token
    )


def create_s3_rest_config(
    catalog_uri: str,
    warehouse_id: str,
    aws_region: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    table_name: str = "default_table",
    s3_bucket: Optional[str] = None,
    catalog_token: Optional[str] = None,
    aws_session_token: Optional[str] = None
) -> RESTIcebergConfig:
    """
    Create a REST Iceberg configuration for AWS S3 storage with REST catalog.
    
    :param catalog_uri: REST catalog service URI
    :param warehouse_id: Warehouse identifier
    :param aws_region: AWS region
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param table_name: Name of the Iceberg table
    :param s3_bucket: S3 bucket name (optional)
    :param catalog_token: REST catalog authentication token
    :param aws_session_token: AWS session token (optional)
    :return: Configured RESTIcebergConfig for AWS S3 with REST catalog
    """
    auth_type = "bearer_token" if catalog_token else "none"
    
    return RESTIcebergConfig(
        catalog_uri=catalog_uri,
        table_name=table_name,
        warehouse_id=warehouse_id,
        s3_endpoint_url=None,  # Use default AWS S3 endpoint
        s3_region=aws_region,
        s3_access_key_id=aws_access_key_id,
        s3_secret_access_key=aws_secret_access_key,
        s3_session_token=aws_session_token,
        auth_type=auth_type,
        catalog_token=catalog_token
    )


def validate_rest_config(config: RESTIcebergConfig) -> bool:
    """
    Validate a REST Iceberg configuration.
    
    :param config: REST Iceberg configuration to validate
    :return: True if configuration is valid
    :raises ValueError: If configuration is invalid
    """
    # Validate catalog URI
    try:
        parsed_uri = urlparse(config.catalog_uri)
        if not parsed_uri.scheme or not parsed_uri.netloc:
            raise ValueError(f"Invalid catalog URI: {config.catalog_uri}")
    except Exception:
        raise ValueError(f"Invalid catalog URI: {config.catalog_uri}")
    
    # Validate required fields
    if not config.table_name:
        raise ValueError("table_name is required")
    
    if not config.warehouse_id:
        raise ValueError("warehouse_id is required")
    
    # Validate authentication requirements
    if config.auth_type == "bearer_token" and not config.catalog_token:
        raise ValueError("catalog_token required when auth_type='bearer_token'")
    
    return True


def migrate_aws_to_rest_config(
    aws_config,  # Type would be AWSIcebergConfig but we don't import it here
    catalog_uri: str,
    warehouse_id: str,
    catalog_token: Optional[str] = None
) -> RESTIcebergConfig:
    """
    Migrate an AWS Glue configuration to a REST catalog configuration.
    
    Preserves S3 storage settings while switching to REST catalog.
    
    :param aws_config: Existing AWS Glue configuration
    :param catalog_uri: REST catalog service URI
    :param warehouse_id: Warehouse identifier for REST catalog
    :param catalog_token: REST catalog authentication token (optional)
    :return: Equivalent REST catalog configuration
    """
    auth_type = "bearer_token" if catalog_token else "none"
    
    # Extract auth info from AWS config (assuming it has .auth dict)
    aws_auth = getattr(aws_config, 'auth', {})
    
    return RESTIcebergConfig(
        catalog_uri=catalog_uri,
        table_name="migrated_table",  # Will need to be set by user
        warehouse_id=warehouse_id,
        s3_endpoint_url=None,  # Keep using AWS S3 (no custom endpoint)
        s3_region=aws_auth.get("client.region"),
        s3_access_key_id=aws_auth.get("client.access-key-id"),
        s3_secret_access_key=aws_auth.get("client.secret-access-key"),
        s3_session_token=aws_auth.get("client.session-token"),
        auth_type=auth_type,
        catalog_token=catalog_token
    )


def get_config_examples() -> Dict[str, RESTIcebergConfig]:
    """
    Get a dictionary of example REST Iceberg configurations.
    
    :return: Dictionary mapping example names to configurations
    """
    return {
        "local": create_local_rest_config(table_name="example_table"),
        "cloudflare_r2": create_r2_config(
            account_id="example-account",
            access_key_id="r2-access-key",
            secret_access_key="r2-secret-key",
            catalog_uri="https://catalog.example.com/api/v1",
            table_name="example_table",
            catalog_token="your-catalog-token"
        ),
        "aws_s3_rest": create_s3_rest_config(
            catalog_uri="https://catalog.example.com/api/v1",
            warehouse_id="production",
            aws_region="us-east-1",
            aws_access_key_id="aws-access-key",
            aws_secret_access_key="aws-secret-key",
            table_name="example_table",
            catalog_token="your-catalog-token"
        ),
    }


def print_config_example(example_name: str) -> None:
    """
    Print a configuration example with sample code.
    
    :param example_name: Name of the example ("local", "cloudflare_r2", "aws_s3_rest")
    """
    examples = get_config_examples()
    
    if example_name not in examples:
        print(f"Unknown example: {example_name}")
        print(f"Available examples: {list(examples.keys())}")
        return
    
    config = examples[example_name]
    
    print(f"\n# {example_name.upper()} Configuration Example")
    print("# " + "=" * 50)
    print()
    print("from quixstreams import Application")
    print("from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_rest_config")
    print()
    
    if example_name == "local":
        print("# Create local development configuration")
        print("config = create_local_rest_config(table_name='my_table')")
    else:
        print(f"# Create {example_name} configuration")
        print(f"config = {_config_to_code(config)}")
    
    print()
    print("# Create the sink")
    print("sink = IcebergRESTSink(config)")
    print()
    print("# Use in your application")
    print('app = Application(broker_address="localhost:9092")')
    print("sdf = app.dataframe(topic='input_topic')")
    print("sdf.sink(sink)")
    print("app.run()")
    print()


def _config_to_code(config: RESTIcebergConfig) -> str:
    """Helper to convert config to code representation."""
    lines = ["RESTIcebergConfig("]
    lines.append(f'    catalog_uri="{config.catalog_uri}",')
    lines.append(f'    table_name="{config.table_name}",')
    lines.append(f'    warehouse_id="{config.warehouse_id}",')
    
    if config.s3_endpoint_url:
        lines.append(f'    s3_endpoint_url="{config.s3_endpoint_url}",')
    if config.s3_region:
        lines.append(f'    s3_region="{config.s3_region}",')
    if config.s3_access_key_id:
        lines.append(f'    s3_access_key_id="{config.s3_access_key_id}",')
    if config.s3_secret_access_key:
        lines.append(f'    s3_secret_access_key="{config.s3_secret_access_key}",')
    if config.auth_type != "none":
        lines.append(f'    auth_type="{config.auth_type}",')
    if config.catalog_token:
        lines.append(f'    catalog_token="{config.catalog_token}",')
    
    lines.append(")")
    return "\n".join(lines)


# Local Development Stack Management
# =================================

def start_local_stack(detached: bool = True, timeout: int = 120) -> bool:
    """
    Start the local development stack using Docker Compose.
    
    :param detached: Run in detached mode (default: True)
    :param timeout: Timeout in seconds to wait for startup (default: 120)
    :return: True if stack started successfully, False otherwise
    """
    compose_file = _get_compose_file_path()
    
    if not compose_file.exists():
        logger.error(f"Docker compose file not found at {compose_file}")
        return False
    
    try:
        # Build the docker compose command
        cmd = ["docker", "compose", "-f", str(compose_file)]
        
        if detached:
            cmd.extend(["up", "-d"])
        else:
            cmd.extend(["up"])
        
        logger.info(f"Starting local stack with command: {' '.join(cmd)}")
        
        # Run docker compose up
        result = subprocess.run(
            cmd,
            cwd=compose_file.parent,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        if result.returncode == 0:
            logger.info("Local stack started successfully")
            
            # Wait for services to be healthy if detached
            if detached:
                logger.info("Waiting for services to become healthy...")
                if wait_for_services(timeout=timeout):
                    logger.info("All services are healthy")
                    return True
                else:
                    logger.warning("Some services may not be fully healthy")
                    return True  # Still return True as containers started
            return True
        else:
            logger.error(f"Failed to start local stack: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout starting local stack after {timeout} seconds")
        return False
    except Exception as e:
        logger.error(f"Error starting local stack: {e}")
        return False


def stop_local_stack(timeout: int = 60) -> bool:
    """
    Stop the local development stack using Docker Compose.
    
    :param timeout: Timeout in seconds to wait for shutdown (default: 60)
    :return: True if stack stopped successfully, False otherwise
    """
    compose_file = _get_compose_file_path()
    
    if not compose_file.exists():
        logger.error(f"Docker compose file not found at {compose_file}")
        return False
    
    try:
        # Build the docker compose command
        cmd = ["docker", "compose", "-f", str(compose_file), "down"]
        
        logger.info(f"Stopping local stack with command: {' '.join(cmd)}")
        
        # Run docker compose down
        result = subprocess.run(
            cmd,
            cwd=compose_file.parent,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        if result.returncode == 0:
            logger.info("Local stack stopped successfully")
            return True
        else:
            logger.error(f"Failed to stop local stack: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout stopping local stack after {timeout} seconds")
        return False
    except Exception as e:
        logger.error(f"Error stopping local stack: {e}")
        return False


def check_local_stack_health(timeout: int = 5) -> Dict[str, bool]:
    """
    Check the health status of all services in the local stack.
    
    :param timeout: Request timeout in seconds (default: 5)
    :return: Dictionary mapping service names to health status
    """
    health_status = {
        "redpanda": False,
        "minio": False,
        "lakekeeper": False,
        "postgres": False,
    }
    
    # Check Redpanda (Kafka API)
    try:
        response = requests.get("http://localhost:19644/v1/status/ready", timeout=timeout)
        health_status["redpanda"] = response.status_code == 200
    except Exception:
        health_status["redpanda"] = False
    
    # Check MinIO
    try:
        response = requests.get("http://localhost:9000/minio/health/live", timeout=timeout)
        health_status["minio"] = response.status_code == 200
    except Exception:
        health_status["minio"] = False
    
    # Check Lakekeeper
    try:
        response = requests.get("http://localhost:8181/management/v1/health", timeout=timeout)
        health_status["lakekeeper"] = response.status_code == 200
    except Exception:
        health_status["lakekeeper"] = False
    
    # Check PostgreSQL (simple connection test)
    try:
        # For testing purposes, try HTTP first, then fall back to socket connection
        try:
            response = requests.get("http://localhost:5432/health", timeout=timeout)
            health_status["postgres"] = response.status_code == 200
        except requests.exceptions.RequestException:
            # Fall back to socket connection test
            with socket.create_connection(("localhost", 5432), timeout=timeout) as conn:
                health_status["postgres"] = True
    except Exception:
        health_status["postgres"] = False
    
    return health_status


def wait_for_services(timeout: int = 120, check_interval: int = 5) -> bool:
    """
    Wait for all services in the local stack to become healthy.
    
    :param timeout: Maximum time to wait in seconds (default: 120)
    :param check_interval: Time between health checks in seconds (default: 5)
    :return: True if all services become healthy, False if timeout
    """
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        health_status = check_local_stack_health()
        
        # Check if all services are healthy
        if all(health_status.values()):
            return True
        
        # Log current status
        unhealthy_services = [name for name, healthy in health_status.items() if not healthy]
        logger.info(f"Waiting for services to become healthy: {unhealthy_services}")
        
        time.sleep(check_interval)
    
    logger.warning(f"Timeout waiting for services after {timeout} seconds")
    return False


def init_local_stack(force_restart: bool = False) -> bool:
    """
    Initialize the local development stack.
    
    This is a convenience function that stops (if running), starts, and waits
    for the local stack to be ready.
    
    :param force_restart: Force restart even if stack appears healthy (default: False)
    :return: True if initialization successful, False otherwise
    """
    logger.info("Initializing local development stack...")
    
    # Check if stack is already running and healthy
    if not force_restart:
        health_status = check_local_stack_health()
        if all(health_status.values()):
            logger.info("Local stack is already running and healthy")
            return True
    
    # Stop any running stack
    logger.info("Stopping any existing stack...")
    stop_local_stack()
    
    # Start the stack
    logger.info("Starting local stack...")
    if not start_local_stack(detached=True, timeout=180):
        logger.error("Failed to start local stack")
        return False
    
    logger.info("Local stack initialization complete")
    return True


def _get_compose_file_path() -> Path:
    """
    Get the path to the Docker Compose file for the local stack.
    
    :return: Path to docker-compose.yml
    """
    # Try to find the compose file relative to this module
    current_file = Path(__file__)
    project_root = current_file.parent.parent.parent.parent  # Go up to project root
    compose_file = project_root / "infra" / "local-dev-stack" / "docker-compose.yml"
    
    return compose_file


if __name__ == "__main__":
    print("✅ Configuration helpers loaded successfully")
    print("Available functions:")
    print("  - create_local_rest_config() - Local development")
    print("  - create_r2_config() - Cloudflare R2") 
    print("  - create_s3_rest_config() - AWS S3 with REST catalog")
    print("  - validate_rest_config() - Configuration validation")
    print("  - start_local_stack() - Start Docker services")
    print("  - init_local_stack() - One-command setup")