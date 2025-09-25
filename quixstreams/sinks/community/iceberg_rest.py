import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime
from importlib import import_module
from io import BytesIO
from typing import Dict, Literal, Optional, Type, Union, get_args
from urllib.parse import urlparse
import subprocess
import time
import requests
import os
from pathlib import Path

from quixstreams.sinks import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBackpressureError,
    SinkBatch,
)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg.catalog import MetastoreCatalog
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import NestedField, Schema
    from pyiceberg.table import Table
    from pyiceberg.transforms import DayTransform, IdentityTransform
    from pyiceberg.types import StringType, TimestampType
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        f'run "pip install quixstreams[iceberg]" to use IcebergSink'
    ) from exc

__all__ = (
    "IcebergSink", 
    "AWSIcebergConfig", 
    "RESTIcebergConfig",
    # Configuration helpers
    "create_local_rest_config",
    "create_r2_config", 
    "create_s3_rest_config",
    "validate_rest_config",
    "migrate_aws_to_rest_config",
    "get_config_examples",
    "print_config_example",
    # Local stack management
    "start_local_stack",
    "stop_local_stack",
    "check_local_stack_health",
    "init_local_stack",
    "wait_for_services",
)

logger = logging.getLogger(__name__)

DataCatalogSpec = Literal["aws_glue", "rest"]

_SUPPORTED_DATA_CATALOG_SPECS = get_args(DataCatalogSpec)


@dataclass
class BaseIcebergConfig:
    location: str
    auth: dict


class AWSIcebergConfig(BaseIcebergConfig):
    def __init__(
        self,
        aws_s3_uri: str,
        aws_region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
    ):
        """
        Configure IcebergSink to work with AWS Glue.

        :param aws_s3_uri: The S3 URI where the table data will be stored
            (e.g., 's3://your-bucket/warehouse/').
        :param aws_region: The AWS region for the S3 bucket and Glue catalog.
        :param aws_access_key_id: the AWS access key ID.
            NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
            when using AWS Glue.
        :param aws_secret_access_key: the AWS secret access key.
            NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
            when using AWS Glue.
        :param aws_session_token: a session token (or will be generated for you).
            NOTE: can alternatively set the AWS_SESSION_TOKEN environment variable when
            using AWS Glue.
        """
        self.location = aws_s3_uri
        self.auth = {
            "client.region": aws_region,
            "client.access-key-id": aws_access_key_id,
            "client.secret-access-key": aws_secret_access_key,
            "client.session-token": aws_session_token,
        }


class RESTIcebergConfig(BaseIcebergConfig):
    def __init__(
        self,
        rest_uri: str,
        warehouse_id: str,
        # S3-compatible storage configuration
        endpoint_url: Optional[str] = None,
        aws_region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        # REST catalog authentication
        auth_type: Literal["none", "bearer_token", "basic"] = "none",
        auth_token: Optional[str] = None,
        auth_username: Optional[str] = None,
        auth_password: Optional[str] = None,
    ):
        """
        Configure IcebergSink to work with REST catalogs and S3-compatible storage.

        :param rest_uri: The URI of the REST catalog service
            (e.g., 'http://localhost:8181' for Lakekeeper).
        :param warehouse_id: The warehouse identifier in the REST catalog.
        :param endpoint_url: Custom S3-compatible endpoint URL for storage
            (e.g., 'https://account.r2.cloudflarestorage.com' for Cloudflare R2,
            'http://localhost:9000' for MinIO, None for AWS S3).
        :param aws_region: The AWS region or equivalent for S3-compatible storage.
        :param aws_access_key_id: Access key ID for S3-compatible storage.
        :param aws_secret_access_key: Secret access key for S3-compatible storage.
        :param aws_session_token: Session token for S3-compatible storage (optional).
        :param auth_type: Authentication type for REST catalog
            ("none" for local dev, "bearer_token" for token auth, "basic" for username/password).
        :param auth_token: Bearer token for REST catalog authentication.
        :param auth_username: Username for basic authentication.
        :param auth_password: Password for basic authentication.
        """
        # Use warehouse location as base location (will be used with S3-compatible endpoint)
        self.location = f"s3://warehouse/{warehouse_id}/"
        
        # S3-compatible storage configuration
        self.auth = {
            "client.region": aws_region,
            "client.access-key-id": aws_access_key_id,
            "client.secret-access-key": aws_secret_access_key,
            "client.session-token": aws_session_token,
        }
        
        # Add S3 endpoint configuration if provided (for R2, MinIO, etc.)
        if endpoint_url:
            self.auth["s3.endpoint"] = endpoint_url
        
        # REST catalog configuration
        self.rest_uri = rest_uri
        self.warehouse_id = warehouse_id
        self.auth_type = auth_type
        self.auth_token = auth_token
        self.auth_username = auth_username
        self.auth_password = auth_password
        
        # Store endpoint URL for S3 client configuration
        self.endpoint_url = endpoint_url
        
        # Store individual configuration values as instance attributes for easy access
        self.aws_region = aws_region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token


class IcebergSink(BatchingSink):
    """
    IcebergSink writes batches of data to an Apache Iceberg table.

    The data will by default include the kafka message key, value, and timestamp.

    It serializes incoming data batches into Parquet format and appends them to the
    Iceberg table, updating the table schema as necessary.

    Currently, supports Apache Iceberg hosted in:

    - AWS (S3 + Glue)
    - Any S3-compatible storage (Cloudflare R2, MinIO, etc.) with REST catalogs

    Supported data catalogs:

    - AWS Glue
    - REST catalogs (Lakekeeper, etc.)


    :param table_name: The name of the Iceberg table.
    :param config: An IcebergConfig with all the various connection parameters.
    :param data_catalog_spec: data cataloger to use (ex. for AWS Glue, "aws_glue").
    :param schema: The Iceberg table schema. If None, a default schema is used.
    :param partition_spec: The partition specification for the table.
        If None, a default is used.
    :param on_client_connect_success: An optional callback made after successful
        client authentication, primarily for additional logging.
    :param on_client_connect_failure: An optional callback made after failed
        client authentication (which should raise an Exception).
        Callback should accept the raised Exception as an argument.
        Callback must resolve (or propagate/re-raise) the Exception.

    Example setup using AWS-hosted Iceberg with AWS Glue:

    ```
    from quixstreams import Application
    from quixstreams.sinks.community.iceberg_rest import IcebergSink, AWSIcebergConfig

    # Configure S3 bucket credentials
    iceberg_config = AWSIcebergConfig(
        aws_s3_uri="s3://my-bucket/warehouse/", 
        aws_region="us-east-1", 
        aws_access_key_id="...", 
        aws_secret_access_key="..."
    )

    # Configure the sink to write data to S3 with the AWS Glue catalog spec
    iceberg_sink = IcebergSink(
        table_name="glue.sink-test",
        config=iceberg_config,
        data_catalog_spec="aws_glue",
    )
    ```
    
    Example setup using REST catalog with S3-compatible storage (Cloudflare R2):
    
    ```
    from quixstreams import Application
    from quixstreams.sinks.community.iceberg_rest import IcebergSink, RESTIcebergConfig

    # Configure REST catalog with Cloudflare R2 storage
    iceberg_config = RESTIcebergConfig(
        rest_uri="https://catalog.example.com/api/v1",
        warehouse_id="main",
        endpoint_url="https://account-id.r2.cloudflarestorage.com",
        aws_region="auto",
        aws_access_key_id="your-r2-token-id",
        aws_secret_access_key="your-r2-token-secret",
        auth_type="bearer_token",
        auth_token="your-catalog-token"
    )

    # Configure the sink to write data using REST catalog
    iceberg_sink = IcebergSink(
        table_name="crypto.trades",
        config=iceberg_config,
        data_catalog_spec="rest",
    )
    ```
    
    Example setup using local development stack (Lakekeeper + MinIO):
    
    ```
    from quixstreams import Application
    from quixstreams.sinks.community.iceberg_rest import IcebergSink, RESTIcebergConfig

    # Configure local REST catalog with MinIO storage
    iceberg_config = RESTIcebergConfig(
        rest_uri="http://localhost:8181",
        warehouse_id="local",
        endpoint_url="http://localhost:9000",
        aws_region="us-east-1",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        auth_type="none"  # No authentication for local development
    )

    # Configure the sink for local development
    iceberg_sink = IcebergSink(
        table_name="crypto.trades",
        config=iceberg_config,
        data_catalog_spec="rest",
    )
    ```
    """

    def __init__(
        self,
        table_name: str,
        config: Union[AWSIcebergConfig, RESTIcebergConfig],
        data_catalog_spec: DataCatalogSpec,
        schema: Optional[Schema] = None,
        partition_spec: Optional[PartitionSpec] = None,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self._iceberg_config = config
        self._table_name = table_name
        self._table: Optional[Table] = None
        self._data_catalog_spec = data_catalog_spec
        
        # Validate configuration matches catalog specification
        if data_catalog_spec == "aws_glue" and not isinstance(config, AWSIcebergConfig):
            raise ValueError("AWSIcebergConfig required when data_catalog_spec='aws_glue'")
        elif data_catalog_spec == "rest" and not isinstance(config, RESTIcebergConfig):
            raise ValueError("RESTIcebergConfig required when data_catalog_spec='rest'")

        # Configure Iceberg Catalog
        data_catalog_cls = _import_data_catalog(data_catalog_spec)
        self.data_catalog = self._create_catalog(data_catalog_cls, data_catalog_spec)

        # Set up the schema.
        self._schema = schema if schema is not None else self._get_default_schema()

        # Set up the partition specification.
        self._partition_spec = (
            partition_spec
            if partition_spec is not None
            else self._get_default_partition_spec(self._schema)
        )
    
    def _create_catalog(self, data_catalog_cls, data_catalog_spec: DataCatalogSpec):
        """Create catalog instance based on specification type."""
        if data_catalog_spec == "aws_glue":
            # AWS Glue catalog - use existing auth pattern
            return data_catalog_cls(
                name=f"{data_catalog_spec}_catalog",
                **self._iceberg_config.auth,
            )
        elif data_catalog_spec == "rest":
            # REST catalog - use REST-specific configuration
            rest_config = self._iceberg_config
            catalog_config = {
                "uri": rest_config.rest_uri,
                "warehouse": rest_config.warehouse_id,
            }
            
            # Add authentication based on auth_type
            if rest_config.auth_type == "bearer_token" and rest_config.auth_token:
                catalog_config["token"] = rest_config.auth_token
            elif rest_config.auth_type == "basic" and rest_config.auth_username and rest_config.auth_password:
                catalog_config["credential"] = f"{rest_config.auth_username}:{rest_config.auth_password}"
            # "none" auth type requires no additional configuration
            
            # Add S3-compatible storage configuration
            catalog_config.update(rest_config.auth)
            
            return data_catalog_cls(
                name="rest_catalog",
                **catalog_config,
            )
        else:
            raise ValueError(f"Unsupported catalog specification: {data_catalog_spec}")

    def setup(self):
        # Our client is an interface for a table, so for the sake of
        # readability, the client will be called "_table"
        self._table = self.data_catalog.create_table_if_not_exists(
            identifier=self._table_name,
            schema=self._schema,
            location=self._iceberg_config.location,
            partition_spec=self._partition_spec,
            properties={"write.distribution-mode": "fanout"},
        )
        logger.info(
            f"Loaded Iceberg table '{self._table_name}' at "
            f"'{self._iceberg_config.location}'."
        )

    def write(self, batch: SinkBatch):
        """
        Writes a batch of data to the Iceberg table.
        Implements retry logic to handle concurrent write conflicts.

        :param batch: The batch of data to write.
        """
        try:
            # Serialize batch data into Parquet format.
            data = self._serialize_batch_values(batch)

            # Read data into a PyArrow Table.
            input_buffer = pa.BufferReader(data)
            parquet_table = pq.read_table(input_buffer)

            # Reload the table to get the latest metadata
            self._table = self.data_catalog.load_table(self._table.name())

            # Update the table schema if necessary.
            with self._table.update_schema() as update:
                update.union_by_name(parquet_table.schema)

            append_start_epoch = time.time()
            self._table.append(parquet_table)
            logger.info(
                f"Appended {batch.size} records to {self._table.name()} table "
                f"in {time.time() - append_start_epoch}s."
            )

        except CommitFailedException as e:
            # Handle commit conflict
            logger.warning(f"Commit conflict detected.: {e}")
            # encourage staggered backoff
            sleep_time = random.uniform(0, 5)  # noqa: S311
            raise SinkBackpressureError(retry_after=sleep_time)
        except Exception as e:
            logger.error(f"Error writing data to Iceberg table: {e}")
            raise

    def _get_default_schema(self) -> Schema:
        """
        Return a default Iceberg schema when none is provided.
        """
        return Schema(
            fields=(
                NestedField(
                    field_id=1,
                    name="_timestamp",
                    field_type=TimestampType(),
                    required=False,
                ),
                NestedField(
                    field_id=2, name="_key", field_type=StringType(), required=False
                ),
            )
        )

    def _get_default_partition_spec(self, schema: Schema) -> PartitionSpec:
        """
        Set up a default partition specification if none is provided.
        """
        # Map field names to field IDs from the schema.
        field_ids = {field.name: field.field_id for field in schema.fields}

        # Create partition fields for kafka key and timestamp.
        partition_fields = (
            PartitionField(
                source_id=field_ids["_key"],
                field_id=1000,  # Unique partition field ID.
                transform=IdentityTransform(),
                name="_key",
            ),
            PartitionField(
                source_id=field_ids["_timestamp"],
                field_id=1001,
                transform=DayTransform(),
                name="day",
            ),
        )

        # Create the new PartitionSpec.
        return PartitionSpec(fields=partition_fields)

    def _serialize_batch_values(self, batch: SinkBatch) -> bytes:
        """
        Dynamically unpacks each kafka message's value (its dict keys/"columns") within the
        provided batch and preps the messages for reading into a PyArrow Table.
        """
        # TODO: Handle data flattening. Nested properties will cause this to crash.
        # TODO: possible optimizations with all the iterative batch transformations

        # Get all unique "keys" (columns) across all rows
        all_keys = set()
        for row in batch:
            all_keys.update(row.value.keys())

        # Normalize rows: Ensure all rows have the same keys, filling missing ones with None
        normalized_values = [
            {key: row.value.get(key, None) for key in all_keys} for row in batch
        ]

        columns = {
            "_timestamp": [
                datetime.fromtimestamp(row.timestamp / 1000.0) for row in batch
            ],
            "_key": [
                row.key.decode() if isinstance(row.key, bytes) else row.key
                for row in batch
            ],
        }

        # Convert normalized values to a pyarrow Table
        columns = {
            **columns,
            **{key: [row[key] for row in normalized_values] for key in all_keys},
        }

        table = pa.Table.from_pydict(columns)

        with BytesIO() as f:
            pq.write_table(table, f, compression="snappy")
            return f.getvalue()


def _import_data_catalog(data_catalog_spec: DataCatalogSpec) -> Type[MetastoreCatalog]:
    """
    A way to dynamically load data catalogs which may require other imports
    """
    if data_catalog_spec not in _SUPPORTED_DATA_CATALOG_SPECS:
        raise ValueError(f"Unsupported data_catalog_spec: {data_catalog_spec}")

    data_catalogs = {
        "aws_glue": ("[iceberg_aws]", "glue.GlueCatalog"),
        "rest": ("[iceberg]", "rest.RestCatalog"),
    }

    install_name, module_path = data_catalogs[data_catalog_spec]
    module, catalog_cls_name = module_path.split(".")
    try:
        return getattr(import_module(f"pyiceberg.catalog.{module}"), catalog_cls_name)
    except ImportError as exc:
        raise ImportError(
            f"Package {exc.name} is missing: "
            f"do 'pip install quixstreams{install_name}' to use "
            f"data_catalog_spec {data_catalog_spec}"
        ) from exc


# Configuration Helper Functions
# =============================

def create_local_rest_config(
    catalog_port: int = 8181,
    minio_port: int = 9000,
    warehouse_id: str = "local",
    catalog_host: str = "localhost",
    minio_host: str = "localhost",
    access_key_id: str = "minioadmin",
    secret_access_key: str = "minioadmin",
    region: str = "us-east-1"
) -> RESTIcebergConfig:
    """
    Create a REST Iceberg configuration for local development.
    
    This helper creates a configuration suitable for local development
    with Lakekeeper REST catalog and MinIO object storage.
    
    :param catalog_port: Port for the REST catalog service (default: 8181)
    :param minio_port: Port for MinIO object storage (default: 9000)
    :param warehouse_id: Warehouse identifier (default: "local")
    :param catalog_host: Hostname for REST catalog (default: "localhost")
    :param minio_host: Hostname for MinIO storage (default: "localhost")
    :param access_key_id: MinIO access key (default: "minioadmin")
    :param secret_access_key: MinIO secret key (default: "minioadmin")
    :param region: AWS region equivalent (default: "us-east-1")
    :return: Configured RESTIcebergConfig for local development
    """
    return RESTIcebergConfig(
        rest_uri=f"http://{catalog_host}:{catalog_port}",
        warehouse_id=warehouse_id,
        endpoint_url=f"http://{minio_host}:{minio_port}",
        aws_region=region,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        auth_type="none"
    )


def create_r2_config(
    account_id: str,
    access_key_id: str,
    secret_access_key: str,
    catalog_uri: str,
    catalog_token: str,
    warehouse_id: str = "main",
    region: str = "auto"
) -> RESTIcebergConfig:
    """
    Create a REST Iceberg configuration for Cloudflare R2 storage.
    
    :param account_id: Cloudflare R2 account ID
    :param access_key_id: R2 API token access key
    :param secret_access_key: R2 API token secret
    :param catalog_uri: REST catalog service URI
    :param catalog_token: REST catalog authentication token
    :param warehouse_id: Warehouse identifier (default: "main")
    :param region: R2 region equivalent (default: "auto")
    :return: Configured RESTIcebergConfig for Cloudflare R2
    """
    return RESTIcebergConfig(
        rest_uri=catalog_uri,
        warehouse_id=warehouse_id,
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_region=region,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        auth_type="bearer_token",
        auth_token=catalog_token
    )


def create_s3_rest_config(
    catalog_uri: str,
    warehouse_id: str,
    aws_region: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    catalog_token: str,
    aws_session_token: Optional[str] = None
) -> RESTIcebergConfig:
    """
    Create a REST Iceberg configuration for AWS S3 storage with REST catalog.
    
    :param catalog_uri: REST catalog service URI
    :param warehouse_id: Warehouse identifier
    :param aws_region: AWS region
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param catalog_token: REST catalog authentication token
    :param aws_session_token: AWS session token (optional)
    :return: Configured RESTIcebergConfig for AWS S3 with REST catalog
    """
    return RESTIcebergConfig(
        rest_uri=catalog_uri,
        warehouse_id=warehouse_id,
        endpoint_url=None,  # Use default AWS S3 endpoint
        aws_region=aws_region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        auth_type="bearer_token",
        auth_token=catalog_token
    )


def validate_rest_config(config: RESTIcebergConfig) -> bool:
    """
    Validate a REST Iceberg configuration.
    
    :param config: REST Iceberg configuration to validate
    :return: True if configuration is valid
    :raises ValueError: If configuration is invalid
    """
    # Validate REST URI
    try:
        parsed_uri = urlparse(config.rest_uri)
        if not parsed_uri.scheme or not parsed_uri.netloc:
            raise ValueError(f"Invalid REST URI: {config.rest_uri}")
    except Exception:
        raise ValueError(f"Invalid REST URI: {config.rest_uri}")
    
    # Validate authentication requirements
    if config.auth_type == "bearer_token" and not config.auth_token:
        raise ValueError("auth_token required when auth_type='bearer_token'")
    
    if config.auth_type == "basic":
        if not config.auth_username or not config.auth_password:
            raise ValueError("auth_username and auth_password required when auth_type='basic'")
    
    return True


def migrate_aws_to_rest_config(
    aws_config: AWSIcebergConfig,
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
    
    return RESTIcebergConfig(
        rest_uri=catalog_uri,
        warehouse_id=warehouse_id,
        endpoint_url=None,  # Keep using AWS S3 (no custom endpoint)
        aws_region=aws_config.auth.get("client.region"),
        aws_access_key_id=aws_config.auth.get("client.access-key-id"),
        aws_secret_access_key=aws_config.auth.get("client.secret-access-key"),
        aws_session_token=aws_config.auth.get("client.session-token"),
        auth_type=auth_type,
        auth_token=catalog_token
    )


def get_config_examples() -> Dict[str, RESTIcebergConfig]:
    """
    Get a dictionary of example REST Iceberg configurations.
    
    :return: Dictionary mapping example names to configurations
    """
    return {
        "local": create_local_rest_config(),
        "cloudflare_r2": create_r2_config(
            account_id="example-account",
            access_key_id="r2-access-key",
            secret_access_key="r2-secret-key",
            catalog_uri="https://catalog.example.com/api/v1",
            catalog_token="your-catalog-token"
        ),
        "aws_s3_rest": create_s3_rest_config(
            catalog_uri="https://catalog.example.com/api/v1",
            warehouse_id="production",
            aws_region="us-east-1",
            aws_access_key_id="aws-access-key",
            aws_secret_access_key="aws-secret-key",
            catalog_token="your-catalog-token"
        ),
        "minio": create_local_rest_config(
            catalog_port=8181,
            minio_port=9000,
            warehouse_id="development"
        ),
    }


def print_config_example(example_name: str) -> None:
    """
    Print a configuration example with sample code.
    
    :param example_name: Name of the example ("local", "cloudflare_r2", "aws_s3_rest", "minio")
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
    print("from quixstreams.sinks.community.iceberg_rest import IcebergSink, create_local_rest_config")
    print()
    
    if example_name == "local":
        print("# Create local development configuration")
        print("config = create_local_rest_config()")
    else:
        print(f"# Create {example_name} configuration")
        print(f"config = {_config_to_code(config)}")
    
    print()
    print("# Create the sink")
    print("sink = IcebergSink(")
    print('    table_name="crypto.trades",') 
    print("    config=config,")
    print('    data_catalog_spec="rest"')
    print(")")
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
    lines.append(f'    rest_uri="{config.rest_uri}",')
    lines.append(f'    warehouse_id="{config.warehouse_id}",')
    
    if config.endpoint_url:
        lines.append(f'    endpoint_url="{config.endpoint_url}",')
    if config.aws_region:
        lines.append(f'    aws_region="{config.aws_region}",')
    if config.aws_access_key_id:
        lines.append(f'    aws_access_key_id="{config.aws_access_key_id}",')
    if config.aws_secret_access_key:
        lines.append(f'    aws_secret_access_key="{config.aws_secret_access_key}",')
    if config.auth_type != "none":
        lines.append(f'    auth_type="{config.auth_type}",')
    if config.auth_token:
        lines.append(f'    auth_token="{config.auth_token}",')
    
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
            import socket
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
