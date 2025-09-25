"""Integration harness expectations for Iceberg REST sink."""

from __future__ import annotations

def test_rest_catalog_fixture_provides_catalog(rest_catalog):
    catalog = rest_catalog()
    assert hasattr(catalog, "load_table"), "expected catalog to expose load_table"


def test_minio_bucket_fixture_returns_bucket(minio_bucket):
    bucket = minio_bucket()
    assert bucket.startswith("s3://"), "expected S3-style bucket uri"
