import pytest

# Skip the entire RocksDB test subtree if rocksdict is not available
pytest.importorskip("rocksdict", reason="rocksdict not available")