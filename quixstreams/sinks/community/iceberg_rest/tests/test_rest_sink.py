"""
REST Sink Tests

Placeholder tests for the IcebergRESTSink implementation.
Will be expanded during VALIDATE-001-GREEN phase.

Author: TDD Sprint 3 - GREEN Phase
Date: September 19, 2025
"""

import pytest

from quixstreams.sinks.community.iceberg_rest import IcebergRESTSink, create_local_config


class TestIcebergRESTSink:
    """Tests covering IcebergRESTSink edge cases."""

    def test_process_nested_data_unknown_strategy_raises(self):
        config = create_local_config(table_name="test_events")
        sink = IcebergRESTSink(config=config)
        sink._data_flattening_strategy = "totally_unknown"

        with pytest.raises(ValueError):
            sink._process_nested_data({"foo": "bar"})


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
