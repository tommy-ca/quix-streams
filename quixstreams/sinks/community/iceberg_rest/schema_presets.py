"""Built-in schema presets for the Iceberg REST sink."""

from __future__ import annotations

from typing import Dict, List


_PRESETS: Dict[str, Dict[str, List[Dict[str, object]]]] = {
    "crypto_trades": {
        "fields": [
            {"name": "exchange", "type": "string", "required": True},
            {"name": "symbol", "type": "string", "required": True},
            {"name": "price", "type": "double"},
            {"name": "quantity", "type": "double"},
            {"name": "trade_time", "type": "long"},
        ],
        "partition_fields": [{"name": "event_date"}],
    },
    "iot_timeseries": {
        "fields": [
            {"name": "device_id", "type": "string", "required": True},
            {"name": "reading", "type": "double"},
            {"name": "ts", "type": "long", "required": True},
        ],
        "partition_fields": [{"name": "event_date"}],
    },
}


def load_schema_preset(name: str) -> Dict[str, List[Dict[str, object]]]:
    try:
        preset = _PRESETS[name]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise KeyError(f"Unknown schema preset '{name}'") from exc

    return {
        "fields": [dict(field) for field in preset.get("fields", [])],
        "partition_fields": [dict(field) for field in preset.get("partition_fields", [])],
    }


def available_presets() -> List[str]:
    return sorted(_PRESETS.keys())

