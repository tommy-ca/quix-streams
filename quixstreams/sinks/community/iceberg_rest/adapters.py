#!/usr/bin/env python3
"""
Pluggable adapter system for Iceberg REST sink.

Provides extensible adapters for serialization, monitoring, and other concerns.
Enables configuration-driven customization without code changes.
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Protocol
from dataclasses import dataclass, field


logger = logging.getLogger(__name__)


class SerializationAdapter(Protocol):
    """Protocol for serialization adapters."""
    
    def serialize(self, data: Dict[str, Any]) -> bytes:
        """Serialize data to bytes."""
        ...
    
    def deserialize(self, data: bytes) -> Dict[str, Any]:
        """Deserialize bytes to data."""
        ...


class MonitoringAdapter(Protocol):
    """Protocol for monitoring adapters."""
    
    def increment_counter(self, name: str, labels: Dict[str, str] = None):
        """Increment a counter metric."""
        ...
    
    def record_histogram(self, name: str, value: float, labels: Dict[str, str] = None):
        """Record a histogram value."""
        ...
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        ...


class DefaultSerializationAdapter:
    """Default JSON serialization adapter."""
    
    def serialize(self, data: Dict[str, Any]) -> bytes:
        """Serialize data to JSON bytes."""
        return json.dumps(data, separators=(',', ':')).encode('utf-8')
    
    def deserialize(self, data: bytes) -> Dict[str, Any]:
        """Deserialize JSON bytes to data."""
        return json.loads(data.decode('utf-8'))


class DefaultMonitoringAdapter:
    """Default in-memory monitoring adapter."""
    
    def __init__(self):
        self.counters: Dict[str, int] = {}
        self.histograms: Dict[str, List[float]] = {}
    
    def increment_counter(self, name: str, labels: Dict[str, str] = None):
        """Increment a counter metric."""
        key = self._make_key(name, labels)
        self.counters[key] = self.counters.get(key, 0) + 1
        logger.debug(f"Incremented counter {key} to {self.counters[key]}")
    
    def record_histogram(self, name: str, value: float, labels: Dict[str, str] = None):
        """Record a histogram value."""
        key = self._make_key(name, labels)
        if key not in self.histograms:
            self.histograms[key] = []
        self.histograms[key].append(value)
        logger.debug(f"Recorded histogram {key}: {value}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return {
            'counters': self.counters.copy(),
            'histograms': self.histograms.copy()
        }
    
    def _make_key(self, name: str, labels: Dict[str, str] = None) -> str:
        """Create metric key from name and labels."""
        if labels:
            label_str = ','.join(f"{k}={v}" for k, v in sorted(labels.items()))
            return f"{name}[{label_str}]"
        return name


class SerializationAdapterRegistry:
    """Registry for serialization adapters."""
    
    def __init__(self):
        self._adapters: Dict[str, SerializationAdapter] = {}
        self._default_adapter = DefaultSerializationAdapter()
        self.register('default', self._default_adapter)
    
    def register(self, name: str, adapter: SerializationAdapter):
        """Register a serialization adapter."""
        self._adapters[name] = adapter
        logger.info(f"Registered serialization adapter: {name}")
    
    def get_adapter(self, name: str, fallback: Optional[str] = 'default') -> SerializationAdapter:
        """Get a serialization adapter by name."""
        if name in self._adapters:
            return self._adapters[name]
        
        if fallback is not None and fallback in self._adapters:
            logger.warning(f"Adapter '{name}' not found, using fallback '{fallback}'")
            return self._adapters[fallback]
        
        raise KeyError(f"Serialization adapter '{name}' not found and no valid fallback")
    
    def list_adapters(self) -> List[str]:
        """List available adapter names."""
        return list(self._adapters.keys())


class MonitoringAdapterRegistry:
    """Registry for monitoring adapters."""
    
    def __init__(self):
        self._adapters: Dict[str, MonitoringAdapter] = {}
        self._default_adapter = DefaultMonitoringAdapter()
        self.register('default', self._default_adapter)
    
    def register(self, name: str, adapter: MonitoringAdapter):
        """Register a monitoring adapter."""
        self._adapters[name] = adapter
        logger.info(f"Registered monitoring adapter: {name}")
    
    def get_adapter(self, name: str, fallback: Optional[str] = 'default') -> MonitoringAdapter:
        """Get a monitoring adapter by name."""
        if name in self._adapters:
            return self._adapters[name]
        
        if fallback is not None and fallback in self._adapters:
            logger.warning(f"Adapter '{name}' not found, using fallback '{fallback}'")
            return self._adapters[fallback]
        
        raise KeyError(f"Monitoring adapter '{name}' not found and no valid fallback")
    
    def get_adapters(self, names: List[str]) -> List[MonitoringAdapter]:
        """Get multiple monitoring adapters by names."""
        adapters = []
        for name in names:
            adapters.append(self.get_adapter(name))
        return adapters
    
    def list_adapters(self) -> List[str]:
        """List available adapter names."""
        return list(self._adapters.keys())