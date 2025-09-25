# Technical Design Document

## Engineering Principles

This design strictly follows project engineering principles:
- **KISS**: Simple, focused components with clear responsibilities
- **SOLID**: Single responsibility, dependency injection, interface segregation
- **DRY**: Shared configuration and utilities, no code duplication
- **YAGNI**: Build only what's needed now, avoid speculative features
- **NO MOCKS**: Tests use real implementations with minimal test doubles
- **NO LEGACY**: Clean slate implementation, no backward compatibility
- **START SMALL**: Minimal viable implementation, iterate based on usage
- **CONSISTENT NAMING**: Simple names without prefixes (Pipeline, not CryptoPipeline)
- **TDD**: Test-first development with clear functional requirements
- **FRs over NFRs**: Focus on functional requirements, not performance optimization

## System Overview

The package provides a simple configuration layer to connect crypto sources with the Iceberg sink. Core function: load config → create pipeline → run.

### Core Value Proposition
- Replace manual source+sink wiring with declarative configuration
- Provide basic templates for common crypto data patterns
- Enable quick pipeline prototyping and testing

## Simple Architecture

```
Config File ──► Pipeline ──► QuixStreams App ──► Iceberg Sink
                   │              │                   │
                   └──────────────┼───────────────────┘
                                  │
                            Data Processing
```

## Core Components

### 1. Pipeline (`Pipeline`)

**Single Responsibility**: Create and run QuixStreams application from config.

```python
class Pipeline:
    """Creates and runs QuixStreams pipeline from configuration."""
    
    def __init__(self, config: Config):
        self.config = config
        self.app = None
        
    def create(self) -> Application:
        """Create QuixStreams application with sources and sink."""
        self.app = Application(broker=self.config.kafka)
        
        # Add sources
        for source_config in self.config.sources:
            source = self._create_source(source_config)
            topic = self.app.topic(source_config.topic)
            source.start(topic)
            
        # Add sink
        sink = IcebergRESTSink(self.config.sink)
        topic = self.app.topic(self.config.sink_topic)
        sdf = self.app.dataframe(topic)
        sdf.sink(sink)
        
        return self.app
        
    def run(self) -> None:
        """Run the pipeline."""
        if not self.app:
            self.create()
        self.app.run()
        
    def _create_source(self, source_config: SourceConfig) -> Source:
        """Factory method for creating sources."""
        if source_config.type == "cryptofeed":
            return CryptofeedSource(source_config.params)
        elif source_config.type == "ccxt":
            return CCXTSource(source_config.params)
        elif source_config.type == "binance_s3":
            return BinanceS3Source(source_config.params)
        else:
            raise ValueError(f"Unknown source type: {source_config.type}")
```

### 2. Configuration (`Config`)

**Single Responsibility**: Load and validate pipeline configuration.

```python
@dataclass
class Config:
    """Pipeline configuration."""
    kafka: Dict[str, Any]
    sources: List[SourceConfig]
    sink: Dict[str, Any]
    sink_topic: str
    
    @classmethod
    def from_file(cls, path: str) -> 'Config':
        """Load configuration from YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)
        
    @classmethod
    def from_template(cls, template_name: str) -> 'Config':
        """Load configuration from template."""
        template_path = Path(__file__).parent / "templates" / f"{template_name}.yaml"
        return cls.from_file(template_path)

@dataclass
class SourceConfig:
    """Source configuration."""
    type: str  # cryptofeed, ccxt, binance_s3
    topic: str
    params: Dict[str, Any]
```

### 3. Templates (`Templates`)

**Single Responsibility**: Provide pre-configured pipeline templates.

Templates are simple YAML files:

```yaml
# templates/binance_spot.yaml
kafka:
  bootstrap_servers: "localhost:9092"

sources:
  - type: "cryptofeed"
    topic: "crypto.binance.spot"
    params:
      exchange: "BINANCE"
      symbols: ["BTCUSDT", "ETHUSDT"]
      channels: ["trades", "book"]

sink:
  catalog_uri: "http://localhost:8181"
  table_name: "crypto.binance_spot"
  
sink_topic: "crypto.binance.spot"
```

```yaml
# templates/multi_exchange.yaml
kafka:
  bootstrap_servers: "localhost:9092"

sources:
  - type: "cryptofeed"
    topic: "crypto.binance"
    params:
      exchange: "BINANCE"
      symbols: ["BTCUSDT"]
      channels: ["trades"]
      
  - type: "ccxt"
    topic: "crypto.coinbase"
    params:
      exchange: "coinbase"
      symbols: ["BTC/USD"]
      timeframe: "1m"

sink:
  catalog_uri: "http://localhost:8181"
  table_name: "crypto.multi_exchange"
  
sink_topic: "crypto.all"
```

## Usage

### Spec Linkage (2025-09-24)
- Crypto sources spec: docs/specs/sources/crypto.md (topic naming, keys/ts, env matrix)
- Iceberg REST sink spec: docs/specs/sinks/iceberg_rest.md (timestamp conventions, REST contract)
- Integration patterns doc: docs/integration/crypto-lakehouse-patterns.md (as created in this spec)

### Basic Usage

```python
from quixstreams.sources.community.crypto.lakehouse import Pipeline, Config

# From template
config = Config.from_template("binance_spot")
pipeline = Pipeline(config)
pipeline.run()

# From file
config = Config.from_file("my_pipeline.yaml")
pipeline = Pipeline(config)
pipeline.run()
```

### CLI Usage

```bash
# Run from template
quixstreams crypto run --template binance_spot

# Run from config file
quixstreams crypto run --config my_pipeline.yaml

# List available templates
quixstreams crypto templates
```

## Testing Strategy (TDD)

### Test Structure

```python
class TestPipeline:
    """Test pipeline creation and execution."""
    
    def test_create_from_config(self):
        """Test pipeline creation from configuration."""
        config = Config(
            kafka={"bootstrap_servers": "localhost:9092"},
            sources=[SourceConfig(
                type="cryptofeed",
                topic="test",
                params={"exchange": "BINANCE"}
            )],
            sink={"catalog_uri": "http://localhost:8181"},
            sink_topic="test"
        )
        
        pipeline = Pipeline(config)
        app = pipeline.create()
        
        assert app is not None
        assert len(app.topics) == 2  # source + sink topics
        
    def test_run_with_real_kafka(self):
        """Integration test with real Kafka and sources."""
        # Use real Kafka testcontainer
        # Use real crypto sources with paper trading
        # Use real Iceberg with in-memory catalog
        pass

class TestConfig:
    """Test configuration loading and validation."""
    
    def test_from_file(self):
        """Test loading config from YAML file."""
        pass
        
    def test_from_template(self):
        """Test loading config from template."""
        pass
        
    def test_validation(self):
        """Test config validation."""
        pass
```

### No Mocks Policy

Tests use real implementations:
- Real Kafka (testcontainers)
- Real crypto sources (paper trading mode or replay)
- Real Iceberg sink (in-memory catalog)
- Real file system for config loading

## Implementation Plan

### Phase 1: Minimal Viable Implementation (Week 1)

1. **Basic Configuration** (Day 1-2)
   - `Config` and `SourceConfig` classes
   - YAML file loading
   - Basic validation

2. **Simple Pipeline** (Day 3-4)
   - `Pipeline` class with source factory
   - QuixStreams application creation
   - Basic source-to-sink wiring

3. **Templates** (Day 5)
   - Two basic templates: `binance_spot`, `multi_exchange`
   - Template loading mechanism

### Phase 2: CLI and Testing (Week 2)

1. **CLI Interface** (Day 1-2)
   - Basic CLI commands: `run`, `templates`
   - Configuration file handling

2. **Test Suite** (Day 3-5)
   - Unit tests for all components
   - Integration tests with real services
   - Test containers setup

### Phase 3: Iteration Based on Usage (Week 3+)

Add features only when needed:
- Additional templates based on user requests
- Error handling improvements based on failure patterns
- Performance optimizations based on actual bottlenecks

## File Structure

```
quixstreams/sources/community/crypto/lakehouse/
├── __init__.py
├── pipeline.py          # Pipeline class
├── config.py            # Config classes
├── cli.py               # CLI interface
├── templates/
│   ├── binance_spot.yaml
│   ├── multi_exchange.yaml
│   └── research.yaml
└── tests/
    ├── test_pipeline.py
    ├── test_config.py
    ├── test_cli.py
    └── fixtures/
        └── test_configs/
```

## Summary

This design follows engineering principles by:

- **KISS**: Three simple classes with clear responsibilities
- **SOLID**: Each class has single responsibility, dependency injection
- **DRY**: Shared config format across templates
- **YAGNI**: Only builds what's needed for basic functionality
- **START SMALL**: Minimal viable implementation first
- **FRs over NFRs**: Focuses on connecting sources to sink, not optimization
- **TDD**: Clear test structure with real implementations
- **Simple naming**: Pipeline, Config, Templates (no prefixes)