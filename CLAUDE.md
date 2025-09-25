# Claude Code Spec-Driven Development

Kiro-style Spec Driven Development implementation using claude code slash commands, hooks and agents.

## Shared Agent Memory
**📋 Agents Memory**: [AGENTS.md](./AGENTS.md) - Shared context and decisions across all agent interactions
- Current project status, architecture decisions, and implementation priorities
- Updated by agents to maintain consistency across sessions
- **Always reference for project context before starting work**

## Project Context

### Paths
- Steering: `.kiro/steering/`
- Specs: `.kiro/specs/`
- Commands: `.claude/commands/`

### Steering vs Specification

**Steering** (`.kiro/steering/`) - Guide AI with project-wide rules and context
**Specs** (`.kiro/specs/`) - Formalize development process for individual features

### Active Specifications
- **iceberg-rest-sink-production**: Production-ready Iceberg REST sink with REST catalog integration
  - **Status**: ✅ **COMPLETED** - Full implementation with comprehensive testing
  - **Features**: Multi-provider S3 support, schema evolution, error hierarchy, retry logic
  - **Testing**: 15+ test scenarios, unit/integration/e2e coverage
  - **Documentation**: Complete API reference, usage examples, deployment guides

- **crypto-sources-production**: Comprehensive crypto data sources with Pydantic v2 configuration
  - **Status**: ✅ **COMPLETED** - Full implementation with AGENTS pattern
  - **Sources**: Cryptofeed, CCXT, BinanceS3 with unified configuration system
  - **Features**: Secure credential handling, environment loading, migration support
  - **Testing**: Unit tests, integration validation, real data flow verification

- **crypto-lakehouse-package**: End-to-end crypto lakehouse pipeline integration
  - **Status**: ✅ **COMPLETED** - Comprehensive implementation and testing
  - **Components**: Sources → Processing → Iceberg sinks with full observability
  - **Infrastructure**: Docker Compose stacks, deployment automation
  - **Testing**: End-to-end pipeline validation with real data flows

- **sinks-sources-refactor**: Modern architecture refactoring with engineering principles
  - **Status**: ✅ **COMPLETED** - TDD-driven refactoring following SOLID, KISS, DRY
  - **Achievements**: Clean modular design, comprehensive test coverage, documentation
  - **Quality**: Real object testing, performance benchmarks, CI/CD integration

- Use `/kiro:spec-status [feature-name]` to check detailed progress and metrics

## Development Guidelines
- Think in English, generate responses in English

## Workflow

### Phase 0: Steering (Optional)
`/kiro:steering` - Create/update steering documents
`/kiro:steering-custom` - Create custom steering for specialized contexts

Note: Optional for new features or small additions. You can proceed directly to spec-init.

### Phase 1: Specification Creation
1. `/kiro:spec-init [detailed description]` - Initialize spec with detailed project description
2. `/kiro:spec-requirements [feature]` - Generate requirements document
3. `/kiro:spec-design [feature]` - Interactive: "Have you reviewed requirements.md? [y/N]"
4. `/kiro:spec-tasks [feature]` - Interactive: Confirms both requirements and design review

### Phase 2: Progress Tracking
`/kiro:spec-status [feature]` - Check current progress and phases

## Project Structure

### Core Components
```
quix-streams/
├── quixstreams/
│   ├── sinks/community/iceberg_rest/    # Iceberg REST sink implementation
│   │   ├── sink.py                      # Main sink class
│   │   ├── config.py                    # Configuration management
│   │   ├── errors.py                    # Error hierarchy
│   │   ├── config_v2.py                 # Pydantic v2 configuration
│   │   └── tests/                       # Comprehensive test suite
│   └── sources/community/crypto/        # Crypto data sources
│       ├── cryptofeed_source.py         # Cryptofeed integration
│       ├── ccxt_source.py               # CCXT exchange integration
│       ├── binance_s3_source.py         # Binance S3 historical data
│       ├── simple_config.py             # Pydantic v2 configuration
│       └── tests/                       # Unit and integration tests
├── tests/
│   ├── e2e/                             # End-to-end pipeline tests
│   ├── integration/                     # Cross-component integration
│   ├── unit/                            # Component unit tests
│   └── performance/                     # Performance benchmarks
├── examples/
│   ├── crypto/                          # Crypto data pipeline examples
│   └── crypto-lakehouse/                # Full lakehouse examples
├── infra/
│   ├── local-dev-stack/                 # Docker Compose local environment
│   ├── lakekeeper/                      # Lakekeeper REST catalog setup
│   └── e2e-crypto-lakehouse/            # End-to-end testing infrastructure
├── docs/
│   ├── specs/                           # Technical specifications
│   ├── architecture/                    # Architecture documentation
│   └── integration/                     # Integration patterns
└── .kiro/                               # Kiro spec-driven development
```

### Test Organization
- **Unit Tests**: Component isolation, fast execution, comprehensive coverage
- **Integration Tests**: Cross-component interaction, real data flows
- **End-to-End Tests**: Full pipeline validation, realistic scenarios
- **Performance Tests**: Throughput, latency, memory benchmarks

## Project Engineering Principles

### Core Engineering Rules
- **SOLID**: Single responsibility, Open/closed, Liskov substitution, Interface segregation, Dependency inversion
- **KISS**: Keep implementations simple, readable, and maintainable
- **DRY**: Extract common utilities, avoid code duplication
- **NO MOCKS**: Use real objects for testing, verify actual behavior
- **NO LEGACY**: Modern tools and patterns, no deprecated dependencies
- **CONSISTENT NAMING**: Clear, descriptive names following project conventions
- **START SMALL**: Incremental development with iterative improvement

### Test-Driven Development (TDD)
- **RED Phase**: Write failing tests first, define expected behavior
- **GREEN Phase**: Implement minimal code to pass tests
- **REFACTOR Phase**: Improve code quality while maintaining functionality
- **Real Data Testing**: Use actual data sources and sinks, avoid mocking

### Configuration Management
- **Pydantic v2**: Strict validation, immutable configurations
- **Environment Variables**: Support for containerized deployments
- **Migration Support**: Backward compatibility with legacy configurations
- **Secret Management**: Secure handling and masking of sensitive data

### Error Handling
- **Hierarchical Errors**: Structured error classes for different failure modes
- **Retry Logic**: Exponential backoff for transient failures
- **Graceful Degradation**: Continue processing when possible
- **Detailed Context**: Rich error information for debugging

### Performance Optimization
- **Batch Processing**: Configurable batch sizes for throughput optimization
- **Connection Pooling**: Efficient resource utilization
- **Schema Evolution**: Automatic detection and compatibility validation
- **Memory Management**: Bounded memory usage, efficient data structures

## Context Engineering Best Practices

### Agent Memory Management
- **Persistent Context**: Use [AGENTS.md](./AGENTS.md) for cross-session consistency
- **Decision Tracking**: Document architectural decisions and rationale
- **Progress Updates**: Maintain current implementation status
- **Dependency Mapping**: Track inter-component relationships

### Code Organization
- **Modular Design**: Clear separation of concerns, minimal coupling
- **Interface Contracts**: Well-defined APIs between components
- **Documentation**: Comprehensive docstrings, usage examples
- **Type Safety**: Strong typing with Python type hints

### Development Workflow
- **Specification-Driven**: Use Kiro specs for structured development
- **Incremental Delivery**: Small, focused commits with clear messages
- **Continuous Integration**: Automated testing at multiple levels
- **Code Review**: Peer review for quality assurance

### Quality Assurance
- **Test Coverage**: Aim for comprehensive test coverage across all layers
- **Performance Monitoring**: Establish baselines and regression detection
- **Security**: Validate inputs, secure credential handling
- **Maintainability**: Regular refactoring, technical debt management

### Deployment Patterns
- **Multi-Environment Support**: Local development, staging, production configurations
- **Container-First**: Docker Compose for local stacks, Kubernetes for production
- **Infrastructure as Code**: Declarative infrastructure definitions
- **Environment Parity**: Consistent environments across development lifecycle

### Storage Integration
- **Multi-Provider Support**: AWS S3, Cloudflare R2, MinIO, local filesystems
- **REST Catalog**: Apache Iceberg REST catalog for metadata management
- **Schema Evolution**: Backward-compatible schema changes with validation
- **Partition Management**: Efficient data organization and query performance

## Development Rules
1. **Reference agent memory**: Check [AGENTS.md](./AGENTS.md) for current project context and decisions
2. **Consider steering**: Run `/kiro:steering` before major development (optional for new features)
3. **Follow 3-phase approval workflow**: Requirements → Design → Tasks → Implementation
4. **Approval required**: Each phase requires human review (interactive prompt or manual)
5. **No skipping phases**: Design requires approved requirements; Tasks require approved design
6. **Update task status**: Mark tasks as completed when working on them
7. **Keep steering current**: Run `/kiro:steering` after significant changes
8. **Check spec compliance**: Use `/kiro:spec-status` to verify alignment
9. **Update agent memory**: Add significant decisions to [AGENTS.md](./AGENTS.md) for consistency
10. **Follow engineering principles**: Apply SOLID, KISS, DRY, NO MOCKS, NO LEGACY consistently
11. **Test-driven development**: Write tests first, implement incrementally
12. **Real object testing**: Use actual components, avoid mocking external dependencies

## Steering Configuration

### Current Steering Files
Managed by `/kiro:steering` command. Updates here reflect command changes.

### Active Steering Files
- `product.md`: Always included - Product context and business objectives
- `tech.md`: Always included - Technology stack and architectural decisions
- `structure.md`: Always included - File organization and code patterns

### Custom Steering Files
<!-- Added by /kiro:steering-custom command -->
<!-- Format:
- `filename.md`: Mode - Pattern(s) - Description
  Mode: Always|Conditional|Manual
  Pattern: File patterns for Conditional mode
-->

### Inclusion Modes
- **Always**: Loaded in every interaction (default)
- **Conditional**: Loaded for specific file patterns (e.g., "*.test.js")
- **Manual**: Reference with `@filename.md` syntax

