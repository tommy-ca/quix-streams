# Agents Memory - QuixStreams Crypto Lakehouse Project

This document maintains shared memory across all agent interactions for consistent project context and decision tracking.

## Project Context (Linked from CLAUDE.md)

**Primary Project**: Apache Iceberg REST Sink for QuixStreams
**Architecture**: Schema-agnostic streaming data lakehouse integration
**Status**: 70% complete (4600+ lines), implementation-ready

### Key Project Links
- **Main Documentation**: [CLAUDE.md](./CLAUDE.md) - Kiro spec-driven development workflow
- **Specifications**: `.kiro/specs/iceberg-rest-sink-production/` - Complete spec documentation

## Active Development Context

### Current Implementation Status
- ✅ **Core Infrastructure Complete** (70%): Configuration, error handling, REST client, storage abstraction
- 🚧 **Remaining Work** (30%): Iceberg table operations, observability, testing, schema parameterization
- 📋 **Tasks**: 18 focused tasks for completion

### Architecture Decisions Made
1. **Schema-Agnostic Design**: Schemas passed as parameters instead of hardcoded domain logic
2. **Multi-Provider Storage**: AWS S3, Cloudflare R2, MinIO through unified abstraction
3. **SOLID Principles**: Clean separation of concerns, extensible configuration
4. **Performance Optimizations**: Adaptive batching, memory management, JSON optimization

### Key Implementation Files
- `quixstreams/sinks/community/iceberg_rest/sink.py` (1009 lines) - Main sink implementation
- `quixstreams/sinks/community/iceberg_rest/config.py` (750 lines) - Configuration system
- `quixstreams/sinks/community/iceberg_rest/client.py` (398 lines) - REST catalog client
- `quixstreams/sinks/community/iceberg_rest/errors.py` (278 lines) - Error hierarchy

## Recent Decisions and Context

### Major Architectural Simplification (Latest)
- **Decision**: Made IcebergRESTSink schema-agnostic
- **Rationale**: Remove hardcoded crypto schemas, use parameterized schemas instead
- **Impact**: Reduced complexity, increased flexibility, supports any domain through configuration
- **Implementation**: Schemas and partitioning strategies passed as parameters

### Engineering Principles Applied
- **NO COMPATIBILITY/LEGACY**: Clean modern implementation only
- **CONSISTENT NAMING**: IcebergRESTSink (distinguishes from AWS Glue-based sink)
- **SOLID PRINCIPLES**: Single responsibility, open/closed, clean interfaces
- **KISS/DRY/YAGNI**: Simple, no duplication, essential features only

## Agent Guidelines

### When Working on This Project
1. **Check Implementation Status**: 70% complete, focus on remaining 30%
2. **Maintain Schema Agnostic Approach**: No hardcoded domain schemas
3. **Follow Task Priority**: Iceberg integration → Observability → Testing → Production
4. **Reference Existing Code**: 4600+ lines already implemented with production patterns
5. **Update This Memory**: Add significant decisions and context changes

### Key Constraints
- **No Crypto Schema Hardcoding**: Use parameterized schemas instead
- **Distinguish from Standard Sink**: Use "IcebergRESTSink" naming (not just "IcebergSink")
- **Follow Existing Patterns**: Configuration system, error hierarchy, performance optimizations
- **Maintain SOLID Principles**: Clean separation of concerns

### Implementation Priorities
1. **Task 1.1-1.2**: Complete Iceberg table operations (pyiceberg integration, Parquet writing)
2. **Task 2.1-2.2**: Add observability (metrics, health checks, monitoring)
3. **Task 3.1-3.2**: Implement schema parameterization support
4. **Task 4.1-4.2**: Build comprehensive testing infrastructure

### Spec-Driven Development Flow (Kiro Commands)
- Specs live under `.kiro/specs/` and are orchestrated via the command definitions stored in `.claude/commands/kiro/` (e.g., `spec-init.md`, `spec-requirements.md`, `spec-design.md`, `spec-tasks.md`, `spec-impl.md`, `spec-status.md`).
- `/kiro:spec-init <description>` (see `spec-init.md`) creates a new spec folder, writes `spec.json`, seeds `requirements.md`, and registers the feature in `CLAUDE.md`; subsequent phases depend on this metadata.
- `/kiro:spec-requirements <feature>` (see `spec-requirements.md`) generates EARS-formatted requirements after loading steering context; it updates `spec.json` to `requirements-generated` and marks requirements as pending approval.
- `/kiro:spec-design <feature> [-y]` (see `spec-design.md`) requires approved requirements, loads steering plus existing artifacts, and produces `design.md` with architecture decisions, diagrams, and risk analysis before flagging the design state in `spec.json`.
- `/kiro:spec-tasks <feature> [-y]` (see `spec-tasks.md`) consumes approved design/requirements and emits numbered implementation tasks in `tasks.md`, enforcing two-level hierarchy and requirement traceability while updating the spec metadata to `tasks-generated`.
- Implementation proceeds with `/kiro:spec-impl` commands, while `/kiro:spec-status`, `spec-impl`, `spec-design`, and `spec-tasks` share the gating/approval workflow described above; every phase update toggles the relevant `approvals.*` flags in `spec.json` and keeps the project aligned with SOLID, KISS, DRY, YAGNI, and TDD expectations captured in steering.

## Cross-References

### From CLAUDE.md
- **Active Specification**: `iceberg-rest-sink-production` (implementation-ready)
- **Kiro Commands**: `/kiro:spec-status`, `/kiro:spec-tasks`, etc.
- **Development Workflow**: Requirements → Design → Tasks → Implementation

### To CLAUDE.md
This AGENTS.md file provides shared memory context for all agent interactions, ensuring consistent understanding of project status, architecture decisions, and implementation priorities.

## Memory Updates

### Latest Context (Current Session)
- **Cycle 5 (2025-09-23)**: PyIceberg-backed catalog factory, partition spec synthesis, and transactional commit retries with rollback landed under TDD (TSK-010/011).
- **Schema & Presets**: Config-driven presets and partition templates remain green from Cycle 3.
- **Observability**: Prometheus export, alert thresholds, and logging controls stable (Cycle 2).
- **Status**: Implementation at 95%+, spec synced with Kiro; only production validation tasks remain.
- **Architecture**: Schema-agnostic design finalized; Iceberg lifecycle now drives real pyIceberg tables.
- **Tasks**: 2 remaining (production validation & readiness checks).
- **Ready**: Focus shifts to performance validation and deployment readiness once scheduled.

---

**Note**: This file should be updated by agents when making significant decisions or discovering important project context to maintain consistency across all interactions.
