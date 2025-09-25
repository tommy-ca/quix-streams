# Project Tasks – Crypto Sources and Iceberg REST Sink

Status: Living backlog (auto-updated via PRs)
Owners: Data Integrations
Last Updated: 2025-09-24

Guiding principles
- START SMALL: Ship thin, independent increments per milestone
- KISS: Minimal surface area, explicit configuration, no magic
- SOLID: Clear responsibilities (Config vs Client vs Sink vs Source)
- DRY: One canonical name/implementation per concept
- NO MOCKS: Prefer fakes/fixtures; deterministic tests
- NO LEGACY / NO COMPATIBILITY: Specs focus on Pydantic v2 configs; legacy helpers documented only for migration
- CONSISTENT NAMING: lower_snake_case for fields; uniform logs; consistent topic/key/ts rules
- Python defaults: uv for env, ruff for lint

Milestones and schedule
- M1 – Docs finalize (start small)
  - Deliver: Complete examples/acceptance criteria, tighten specs
  - ETA: 0.5 day
- M2 – Test reconciliation (Iceberg)
  - Deliver: Align token immutability and catalog validation semantics with tests
  - ETA: 0.5–1 day
- M3 – Env matrix + missing test coverage (Crypto)
  - Deliver: Env var matrix for Binance S3; CSV headerless + nested gzip tests
  - ETA: 1 day
- M4 – End-to-end doc examples + fake client
  - Deliver: In-memory/fake client snippet; confirm timestamp naming end-to-end
  - ETA: 0.5–1 day
- M5 – Review & publish
  - Deliver: ruff pass, docs PR, publish plan
  - ETA: 0.5 day

Task backlog

A. Documentation – Crypto Sources
1) Env matrix for Binance S3 load_from_env
- Description: Document all environment variables supported by load_from_env(BinanceS3Config), including prefix_template variables and access_mode specifics
- Output: Section in docs/specs/sources/crypto.md
- Priority: P1
- Owner: DI
- Dependencies: None
- Tags: @docs @crypto @spec

2) Clarify symbol normalization and case conventions
- Description: Document CCXT symbol formatting vs Binance CSV path-derived symbols; define expected casing and normalization per source
- Output: Crypto spec update; add examples
- Priority: P2
- Owner: DI
- Dependencies: Tests mapping review
- Tags: @docs @crypto @spec

3) Example hardening (uv + ruff)
- Description: Ensure examples run with uv env; pass ruff check; keep minimal
- Output: Example blocks verified
- Priority: P2
- Owner: DI
- Dependencies: M1
- Tags: @docs @spec @tooling

B. Documentation – Iceberg REST Sink
4) Confirm/document timestamp conventions E2E
- Description: Define the canonical timestamp field mapping from crypto sources to sink payloads (ts_event -> payload timestamp field)
- Output: Section in docs/specs/sinks/iceberg_rest.md
- Priority: P1
- Owner: DI
- Dependencies: Crypto spec finalized
- Tags: @docs @iceberg @spec

5) Provide minimal in-memory fake client example
- Description: Add a doc/example for a local non-network fake REST client (for illustration), aligned with NO MOCKS
- Output: Example snippet in Iceberg spec
- Priority: P3
- Owner: DI
- Dependencies: None
- Tags: @docs @iceberg @examples

C. Test reconciliation – Iceberg
6) Token immutability: tests vs implementation
- Description: Update tests to set token via create_config/CatalogConfig and avoid post-construction mutation OR enforce sink to reject runtime token changes
- Proposal: Update tests (minimal code change; respects KISS & SOLID)
- Output: PR to tests/test_iceberg_rest.py (test_write_calls_rest_endpoint_with_token_header)
- Priority: P0
- Owner: DI
- Dependencies: None
- Status: In Progress
- Decision: Token set only at construction (immutable thereafter)
- Tags: @tests @iceberg @p0

7) Fail-fast catalog validation location
- Description: Decide where empty/invalid catalog URI is rejected:
  - Option A (preferred): Config-layer (CatalogConfig) raises ValueError
  - Option B: Sink calls validate_sink_config(config) and raises ConfigurationError early
- Proposal: Adjust tests to expect config-layer error; add explicit config construction in tests
- Output: PR to tests and/or add sink validation call
- Priority: P0
- Owner: DI
- Dependencies: Decision
- Status: In Progress
- Decision: Option A (config-layer validation) – expect ValueError on invalid CatalogConfig
- Tags: @tests @iceberg @p0

D. Additional testing – Crypto
8) CSV headerless trades test
- Description: Add a focused unit test for headerless trades CSV parsing
- Output: New test under tests/test_quixstreams/.../binance_s3/
- Priority: P2
- Owner: DI
- Dependencies: None
- Tags: @tests @crypto

9) Nested gzip-in-zip parsing test
- Description: Add a unit test that covers .zip containing .gz containing JSONL/CSV
- Output: New test under tests/test_quixstreams/.../binance_s3/
- Priority: P2
- Owner: DI
- Dependencies: None
- Tags: @tests @crypto

10) Cross-source timestamp mapping to sink
- Description: Add an integration-style test that verifies ts_event from sources maps correctly through sink payload builder
- Output: New test under tests/integration/
- Priority: P3
- Owner: DI
- Dependencies: Task 4
- Tags: @tests @integration @crypto @iceberg

E. Review & publish
11) Docs review and publish
- Description: Run ruff on code snippets; consistency pass; open docs PR; add links to README
- Output: PR with updated docs/specs and README pointers
- Priority: P1
- Owner: DI
- Dependencies: M1–M4
- Tags: @docs @spec @release

Rescheduling and plan (START SMALL)
- Today's Plan (@docs/tasks selection)
  - In Progress (P0): Task 6 – Token immutability (tests) [This session]
  - In Progress (P0): Task 7 – Catalog validation Option A (config-layer) [This session]
  - In Progress (P1): Task 1 – Env matrix for Binance S3 (docs) [This session]
  - In Progress (P1): Task 4 – Timestamp conventions E2E (docs) [This session]
  - Next (P1): Task 11 – Docs review & publish (after tasks above)
- Near term (Next 1–2 days) – M3/M4
  - P2: Tasks 8–9 (additional crypto tests)
  - P3: Task 5 (fake client example) and Task 10 (cross-source timestamp integration)
- After (M5)
  - Final doc PR and sign-off

Risk notes and mitigations
- Divergent test expectations vs spec: address with smallest diffs to tests; prefer config-layer validation and construction-time token setting
- Naming drift: enforce consistent field names in examples and logs
- Over-engineering: keep examples minimal; avoid adding compatibility layers

Tracking
- Update this document in each PR touching docs/specs or tests
- Reference task IDs in commit messages, e.g., [Task-6] Update token immutability tests
