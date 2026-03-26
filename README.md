# The Ledger — Weeks 9-10 Starter Code

This starter is intentionally scaffold-heavy. Phase 0 is mostly complete; the
event store, aggregates, agents, projections, and MCP layer still need to be
implemented.

## Interim Submission Quick Start
```bash
# 1. Install dependencies
uv sync

# 2. Start PostgreSQL
docker run -d -e POSTGRES_PASSWORD=apex -e POSTGRES_DB=ledger -p 5432:5432 postgres:16

# 3. Apply the core event-store schema
psql postgresql://postgres:apex@localhost/ledger -f src/schema.sql

# 4. Seed registry data + events
uv run python datagen/generate_all.py --db-url postgresql://postgres:apex@localhost/ledger

# 5. Run the interim-facing tests
uv run pytest tests/test_concurrency.py -q
uv run pytest tests/test_loan_application_aggregate.py -q
uv run pytest tests/test_event_store.py -q
```

## LLM Configuration
Agents use an Anthropic-compatible async client. You can run them against:

- Anthropic directly via `ANTHROPIC_API_KEY`
- OpenRouter via `OPENROUTER_API_KEY`

Environment knobs:
- `OPENROUTER_API_KEY`
- `OPENROUTER_BASE_URL` defaults to `https://openrouter.ai/api`
- `OPENROUTER_MODEL` for an OpenRouter-specific model slug
- `LLM_MODEL` for a generic override regardless of provider

The client factory lives in `ledger/llm/client.py`.

## Canonical Files For Review
- `src/schema.sql` — PostgreSQL schema for `events`, `event_streams`, `projection_checkpoints`, and `outbox`
- `src/event_store.py` — interim-facing async event store surface
- `src/models/events.py` — event models plus `StoredEvent`, `StreamMetadata`, and domain exceptions
- `src/aggregates/loan_application.py` — interim-facing aggregate entry point
- `src/aggregates/agent_session.py` — session replay aggregate for Gas Town-style recovery metadata
- `src/commands/handlers.py` — command handlers using load -> validate -> determine -> append
- `tests/test_concurrency.py` — dedicated double-decision OCC test

## Project Quick Start
The `ledger/` package remains the canonical implementation surface. The `src/`
files above are thin interim-submission adapters layered on top of it so the
artifact names match the checkpoint checklist exactly.

## What Works Out of the Box
- Full event schema (45 event types) — `ledger/schema/events.py`
- Complete data generator (GAAP PDFs, Excel, CSV, 1,800+ seed events)
- Event simulator (all 5 agent pipelines, deterministic)
- Schema validator (validates all events against EVENT_REGISTRY)
- Phase 0 tests: 10/10 passing

## Canonical Implementation Surface
Use these files as the source of truth while you build:
- `ledger/event_store.py` for the PostgreSQL store and in-memory contract
- `ledger/registry/client.py` for read-only Applicant Registry access
- `ledger/domain/aggregates/loan_application.py` for the first aggregate
- `ledger/agents/base_agent.py` for shared agent runtime only
- `ledger/agents/credit_analysis_agent.py` for the reference agent
- `ledger/agents/stub_agents.py` for the remaining four agent scaffolds
- `ledger/agents/__init__.py` for canonical package imports

## What You Implement
| Component | File | Phase |
|-----------|------|-------|
| EventStore | `ledger/event_store.py` | 1 |
| ApplicantRegistryClient | `ledger/registry/client.py` | 1 |
| Domain aggregates | `ledger/domain/aggregates/` | 2 |
| DocumentProcessingAgent | `ledger/agents/stub_agents.py` | 2 |
| CreditAnalysisAgent | `ledger/agents/credit_analysis_agent.py` | 2 (reference given) |
| FraudDetectionAgent | `ledger/agents/stub_agents.py` | 3 |
| ComplianceAgent | `ledger/agents/stub_agents.py` | 3 |
| DecisionOrchestratorAgent | `ledger/agents/stub_agents.py` | 3 |
| Projections + daemon | `ledger/projections/` | 4 |
| Upcasters | `ledger/upcasters.py` | 4 |
| MCP server | `ledger/mcp_server.py` (to add) | 5 |

## Gate Tests by Phase
```bash
pytest tests/test_schema_and_generator.py -v  # Phase 0: all must pass before Phase 1
pytest tests/phase1/test_event_store.py -v   # Phase 1 contract tests (in-memory)
pytest tests/test_event_store.py -v          # Phase 1 real-DB tests
pytest tests/test_narratives.py -v           # Narrative scenarios (currently scaffolded/skipped)
```

## Week 3 Bridge (Document Corpus -> DocumentProcessingAgent)
This workspace now includes a Week 3 extraction adapter used by DocumentProcessingAgent:
- `ledger/agents/week3_adapter.py`
- `ledger/agents/stub_agents.py` (DocumentProcessingAgent nodes call the adapter)

The adapter supports either:
1. Local fallback extraction (deterministic facts, no external package required), or
2. Your Week 3 extraction pipeline via environment variables.

Configure external Week 3 extractor:
```bash
export WEEK3_PIPELINE_PATH=/absolute/path/to/week3/repo
export WEEK3_EXTRACTOR_MODULE=document_refinery.pipeline
export WEEK3_EXTRACTOR_FUNCTION=extract_financial_facts
```

Validate corpus + bridge quickly:
```bash
.venv311/bin/python scripts/validate_week3_bridge.py --docs-dir documents --min-files 160
```

Run focused Phase 2 tests:
```bash
.venv311/bin/pytest tests/phase2/test_week3_bridge.py -v
```
