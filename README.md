# The Ledger — Weeks 9-10 Starter Code

This starter is intentionally scaffold-heavy. Phase 0 is mostly complete; the
event store, aggregates, agents, projections, and MCP layer still need to be
implemented.

## Quick Start
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start PostgreSQL
docker run -d -e POSTGRES_PASSWORD=apex -e POSTGRES_DB=apex_ledger -p 5432:5432 postgres:16

# 3. Set environment
cp .env.example .env
# Edit .env — add your ANTHROPIC_API_KEY

# 4. Generate all data (companies + documents + seed events → DB)
python datagen/generate_all.py --db-url postgresql://postgres:apex@localhost/apex_ledger

# 5. Validate schema (no DB needed)
python datagen/generate_all.py --skip-db --skip-docs --validate-only

# 6. Run Phase 0 tests (must pass before starting Phase 1)
pytest tests/test_schema_and_generator.py -v

# 7. Begin Phase 1: implement EventStore
# Edit: ledger/event_store.py
# Test the in-memory contract first:
#   pytest tests/phase1/test_event_store.py -v
# Then run the real-DB tests when PostgreSQL is ready:
#   pytest tests/test_event_store.py -v
```

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
