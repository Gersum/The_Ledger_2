Project: The Ledger (starter)
Goal: End-to-end Agentic Document-to-Decision flow with UI process tracking and Week 3 bridge integration.

Current validated environment:
- OS: macOS
- Working DB URL: postgresql://postgres:postgres@localhost:5433/ledger
- UI server runs via uvicorn on port 8000

Major completed work:
1) Week 3 extraction bridge
- Added a configurable adapter to integrate Week 3 extraction pipeline with fallback mode.
- Supports env-based wiring and native invocation against /Users/gersumasfaw/Downloads/week3.
- Validated corpus + bridge and phase2 tests passed.

2) Document corpus requirement
- documents/ contains 400 files across 80 company dirs (>=160 expected files, met).
- Validation script confirms corpus and extraction bridge behavior.

3) DocumentProcessingAgent integration improvements
- Updated to emit schema-aligned document processing events.
- Uses adapter for extraction and supports quality assessment fallback when LLM unavailable.

4) Pipeline runner fixes
- scripts/run_pipeline.py was rewritten from outdated single-agent behavior to true phase routing:
  document, credit, fraud, compliance, decision, and all.
- Fixed usage of wrong/legacy agent import path.

5) Compliance graph bug fixed
- ComplianceAgent had lambda node wiring returning coroutines, causing LangGraph InvalidUpdateError.
- Replaced with async wrapper nodes for each rule evaluator.

6) UI full-flow control added
- Added new UI action “Run Full Pipeline”.
- Added backend endpoint POST /api/run_full_pipeline.
- Endpoint runs credit -> fraud -> compliance -> decision and returns per-phase outputs + final state.
- UI banner now shows phase-by-phase outputs and final state; tracker updates via existing projections refresh.

7) Projection runtime fixes
- compliance_audit projection was failing due to missing parse_dt and string timestamp handling.
- Added parse_dt and normalized completed_at to datetime, eliminating repeated projection errors.

Latest confirmed behavior:
- POST /api/run_full_pipeline returns status ok, phase outputs, and final_state (observed APPROVED).
- UI server currently starts cleanly and serves /api/applications + /api/events.
- Full flow from UI button works and updates process tracker in Applications view.

Known caveats:
- Terminal shell occasionally had heredoc contamination/noise (compdef errors) but core app behavior was validated via clean runs.
- Existing UI has process tracker in Applications table (stepper), not yet a dedicated dashboard tracker panel.
- Some historical app IDs may not be in the same lifecycle stage; endpoint handles this by selecting credit-ready app or seeding one.

Key files changed in this session:
- ledger/agents/week3_adapter.py
- ledger/agents/stub_agents.py
- scripts/validate_week3_bridge.py
- tests/phase2/test_week3_bridge.py
- scripts/run_pipeline.py
- ledger/ui/server.py
- ledger/ui/public/index.html
- ledger/ui/public/app.js
- ledger/projections/compliance_audit.py
- README.md
- .env (week3 bridge vars)

Important run commands:
- Validate week3 bridge:
  .venv311/bin/python scripts/validate_week3_bridge.py --docs-dir documents --min-files 160
- Run phase2 bridge tests:
  .venv311/bin/pytest tests/phase2/test_week3_bridge.py -v
- Start UI server:
  LEDGER_UI_DB_URL=postgresql://postgres:postgres@localhost:5433/ledger DATABASE_URL=postgresql://postgres:postgres@localhost:5433/ledger .venv311/bin/python -m uvicorn ledger.ui.server:app --host 0.0.0.0 --port 8000
- Smoke full pipeline endpoint:
  curl -s -X POST http://127.0.0.1:8000/api/run_full_pipeline | python -m json.tool

Recommended next steps:
1) Add a dedicated dashboard Process Tracker panel (live phase timeline for selected app).
2) Add integration tests for /api/run_full_pipeline endpoint.
3) Optionally commit all current changes with a single “full-flow + week3 bridge + ui tracker” message.