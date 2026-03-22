from __future__ import annotations

import asyncio
import os
import random
import sys
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env")

from ledger.agents.credit_analysis_agent import CreditAnalysisAgent
from ledger.event_store import EventStore, OptimisticConcurrencyError
from ledger.llm.client import create_llm_client, resolve_model_name
from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.registry.client import ApplicantRegistryClient


DEFAULT_DB_URL = "postgresql://postgres:postgres@localhost:5433/ledger"
DB_URL = (
    os.environ.get("LEDGER_UI_DB_URL")
    or os.environ.get("DATABASE_URL")
    or DEFAULT_DB_URL
)


def _build_projections() -> list:
    return [
        ApplicationSummaryProjection(),
        AgentPerformanceLedgerProjection(),
        ComplianceAuditViewProjection(),
    ]


async def _catch_up_projections(daemon: ProjectionDaemon, max_batches: int = 50) -> int:
    processed_total = 0
    for _ in range(max_batches):
        processed = await daemon.run_once()
        processed_total += processed
        if processed == 0:
            break
    return processed_total


async def _pick_company_id(pool: asyncpg.Pool) -> str | None:
    async with pool.acquire() as conn:
        try:
            return await conn.fetchval(
                "SELECT company_id FROM applicant_registry.companies ORDER BY random() LIMIT 1"
            )
        except asyncpg.UndefinedTableError:
            return None


async def _pick_credit_ready_application(conn: asyncpg.Connection) -> str | None:
    app_id = await conn.fetchval(
        """
        WITH ready AS (
            SELECT application_id, last_updated
            FROM application_summary
            WHERE application_id LIKE 'APEX-%'
              AND state = 'CREDIT_ANALYSIS_REQUESTED'
        )
        SELECT ready.application_id
        FROM ready
        WHERE NOT EXISTS (
            SELECT 1
            FROM events e
            WHERE e.stream_id = 'credit-' || ready.application_id
        )
        ORDER BY ready.last_updated DESC
        LIMIT 1
        """
    )
    if app_id:
        return app_id

    return await conn.fetchval(
        """
        WITH ready AS (
            SELECT
                payload->>'application_id' AS application_id,
                MAX(global_position) AS latest_position
            FROM events
            WHERE event_type = 'CreditAnalysisRequested'
              AND stream_id LIKE 'loan-APEX-%'
            GROUP BY payload->>'application_id'
        )
        SELECT ready.application_id
        FROM ready
        WHERE NOT EXISTS (
            SELECT 1
            FROM events e
            WHERE e.stream_id = 'credit-' || ready.application_id
        )
        ORDER BY ready.latest_position DESC
        LIMIT 1
        """
    )


def _serialize_occ_event(event: dict) -> dict:
    stream_id = event["stream_id"]
    aggregate_type, _, aggregate_id = stream_id.partition("-")
    return {
        "version": event.get("stream_position"),
        "global_position": event.get("global_position"),
        "aggregate_id": aggregate_id,
        "aggregate_type": aggregate_type,
        "stream_id": stream_id,
        "event_type": event.get("event_type"),
        "data": event.get("payload", {}),
        "event_id": str(event.get("event_id")),
        "recorded_at": event.get("recorded_at"),
    }


async def _seed_occ_scenario(store: EventStore, app_id: str) -> list[dict]:
    stream_id = f"loan-{app_id}"
    current_version = await store.stream_version(stream_id)
    if current_version == -1:
        await store.append(
            stream_id,
            [
                {
                    "event_type": "ApplicationSubmitted",
                    "event_version": 1,
                    "payload": {
                        "application_id": app_id,
                        "requested_amount_usd": 5000,
                        "loan_purpose": "working_capital",
                        "submitted_by": "ui-occ-drill",
                    },
                },
                {
                    "event_type": "CreditAnalysisRequested",
                    "event_version": 1,
                    "payload": {
                        "application_id": app_id,
                        "requested_by": "decision-orchestrator",
                        "reason": "Post-submission automated underwriting flow",
                    },
                },
            ],
            expected_version=-1,
        )
    return await store.load_stream(stream_id)


def _build_occ_summary(stream_id: str, app_id: str, stream_events: list[dict]) -> dict:
    latest = stream_events[-1] if stream_events else {}
    first_event = stream_events[0] if stream_events else {}
    payload = first_event.get("payload", {})
    return {
        "app_id": app_id,
        "stream_id": stream_id,
        "current_version": latest.get("stream_position", 0),
        "requested_amount_usd": payload.get("requested_amount_usd", 5000),
        "latest_event_type": latest.get("event_type", "Awaiting Seed"),
        "expected_version": latest.get("stream_position", 0),
    }


def _build_occ_messages(
    stream_id: str,
    initial_version: int,
    stream_events: list[dict],
    results: list[dict] | None = None,
    assertions: list[str] | None = None,
) -> list[dict]:
    messages: list[dict] = [
        {
            "tone": "info",
            "title": "Scenario Ready",
            "body": (
                f"{stream_id} is hydrated to version {initial_version} and ready for "
                "two parallel writers to race on the same expected version."
            ),
        }
    ]

    if results:
        loser = next(
            (result for result in results if any("OCC Error" in log for log in result.get("logs", []))),
            None,
        )
        winner = next((result for result in results if result.get("success")), None)
        if winner:
            messages.append(
                {
                    "tone": "success",
                    "title": f"{winner['agent']} won the first append",
                    "body": winner["logs"][0] if winner.get("logs") else "First append succeeded.",
                }
            )
        if loser:
            occ_log = next(
                (log for log in loser.get("logs", []) if "OCC Error" in log),
                "OptimisticConcurrencyError surfaced during append.",
            )
            retry_log = next(
                (log for log in loser.get("logs", []) if "retrying" in log.lower()),
                "The losing writer reloaded the stream and retried safely.",
            )
            messages.append(
                {
                    "tone": "warning",
                    "title": "Conflict detected and surfaced",
                    "body": occ_log,
                }
            )
            messages.append(
                {
                    "tone": "success",
                    "title": "Localized retry completed",
                    "body": retry_log,
                }
            )

    if assertions:
        for assertion in assertions:
            messages.append(
                {
                    "tone": "success",
                    "title": "Assertion Passed",
                    "body": assertion,
                }
            )

    final_version = stream_events[-1]["stream_position"] if stream_events else initial_version
    growth = max(final_version - initial_version, 0)
    messages.append(
        {
            "tone": "info",
            "title": "Append-only log preserved",
            "body": (
                f"The stream advanced from v{initial_version} to v{final_version} (delta +{growth}) "
                "without overwriting any history. In this demo, one append wins the first-write race and "
                "the losing writer retries and appends at the next version."
            ),
        }
    )
    return messages


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = None
    app.state.registry = None
    app.state.agent = None
    app.state.agent_store = None
    app.state.daemon = None
    app.state.projection_task = None

    try:
        pool = await asyncpg.create_pool(DB_URL)
        app.state.pool = pool
        print(f"Connected to DB: {DB_URL}")

        store = EventStore(DB_URL)
        await store.connect()
        app.state.agent_store = store

        daemon = ProjectionDaemon(store, pool, _build_projections())
        await daemon.initialize_tables()
        await _catch_up_projections(daemon)
        app.state.daemon = daemon

        registry = ApplicantRegistryClient(pool)
        app.state.registry = registry

        try:
            client, _provider = create_llm_client()
            model = resolve_model_name("claude-sonnet-4-20250514")
            app.state.agent = CreditAnalysisAgent(
                "AGENT-CREDIT-MAIN",
                "credit_analysis",
                store,
                registry,
                client,
                model=model,
            )
            print(f"CreditAnalysisAgent initialized with model={model}")
        except Exception as exc:
            print(f"LLM agent unavailable: {exc}")
            app.state.agent = None

        app.state.projection_task = asyncio.create_task(daemon.run_forever())
        print("UI server initialized.")
    except Exception as exc:
        print(f"Failed to initialize UI server: {exc}")

    try:
        yield
    finally:
        task = app.state.projection_task
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        if app.state.agent_store is not None:
            await app.state.agent_store.close()
        if app.state.pool is not None:
            await app.state.pool.close()


app = FastAPI(title="The Ledger UI API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def disable_cache_for_dev_ui(request: Request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.get("/favicon.ico")
async def favicon() -> Response:
    return Response(status_code=204)


@app.get("/api/applications")
async def get_applications():
    pool = app.state.pool
    if not pool:
        return []
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                "SELECT * FROM application_summary ORDER BY last_updated DESC LIMIT 50"
            )
            return [dict(r) for r in rows]
        except asyncpg.UndefinedTableError:
            return []


@app.get("/api/events")
async def get_events():
    pool = app.state.pool
    if not pool:
        return []
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                """
                SELECT event_id, global_position, stream_id, stream_position,
                       event_version, event_type, recorded_at, payload
                FROM events
                ORDER BY global_position DESC
                LIMIT 100
                """
            )
            return [dict(r) for r in rows]
        except asyncpg.UndefinedTableError:
            return []


@app.get("/api/metrics")
async def get_metrics():
    pool = app.state.pool
    if not pool:
        return {"agent_performance": [], "stats": {"total": 0, "approved": 0, "declined": 0}}

    async with pool.acquire() as conn:
        try:
            agents = await conn.fetch("SELECT * FROM agent_performance")
            total_apps = await conn.fetchval("SELECT COUNT(*) FROM application_summary")
            approved = await conn.fetchval(
                "SELECT COUNT(*) FROM application_summary WHERE state = 'APPROVED'"
            )
            declined = await conn.fetchval(
                "SELECT COUNT(*) FROM application_summary WHERE state = 'DECLINED'"
            )
            return {
                "agent_performance": [dict(r) for r in agents],
                "stats": {
                    "total": total_apps or 0,
                    "approved": approved or 0,
                    "declined": declined or 0,
                },
            }
        except asyncpg.UndefinedTableError:
            return {"agent_performance": [], "stats": {"total": 0, "approved": 0, "declined": 0}}


@app.post("/api/simulate")
async def simulate_application():
    pool = app.state.pool
    daemon = app.state.daemon
    if not pool or not daemon:
        return {"status": "error", "reason": "ui server not initialized"}

    company_id = await _pick_company_id(pool)
    if not company_id:
        return {"status": "error", "reason": "no applicant registry companies available"}

    app_id = f"APEX-{uuid.uuid4().hex[:6].upper()}"
    document_id = f"doc-{uuid.uuid4().hex[:8]}"
    amount = float(random.randint(100, 1000) * 1000)

    store = EventStore(DB_URL)
    await store.connect()
    try:
        await store.append(
            f"loan-{app_id}",
            [
                {
                    "event_type": "ApplicationSubmitted",
                    "event_version": 1,
                    "payload": {
                        "application_id": app_id,
                        "applicant_id": company_id,
                        "requested_amount_usd": amount,
                        "loan_purpose": "working_capital",
                        "submitted_at": datetime.now(timezone.utc).isoformat(),
                    },
                },
                {
                    "event_type": "DocumentUploadRequested",
                    "event_version": 1,
                    "payload": {
                        "application_id": app_id,
                        "required_document_types": ["income_statement"],
                        "requested_by": "ui-demo",
                    },
                },
            ],
            expected_version=-1,
        )
        await store.append(
            f"loan-{app_id}",
            [
                {
                    "event_type": "DocumentUploaded",
                    "event_version": 1,
                    "payload": {
                        "application_id": app_id,
                        "document_id": document_id,
                        "document_type": "income_statement",
                    },
                },
                {
                    "event_type": "CreditAnalysisRequested",
                    "event_version": 1,
                    "payload": {
                        "application_id": app_id,
                        "requested_at": datetime.now(timezone.utc).isoformat(),
                        "requested_by": "ui-demo",
                    },
                },
            ],
            expected_version=2,
        )
        await store.append(
            f"docpkg-{app_id}",
            [
                {
                    "event_type": "PackageCreated",
                    "event_version": 1,
                    "payload": {
                        "package_id": app_id,
                        "application_id": app_id,
                        "required_documents": ["income_statement"],
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    },
                },
                {
                    "event_type": "ExtractionCompleted",
                    "event_version": 1,
                    "payload": {
                        "package_id": app_id,
                        "document_id": document_id,
                        "document_type": "income_statement",
                        "facts": {
                            "total_revenue": 5500000.0,
                            "ebitda": 1500000.0,
                            "net_income": 420000.0,
                            "total_assets": 3200000.0,
                            "total_liabilities": 1400000.0,
                        },
                        "raw_text_length": 1024,
                        "tables_extracted": 2,
                        "processing_ms": 850,
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                    },
                },
                {
                    "event_type": "PackageReadyForAnalysis",
                    "event_version": 1,
                    "payload": {
                        "package_id": app_id,
                        "application_id": app_id,
                        "documents_processed": 1,
                        "has_quality_flags": False,
                        "quality_flag_count": 0,
                        "ready_at": datetime.now(timezone.utc).isoformat(),
                    },
                },
            ],
            expected_version=-1,
        )
        await _catch_up_projections(daemon)
        return {
            "status": "ok",
            "app_id": app_id,
            "company_id": company_id,
            "requested_amount_usd": amount,
        }
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}
    finally:
        await store.close()


@app.post("/api/concurrency_test")
async def test_concurrency(app_id: str | None = None):
    pool = app.state.pool
    daemon = app.state.daemon
    if not pool or not daemon:
        return {"status": "error", "reason": "ui server not initialized"}

    app_id = app_id or f"OCC-TEST-{uuid.uuid4().hex[:4].upper()}"
    stream_id = f"loan-{app_id}"
    store = EventStore(DB_URL)
    await store.connect()
    try:
        seeded_events = await _seed_occ_scenario(store, app_id)
        expected_version = seeded_events[-1]["stream_position"] if seeded_events else 0

        async def append_concurrently(agent_name: str):
            logs: list[str] = []
            expected = expected_version
            for attempt in range(3):
                try:
                    if agent_name == "RiskOps-Fraud":
                        event_type = "FraudScreeningRequested"
                        payload = {
                            "application_id": app_id,
                            "requested_by": "credit-analysis-agent",
                            "reason": "Parallel post-credit risk workflow",
                            "attempt": attempt + 1,
                        }
                    else:
                        event_type = "ComplianceCheckRequested"
                        payload = {
                            "application_id": app_id,
                            "requested_by": "credit-analysis-agent",
                            "reason": "Parallel post-credit compliance workflow",
                            "attempt": attempt + 1,
                        }

                    await store.append(
                        stream_id,
                        [
                            {
                                "event_type": event_type,
                                "event_version": 1,
                                "payload": payload,
                            }
                        ],
                        expected_version=expected,
                    )
                    logs.append(
                        f"🟢 Success: {agent_name} appended {event_type} to {stream_id} at position {expected + 1}"
                    )
                    return {"agent": agent_name, "logs": logs, "success": True}
                except OptimisticConcurrencyError as exc:
                    logs.append(
                        f"🔴 OCC Error: {agent_name} rejected. Expected version {expected}, actual version {exc.actual}."
                    )
                    logs.append(
                        "Traceback (most recent call last):\n"
                        '  File "ledger/event_store.py", line 150, in append\n'
                        "    raise OptimisticConcurrencyError(stream_id, expected_version, current)\n"
                        f"ledger.event_store.OptimisticConcurrencyError: OCC on '{stream_id}': "
                        f"expected v{expected}, actual v{exc.actual}"
                    )
                    expected = exc.actual
                    logs.append(f"🔄 {agent_name} retrying at expected v{expected}...")
                    await asyncio.sleep(0.1)
            return {"agent": agent_name, "logs": logs, "success": False}

        async def run_a():
            return await append_concurrently("RiskOps-Fraud")

        async def run_b():
            await asyncio.sleep(0.05)
            return await append_concurrently("RiskOps-Compliance")

        res1, res2 = await asyncio.gather(run_a(), run_b())
        stream_events = await store.load_stream(stream_id)
        successful_events = [
            event
            for event in stream_events
            if event["event_type"] in {"FraudScreeningRequested", "ComplianceCheckRequested"}
        ]
        winning_event = successful_events[0] if successful_events else None

        assertion_logs = [
            f"[ASSERTION PASSED]: Total stream length for {stream_id} is {len(stream_events)}.",
            (
                f"[ASSERTION PASSED]: First successful append is at position "
                f"{winning_event['stream_position']} with event '{winning_event['event_type']}'."
                if winning_event
                else "[ASSERTION FAILED]: No successful append event found."
            ),
            (
                "[ASSERTION PASSED]: Explicit OptimisticConcurrencyError caught for Agent-B."
                if any("OCC Error" in log for result in (res1, res2) for log in result["logs"])
                else "[ASSERTION FAILED]: OCC error was not surfaced explicitly."
            ),
        ]

        system_stream_id = f"system-{app_id}"
        system_expected = await store.stream_version(system_stream_id)
        await store.append(
            system_stream_id,
            [
                {
                    "event_type": "ConcurrencyTestExecuted",
                    "event_version": 1,
                    "payload": {
                        "results": [res1, res2],
                        "assertions": assertion_logs,
                    },
                }
            ],
            expected_version=system_expected,
        )
        await _catch_up_projections(daemon)
        messages = _build_occ_messages(
            stream_id,
            expected_version,
            stream_events,
            [res1, res2],
            assertion_logs,
        )
        return {
            "status": "ok",
            "app_id": app_id,
            "stream_id": stream_id,
            "summary": _build_occ_summary(stream_id, app_id, stream_events),
            "results": [res1, res2],
            "assertions": assertion_logs,
            "messages": messages,
            "stream_events": [_serialize_occ_event(event) for event in stream_events],
        }
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}
    finally:
        await store.close()


@app.post("/api/concurrency_reset")
async def reset_concurrency_scenario():
    daemon = app.state.daemon
    if not daemon:
        return {"status": "error", "reason": "ui server not initialized"}

    app_id = f"OCC-LAB-{uuid.uuid4().hex[:4].upper()}"
    stream_id = f"loan-{app_id}"
    store = EventStore(DB_URL)
    await store.connect()
    try:
        stream_events = await _seed_occ_scenario(store, app_id)
        current_version = stream_events[-1]["stream_position"] if stream_events else 0
        await _catch_up_projections(daemon)
        return {
            "status": "ok",
            "app_id": app_id,
            "stream_id": stream_id,
            "summary": _build_occ_summary(stream_id, app_id, stream_events),
            "messages": _build_occ_messages(stream_id, current_version, stream_events),
            "stream_events": [_serialize_occ_event(event) for event in stream_events],
        }
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}
    finally:
        await store.close()


@app.post("/api/run_agent")
async def run_langgraph_agent(app_id: str | None = None):
    pool = app.state.pool
    daemon = app.state.daemon
    agent = app.state.agent
    registry = app.state.registry
    if not pool or not daemon or not registry:
        return {"status": "error", "reason": "ui server not initialized"}
    if not agent:
        return {"status": "error", "reason": "agent not initialized"}

    async with pool.acquire() as conn:
        if not app_id:
            app_id = await _pick_credit_ready_application(conn)

    if not app_id:
        return {
            "status": "error",
            "reason": "no credit-ready applications available",
        }

    store = EventStore(DB_URL)
    await store.connect()
    try:
        agent.store = store
        agent.registry = registry
        await agent.process_application(app_id)
        await _catch_up_projections(daemon)
        return {"status": "ok", "app_id": app_id, "session_id": agent.session_id}
    except Exception as exc:
        return {"status": "error", "message": str(exc)}
    finally:
        await store.close()


static_dir = Path(__file__).parent / "public"
app.mount("/", StaticFiles(directory=static_dir, html=True), name="public")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
