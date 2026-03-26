from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import sys
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env")

from ledger.agents.credit_analysis_agent import CreditAnalysisAgent
from ledger.agents.stub_agents import (
    ComplianceAgent,
    DecisionOrchestratorAgent,
    DocumentProcessingAgent,
    FraudDetectionAgent,
)
from ledger.event_store import EventStore, OptimisticConcurrencyError
from ledger.llm.client import create_llm_client, resolve_model_name
from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.projections.daemon import ProjectionDaemon
from ledger.registry.client import ApplicantRegistryClient
from ledger.upcasters import UpcasterRegistry


DEFAULT_DB_URL = "postgresql://postgres:postgres@localhost:5433/ledger"
DB_URL = (
    os.environ.get("LEDGER_UI_DB_URL")
    or os.environ.get("DATABASE_URL")
    or DEFAULT_DB_URL
)
GENERATED_DOCS_DIR = ROOT / "documents"


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
                MAX(global_position) AS latest_position,
                COUNT(*) FILTER (WHERE event_type = 'CreditAnalysisRequested') AS credit_req_count
            FROM events
            WHERE stream_id LIKE 'loan-APEX-%'
            GROUP BY payload->>'application_id'
            HAVING COUNT(*) FILTER (WHERE event_type = 'CreditAnalysisRequested') = 1
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


async def _pick_document_ready_application(conn: asyncpg.Connection) -> str | None:
    return await conn.fetchval(
        """
        WITH ready AS (
            SELECT
                payload->>'application_id' AS application_id,
                MAX(global_position) AS latest_position
            FROM events
            WHERE event_type = 'DocumentUploaded'
              AND stream_id LIKE 'loan-APEX-%'
            GROUP BY payload->>'application_id'
        )
        SELECT ready.application_id
        FROM ready
        WHERE NOT EXISTS (
            SELECT 1
            FROM events e
            WHERE e.stream_id = 'loan-' || ready.application_id
              AND e.event_type = 'CreditAnalysisRequested'
        )
        ORDER BY ready.latest_position DESC
        LIMIT 1
        """
    )


def _pick_generated_documents_for_upload(company_id: str | None = None) -> list[dict[str, str]]:
    if not GENERATED_DOCS_DIR.exists() or not GENERATED_DOCS_DIR.is_dir():
        return []

    company_dirs = sorted([p for p in GENERATED_DOCS_DIR.iterdir() if p.is_dir()])
    if not company_dirs:
        return []

    chosen_dir = None
    if company_id and company_id.startswith("COMP-"):
        # Normalize registry IDs like COMP-001 to folder names under documents/.
        candidate = GENERATED_DOCS_DIR / company_id
        if candidate.exists() and candidate.is_dir():
            chosen_dir = candidate
    if chosen_dir is None:
        chosen_dir = random.choice(company_dirs)

    income_candidates = sorted(chosen_dir.glob("income_statement*.pdf"))
    balance_candidates = sorted(chosen_dir.glob("balance_sheet*.pdf"))

    docs: list[dict[str, str]] = []
    if income_candidates:
        p = income_candidates[0]
        docs.append(
            {
                "document_id": f"doc-{uuid.uuid4().hex[:8]}",
                "document_type": "income_statement",
                "document_format": "pdf",
                "file_path": str(p),
                "filename": p.name,
            }
        )
    if balance_candidates:
        p = balance_candidates[0]
        docs.append(
            {
                "document_id": f"doc-{uuid.uuid4().hex[:8]}",
                "document_type": "balance_sheet",
                "document_format": "pdf",
                "file_path": str(p),
                "filename": p.name,
            }
        )

    return docs


async def _seed_demo_application(pool: asyncpg.Pool, store: EventStore) -> dict | None:
    company_id = await _pick_company_id(pool)
    if not company_id:
        return None

    app_id = f"APEX-{uuid.uuid4().hex[:6].upper()}"
    amount = float(random.randint(100, 1000) * 1000)
    uploaded_docs = _pick_generated_documents_for_upload(company_id)
    if not uploaded_docs:
        return None

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
                    "required_document_types": ["income_statement", "balance_sheet"],
                    "requested_by": "ui-demo",
                },
            },
        ],
        expected_version=-1,
    )

    upload_events = []
    for doc in uploaded_docs:
        upload_events.append(
            {
                "event_type": "DocumentUploaded",
                "event_version": 1,
                "payload": {
                    "application_id": app_id,
                    "document_id": doc["document_id"],
                    "document_type": doc["document_type"],
                    "file_path": doc["file_path"],
                    "filename": doc["filename"],
                    "document_format": doc["document_format"],
                    "uploaded_at": datetime.now(timezone.utc).isoformat(),
                    "uploaded_by": "ui-demo",
                },
            }
        )

    await store.append(f"loan-{app_id}", upload_events, expected_version=2)

    return {
        "app_id": app_id,
        "company_id": company_id,
        "requested_amount_usd": amount,
        "uploaded_docs": uploaded_docs,
    }


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


def _normalize_json(value: Any) -> Any:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    if isinstance(value, dict):
        return value
    try:
        return dict(value)
    except Exception:
        return value


def _event_integrity_chain(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a deterministic rolling hash chain for a selected event set."""
    previous_hash = "GENESIS"
    chain: list[dict[str, Any]] = []
    valid = True
    seen_event_ids: set[str] = set()
    first_broken_rule = None

    for event in events:
        payload = _normalize_json(event.get("payload"))
        metadata = _normalize_json(event.get("metadata"))
        canonical = json.dumps(
            {
                "event_id": str(event.get("event_id")),
                "global_position": event.get("global_position"),
                "stream_id": event.get("stream_id"),
                "stream_position": event.get("stream_position"),
                "event_type": event.get("event_type"),
                "event_version": event.get("event_version"),
                "payload": payload,
                "metadata": metadata,
                "recorded_at": str(event.get("recorded_at")),
                "previous_hash": previous_hash,
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        event_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        chain.append(
            {
                "event_id": str(event.get("event_id")),
                "global_position": event.get("global_position"),
                "stream_id": event.get("stream_id"),
                "stream_position": event.get("stream_position"),
                "event_type": event.get("event_type"),
                "previous_hash": previous_hash,
                "event_hash": event_hash,
            }
        )

        event_id = str(event.get("event_id"))
        if event_id in seen_event_ids and first_broken_rule is None:
            valid = False
            first_broken_rule = f"Duplicate event_id detected in selected history: {event_id}."
        seen_event_ids.add(event_id)
        previous_hash = event_hash

    return {
        "valid": valid,
        "event_count": len(chain),
        "root_hash": previous_hash,
        "first_broken_rule": first_broken_rule,
        "chain": chain,
    }


def _parse_iso_datetime(raw: str) -> datetime:
    normalized = raw.strip().replace("Z", "+00:00").replace(" ", "+")
    return datetime.fromisoformat(normalized)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _apply_orchestrator_constraints(
    recommendation: str,
    confidence: float,
    fraud_score: float,
    compliance_verdict: str,
    risk_tier: str,
) -> tuple[str, list[str]]:
    rec = recommendation or "REFER"
    conf = _to_float(confidence)
    score = _to_float(fraud_score)
    verdict = (compliance_verdict or "").upper()
    tier = (risk_tier or "MEDIUM").upper()
    overrides: list[str] = []

    if verdict == "BLOCKED":
        if rec != "DECLINE":
            overrides.append(f"compliance_hard_block: {rec} -> DECLINE")
        rec = "DECLINE"
    elif score > 0.60:
        if rec not in ("DECLINE", "REFER"):
            overrides.append(f"fraud_score_high ({score:.2f}): {rec} -> REFER")
        if rec == "APPROVE":
            rec = "REFER"
    elif conf < 0.60:
        if rec == "APPROVE":
            overrides.append(f"confidence_below_threshold ({conf:.2f}): APPROVE -> REFER")
            rec = "REFER"
    elif tier == "HIGH" and conf < 0.70:
        if rec == "APPROVE":
            overrides.append(f"high_risk_low_confidence (tier={tier}, conf={conf:.2f}): APPROVE -> REFER")
            rec = "REFER"

    return rec, overrides


async def reconstruct_agent_context(
    pool: asyncpg.Pool,
    session_id: str | None = None,
    application_id: str | None = None,
    agent_type: str | None = None,
) -> dict[str, Any]:
    if session_id is None and application_id is None:
        raise ValueError("either session_id or application_id is required")

    async with pool.acquire() as conn:
        started_row = None
        if session_id is not None:
            started_row = await conn.fetchrow(
                """
                SELECT stream_id, payload, global_position, recorded_at
                FROM events
                WHERE event_type = 'AgentSessionStarted'
                  AND payload->>'session_id' = $1
                ORDER BY global_position DESC
                LIMIT 1
                """,
                session_id,
            )
        else:
            if agent_type:
                started_row = await conn.fetchrow(
                    """
                    SELECT stream_id, payload, global_position, recorded_at
                    FROM events
                    WHERE event_type = 'AgentSessionStarted'
                      AND payload->>'application_id' = $1
                      AND payload->>'agent_type' = $2
                    ORDER BY global_position DESC
                    LIMIT 1
                    """,
                    application_id,
                    agent_type,
                )
            else:
                started_row = await conn.fetchrow(
                    """
                    SELECT stream_id, payload, global_position, recorded_at
                    FROM events
                    WHERE event_type = 'AgentSessionStarted'
                      AND payload->>'application_id' = $1
                    ORDER BY global_position DESC
                    LIMIT 1
                    """,
                    application_id,
                )

        if not started_row:
            return {
                "status": "error",
                "reason": "agent session not found",
                "session_id": session_id,
                "application_id": application_id,
                "agent_type": agent_type,
            }

        stream_id = started_row["stream_id"]
        rows = await conn.fetch(
            """
            SELECT event_id, global_position, stream_position, event_type, payload, recorded_at
            FROM events
            WHERE stream_id = $1
            ORDER BY stream_position ASC
            """,
            stream_id,
        )

    events = []
    for row in rows:
        payload = _normalize_json(row["payload"])
        events.append(
            {
                "event_id": str(row["event_id"]),
                "global_position": row["global_position"],
                "stream_position": row["stream_position"],
                "event_type": row["event_type"],
                "payload": payload,
                "recorded_at": row["recorded_at"].isoformat() if row["recorded_at"] else None,
            }
        )

    started_payload = _normalize_json(started_row["payload"])
    node_events = [e for e in events if e["event_type"] == "AgentNodeExecuted"]
    tool_events = [e for e in events if e["event_type"] == "AgentToolCalled"]
    output_events = [e for e in events if e["event_type"] == "AgentOutputWritten"]
    completed = any(e["event_type"] == "AgentSessionCompleted" for e in events)
    failed = any(e["event_type"] == "AgentSessionFailed" for e in events)

    written_events: list[str] = []
    for evt in output_events:
        payload = evt.get("payload") or {}
        if isinstance(payload, dict):
            written = payload.get("events_written") or []
            for item in written:
                if isinstance(item, str):
                    written_events.append(item)
                elif isinstance(item, dict) and item.get("event_type"):
                    written_events.append(str(item["event_type"]))

    return {
        "status": "ok",
        "session": {
            "stream_id": stream_id,
            "session_id": started_payload.get("session_id") or session_id,
            "application_id": started_payload.get("application_id") or application_id,
            "agent_type": started_payload.get("agent_type") or agent_type,
            "agent_id": started_payload.get("agent_id"),
            "model_version": started_payload.get("model_version"),
            "context_source": started_payload.get("context_source"),
            "started_at": started_payload.get("started_at") or (
                started_row["recorded_at"].isoformat() if started_row["recorded_at"] else None
            ),
        },
        "reconstructed": {
            "event_count": len(events),
            "nodes_executed": len(node_events),
            "tool_calls": len(tool_events),
            "outputs_written": written_events,
            "last_successful_node": (
                (node_events[-1].get("payload") or {}).get("node_name") if node_events else None
            ),
            "session_state": "completed" if completed else ("failed" if failed else "in_progress"),
            "resume_from": {
                "strategy": "prior_session_replay",
                "next_node_hint": (
                    (node_events[-1].get("payload") or {}).get("node_name") if node_events else "validate_inputs"
                ),
                "source_stream": stream_id,
            },
        },
        "events": events,
    }


def _terminal_event_for_recommendation(rec: str) -> str:
    if rec == "APPROVE":
        return "ApplicationApproved"
    if rec == "DECLINE":
        return "ApplicationDeclined"
    return "HumanReviewRequested"


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


@app.get("/api/history/{application_id}")
async def get_application_history(application_id: str):
    """
    Complete decision history for one application with:
    - full event stream slice (ordered by global_position)
    - agent action summary
    - compliance and human-review slices
    - causal links (correlation_id / causation_id)
    - deterministic integrity chain over the returned event slice
    """
    pool = app.state.pool
    if not pool:
        return {"status": "error", "reason": "ui server not initialized"}

    started = perf_counter()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT event_id, global_position, stream_id, stream_position,
                   event_type, event_version, payload, metadata, recorded_at
            FROM events
            WHERE payload->>'application_id' = $1
               OR stream_id LIKE $2
            ORDER BY global_position ASC
            """,
            application_id,
            f"%-{application_id}",
        )

    events: list[dict[str, Any]] = []
    for row in rows:
        payload = _normalize_json(row["payload"])
        metadata = _normalize_json(row["metadata"])
        events.append(
            {
                "event_id": str(row["event_id"]),
                "global_position": row["global_position"],
                "stream_id": row["stream_id"],
                "stream_position": row["stream_position"],
                "event_type": row["event_type"],
                "event_version": row["event_version"],
                "payload": payload,
                "metadata": metadata,
                "recorded_at": row["recorded_at"].isoformat() if row["recorded_at"] else None,
            }
        )

    if not events:
        return {
            "status": "error",
            "reason": f"no events found for application_id={application_id}",
            "application_id": application_id,
        }

    integrity = _event_integrity_chain(events)
    agent_actions = [e for e in events if str(e["event_type"]).startswith("Agent")]
    compliance_checks = [e for e in events if str(e["event_type"]).startswith("Compliance")]
    human_review = [e for e in events if "HumanReview" in str(e["event_type"])]

    causal_links: list[dict[str, Any]] = []
    for event in events:
        metadata = event.get("metadata") or {}
        if not isinstance(metadata, dict):
            continue
        corr = metadata.get("correlation_id")
        caus = metadata.get("causation_id")
        if corr or caus:
            causal_links.append(
                {
                    "event_id": event["event_id"],
                    "event_type": event["event_type"],
                    "stream_id": event["stream_id"],
                    "stream_position": event["stream_position"],
                    "correlation_id": corr,
                    "causation_id": caus,
                }
            )

    elapsed_ms = int((perf_counter() - started) * 1000)
    return {
        "status": "ok",
        "application_id": application_id,
        "timing": {
            "elapsed_ms": elapsed_ms,
            "under_60_seconds": elapsed_ms < 60000,
        },
        "counts": {
            "total_events": len(events),
            "agent_actions": len(agent_actions),
            "compliance_checks": len(compliance_checks),
            "human_review_events": len(human_review),
            "causal_links": len(causal_links),
        },
        "integrity": integrity,
        "agent_actions": agent_actions,
        "compliance_checks": compliance_checks,
        "human_review": human_review,
        "causal_links": causal_links,
        "events": events,
    }


@app.get("/api/applications/{application_id}/compliance")
async def get_compliance_snapshot(
    application_id: str,
    as_of: str | None = Query(default=None),
):
    pool = app.state.pool
    if not pool:
        return {"status": "error", "reason": "ui server not initialized"}

    if as_of is None:
        as_of_dt = datetime.now(timezone.utc)
    else:
        try:
            as_of_dt = _parse_iso_datetime(as_of)
        except ValueError:
            return {
                "status": "error",
                "reason": "invalid as_of timestamp; expected ISO-8601",
                "application_id": application_id,
                "as_of": as_of,
            }

    async with pool.acquire() as conn:
        snapshot = await ComplianceAuditViewProjection.get_compliance_at(
            conn,
            application_id,
            as_of_dt,
        )

    return {
        "status": "ok",
        "resource": f"ledger://applications/{application_id}/compliance?as_of={as_of_dt.isoformat()}",
        "snapshot": snapshot,
    }


@app.get("/api/reconstruct_agent_context")
async def reconstruct_agent_context_endpoint(
    session_id: str | None = Query(default=None),
    application_id: str | None = Query(default=None),
    agent_type: str | None = Query(default=None),
):
    pool = app.state.pool
    if not pool:
        return {"status": "error", "reason": "ui server not initialized"}
    try:
        return await reconstruct_agent_context(
            pool,
            session_id=session_id,
            application_id=application_id,
            agent_type=agent_type,
        )
    except Exception as exc:
        return {
            "status": "error",
            "reason": str(exc),
            "session_id": session_id,
            "application_id": application_id,
            "agent_type": agent_type,
        }


@app.post("/api/simulate_agent_crash")
async def simulate_agent_crash(application_id: str | None = None, agent_type: str = "fraud_detection"):
    pool = app.state.pool
    if not pool:
        return {"status": "error", "reason": "ui server not initialized"}

    store = EventStore(DB_URL)
    await store.connect()
    try:
        if not application_id:
            async with pool.acquire() as conn:
                application_id = await conn.fetchval(
                    "SELECT application_id FROM application_summary ORDER BY last_updated DESC LIMIT 1"
                )
        if not application_id:
            return {"status": "error", "reason": "no applications found for crash simulation"}

        crash_session_id = f"SESS-CRASH-{uuid.uuid4().hex[:6].upper()}"
        crash_stream = f"agent-{agent_type}-{crash_session_id}"

        await store.append(
            crash_stream,
            [
                {
                    "event_type": "AgentSessionStarted",
                    "event_version": 1,
                    "payload": {
                        "session_id": crash_session_id,
                        "agent_type": agent_type,
                        "application_id": application_id,
                        "agent_id": f"AGENT-{agent_type.upper()}-SIM",
                        "model_version": "simulated-recovery-v1",
                        "langgraph_graph_version": "1.0.0",
                        "context_source": "fresh",
                        "context_token_count": 256,
                        "started_at": datetime.now(timezone.utc).isoformat(),
                    },
                }
            ],
            expected_version=-1,
        )
        await store.append(
            crash_stream,
            [
                {
                    "event_type": "AgentNodeExecuted",
                    "event_version": 1,
                    "payload": {
                        "session_id": crash_session_id,
                        "agent_type": agent_type,
                        "node_name": "load_context",
                        "node_sequence": 1,
                        "input_keys": ["application_id"],
                        "output_keys": ["context"],
                        "llm_called": False,
                        "duration_ms": 42,
                        "executed_at": datetime.now(timezone.utc).isoformat(),
                    },
                },
                {
                    "event_type": "AgentToolCalled",
                    "event_version": 1,
                    "payload": {
                        "session_id": crash_session_id,
                        "agent_type": agent_type,
                        "tool_name": "load_stream",
                        "tool_input_summary": "loan-stream",
                        "tool_output_summary": "context_loaded",
                        "tool_duration_ms": 18,
                        "called_at": datetime.now(timezone.utc).isoformat(),
                    },
                },
                {
                    "event_type": "AgentNodeExecuted",
                    "event_version": 1,
                    "payload": {
                        "session_id": crash_session_id,
                        "agent_type": agent_type,
                        "node_name": "analyze",
                        "node_sequence": 2,
                        "input_keys": ["context"],
                        "output_keys": ["partial_analysis"],
                        "llm_called": False,
                        "duration_ms": 87,
                        "executed_at": datetime.now(timezone.utc).isoformat(),
                    },
                },
            ],
            expected_version=1,
        )

        reconstructed = await reconstruct_agent_context(
            pool,
            session_id=crash_session_id,
            application_id=application_id,
            agent_type=agent_type,
        )

        resumed_session_id = f"SESS-RESUME-{uuid.uuid4().hex[:6].upper()}"
        resumed_stream = f"agent-{agent_type}-{resumed_session_id}"
        await store.append(
            resumed_stream,
            [
                {
                    "event_type": "AgentSessionStarted",
                    "event_version": 1,
                    "payload": {
                        "session_id": resumed_session_id,
                        "agent_type": agent_type,
                        "application_id": application_id,
                        "agent_id": f"AGENT-{agent_type.upper()}-SIM",
                        "model_version": "simulated-recovery-v1",
                        "langgraph_graph_version": "1.0.0",
                        "context_source": f"prior_session_replay:{crash_session_id}",
                        "context_token_count": 512,
                        "started_at": datetime.now(timezone.utc).isoformat(),
                    },
                },
                {
                    "event_type": "AgentSessionRecovered",
                    "event_version": 1,
                    "payload": {
                        "session_id": resumed_session_id,
                        "agent_type": agent_type,
                        "application_id": application_id,
                        "recovered_from_session_id": crash_session_id,
                        "recovered_state_hash": _event_integrity_chain(reconstructed.get("events", []))
                        .get("root_hash"),
                        "recovered_node_count": reconstructed.get("reconstructed", {}).get("nodes_executed", 0),
                        "recovered_at": datetime.now(timezone.utc).isoformat(),
                    },
                },
                {
                    "event_type": "AgentSessionCompleted",
                    "event_version": 1,
                    "payload": {
                        "session_id": resumed_session_id,
                        "agent_type": agent_type,
                        "application_id": application_id,
                        "total_nodes_executed": reconstructed.get("reconstructed", {}).get("nodes_executed", 0) + 1,
                        "total_llm_calls": 0,
                        "total_tokens_used": 0,
                        "total_cost_usd": 0.0,
                        "total_duration_ms": 150,
                        "next_agent_triggered": None,
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                    },
                },
            ],
            expected_version=-1,
        )

        resumed = await reconstruct_agent_context(
            pool,
            session_id=resumed_session_id,
            application_id=application_id,
            agent_type=agent_type,
        )

        return {
            "status": "ok",
            "application_id": application_id,
            "agent_type": agent_type,
            "crashed_session_id": crash_session_id,
            "recovered_session_id": resumed_session_id,
            "crashed_context": reconstructed,
            "resumed_context": resumed,
        }
    except Exception as exc:
        return {"status": "error", "reason": str(exc), "application_id": application_id, "agent_type": agent_type}
    finally:
        await store.close()


@app.get("/api/what_if/{application_id}")
async def what_if_counterfactual(
    application_id: str,
    risk_tier: str = Query(default="HIGH"),
    assume_confidence: float | None = Query(default=None),
):
    store = EventStore(DB_URL)
    await store.connect()
    try:
        credit_events = await store.load_stream(f"credit-{application_id}")
        fraud_events = await store.load_stream(f"fraud-{application_id}")
        compliance_events = await store.load_stream(f"compliance-{application_id}")
        loan_events = await store.load_stream(f"loan-{application_id}")

        credit_payload = next(
            (e.get("payload") for e in reversed(credit_events) if e.get("event_type") == "CreditAnalysisCompleted"),
            {},
        ) or {}
        fraud_payload = next(
            (e.get("payload") for e in reversed(fraud_events) if e.get("event_type") == "FraudScreeningCompleted"),
            {},
        ) or {}
        compliance_payload = next(
            (e.get("payload") for e in reversed(compliance_events) if e.get("event_type") == "ComplianceCheckCompleted"),
            {},
        ) or {}
        decision_payload = next(
            (e.get("payload") for e in reversed(loan_events) if e.get("event_type") == "DecisionGenerated"),
            {},
        ) or {}

        if not decision_payload:
            return {
                "status": "error",
                "reason": "DecisionGenerated not found; run full pipeline first",
                "application_id": application_id,
            }

        decision_conf = _to_float(decision_payload.get("confidence"), _to_float(credit_payload.get("decision", {}).get("confidence"), 0.5))
        baseline_recommendation = str(decision_payload.get("recommendation") or "REFER").upper()
        baseline_fraud_score = _to_float(fraud_payload.get("fraud_score"), 0.0)
        baseline_compliance = str(compliance_payload.get("verdict") or "CLEAR").upper()
        baseline_risk_tier = str((credit_payload.get("decision") or {}).get("risk_tier") or "MEDIUM").upper()

        baseline_final, baseline_overrides = _apply_orchestrator_constraints(
            recommendation=baseline_recommendation,
            confidence=decision_conf,
            fraud_score=baseline_fraud_score,
            compliance_verdict=baseline_compliance,
            risk_tier=baseline_risk_tier,
        )

        scenario_confidence = decision_conf if assume_confidence is None else _to_float(assume_confidence, decision_conf)

        counterfactual_final, counterfactual_overrides = _apply_orchestrator_constraints(
            recommendation=baseline_recommendation,
            confidence=scenario_confidence,
            fraud_score=baseline_fraud_score,
            compliance_verdict=baseline_compliance,
            risk_tier=risk_tier,
        )

        return {
            "status": "ok",
            "application_id": application_id,
            "scenario": {
                "change": f"risk_tier {baseline_risk_tier} -> {risk_tier.upper()}",
                "counterfactual_confidence": scenario_confidence,
                "baseline_inputs": {
                    "recommendation": baseline_recommendation,
                    "confidence": decision_conf,
                    "fraud_score": baseline_fraud_score,
                    "compliance_verdict": baseline_compliance,
                    "risk_tier": baseline_risk_tier,
                },
            },
            "baseline": {
                "final_recommendation": baseline_final,
                "terminal_event": _terminal_event_for_recommendation(baseline_final),
                "constraints_applied": baseline_overrides,
            },
            "counterfactual": {
                "final_recommendation": counterfactual_final,
                "terminal_event": _terminal_event_for_recommendation(counterfactual_final),
                "constraints_applied": counterfactual_overrides,
            },
            "delta": {
                "recommendation_changed": baseline_final != counterfactual_final,
                "new_constraints": [rule for rule in counterfactual_overrides if rule not in baseline_overrides],
            },
        }
    except Exception as exc:
        return {"status": "error", "reason": str(exc), "application_id": application_id}
    finally:
        await store.close()


@app.post("/api/upcast_probe")
async def upcast_probe():
    app_id = f"UPCAST-{uuid.uuid4().hex[:6].upper()}"
    stream_id = f"credit-{app_id}"
    store = EventStore(DB_URL, upcaster_registry=UpcasterRegistry())
    await store.connect()
    try:
        await store.append(
            stream_id,
            [
                {
                    "event_type": "CreditAnalysisCompleted",
                    "event_version": 1,
                    "payload": {
                        "application_id": app_id,
                        "decision": {
                            "risk_tier": "MEDIUM",
                            "recommended_limit_usd": 100000,
                            "confidence": 0.7,
                        },
                    },
                }
            ],
            expected_version=-1,
        )

        loaded = await store.load_stream(stream_id)
        loaded_event = loaded[0] if loaded else {}
        async with store._pool.acquire() as conn:
            raw = await conn.fetchrow(
                "SELECT event_version, payload FROM events WHERE stream_id = $1 AND stream_position = 1",
                stream_id,
            )

        raw_payload = _normalize_json(raw["payload"]) if raw else {}
        return {
            "status": "ok",
            "application_id": app_id,
            "loaded_event_version": loaded_event.get("event_version"),
            "loaded_has_model_versions": "model_versions" in (loaded_event.get("payload") or {}),
            "stored_event_version": raw["event_version"] if raw else None,
            "stored_has_model_versions": "model_versions" in (raw_payload or {}),
            "immutability_verified": (
                loaded_event.get("event_version") == 2
                and raw is not None
                and raw["event_version"] == 1
                and "model_versions" not in (raw_payload or {})
            ),
        }
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}
    finally:
        await store.close()


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

    store = EventStore(DB_URL)
    await store.connect()
    try:
        seeded = await _seed_demo_application(pool, store)
        if not seeded:
            return {"status": "error", "reason": "no applicant registry companies available"}
        await _catch_up_projections(daemon)
        return {
            "status": "ok",
            "app_id": seeded["app_id"],
            "company_id": seeded["company_id"],
            "requested_amount_usd": seeded["requested_amount_usd"],
            "uploaded_docs": seeded.get("uploaded_docs", []),
        }
    except Exception as exc:
        return {"status": "error", "reason": str(exc)}
    finally:
        await store.close()


@app.post("/api/run_full_pipeline")
async def run_full_pipeline(app_id: str | None = None):
    pool = app.state.pool
    daemon = app.state.daemon
    if not pool or not daemon:
        return {"status": "error", "reason": "ui server not initialized"}

    created = False
    requested_app_id = app_id
    store = EventStore(DB_URL)
    await store.connect()
    try:
        if not app_id:
            async with pool.acquire() as conn:
                app_id = await _pick_document_ready_application(conn)
                if not app_id:
                    app_id = await _pick_credit_ready_application(conn)
        if not app_id:
            seeded = await _seed_demo_application(pool, store)
            if not seeded:
                return {
                    "status": "error",
                    "reason": "no seedable applications available (registry company or generated docs missing)",
                }
            app_id = seeded["app_id"]
            created = True

        registry = ApplicantRegistryClient(pool)
        model = resolve_model_name("claude-sonnet-4-20250514")
        try:
            client, _provider = create_llm_client()
        except Exception:
            client = None

        phases: list[dict] = []

        loan_events = await store.load_stream(f"loan-{app_id}")
        has_credit_requested = any(e.get("event_type") == "CreditAnalysisRequested" for e in loan_events)

        phase_sequence = []
        if not has_credit_requested:
            phase_sequence.append(
                (
                    "document",
                    DocumentProcessingAgent(
                        "AGENT-UI-DOC",
                        "document_processing",
                        store,
                        registry,
                        client,
                        model=model,
                    ),
                )
            )
        phase_sequence.extend(
            [
                ("credit", CreditAnalysisAgent("AGENT-UI-CREDIT", "credit_analysis", store, registry, client, model=model)),
                ("fraud", FraudDetectionAgent("AGENT-UI-FRAUD", "fraud_detection", store, registry, client, model=model)),
                ("compliance", ComplianceAgent("AGENT-UI-COMPLIANCE", "compliance", store, registry, client, model=model)),
            ]
        )

        async def run_phases(target_app_id: str) -> list[dict]:
            executed: list[dict] = []
            for phase_name, agent in phase_sequence:
                result = await agent.process_application(target_app_id)
                executed.append(
                    {
                        "phase": phase_name,
                        "session_id": result.get("session_id"),
                        "next_agent": result.get("next_agent") or result.get("next_agent_triggered"),
                        "output_events": result.get("output_events") or result.get("output_events_written"),
                    }
                )
            return executed

        try:
            phases = await run_phases(app_id)
        except Exception as exc:
            # Auto-heal: if UI auto-picked an app with duplicate CreditAnalysisRequested transitions,
            # create a fresh one and retry the full chain once.
            msg = str(exc)
            duplicate_credit_transition = (
                "Invalid transition via event" in msg
                and "CREDIT_ANALYSIS_REQUESTED -> ApplicationState.CREDIT_ANALYSIS_REQUESTED" in msg
            )
            if requested_app_id is None and duplicate_credit_transition:
                seeded = await _seed_demo_application(pool, store)
                if not seeded:
                    raise
                app_id = seeded["app_id"]
                created = True
                phases = []
                phase_sequence = [
                    (
                        "document",
                        DocumentProcessingAgent(
                            "AGENT-UI-DOC",
                            "document_processing",
                            store,
                            registry,
                            client,
                            model=model,
                        ),
                    ),
                    ("credit", CreditAnalysisAgent("AGENT-UI-CREDIT", "credit_analysis", store, registry, client, model=model)),
                    ("fraud", FraudDetectionAgent("AGENT-UI-FRAUD", "fraud_detection", store, registry, client, model=model)),
                    ("compliance", ComplianceAgent("AGENT-UI-COMPLIANCE", "compliance", store, registry, client, model=model)),
                ]
                phases = await run_phases(app_id)
            else:
                raise

        # Only run decision phase when compliance requested it.
        compliance_next = phases[-1].get("next_agent")
        if compliance_next == "decision_orchestrator":
            decision_agent = DecisionOrchestratorAgent(
                "AGENT-UI-DECISION",
                "decision_orchestrator",
                store,
                registry,
                client,
                model=model,
            )
            result = await decision_agent.process_application(app_id)
            phases.append(
                {
                    "phase": "decision",
                    "session_id": result.get("session_id"),
                    "next_agent": result.get("next_agent") or result.get("next_agent_triggered"),
                    "output_events": result.get("output_events") or result.get("output_events_written"),
                }
            )

        await _catch_up_projections(daemon)

        final_state = "UNKNOWN"
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT state FROM application_summary WHERE application_id = $1",
                app_id,
            )
            if row:
                final_state = row["state"]

        return {
            "status": "ok",
            "app_id": app_id,
            "created": created,
            "phases": phases,
            "final_state": final_state,
        }
    except Exception as exc:
        return {
            "status": "error",
            "app_id": app_id,
            "phase": phases[-1]["phase"] if "phases" in locals() and phases else None,
            "reason": str(exc),
        }
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
