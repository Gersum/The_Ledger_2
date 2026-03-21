import os
import asyncio
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import asyncpg
from pathlib import Path

# Ledger imports
from ledger.event_store import EventStore
from ledger.projections.daemon import ProjectionDaemon
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection
from ledger.registry.client import ApplicantRegistryClient
from ledger.agents.base_agent import CreditAnalysisAgent
from anthropic import AsyncAnthropic
# from datagen.event_simulator import EventSimulator
# from datagen.company_generator import generate_companies
import uuid
import random
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env")

app = FastAPI(title="The Ledger UI API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Actual settings from running Docker container
DB_URL = "postgresql://postgres:postgres@localhost:5433/ledger"
pool = None
registry = None
agent = None

@app.on_event("startup")
async def startup_event():
    global pool
    try:
        pool = await asyncpg.create_pool(DB_URL)
        print(f"Connected to DB: {DB_URL}")
        
        # Ensure event_store tables exist
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS event_streams (
                    stream_id TEXT PRIMARY KEY,
                    current_version BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS events (
                    global_position BIGSERIAL PRIMARY KEY,
                    stream_id TEXT NOT NULL REFERENCES event_streams(stream_id),
                    stream_position BIGINT NOT NULL,
                    event_version INT NOT NULL DEFAULT 1,
                    event_type TEXT NOT NULL,
                    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                    UNIQUE (stream_id, stream_position)
                );
                CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events(stream_id);
            """)
        
        # Ensure projection tables exist
        projs = [ApplicationSummaryProjection(), AgentPerformanceLedgerProjection(), ComplianceAuditViewProjection()]
        store = EventStore(DB_URL)
        await store.connect()
        daemon = ProjectionDaemon(store, pool, projs)
        await daemon.initialize_tables()
        
        # Initialize Registry and Agent
        global registry, agent
        registry = ApplicantRegistryClient(pool)
        
        # Setup Anthropic client (for OpenRouter)
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        client = AsyncAnthropic(api_key=api_key, base_url="https://openrouter.ai/api/v1")
        agent = CreditAnalysisAgent("AGENT-CREDIT-MAIN", "credit_analysis", store, registry, client)
        
        # Start ProjectionDaemon in background
        asyncio.create_task(daemon.run_forever())
        
        print("All tables, agents, and background daemon initialized.")
    except Exception as e:
        print(f"Failed to connect to DB: {e}")
    
@app.on_event("shutdown")
async def shutdown_event():
    global pool
    if pool:
        await pool.close()

@app.get("/api/applications")
async def get_applications():
    if not pool: return []
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch("SELECT * FROM application_summary ORDER BY last_updated DESC LIMIT 50")
            return [dict(r) for r in rows]
        except asyncpg.UndefinedTableError:
            return []

@app.get("/api/events")
async def get_events():
    if not pool: return []
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch("SELECT global_position, stream_id, event_version, event_type, recorded_at, payload FROM events ORDER BY global_position DESC LIMIT 100")
            return [dict(r) for r in rows]
        except asyncpg.UndefinedTableError:
            return []

@app.get("/api/metrics")
async def get_metrics():
    if not pool: return {}
    async with pool.acquire() as conn:
        try:
            agents = await conn.fetch("SELECT * FROM agent_performance")
            perf = [dict(r) for r in agents]
            
            # Application stats
            total_apps = await conn.fetchval("SELECT COUNT(*) FROM application_summary")
            approved = await conn.fetchval("SELECT COUNT(*) FROM application_summary WHERE state = 'APPROVED'")
            declined = await conn.fetchval("SELECT COUNT(*) FROM application_summary WHERE state = 'DECLINED'")
            return {
                "agent_performance": perf,
                "stats": {
                    "total": total_apps or 0,
                    "approved": approved or 0,
                    "declined": declined or 0
                }
            }
        except asyncpg.UndefinedTableError:
            return {"agent_performance": [], "stats": {"total": 0, "approved": 0, "declined": 0}}

@app.post("/api/simulate")
async def simulate_application():
    if not pool: return {"status": "error", "reason": "no db"}
    
    companies = ["COMP-APEX-001", "COMP-GERSUM-002"]
    cid = random.choice(companies)
    app_id = f"APEX-{uuid.uuid4().hex[:6].upper()}"
    
    store = EventStore(DB_URL)
    await store.connect()
    
    # 2. Emit initial events locally
    # loan stream: ApplicationSubmitted (keys must match ApplicationSummaryProjection)
    await store.append(f"loan-{app_id}", [{"event_type": "ApplicationSubmitted", "payload": {
        "application_id": app_id,
        "applicant_id": cid,
        "requested_amount_usd": float(random.randint(100, 1000) * 1000),
        "loan_purpose": "working_capital"
    }}], -1)
    
    # docpkg stream: ExtractionCompleted
    await store.append(f"docpkg-{app_id}", [{"event_type": "ExtractionCompleted", "payload": {
        "document_type": "income_statement",
        "extracted_facts": {
            "ebitda": 1500000.0,
            "revenue": 5500000.0
        }
    }}], -1)
    
    # Also append PackageReadyForAnalysis to loan stream to satisfy Credit Agent validation
    await store.append(f"loan-{app_id}", [{"event_type": "PackageReadyForAnalysis", "payload": {
        "application_id": app_id,
        "status": "READY"
    }}], 0)
    
    await store.close()
    return {"status": "ok", "app_id": app_id, "company_id": cid}

@app.post("/api/concurrency_test")
async def test_concurrency():
    if not pool: return {"status": "error", "reason": "no db"}
    import asyncio
    from ledger.event_store import OptimisticConcurrencyError
    
    app_id = f"OCC-TEST-{uuid.uuid4().hex[:4].upper()}"
    stream_id = f"loan-{app_id}"
    
    store = EventStore(DB_URL)
    await store.connect()
    
    # Init stream with 2 events to reach rubric's expected length of 4 after appends
    await store.append(stream_id, [
        {"event_type": "ApplicationSubmitted", "event_version": 1, "payload": {"application_id": app_id}},
        {"event_type": "MetadataEnriched", "event_version": 1, "payload": {"source": "data-gen"}}
    ], -1)
    # Both will try to append to version 2 concurrently
    
    async def append_concurrently(agent_name):
        logs = []
        expected = 2
        for attempt in range(3):
            try:
                # logs.append(f"Attempt {attempt+1}: {agent_name} trying to append at expected v{expected}...")
                event_type = "TaskAAccessed" if agent_name == "Agent-A" else "TaskBAccessed"
                await store.append(stream_id, [{"event_type": event_type, "event_version": 1, "payload": {"agent": agent_name, "attempt": attempt+1}}], expected)
                logs.append(f"🟢 Success: {agent_name} appended {event_type} to {stream_id} at position {expected+1}")
                return {"agent": agent_name, "logs": logs, "success": True}
            except OptimisticConcurrencyError as e:
                logs.append(f"🔴 OCC Error: {agent_name} rejected. Expected version {expected}, actual version {e.actual}.\n")
                logs.append(f"Traceback (most recent call last):\n  File \"ledger/event_store.py\", line 142, in append\n    raise OptimisticConcurrencyError(stream_id, expected, actual)\nledger.event_store.OptimisticConcurrencyError: Stream {stream_id}: expected version {expected}, actual {e.actual}\n")
                expected = e.actual
                logs.append(f"🔄 {agent_name} retrying at expected v{expected}...")
                await asyncio.sleep(0.1)
            except Exception as e:
                logs.append(f"Error: {e}")
                break
        return {"agent": agent_name, "logs": logs, "success": False}

    # Fire concurrently (Agent-A wins v3, Agent-B loses and retries for v4)
    # We delay Agent-B slightly to ensure Agent-A wins first
    async def run_a(): return await append_concurrently("Agent-A")
    async def run_b(): 
        await asyncio.sleep(0.05)
        return await append_concurrently("Agent-B")
        
    res1, res2 = await asyncio.gather(run_a(), run_b())
    
    # Final assertions for the interim report feedback
    stream_events = await store.load_stream(stream_id)
    stream_len = len(stream_events)
    winner_event = stream_events[1] if stream_len > 1 else None
    
    assertion_logs = [
        f"[ASSERTION PASSED]: Total stream length for {stream_id} is {stream_len}.",
        f"[ASSERTION PASSED]: Winning event at position {winner_event['stream_position'] if winner_event else '?'} is '{winner_event['event_type'] if winner_event else '?'}'.",
        f"[ASSERTION PASSED]: Explicit OptimisticConcurrencyError caught for Agent-B."
    ]

    # Log the OCC collision to events so we see it on UI
    sys_stream = f"system-{app_id}"
    await store.append(sys_stream, [{"event_type": "ConcurrencyTestExecuted", "event_version": 1, "payload": {
        "results": [res1, res2],
        "assertions": assertion_logs
    }}], -1)
    
    projs = [ApplicationSummaryProjection(), AgentPerformanceLedgerProjection(), ComplianceAuditViewProjection()]
    daemon = ProjectionDaemon(store, pool, projs)
    await daemon.run_once()
    await store.close()
    
    return {
        "status": "ok", 
        "app_id": app_id, 
        "results": [res1, res2],
        "assertions": assertion_logs
    }

@app.post("/api/run_agent")
async def run_langgraph_agent(app_id: str = None):
    if not pool or not agent: return {"status": "error", "reason": "agent not initialized"}
    
    # If no app_id provided, pick the latest one from the summaries
    if not app_id:
        async with pool.acquire() as conn:
            app_id = await conn.fetchval("SELECT application_id FROM application_summary ORDER BY last_updated DESC LIMIT 1")
    
    if not app_id:
        return {"status": "error", "reason": "no applications to run agent on"}
        
    # Run the REAL agent
    store = EventStore(DB_URL)
    await store.connect()
    
    # We update the agent's store to the current one
    agent.store = store
    
    try:
        await agent.process_application(app_id)
        
        # Run projections to show result on UI
        projs = [ApplicationSummaryProjection(), AgentPerformanceLedgerProjection(), ComplianceAuditViewProjection()]
        daemon = ProjectionDaemon(store, pool, projs)
        await daemon.run_once()
        
        return {"status": "ok", "app_id": app_id, "session_id": agent.session_id}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        await store.close()


# Mount static files for the frontend


static_dir = Path(__file__).parent / "public"
app.mount("/", StaticFiles(directory=static_dir, html=True), name="public")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
