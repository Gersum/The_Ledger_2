"""
ledger/projections/agent_performance.py
========================================
AgentPerformanceLedger projection — aggregate metrics per agent_type + model_version.
SLO: p99 < 50ms. Idempotent on event_id deduplication.
"""
from __future__ import annotations
from datetime import datetime


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS agent_performance (
    agent_type          TEXT NOT NULL,
    model_version       TEXT NOT NULL,
    total_sessions      BIGINT NOT NULL DEFAULT 0,
    total_nodes         BIGINT NOT NULL DEFAULT 0,
    total_llm_calls     BIGINT NOT NULL DEFAULT 0,
    total_tokens        BIGINT NOT NULL DEFAULT 0,
    total_cost_usd      NUMERIC NOT NULL DEFAULT 0,
    total_duration_ms   BIGINT NOT NULL DEFAULT 0,
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agent_type, model_version)
);

CREATE TABLE IF NOT EXISTS agent_performance_processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class AgentPerformanceLedgerProjection:
    """Tracks cumulative metrics per agent_type + model_version pair."""

    def can_handle(self, event_type: str) -> bool:
        return event_type in {"AgentSessionCompleted", "AgentNodeExecuted"}

    async def handle(self, event: dict, conn) -> None:
        et = event["event_type"]
        p = event.get("payload", {})
        event_id = str(event.get("event_id") or event.get("global_position") or "")
        now = datetime.utcnow().isoformat()

        # Idempotency: skip if already processed
        if event_id:
            already = await conn.fetchval(
                "SELECT 1 FROM agent_performance_processed_events WHERE event_id = $1",
                event_id,
            )
            if already:
                return
            await conn.execute(
                "INSERT INTO agent_performance_processed_events (event_id, processed_at) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                event_id, now,
            )

        agent_type = p.get("agent_type", "unknown")
        model_version = p.get("model_version") or "unknown"

        if et == "AgentSessionCompleted":
            await conn.execute(
                """
                INSERT INTO agent_performance
                    (agent_type, model_version, total_sessions, total_nodes, total_llm_calls,
                     total_tokens, total_cost_usd, total_duration_ms, last_updated)
                VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (agent_type, model_version) DO UPDATE SET
                    total_sessions    = agent_performance.total_sessions + 1,
                    total_nodes       = agent_performance.total_nodes + EXCLUDED.total_nodes,
                    total_llm_calls   = agent_performance.total_llm_calls + EXCLUDED.total_llm_calls,
                    total_tokens      = agent_performance.total_tokens + EXCLUDED.total_tokens,
                    total_cost_usd    = agent_performance.total_cost_usd + EXCLUDED.total_cost_usd,
                    total_duration_ms = agent_performance.total_duration_ms + EXCLUDED.total_duration_ms,
                    last_updated      = EXCLUDED.last_updated
                """,
                agent_type,
                model_version,
                p.get("total_nodes_executed") or 0,
                p.get("total_llm_calls") or 0,
                p.get("total_tokens_used") or 0,
                p.get("total_cost_usd") or 0.0,
                p.get("total_duration_ms") or 0,
                now,
            )
