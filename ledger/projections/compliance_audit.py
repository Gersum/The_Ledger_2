"""
ledger/projections/compliance_audit.py
=======================================
ComplianceAuditView projection — one row per rule per application.
SLO: p99 < 200ms. Supports temporal query: get_compliance_at(app_id, timestamp).
Rebuild-from-scratch: drop table, replay all events from position 0.
"""
from __future__ import annotations
from datetime import datetime


def parse_dt(val):
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return None


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS compliance_audit (
    application_id      TEXT NOT NULL,
    rule_id             TEXT NOT NULL,
    rule_name           TEXT,
    rule_version        TEXT,
    verdict             TEXT NOT NULL,          -- PASSED | FAILED | NOTED
    is_hard_block       BOOLEAN NOT NULL DEFAULT FALSE,
    failure_reason      TEXT,
    remediation_steps   TEXT,
    evidence_hash       TEXT,
    evaluated_at        TIMESTAMPTZ,
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (application_id, rule_id)
);

CREATE TABLE IF NOT EXISTS compliance_check_summary (
    application_id      TEXT PRIMARY KEY,
    verdict             TEXT,                   -- CLEAR | BLOCKED | CONDITIONAL
    hard_block_rule     TEXT,
    rules_evaluated     TEXT[],
    completed_at        TIMESTAMPTZ,
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class ComplianceAuditViewProjection:
    """
    Handles compliance events and builds:
    - compliance_audit: per-rule evaluation rows
    - compliance_check_summary: overall verdict per application
    """

    def can_handle(self, event_type: str) -> bool:
        return event_type in {
            "ComplianceCheckInitiated",
            "ComplianceRulePassed",
            "ComplianceRuleFailed",
            "ComplianceRuleNoted",
            "ComplianceCheckCompleted",
        }

    async def handle(self, event: dict, conn) -> None:
        et = event["event_type"]
        p = event.get("payload", {})
        app_id = p.get("application_id")
        if not app_id:
            return
        now = datetime.utcnow()

        if et == "ComplianceRulePassed":
            await conn.execute(
                """
                INSERT INTO compliance_audit
                    (application_id, rule_id, rule_name, rule_version, verdict,
                     is_hard_block, evidence_hash, evaluated_at, last_updated)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (application_id, rule_id) DO UPDATE SET
                    verdict = EXCLUDED.verdict, rule_name = EXCLUDED.rule_name,
                    rule_version = EXCLUDED.rule_version, evidence_hash = EXCLUDED.evidence_hash,
                    evaluated_at = EXCLUDED.evaluated_at, last_updated = EXCLUDED.last_updated
                """,
                app_id, p.get("rule_id"), p.get("rule_name"), p.get("rule_version"),
                'PASSED', False,
                p.get("evidence_hash"), parse_dt(p.get("evaluated_at")) or now, now,
            )

        elif et == "ComplianceRuleFailed":
            await conn.execute(
                """
                INSERT INTO compliance_audit
                    (application_id, rule_id, rule_name, rule_version, verdict,
                     is_hard_block, failure_reason, remediation_steps,
                     evidence_hash, evaluated_at, last_updated)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (application_id, rule_id) DO UPDATE SET
                    verdict = EXCLUDED.verdict, is_hard_block = EXCLUDED.is_hard_block,
                    failure_reason = EXCLUDED.failure_reason,
                    remediation_steps = EXCLUDED.remediation_steps,
                    evidence_hash = EXCLUDED.evidence_hash,
                    evaluated_at = EXCLUDED.evaluated_at, last_updated = EXCLUDED.last_updated
                """,
                app_id, p.get("rule_id"), p.get("rule_name"), p.get("rule_version"),
                'FAILED', p.get("is_hard_block", False), p.get("failure_reason"),
                p.get("remediation_steps"), p.get("evidence_hash"),
                parse_dt(p.get("evaluated_at")) or now, now,
            )

        elif et == "ComplianceRuleNoted":
            await conn.execute(
                """
                INSERT INTO compliance_audit
                    (application_id, rule_id, rule_name, rule_version, verdict,
                     is_hard_block, evaluated_at, last_updated)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (application_id, rule_id) DO UPDATE SET
                    verdict = EXCLUDED.verdict, evaluated_at = EXCLUDED.evaluated_at,
                    last_updated = EXCLUDED.last_updated
                """,
                app_id, p.get("rule_id"), p.get("rule_name"), p.get("rule_version"),
                'NOTED', False,
                parse_dt(p.get("evaluated_at")) or now, now,
            )

        elif et == "ComplianceCheckCompleted":
            await conn.execute(
                """
                INSERT INTO compliance_check_summary
                    (application_id, verdict, hard_block_rule, rules_evaluated, completed_at, last_updated)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (application_id) DO UPDATE SET
                    verdict = EXCLUDED.verdict, hard_block_rule = EXCLUDED.hard_block_rule,
                    rules_evaluated = EXCLUDED.rules_evaluated,
                    completed_at = EXCLUDED.completed_at, last_updated = EXCLUDED.last_updated
                """,
                app_id, p.get("verdict"), p.get("hard_block_rule"),
                p.get("rules_evaluated") or [],
                parse_dt(p.get("completed_at")) or now, now,
            )

    # ── TEMPORAL QUERY ──────────────────────────────────────────────────────────

    @staticmethod
    async def get_compliance_at(conn, application_id: str, as_of: str) -> dict:
        """
        Return compliance state as it existed at `as_of` timestamp.
        This performs a point-in-time snapshot from the live table using evaluated_at.

        SLO: p99 < 200ms
        """
        rows = await conn.fetch(
            """
            SELECT rule_id, rule_name, verdict, is_hard_block, failure_reason,
                   remediation_steps, evidence_hash, evaluated_at
            FROM compliance_audit
            WHERE application_id = $1 AND evaluated_at <= $2
            ORDER BY rule_id
            """,
            application_id,
            as_of,
        )
        summary_row = await conn.fetchrow(
            """
            SELECT verdict, hard_block_rule, rules_evaluated, completed_at
            FROM compliance_check_summary
            WHERE application_id = $1 AND completed_at <= $2
            """,
            application_id,
            as_of,
        )
        return {
            "application_id": application_id,
            "as_of": as_of,
            "overall_verdict": dict(summary_row).get("verdict") if summary_row else None,
            "hard_block_rule": dict(summary_row).get("hard_block_rule") if summary_row else None,
            "rules": [dict(r) for r in rows],
        }

    @staticmethod
    async def rebuild_from_scratch(pool, store) -> None:
        """
        Blue/Green rebuild: build into _new tables, swap atomically.
        Allows live reads to continue without interruption.
        """
        async with pool.acquire() as conn:
            # Create _new tables
            await conn.execute(CREATE_TABLE_SQL.replace(
                "compliance_audit", "compliance_audit_new"
            ).replace(
                "compliance_check_summary", "compliance_check_summary_new"
            ))
            # Replay all events
            projection = ComplianceAuditViewProjection()
            async for event in store.load_all(from_position=0):
                if projection.can_handle(event["event_type"]):
                    await projection.handle(event, conn)
            # Atomic swap
            await conn.execute("""
                BEGIN;
                DROP TABLE IF EXISTS compliance_audit_old;
                DROP TABLE IF EXISTS compliance_check_summary_old;
                ALTER TABLE IF EXISTS compliance_audit RENAME TO compliance_audit_old;
                ALTER TABLE IF EXISTS compliance_check_summary RENAME TO compliance_check_summary_old;
                ALTER TABLE compliance_audit_new RENAME TO compliance_audit;
                ALTER TABLE compliance_check_summary_new RENAME TO compliance_check_summary;
                COMMIT;
            """)
