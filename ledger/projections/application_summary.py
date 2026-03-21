"""
ledger/projections/application_summary.py
==========================================
ApplicationSummary projection — one row per application, current state.
SLO: p99 < 50ms. Idempotent (same event twice = same resulting row).
"""
from __future__ import annotations
from datetime import datetime


def parse_dt(val):
    if not val: return None
    if isinstance(val, datetime): return val
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except:
        return None


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS application_summary (
    application_id      TEXT PRIMARY KEY,
    applicant_id        TEXT,
    state               TEXT NOT NULL DEFAULT 'NEW',
    requested_amount_usd NUMERIC,
    loan_purpose        TEXT,
    recommendation      TEXT,
    confidence          NUMERIC,
    fraud_score         NUMERIC,
    compliance_verdict  TEXT,
    submitted_at        TIMESTAMPTZ,
    decided_at          TIMESTAMPTZ,
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class ApplicationSummaryProjection:
    """
    Handles events and updates the application_summary table.
    Must be idempotent: processing the same event twice yields identical state.
    """

    def can_handle(self, event_type: str) -> bool:
        return event_type in {
            "ApplicationSubmitted",
            "PackageReadyForAnalysis",
            "CreditAnalysisRequested",
            "FraudScreeningRequested",
            "ComplianceCheckRequested",
            "DecisionGenerated",
            "ApplicationApproved",
            "ApplicationDeclined",
            "HumanReviewRequested",
            "HumanReviewCompleted",
        }

    async def handle(self, event: dict, conn) -> None:
        et = event["event_type"]
        p = event.get("payload", {})
        app_id = p.get("application_id")
        if not app_id:
            return

        now = datetime.utcnow()

        if et == "ApplicationSubmitted":
            # Map parameters explicitly to avoid index confusion
            applicant_id = p.get("applicant_id") or p.get("company_id")
            amount = p.get("requested_amount_usd") or p.get("requested_amount")
            purpose = p.get("loan_purpose")
            submitted_at = parse_dt(p.get("submitted_at")) or now
            
            await conn.execute(
                """
                INSERT INTO application_summary
                    (application_id, applicant_id, state, requested_amount_usd,
                     loan_purpose, submitted_at, last_updated)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (application_id) DO UPDATE SET
                    applicant_id = EXCLUDED.applicant_id,
                    state = EXCLUDED.state,
                    requested_amount_usd = EXCLUDED.requested_amount_usd,
                    loan_purpose = EXCLUDED.loan_purpose,
                    submitted_at = EXCLUDED.submitted_at,
                    last_updated = EXCLUDED.last_updated
                """,
                app_id,
                applicant_id,
                'SUBMITTED',
                amount,
                purpose,
                submitted_at,
                now,
            )

        elif et in ("CreditAnalysisRequested",):
            await conn.execute(
                "UPDATE application_summary SET state = 'CREDIT_ANALYSIS_REQUESTED', last_updated = $2 WHERE application_id = $1",
                app_id, now,
            )

        elif et == "FraudScreeningRequested":
            await conn.execute(
                "UPDATE application_summary SET state = 'FRAUD_SCREENING_REQUESTED', last_updated = $2 WHERE application_id = $1",
                app_id, now,
            )

        elif et == "ComplianceCheckRequested":
            await conn.execute(
                "UPDATE application_summary SET state = 'COMPLIANCE_CHECK_REQUESTED', last_updated = $2 WHERE application_id = $1",
                app_id, now,
            )

        elif et == "DecisionGenerated":
            await conn.execute(
                """
                UPDATE application_summary
                SET recommendation = $2, confidence = $3, state = $5,
                    decided_at = $4, last_updated = $4
                WHERE application_id = $1
                """,
                app_id,
                p.get("recommendation"),
                p.get("confidence") or p.get("confidence_score") or 0.0,
                now,
                'PENDING_DECISION',
            )

        elif et == "ApplicationApproved":
            await conn.execute(
                "UPDATE application_summary SET state = 'APPROVED', last_updated = $2 WHERE application_id = $1",
                app_id, now,
            )

        elif et == "ApplicationDeclined":
            await conn.execute(
                "UPDATE application_summary SET state = 'DECLINED', last_updated = $2 WHERE application_id = $1",
                app_id, now,
            )

        elif et == "HumanReviewRequested":
            await conn.execute(
                "UPDATE application_summary SET state = 'PENDING_HUMAN_REVIEW', last_updated = $2 WHERE application_id = $1",
                app_id, now,
            )
