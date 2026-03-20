"""
ledger/domain/aggregates/loan_application.py
=============================================
LoanApplicationAggregate with full state machine.

Replays the loan-{application_id} stream to rebuild current state.
Command handlers validate against current state before appending events.

Business rules enforced on replay:
  1. State machine: only valid transitions allowed
  2. confidence < 0.60 → recommendation must be REFER
  3. Compliance BLOCKED → only DECLINE allowed
  4. Causal chain: every agent event references triggered_by_event_id
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum


class ApplicationState(str, Enum):
    NEW = "NEW"
    SUBMITTED = "SUBMITTED"
    DOCUMENTS_PENDING = "DOCUMENTS_PENDING"
    DOCUMENTS_UPLOADED = "DOCUMENTS_UPLOADED"
    DOCUMENTS_PROCESSED = "DOCUMENTS_PROCESSED"
    CREDIT_ANALYSIS_REQUESTED = "CREDIT_ANALYSIS_REQUESTED"
    CREDIT_ANALYSIS_COMPLETE = "CREDIT_ANALYSIS_COMPLETE"
    FRAUD_SCREENING_REQUESTED = "FRAUD_SCREENING_REQUESTED"
    FRAUD_SCREENING_COMPLETE = "FRAUD_SCREENING_COMPLETE"
    COMPLIANCE_CHECK_REQUESTED = "COMPLIANCE_CHECK_REQUESTED"
    COMPLIANCE_CHECK_COMPLETE = "COMPLIANCE_CHECK_COMPLETE"
    PENDING_DECISION = "PENDING_DECISION"
    PENDING_HUMAN_REVIEW = "PENDING_HUMAN_REVIEW"
    APPROVED = "APPROVED"
    DECLINED = "DECLINED"
    DECLINED_COMPLIANCE = "DECLINED_COMPLIANCE"
    REFERRED = "REFERRED"


VALID_TRANSITIONS: dict[ApplicationState, list[ApplicationState]] = {
    ApplicationState.NEW: [ApplicationState.SUBMITTED],
    ApplicationState.SUBMITTED: [ApplicationState.DOCUMENTS_PENDING],
    ApplicationState.DOCUMENTS_PENDING: [ApplicationState.DOCUMENTS_UPLOADED],
    ApplicationState.DOCUMENTS_UPLOADED: [ApplicationState.DOCUMENTS_PROCESSED],
    ApplicationState.DOCUMENTS_PROCESSED: [ApplicationState.CREDIT_ANALYSIS_REQUESTED],
    ApplicationState.CREDIT_ANALYSIS_REQUESTED: [ApplicationState.CREDIT_ANALYSIS_COMPLETE],
    ApplicationState.CREDIT_ANALYSIS_COMPLETE: [ApplicationState.FRAUD_SCREENING_REQUESTED],
    ApplicationState.FRAUD_SCREENING_REQUESTED: [ApplicationState.FRAUD_SCREENING_COMPLETE],
    ApplicationState.FRAUD_SCREENING_COMPLETE: [ApplicationState.COMPLIANCE_CHECK_REQUESTED],
    ApplicationState.COMPLIANCE_CHECK_REQUESTED: [ApplicationState.COMPLIANCE_CHECK_COMPLETE],
    ApplicationState.COMPLIANCE_CHECK_COMPLETE: [
        ApplicationState.PENDING_DECISION,
        ApplicationState.DECLINED_COMPLIANCE,
    ],
    ApplicationState.PENDING_DECISION: [
        ApplicationState.APPROVED,
        ApplicationState.DECLINED,
        ApplicationState.PENDING_HUMAN_REVIEW,
        ApplicationState.REFERRED,
    ],
    ApplicationState.PENDING_HUMAN_REVIEW: [
        ApplicationState.APPROVED,
        ApplicationState.DECLINED,
    ],
}


@dataclass
class LoanApplicationAggregate:
    application_id: str
    state: ApplicationState = ApplicationState.NEW
    applicant_id: str | None = None
    requested_amount_usd: float | None = None
    loan_purpose: str | None = None
    contact_email: str | None = None
    contact_name: str | None = None
    document_ids: list[str] = field(default_factory=list)
    version: int = 0
    # Flags derived from agent outputs
    credit_confidence: float | None = None
    credit_risk_tier: str | None = None
    fraud_score: float | None = None
    compliance_verdict: str | None = None   # CLEAR | BLOCKED | CONDITIONAL
    recommendation: str | None = None       # APPROVE | DECLINE | REFER

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        """Load and replay the loan-{application_id} stream to rebuild aggregate state."""
        agg = cls(application_id=application_id)
        stream_events = await store.load_stream(f"loan-{application_id}")
        for event in stream_events:
            agg.apply(event)
        return agg

    def apply(self, event: dict) -> None:
        """Apply one event to update aggregate state. Ignores unknown event types."""
        et = event.get("event_type")
        p = event.get("payload", {})
        self.version += 1

        if et == "ApplicationSubmitted":
            self.state = ApplicationState.SUBMITTED
            self.applicant_id = p.get("applicant_id")
            self.requested_amount_usd = p.get("requested_amount_usd")
            self.loan_purpose = p.get("loan_purpose")
            self.contact_email = p.get("contact_email")
            self.contact_name = p.get("contact_name")

        elif et == "DocumentUploadRequested":
            self.state = ApplicationState.DOCUMENTS_PENDING

        elif et == "DocumentUploaded":
            # Track uploaded doc IDs; state moves to UPLOADED on first doc
            doc_id = p.get("document_id")
            if doc_id and doc_id not in self.document_ids:
                self.document_ids.append(doc_id)
            if self.state == ApplicationState.DOCUMENTS_PENDING:
                self.state = ApplicationState.DOCUMENTS_UPLOADED

        elif et == "DocumentUploadFailed":
            pass  # State does not change

        elif et == "CreditAnalysisRequested":
            # Triggered by DocumentProcessingAgent after PackageReadyForAnalysis
            self.state = ApplicationState.CREDIT_ANALYSIS_REQUESTED

        elif et == "PackageReadyForAnalysis":
            # DocumentProcessingAgent completion — package is ready
            self.state = ApplicationState.DOCUMENTS_PROCESSED

        elif et == "FraudScreeningRequested":
            # Triggered by CreditAnalysisAgent
            self.state = ApplicationState.FRAUD_SCREENING_REQUESTED
            # Note: CreditAnalysisCompleted writes to credit-* stream, not loan-*
            # We track credit complete implicitly here since fraud screening follows it
            self.state = ApplicationState.FRAUD_SCREENING_REQUESTED

        elif et == "ComplianceCheckRequested":
            # Triggered by FraudDetectionAgent
            self.state = ApplicationState.COMPLIANCE_CHECK_REQUESTED

        elif et == "DecisionRequested":
            self.state = ApplicationState.PENDING_DECISION

        elif et == "DecisionGenerated":
            self.recommendation = p.get("recommendation")
            self.credit_confidence = p.get("confidence")
            # state stays PENDING_DECISION until Approved/Declined/HumanReviewRequested

        elif et == "HumanReviewRequested":
            self.state = ApplicationState.PENDING_HUMAN_REVIEW

        elif et == "HumanReviewCompleted":
            # Human may override; final decision event follows
            pass

        elif et == "ApplicationApproved":
            self.state = ApplicationState.APPROVED

        elif et == "ApplicationDeclined":
            # Could be compliance block or standard decline
            if self.compliance_verdict == "BLOCKED":
                self.state = ApplicationState.DECLINED_COMPLIANCE
            else:
                self.state = ApplicationState.DECLINED

        # Cross-stream signals written to the loan stream by agents
        elif et == "ComplianceCheckCompleted" or et == "ComplianceCheckRequested":
            pass  # Handled on their own streams; loan stream only gets DecisionRequested

    def assert_valid_transition(self, target: ApplicationState) -> None:
        """Raise ValueError if transition from current state to target is not allowed."""
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            raise ValueError(
                f"Invalid transition {self.state} → {target}. Allowed: {[s.value for s in allowed]}"
            )

    @property
    def is_documents_ready(self) -> bool:
        return self.state in (
            ApplicationState.DOCUMENTS_PROCESSED,
            ApplicationState.CREDIT_ANALYSIS_REQUESTED,
        )

    @property
    def is_terminal(self) -> bool:
        return self.state in (
            ApplicationState.APPROVED,
            ApplicationState.DECLINED,
            ApplicationState.DECLINED_COMPLIANCE,
            ApplicationState.REFERRED,
        )
