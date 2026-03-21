"""
loan_application.py
===================
Replayable aggregate for the top-level loan application stream.

This aggregate only rehydrates facts present on ``loan-{application_id}``.
Cross-stream requirements such as "all 6 compliance rules finished" still need
to be checked by command handlers before they emit the corresponding loan event.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


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


VALID_TRANSITIONS = {
    ApplicationState.NEW: [ApplicationState.SUBMITTED],
    ApplicationState.SUBMITTED: [ApplicationState.DOCUMENTS_PENDING],
    ApplicationState.DOCUMENTS_PENDING: [ApplicationState.DOCUMENTS_UPLOADED],
    ApplicationState.DOCUMENTS_UPLOADED: [ApplicationState.CREDIT_ANALYSIS_REQUESTED],
    ApplicationState.CREDIT_ANALYSIS_REQUESTED: [ApplicationState.FRAUD_SCREENING_REQUESTED],
    ApplicationState.FRAUD_SCREENING_REQUESTED: [ApplicationState.COMPLIANCE_CHECK_REQUESTED],
    ApplicationState.COMPLIANCE_CHECK_REQUESTED: [
        ApplicationState.PENDING_DECISION,
        ApplicationState.DECLINED_COMPLIANCE,
    ],
    ApplicationState.COMPLIANCE_CHECK_COMPLETE: [
        ApplicationState.PENDING_DECISION,
        ApplicationState.DECLINED_COMPLIANCE,
    ],
    ApplicationState.PENDING_DECISION: [
        ApplicationState.PENDING_HUMAN_REVIEW,
        ApplicationState.APPROVED,
        ApplicationState.DECLINED,
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
    required_document_types: list[str] = field(default_factory=list)
    uploaded_document_ids: list[str] = field(default_factory=list)
    latest_event_type: str | None = None
    latest_decision_recommendation: str | None = None
    latest_decision_confidence: float | None = None
    all_analyses_complete: bool = False
    human_review_requested: bool = False
    human_review_completed: bool = False
    final_decision: str | None = None
    decline_reasons: list[str] = field(default_factory=list)
    adverse_action_codes: list[str] = field(default_factory=list)
    version: int = 0
    events: list[dict] = field(default_factory=list)

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        """Load and replay ``loan-{application_id}`` to rebuild state."""
        aggregate = cls(application_id=application_id)
        for event in await store.load_stream(f"loan-{application_id}"):
            aggregate.apply(event)
        return aggregate

    @property
    def is_terminal(self) -> bool:
        return self.state in {
            ApplicationState.APPROVED,
            ApplicationState.DECLINED,
            ApplicationState.DECLINED_COMPLIANCE,
        }

    def apply(self, event: dict) -> None:
        """Apply a loan-stream event and enforce command-side invariants."""
        event_type = event.get("event_type")
        payload = dict(event.get("payload", {}))
        self._assert_application_id(payload, event_type)

        if event_type == "ApplicationSubmitted":
            self._transition(ApplicationState.SUBMITTED, [ApplicationState.NEW])
            self.applicant_id = payload.get("applicant_id")
            self.requested_amount_usd = self._coerce_float(payload.get("requested_amount_usd"))
            self.loan_purpose = payload.get("loan_purpose")

        elif event_type == "DocumentUploadRequested":
            self._transition(ApplicationState.DOCUMENTS_PENDING, [ApplicationState.SUBMITTED])
            self.required_document_types = list(payload.get("required_document_types", []))

        elif event_type == "DocumentUploaded":
            self._transition(
                ApplicationState.DOCUMENTS_UPLOADED,
                [ApplicationState.DOCUMENTS_PENDING, ApplicationState.DOCUMENTS_UPLOADED],
                allow_same_state=True,
            )
            document_id = payload.get("document_id")
            if document_id and document_id not in self.uploaded_document_ids:
                self.uploaded_document_ids.append(document_id)

        elif event_type == "DocumentUploadFailed":
            self._require_state(
                event_type,
                [ApplicationState.DOCUMENTS_PENDING, ApplicationState.DOCUMENTS_UPLOADED],
            )

        elif event_type == "CreditAnalysisRequested":
            self._transition(
                ApplicationState.CREDIT_ANALYSIS_REQUESTED,
                [ApplicationState.DOCUMENTS_UPLOADED, ApplicationState.DOCUMENTS_PROCESSED],
            )

        elif event_type == "FraudScreeningRequested":
            self._require_nonempty(payload, "triggered_by_event_id", event_type)
            self._transition(
                ApplicationState.FRAUD_SCREENING_REQUESTED,
                [ApplicationState.CREDIT_ANALYSIS_REQUESTED, ApplicationState.CREDIT_ANALYSIS_COMPLETE],
            )

        elif event_type == "ComplianceCheckRequested":
            self._require_nonempty(payload, "triggered_by_event_id", event_type)
            self._transition(
                ApplicationState.COMPLIANCE_CHECK_REQUESTED,
                [ApplicationState.FRAUD_SCREENING_REQUESTED, ApplicationState.FRAUD_SCREENING_COMPLETE],
            )

        elif event_type == "DecisionRequested":
            self._require_nonempty(payload, "triggered_by_event_id", event_type)
            if not payload.get("all_analyses_complete"):
                raise ValueError("DecisionRequested requires all_analyses_complete=True")
            self.all_analyses_complete = True
            self._transition(
                ApplicationState.PENDING_DECISION,
                [ApplicationState.COMPLIANCE_CHECK_REQUESTED, ApplicationState.COMPLIANCE_CHECK_COMPLETE],
            )

        elif event_type == "DecisionGenerated":
            self._require_state(event_type, [ApplicationState.PENDING_DECISION])
            recommendation = (payload.get("recommendation") or "").upper()
            confidence = self._coerce_float(payload.get("confidence"))
            if recommendation not in {"APPROVE", "DECLINE", "REFER"}:
                raise ValueError("DecisionGenerated recommendation must be APPROVE, DECLINE, or REFER")
            if confidence is None:
                raise ValueError("DecisionGenerated requires confidence")
            if confidence < 0.60 and recommendation != "REFER":
                raise ValueError("confidence < 0.60 must produce recommendation=REFER")
            self.latest_decision_recommendation = recommendation
            self.latest_decision_confidence = confidence
            if recommendation == "REFER":
                self.state = ApplicationState.PENDING_HUMAN_REVIEW

        elif event_type == "HumanReviewRequested":
            self._require_nonempty(payload, "decision_event_id", event_type)
            self.human_review_requested = True
            self._transition(
                ApplicationState.PENDING_HUMAN_REVIEW,
                [ApplicationState.PENDING_DECISION, ApplicationState.PENDING_HUMAN_REVIEW],
                allow_same_state=True,
            )

        elif event_type == "HumanReviewCompleted":
            self._require_state(event_type, [ApplicationState.PENDING_HUMAN_REVIEW])
            self.human_review_completed = True
            self.final_decision = payload.get("final_decision")

        elif event_type == "ApplicationApproved":
            self._transition(
                ApplicationState.APPROVED,
                [ApplicationState.PENDING_DECISION, ApplicationState.PENDING_HUMAN_REVIEW],
            )
            self.final_decision = "APPROVE"

        elif event_type == "ApplicationDeclined":
            target = (
                ApplicationState.DECLINED_COMPLIANCE
                if self._is_compliance_decline(payload)
                else ApplicationState.DECLINED
            )
            allowed_from = (
                [ApplicationState.COMPLIANCE_CHECK_REQUESTED, ApplicationState.COMPLIANCE_CHECK_COMPLETE]
                if target is ApplicationState.DECLINED_COMPLIANCE
                else [ApplicationState.PENDING_DECISION, ApplicationState.PENDING_HUMAN_REVIEW]
            )
            self._transition(target, allowed_from)
            self.final_decision = "DECLINE"
            self.decline_reasons = list(payload.get("decline_reasons", []))
            self.adverse_action_codes = list(payload.get("adverse_action_codes", []))

        else:
            raise ValueError(f"Unsupported loan event type: {event_type}")

        self.latest_event_type = event_type
        self.events.append(event)
        self.version = self._next_version(event, self.version)

    def assert_valid_transition(self, target: ApplicationState) -> None:
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            raise ValueError(f"Invalid transition {self.state} -> {target}. Allowed: {allowed}")

    def _transition(
        self,
        target: ApplicationState,
        allowed_from: list[ApplicationState],
        *,
        allow_same_state: bool = False,
    ) -> None:
        if allow_same_state and self.state == target:
            return
        if self.state not in allowed_from:
            raise ValueError(
                f"Invalid transition via event: {self.state} -> {target}. Allowed from: {allowed_from}"
            )
        self.state = target

    def _require_state(self, event_type: str, allowed_states: list[ApplicationState]) -> None:
        if self.state not in allowed_states:
            raise ValueError(
                f"{event_type} is not valid while state={self.state}. Allowed states: {allowed_states}"
            )

    def _assert_application_id(self, payload: dict[str, Any], event_type: str | None) -> None:
        event_application_id = payload.get("application_id")
        if event_application_id and event_application_id != self.application_id:
            raise ValueError(
                f"{event_type} application_id mismatch: expected {self.application_id}, got {event_application_id}"
            )

    @staticmethod
    def _require_nonempty(payload: dict[str, Any], field_name: str, event_type: str | None) -> None:
        if not payload.get(field_name):
            raise ValueError(f"{event_type} requires non-empty {field_name}")

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        return float(str(value))

    @staticmethod
    def _next_version(event: dict, current_version: int) -> int:
        stream_position = event.get("stream_position")
        return stream_position if isinstance(stream_position, int) else current_version + 1

    @staticmethod
    def _is_compliance_decline(payload: dict[str, Any]) -> bool:
        declined_by = (payload.get("declined_by") or "").lower()
        reasons = " ".join(payload.get("decline_reasons", [])).lower()
        codes = {code.upper() for code in payload.get("adverse_action_codes", [])}
        return (
            declined_by == "compliance-system"
            or "compliance" in reasons
            or "COMPLIANCE_BLOCK" in codes
        )
