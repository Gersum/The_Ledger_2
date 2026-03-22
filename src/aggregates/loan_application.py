from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import re
from typing import Any

from src.models.events import DomainError


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
    version: int = -1
    events: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
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

    def guard_can_submit_application(self) -> None:
        if self.version != -1 or self.state != ApplicationState.NEW:
            raise DomainError(f"Loan application already exists: {self.application_id}")

    def guard_can_complete_credit_analysis(self) -> None:
        self._require_state(
            "CreditAnalysisCompleted",
            [ApplicationState.CREDIT_ANALYSIS_REQUESTED],
        )

    def apply(self, event: dict[str, Any]) -> None:
        event_type = event.get("event_type")
        if not event_type:
            raise DomainError("Event is missing event_type")

        payload = dict(event.get("payload", {}))
        self._assert_application_id(payload, event_type)

        handler_name = self._handler_name(event_type)
        handler = getattr(self, handler_name, None)
        if handler is None:
            raise DomainError(f"Unsupported loan event type: {event_type}")

        handler(payload)
        self.latest_event_type = event_type
        self.events.append(event)
        self.version = self._next_version(event, self.version)

    def _apply_application_submitted(self, payload: dict[str, Any]) -> None:
        self._transition(ApplicationState.SUBMITTED, [ApplicationState.NEW])
        self.applicant_id = payload.get("applicant_id")
        self.requested_amount_usd = self._coerce_float(payload.get("requested_amount_usd"))
        self.loan_purpose = payload.get("loan_purpose")

    def _apply_document_upload_requested(self, payload: dict[str, Any]) -> None:
        self._transition(ApplicationState.DOCUMENTS_PENDING, [ApplicationState.SUBMITTED])
        self.required_document_types = list(payload.get("required_document_types", []))

    def _apply_document_uploaded(self, payload: dict[str, Any]) -> None:
        self._transition(
            ApplicationState.DOCUMENTS_UPLOADED,
            [ApplicationState.DOCUMENTS_PENDING, ApplicationState.DOCUMENTS_UPLOADED],
            allow_same_state=True,
        )
        document_id = payload.get("document_id")
        if document_id and document_id not in self.uploaded_document_ids:
            self.uploaded_document_ids.append(document_id)

    def _apply_document_upload_failed(self, _: dict[str, Any]) -> None:
        self._require_state(
            "DocumentUploadFailed",
            [ApplicationState.DOCUMENTS_PENDING, ApplicationState.DOCUMENTS_UPLOADED],
        )

    def _apply_credit_analysis_requested(self, _: dict[str, Any]) -> None:
        self._transition(
            ApplicationState.CREDIT_ANALYSIS_REQUESTED,
            [ApplicationState.DOCUMENTS_UPLOADED, ApplicationState.DOCUMENTS_PROCESSED],
        )

    def _apply_fraud_screening_requested(self, payload: dict[str, Any]) -> None:
        self._require_nonempty(payload, "triggered_by_event_id", "FraudScreeningRequested")
        self._transition(
            ApplicationState.FRAUD_SCREENING_REQUESTED,
            [ApplicationState.CREDIT_ANALYSIS_REQUESTED, ApplicationState.CREDIT_ANALYSIS_COMPLETE],
        )

    def _apply_compliance_check_requested(self, payload: dict[str, Any]) -> None:
        self._require_nonempty(payload, "triggered_by_event_id", "ComplianceCheckRequested")
        self._transition(
            ApplicationState.COMPLIANCE_CHECK_REQUESTED,
            [ApplicationState.FRAUD_SCREENING_REQUESTED, ApplicationState.FRAUD_SCREENING_COMPLETE],
        )

    def _apply_decision_requested(self, payload: dict[str, Any]) -> None:
        self._require_nonempty(payload, "triggered_by_event_id", "DecisionRequested")
        if not payload.get("all_analyses_complete"):
            raise DomainError("DecisionRequested requires all_analyses_complete=True")
        self.all_analyses_complete = True
        self._transition(
            ApplicationState.PENDING_DECISION,
            [ApplicationState.COMPLIANCE_CHECK_REQUESTED, ApplicationState.COMPLIANCE_CHECK_COMPLETE],
        )

    def _apply_decision_generated(self, payload: dict[str, Any]) -> None:
        self._require_state("DecisionGenerated", [ApplicationState.PENDING_DECISION])
        recommendation = (payload.get("recommendation") or "").upper()
        confidence = self._coerce_float(payload.get("confidence"))
        if recommendation not in {"APPROVE", "DECLINE", "REFER"}:
            raise DomainError("DecisionGenerated recommendation must be APPROVE, DECLINE, or REFER")
        if confidence is None:
            raise DomainError("DecisionGenerated requires confidence")
        if confidence < 0.60 and recommendation != "REFER":
            raise DomainError("confidence < 0.60 must produce recommendation=REFER")

        self.latest_decision_recommendation = recommendation
        self.latest_decision_confidence = confidence
        if recommendation == "REFER":
            self.state = ApplicationState.PENDING_HUMAN_REVIEW

    def _apply_human_review_requested(self, payload: dict[str, Any]) -> None:
        self._require_nonempty(payload, "decision_event_id", "HumanReviewRequested")
        self.human_review_requested = True
        self._transition(
            ApplicationState.PENDING_HUMAN_REVIEW,
            [ApplicationState.PENDING_DECISION, ApplicationState.PENDING_HUMAN_REVIEW],
            allow_same_state=True,
        )

    def _apply_human_review_completed(self, payload: dict[str, Any]) -> None:
        self._require_state("HumanReviewCompleted", [ApplicationState.PENDING_HUMAN_REVIEW])
        self.human_review_completed = True
        self.final_decision = payload.get("final_decision")

    def _apply_application_approved(self, _: dict[str, Any]) -> None:
        self._transition(
            ApplicationState.APPROVED,
            [ApplicationState.PENDING_DECISION, ApplicationState.PENDING_HUMAN_REVIEW],
        )
        self.final_decision = "APPROVE"

    def _apply_application_declined(self, payload: dict[str, Any]) -> None:
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
            raise DomainError(
                f"Invalid transition via event: {self.state} -> {target}. Allowed from: {allowed_from}"
            )
        self.state = target

    def _require_state(self, event_type: str, allowed_states: list[ApplicationState]) -> None:
        if self.state not in allowed_states:
            raise DomainError(
                f"{event_type} is not valid while state={self.state}. Allowed states: {allowed_states}"
            )

    def _assert_application_id(self, payload: dict[str, Any], event_type: str) -> None:
        event_application_id = payload.get("application_id")
        if event_application_id and event_application_id != self.application_id:
            raise DomainError(
                f"{event_type} application_id mismatch: expected {self.application_id}, got {event_application_id}"
            )

    @staticmethod
    def _require_nonempty(payload: dict[str, Any], field_name: str, event_type: str) -> None:
        if not payload.get(field_name):
            raise DomainError(f"{event_type} requires non-empty {field_name}")

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        return float(str(value))

    @staticmethod
    def _next_version(event: dict[str, Any], current_version: int) -> int:
        stream_position = event.get("stream_position")
        if isinstance(stream_position, int):
            return stream_position
        return 1 if current_version == -1 else current_version + 1

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

    @staticmethod
    def _handler_name(event_type: str) -> str:
        snake = re.sub(r"(?<!^)(?=[A-Z])", "_", event_type).lower()
        return f"_apply_{snake}"


__all__ = ["ApplicationState", "LoanApplicationAggregate"]
