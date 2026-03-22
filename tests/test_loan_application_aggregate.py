from __future__ import annotations

import pytest

from ledger.domain.aggregates import ApplicationState, LoanApplicationAggregate
from ledger.event_store import InMemoryEventStore
from src.models.events import DomainError


def _event(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


@pytest.mark.asyncio
async def test_load_replays_refer_case() -> None:
    store = InMemoryEventStore()
    application_id = "APEX-4242"
    stream_id = f"loan-{application_id}"
    events = [
        _event(
            "ApplicationSubmitted",
            application_id=application_id,
            applicant_id="COMP-4242",
            requested_amount_usd="1518000.0",
            loan_purpose="expansion",
        ),
        _event(
            "DocumentUploadRequested",
            application_id=application_id,
            required_document_types=["application_proposal", "income_statement", "balance_sheet"],
        ),
        _event("DocumentUploaded", application_id=application_id, document_id="doc-1"),
        _event("DocumentUploaded", application_id=application_id, document_id="doc-2"),
        _event("CreditAnalysisRequested", application_id=application_id),
        _event(
            "FraudScreeningRequested",
            application_id=application_id,
            triggered_by_event_id="evt-credit-done",
        ),
        _event(
            "ComplianceCheckRequested",
            application_id=application_id,
            triggered_by_event_id="evt-fraud-done",
        ),
        _event(
            "DecisionRequested",
            application_id=application_id,
            all_analyses_complete=True,
            triggered_by_event_id="evt-compliance-done",
        ),
        _event(
            "DecisionGenerated",
            application_id=application_id,
            recommendation="REFER",
            confidence=0.54,
        ),
    ]

    await store.append(stream_id, events, expected_version=-1)
    aggregate = await LoanApplicationAggregate.load(store, application_id)

    assert aggregate.version == len(events)
    assert aggregate.state == ApplicationState.PENDING_HUMAN_REVIEW
    assert aggregate.applicant_id == "COMP-4242"
    assert aggregate.requested_amount_usd == 1518000.0
    assert aggregate.loan_purpose == "expansion"
    assert aggregate.uploaded_document_ids == ["doc-1", "doc-2"]
    assert aggregate.latest_decision_recommendation == "REFER"
    assert aggregate.latest_decision_confidence == 0.54


def test_allows_multiple_document_uploads_on_same_application() -> None:
    aggregate = LoanApplicationAggregate(application_id="APEX-0007")
    aggregate.apply(
        _event(
            "ApplicationSubmitted",
            application_id="APEX-0007",
            applicant_id="COMP-007",
            requested_amount_usd="500000",
            loan_purpose="working_capital",
        )
    )
    aggregate.apply(
        _event(
            "DocumentUploadRequested",
            application_id="APEX-0007",
            required_document_types=["application_proposal"],
        )
    )
    aggregate.apply(_event("DocumentUploaded", application_id="APEX-0007", document_id="doc-a"))
    aggregate.apply(_event("DocumentUploaded", application_id="APEX-0007", document_id="doc-b"))

    assert aggregate.state == ApplicationState.DOCUMENTS_UPLOADED
    assert aggregate.version == 4
    assert aggregate.uploaded_document_ids == ["doc-a", "doc-b"]


def test_rejects_low_confidence_non_refer_decision() -> None:
    aggregate = LoanApplicationAggregate(application_id="APEX-0999")
    for event in [
        _event(
            "ApplicationSubmitted",
            application_id="APEX-0999",
            applicant_id="COMP-0999",
            requested_amount_usd="800000",
            loan_purpose="expansion",
        ),
        _event(
            "DocumentUploadRequested",
            application_id="APEX-0999",
            required_document_types=["application_proposal"],
        ),
        _event("DocumentUploaded", application_id="APEX-0999", document_id="doc-1"),
        _event("CreditAnalysisRequested", application_id="APEX-0999"),
        _event(
            "FraudScreeningRequested",
            application_id="APEX-0999",
            triggered_by_event_id="evt-1",
        ),
        _event(
            "ComplianceCheckRequested",
            application_id="APEX-0999",
            triggered_by_event_id="evt-2",
        ),
        _event(
            "DecisionRequested",
            application_id="APEX-0999",
            all_analyses_complete=True,
            triggered_by_event_id="evt-3",
        ),
    ]:
        aggregate.apply(event)

    with pytest.raises(DomainError, match="confidence < 0.60 must produce recommendation=REFER"):
        aggregate.apply(
            _event(
                "DecisionGenerated",
                application_id="APEX-0999",
                recommendation="APPROVE",
                confidence=0.55,
            )
        )


def test_supports_direct_compliance_decline_before_decision() -> None:
    aggregate = LoanApplicationAggregate(application_id="APEX-0303")
    for event in [
        _event(
            "ApplicationSubmitted",
            application_id="APEX-0303",
            applicant_id="COMP-0303",
            requested_amount_usd="250000",
            loan_purpose="working_capital",
        ),
        _event(
            "DocumentUploadRequested",
            application_id="APEX-0303",
            required_document_types=["application_proposal"],
        ),
        _event("DocumentUploaded", application_id="APEX-0303", document_id="doc-1"),
        _event("CreditAnalysisRequested", application_id="APEX-0303"),
        _event(
            "FraudScreeningRequested",
            application_id="APEX-0303",
            triggered_by_event_id="evt-1",
        ),
        _event(
            "ComplianceCheckRequested",
            application_id="APEX-0303",
            triggered_by_event_id="evt-2",
        ),
    ]:
        aggregate.apply(event)

    aggregate.apply(
        _event(
            "ApplicationDeclined",
            application_id="APEX-0303",
            decline_reasons=["Compliance hard block: REG-003"],
            declined_by="compliance-system",
            adverse_action_codes=["COMPLIANCE_BLOCK"],
        )
    )

    assert aggregate.state == ApplicationState.DECLINED_COMPLIANCE
    assert aggregate.final_decision == "DECLINE"
