from __future__ import annotations

from datetime import datetime

import pytest

from ledger.event_store import InMemoryEventStore
from src.aggregates.agent_session import AgentSessionAggregate
from src.commands.handlers import (
    handle_credit_analysis_completed,
    handle_submit_application,
)
from src.models.events import DomainError


def _event(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


@pytest.mark.asyncio
async def test_handle_submit_application_uses_loaded_aggregate_version_and_metadata() -> None:
    store = InMemoryEventStore()
    event = _event(
        "ApplicationSubmitted",
        application_id="APEX-SUBMIT-1",
        applicant_id="COMP-001",
        requested_amount_usd="250000",
        loan_purpose="working_capital",
    )

    positions = await handle_submit_application(
        store,
        event,
        correlation_id="corr-submit-1",
        causation_id="cmd-submit-1",
        metadata={"origin": "test"},
    )

    assert positions == [1]
    stored = await store.load_stream("loan-APEX-SUBMIT-1")
    assert stored[0]["metadata"]["correlation_id"] == "corr-submit-1"
    assert stored[0]["metadata"]["causation_id"] == "cmd-submit-1"
    assert stored[0]["metadata"]["origin"] == "test"

    with pytest.raises(DomainError, match="already exists"):
        await handle_submit_application(store, event)


@pytest.mark.asyncio
async def test_handle_credit_analysis_completed_loads_agent_session_and_threads_metadata() -> None:
    store = InMemoryEventStore()
    application_id = "APEX-CREDIT-1"
    session_id = "sess-credit-1"

    await store.append(
        f"loan-{application_id}",
        [
            _event(
                "ApplicationSubmitted",
                application_id=application_id,
                applicant_id="COMP-010",
                requested_amount_usd="500000",
                loan_purpose="expansion",
            ),
            _event(
                "DocumentUploadRequested",
                application_id=application_id,
                required_document_types=["application_proposal"],
            ),
            _event("DocumentUploaded", application_id=application_id, document_id="doc-1"),
            _event("CreditAnalysisRequested", application_id=application_id),
        ],
        expected_version=-1,
    )
    await store.append(
        f"agent-credit_analysis-{session_id}",
        [
            _event(
                "AgentSessionStarted",
                session_id=session_id,
                agent_type="credit_analysis",
                application_id=application_id,
                agent_id="credit-agent-1",
                model_version="claude-sonnet-test",
                context_source="prior_session_replay:loan-stream",
            )
        ],
        expected_version=-1,
    )

    result = await handle_credit_analysis_completed(
        store,
        _event(
            "CreditAnalysisCompleted",
            application_id=application_id,
            session_id=session_id,
            decision={
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": "400000",
                "confidence": 0.74,
                "rationale": "Adequate debt service capacity.",
            },
            model_version="claude-sonnet-test",
            model_deployment_id="dep-1",
            input_data_hash="hash-1",
            analysis_duration_ms=1200,
            completed_at=datetime.utcnow().isoformat(),
        ),
        correlation_id="corr-credit-1",
        causation_id="cmd-credit-1",
        metadata={"origin": "test"},
    )

    assert result == {"credit_positions": [1], "loan_positions": [5]}

    credit_events = await store.load_stream(f"credit-{application_id}")
    loan_events = await store.load_stream(f"loan-{application_id}")

    assert credit_events[0]["metadata"]["correlation_id"] == "corr-credit-1"
    assert credit_events[0]["metadata"]["causation_id"] == "cmd-credit-1"
    assert loan_events[-1]["event_type"] == "FraudScreeningRequested"
    assert loan_events[-1]["metadata"]["correlation_id"] == "corr-credit-1"
    assert loan_events[-1]["metadata"]["causation_id"] == "cmd-credit-1"


def test_agent_session_guards_context_and_model_version() -> None:
    aggregate = AgentSessionAggregate(session_id="sess-guard-1")

    with pytest.raises(DomainError, match="context has not been declared"):
        aggregate.guard_context_declared()

    aggregate.apply(
        _event(
            "AgentSessionStarted",
            session_id="sess-guard-1",
            agent_type="credit_analysis",
            application_id="APEX-GUARD-1",
            agent_id="credit-agent-2",
            model_version="claude-sonnet-test",
            context_source="initial_registry_load",
        )
    )

    aggregate.guard_context_declared()
    aggregate.guard_model_version("claude-sonnet-test")

    with pytest.raises(DomainError, match="model_version mismatch"):
        aggregate.guard_model_version("different-model")
