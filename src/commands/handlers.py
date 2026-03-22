from __future__ import annotations

from datetime import datetime
from typing import Any

from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.credit_record import CreditRecordAggregate
from src.aggregates.loan_application import LoanApplicationAggregate
from src.models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    FraudScreeningRequested,
)


async def handle_submit_application(
    store,
    command: ApplicationSubmitted | dict[str, Any],
    *,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[int]:
    event = _ensure_store_dict(command)
    application_id = event["payload"]["application_id"]
    stream_id = f"loan-{application_id}"
    loan = await LoanApplicationAggregate.load(store, application_id)
    loan.guard_can_submit_application()

    return await store.append(
        stream_id,
        [event],
        expected_version=loan.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
        metadata=metadata,
    )


async def handle_credit_analysis_completed(
    store,
    command: CreditAnalysisCompleted | dict[str, Any],
    *,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, list[int]]:
    event = _ensure_store_dict(command)
    payload = event["payload"]
    application_id = payload["application_id"]

    loan = await LoanApplicationAggregate.load(store, application_id)
    loan.guard_can_complete_credit_analysis()
    credit_record = await CreditRecordAggregate.load(store, application_id)

    session_id = str(payload["session_id"])
    session_stream_id = f"agent-credit_analysis-{session_id}"
    agent_session = await AgentSessionAggregate.load(store, session_stream_id)
    agent_session.guard_context_declared()
    agent_session.guard_model_version(str(payload["model_version"]))

    credit_stream_id = f"credit-{application_id}"
    effective_correlation_id = correlation_id or session_id
    effective_causation_id = causation_id or session_id
    credit_positions = await store.append(
        credit_stream_id,
        [event],
        expected_version=credit_record.version,
        correlation_id=effective_correlation_id,
        causation_id=effective_causation_id,
        metadata=metadata,
    )

    trigger = FraudScreeningRequested(
        application_id=application_id,
        requested_at=datetime.utcnow(),
        triggered_by_event_id=effective_causation_id,
    ).to_store_dict()
    loan_positions = await store.append(
        f"loan-{application_id}",
        [trigger],
        expected_version=loan.version,
        correlation_id=effective_correlation_id,
        causation_id=effective_causation_id,
        metadata=metadata,
    )

    return {"credit_positions": credit_positions, "loan_positions": loan_positions}


def _ensure_store_dict(command: Any) -> dict[str, Any]:
    if isinstance(command, dict):
        return command
    if hasattr(command, "to_store_dict"):
        return command.to_store_dict()
    raise TypeError("Command must be a store dict or BaseEvent with to_store_dict()")
