from __future__ import annotations

from datetime import datetime
from typing import Any

from src.aggregates.loan_application import ApplicationState, LoanApplicationAggregate
from src.models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    DomainError,
    FraudScreeningRequested,
)


async def handle_submit_application(
    store,
    command: ApplicationSubmitted | dict[str, Any],
    *,
    metadata: dict[str, Any] | None = None,
) -> list[int]:
    event = _ensure_store_dict(command)
    application_id = event["payload"]["application_id"]
    stream_id = f"loan-{application_id}"

    if await store.stream_version(stream_id) != -1:
        raise DomainError(f"Loan application already exists: {application_id}")

    return await store.append(stream_id, [event], expected_version=-1, metadata=metadata)


async def handle_credit_analysis_completed(
    store,
    command: CreditAnalysisCompleted | dict[str, Any],
    *,
    metadata: dict[str, Any] | None = None,
) -> dict[str, list[int]]:
    event = _ensure_store_dict(command)
    payload = event["payload"]
    application_id = payload["application_id"]

    loan = await LoanApplicationAggregate.load(store, application_id)
    if loan.state != ApplicationState.CREDIT_ANALYSIS_REQUESTED:
        raise DomainError(
            f"Credit analysis can only complete from CREDIT_ANALYSIS_REQUESTED, got {loan.state}"
        )

    credit_stream_id = f"credit-{application_id}"
    credit_positions = await store.append(
        credit_stream_id,
        [event],
        expected_version=await store.stream_version(credit_stream_id),
        causation_id=str(payload.get("session_id") or application_id),
        metadata=metadata,
    )

    trigger = FraudScreeningRequested(
        application_id=application_id,
        requested_at=datetime.utcnow(),
        triggered_by_event_id=str(payload.get("session_id") or application_id),
    ).to_store_dict()
    loan_positions = await store.append(
        f"loan-{application_id}",
        [trigger],
        expected_version=loan.version,
        causation_id=str(payload.get("session_id") or application_id),
        metadata=metadata,
    )

    return {"credit_positions": credit_positions, "loan_positions": loan_positions}


def _ensure_store_dict(command: Any) -> dict[str, Any]:
    if isinstance(command, dict):
        return command
    if hasattr(command, "to_store_dict"):
        return command.to_store_dict()
    raise TypeError("Command must be a store dict or BaseEvent with to_store_dict()")
