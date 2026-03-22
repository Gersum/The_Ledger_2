import asyncio

import pytest

from ledger.event_store import InMemoryEventStore, OptimisticConcurrencyError


def _event(event_type: str, **payload) -> dict:
    return {"event_type": event_type, "event_version": 1, "payload": payload}


@pytest.mark.asyncio
async def test_double_decision_occ_collision() -> None:
    store = InMemoryEventStore()
    stream_id = "loan-APEX-OCC-0001"

    await store.append(
        stream_id,
        [
            _event("ApplicationSubmitted", application_id="APEX-OCC-0001"),
            _event("DocumentUploadRequested", application_id="APEX-OCC-0001"),
            _event("DecisionRequested", application_id="APEX-OCC-0001", all_analyses_complete=True, triggered_by_event_id="seed"),
        ],
        expected_version=-1,
    )

    async def append_decision(label: str) -> dict[str, object]:
        positions = await store.append(
            stream_id,
            [_event("DecisionGenerated", application_id="APEX-OCC-0001", recommendation=label, confidence=0.9)],
            expected_version=3,
        )
        return {"label": label, "positions": positions}

    results = await asyncio.gather(
        append_decision("APPROVE"),
        append_decision("DECLINE"),
        return_exceptions=True,
    )

    successes = [result for result in results if isinstance(result, dict)]
    errors = [result for result in results if isinstance(result, OptimisticConcurrencyError)]

    assert len(successes) == 1
    assert len(errors) == 1

    events = await store.load_stream(stream_id)
    assert len(events) == 4

    winner = successes[0]
    winning_position = winner["positions"][-1]
    losing_error = errors[0]

    assert winning_position == 4
    assert losing_error.expected == 3
    assert losing_error.actual == 4

    print(
        f"SUCCESS: winner={winner['label']} stream_id={stream_id} stream_position={winning_position}"
    )
    print(
        "OCC ERROR: loser rejected with "
        f"OptimisticConcurrencyError(stream_id={losing_error.stream_id}, "
        f"expected={losing_error.expected}, actual={losing_error.actual})"
    )
    print(f"[ASSERTION PASSED] Total stream length = {len(events)}")
    print(f"[ASSERTION PASSED] Winning append stream_position = {winning_position}")
    print(
        "[ASSERTION PASSED] Losing task raised "
        f"OptimisticConcurrencyError(expected={losing_error.expected}, actual={losing_error.actual})"
    )
