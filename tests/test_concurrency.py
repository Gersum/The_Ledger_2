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

    async def append_decision(label: str):
        return await store.append(
            stream_id,
            [_event("DecisionGenerated", application_id="APEX-OCC-0001", recommendation=label, confidence=0.9)],
            expected_version=3,
        )

    results = await asyncio.gather(
        append_decision("APPROVE"),
        append_decision("DECLINE"),
        return_exceptions=True,
    )

    successes = [result for result in results if isinstance(result, list)]
    errors = [result for result in results if isinstance(result, OptimisticConcurrencyError)]

    assert len(successes) == 1
    assert len(errors) == 1

    events = await store.load_stream(stream_id)
    assert len(events) == 4
