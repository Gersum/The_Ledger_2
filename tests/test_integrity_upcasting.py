import os
import pytest
import pytest_asyncio
import json
import hashlib
from datetime import datetime, timezone
from ledger.event_store import EventStore, OptimisticConcurrencyError
from ledger.upcasters import registry

DB_URL = os.environ.get(
    "TEST_DB_URL",
    os.environ.get("DATABASE_URL", "postgresql://postgres:apex@localhost/ledger"),
)

@pytest_asyncio.fixture
async def store():
    s = EventStore(DB_URL, upcaster_registry=registry)
    try:
        await s.connect()
    except Exception as exc:
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    try:
        yield s
    finally:
        await s.close()

@pytest.mark.asyncio
async def test_upcasting_and_immutability(store):
    old_payload = '{"amount": 50000, "risk_tier": "B"}'
    
    async with store._pool.acquire() as conn:
        await conn.execute("INSERT INTO event_streams(stream_id, current_version) VALUES($1, 1) ON CONFLICT DO NOTHING", "loan-upcast-test")
        await conn.execute(
            """INSERT INTO events(stream_id, stream_position, global_position, event_type, event_version, payload, recorded_at)
               VALUES($1, 1, (SELECT COALESCE(MAX(global_position), 0) + 1 FROM events), 'CreditAnalysisCompleted', 1, $2::jsonb, $3)
               ON CONFLICT DO NOTHING""",
            "loan-upcast-test", old_payload, datetime(2024, 1, 1, tzinfo=timezone.utc)
        )
    
    events = await store.load_stream("loan-upcast-test")
    e = events[0]
    assert e["event_version"] == 2
    assert getattr(e["payload"], "get", lambda x: e["payload"][x])("model_version") == "legacy-pre-2025"
    assert "confidence_score" in e["payload"]
    assert e["payload"]["confidence_score"] is None

    async with store._pool.acquire() as conn:
        raw_payload = await conn.fetchval("SELECT payload FROM events WHERE stream_id='loan-upcast-test'")
    
    assert json.loads(raw_payload) == json.loads(old_payload)

@pytest.mark.asyncio
async def test_hash_chain_tamper_detection(store):
    def _event_integrity_chain(events):
        previous_hash = "GENESIS"
        for i, e in enumerate(events):
            canonical = json.dumps({
                "stream_id": e["stream_id"],
                "position": e["stream_position"],
                "type": e["event_type"],
                "payload": e["payload"],
            }, sort_keys=True)
            event_hash = hashlib.sha256((previous_hash + canonical).encode("utf-8")).hexdigest()
            e["_hash"] = event_hash
            previous_hash = event_hash
        return previous_hash, events

    stream_id = "loan-tamper-test"
    await store.append(stream_id, [{"event_type": "T1", "event_version": 1, "payload": {"k": "v1"}}], -1)
    await store.append(stream_id, [{"event_type": "T2", "event_version": 1, "payload": {"k": "v2"}}], 1)
    await store.append(stream_id, [{"event_type": "T3", "event_version": 1, "payload": {"k": "v3"}}], 2)

    events = await store.load_stream(stream_id)
    baseline_root, clean_events = _event_integrity_chain(events)

    async with store._pool.acquire() as conn:
         await conn.execute("UPDATE events SET payload = '{\"k\": \"TAMPERED\"}'::jsonb WHERE stream_id = $1 AND stream_position = 2", stream_id)

    tampered_events = await store.load_stream(stream_id)
    tampered_root, tampered_chain = _event_integrity_chain(tampered_events)
    
    assert tampered_root != baseline_root
    tamper_detected = any(c["_hash"] != t["_hash"] for c, t in zip(clean_events, tampered_chain))
    assert tamper_detected is True
