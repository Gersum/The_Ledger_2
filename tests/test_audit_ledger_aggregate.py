import pytest
from ledger.domain.aggregates.audit_ledger import AuditLedgerAggregate

class MockStore:
    async def load_stream(self, stream_id):
        return [
            {"event_type": "AuditIntegrityCheckRun", "payload": {"tamper_detected": False}}
        ]

@pytest.mark.asyncio
async def test_audit_ledger_load():
    store = MockStore()
    agg = await AuditLedgerAggregate.load("LEDGER-1", store)
    assert agg.chain_validated is True

def test_audit_run_check():
    agg = AuditLedgerAggregate("LEDGER-1")
    event = agg.run_check(False)
    assert event["event_type"] == "AuditIntegrityCheckRun"
    assert event["payload"]["tamper_detected"] is False
    
    agg.apply_events([event])
    assert agg.chain_validated is True