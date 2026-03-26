from ledger.domain.aggregates.audit_ledger import AuditLedgerAggregate

def test_audit_run_check():
    agg = AuditLedgerAggregate("LEDGER-1")
    event = agg.run_check(False)
    assert event["event_type"] == "AuditIntegrityCheckRun"
    assert event["payload"]["tamper_detected"] is False
    
    agg.apply_events([event])
    assert agg.chain_validated is True