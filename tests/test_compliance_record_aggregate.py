import pytest
from ledger.domain.aggregates.compliance_record import ComplianceRecordAggregate, DomainError

def test_compliance_initiate():
    agg = ComplianceRecordAggregate("APP-123")
    event = agg.initiate()
    assert event["event_type"] == "ComplianceCheckInitiated"
    assert event["payload"]["application_id"] == "APP-123"
    agg.apply_events([event])
    assert agg.is_started is True
    
    with pytest.raises(DomainError, match="Already started"):
        agg.initiate()

def test_compliance_pass_fail_rules():
    agg = ComplianceRecordAggregate("APP-123")
    
    with pytest.raises(DomainError, match="Must initiate first"):
        agg.pass_rule("REG-001")
        
    agg.apply_events([agg.initiate()])
    
    e1 = agg.pass_rule("REG-001")
    e2 = agg.fail_rule("REG-002")
    
    assert e1["event_type"] == "ComplianceRulePassed"
    assert e2["event_type"] == "ComplianceRuleFailed"
    
    agg.apply_events([e1, e2])
    assert "REG-001" in agg.passed_rules
    assert "REG-002" in agg.failed_rules
    
def test_compliance_complete():
    agg = ComplianceRecordAggregate("APP-123")
    agg.apply_events([agg.initiate()])
    
    agg.apply_events([agg.pass_rule("REG-001")])
    event = agg.complete()
    assert event["payload"]["verdict"] == "PASS"
    
def test_compliance_complete_fail():
    agg = ComplianceRecordAggregate("APP-123")
    agg.apply_events([agg.initiate()])
    
    agg.apply_events([agg.fail_rule("REG-002")])
    event = agg.complete()
    assert event["payload"]["verdict"] == "FAIL"
    
def test_compliance_already_completed():
    agg = ComplianceRecordAggregate("APP-123")
    agg.apply_events([agg.initiate(), 
                      {"event_type": "ComplianceCheckCompleted", "payload": {}}])
    
    with pytest.raises(DomainError, match="Already completed"):
        agg.pass_rule("REG-003")