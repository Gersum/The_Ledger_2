import pytest
from ledger.domain.aggregates.compliance_record import ComplianceRecordAggregate, DomainError

class MockStore:
    async def load_stream(self, stream_id, **kwargs):
        return [
            {"event_type": "ComplianceCheckInitiated", "payload": {"application_id": "APP-123"}},
            {"event_type": "ComplianceRulePassed", "payload": {"rule_id": "REG-001"}}
        ]

@pytest.mark.asyncio
async def test_compliance_load_stream():
    store = MockStore()
    agg = await ComplianceRecordAggregate.load("APP-123", store)
    assert agg.is_started is True
    assert "REG-001" in agg.passed_rules

def test_compliance_initiate():
    agg = ComplianceRecordAggregate("APP-123")
    event = agg.initiate()
    assert event["event_type"] == "ComplianceCheckInitiated"
    assert event["payload"]["application_id"] == "APP-123"
    agg.apply_events([event])
    assert agg.is_started is True
    
    with pytest.raises(DomainError, match="Already started"):
        agg.initiate()

def test_compliance_6_rules_enforcement():
    agg = ComplianceRecordAggregate("APP-123")
    
    with pytest.raises(DomainError, match="Must initiate first"):
        agg.evaluate_reg_001_aml(False)
        
    agg.apply_events([agg.initiate()])
    
    # REG-001
    e1 = agg.evaluate_reg_001_aml(has_flags=True)
    assert e1["event_type"] == "ComplianceRuleFailed"
    agg.apply_events([e1])
    
    # REG-002
    e2 = agg.evaluate_reg_002_sanctions(failed_sanctions=False)
    assert e2["event_type"] == "ComplianceRulePassed"
    agg.apply_events([e2])
    
    # REG-003 MT block
    e3 = agg.evaluate_reg_003_jurisdiction("MT")
    assert e3["event_type"] == "ComplianceRuleFailed"
    
    e3_pass = agg.evaluate_reg_003_jurisdiction("CA")
    assert e3_pass["event_type"] == "ComplianceRulePassed"
    agg.apply_events([e3_pass])
    
    # REG-004 SoleProp > $250k
    e4 = agg.evaluate_reg_004_business_entity("Sole Proprietorship", 300000)
    assert e4["event_type"] == "ComplianceRuleFailed"
    
    e4_pass = agg.evaluate_reg_004_business_entity("LLC", 300000)
    assert e4_pass["event_type"] == "ComplianceRulePassed"
    agg.apply_events([e4_pass])
    
    # REG-005 Foundation Year after 2022 block
    e5 = agg.evaluate_reg_005_foundation_year(2023)
    assert e5["event_type"] == "ComplianceRuleFailed"
    
    e5_pass = agg.evaluate_reg_005_foundation_year(2020)
    assert e5_pass["event_type"] == "ComplianceRulePassed"
    agg.apply_events([e5_pass])
    
    # REG-006 CRA
    e6 = agg.evaluate_reg_006_cra(lmi_zone=True)
    assert e6["event_type"] == "ComplianceRulePassed"
    agg.apply_events([e6])
    
    # Duplicate rule test
    with pytest.raises(DomainError, match="Rule REG-001 already evaluated"):
        agg.evaluate_reg_001_aml(False)
        
    assert "REG-001" in agg.failed_rules
    assert "REG-002" in agg.passed_rules
    
def test_compliance_complete():
    agg = ComplianceRecordAggregate("APP-123")
    agg.apply_events([agg.initiate()])
    
    agg.apply_events([agg.evaluate_reg_001_aml(has_flags=False)])
    event = agg.complete()
    assert event["payload"]["verdict"] == "PASS"
    
def test_compliance_complete_fail():
    agg = ComplianceRecordAggregate("APP-123")
    agg.apply_events([agg.initiate()])
    
    agg.apply_events([agg.evaluate_reg_003_jurisdiction("MT")]) # Failed rule
    event = agg.complete()
    assert event["payload"]["verdict"] == "FAIL"
    
def test_compliance_already_completed():
    agg = ComplianceRecordAggregate("APP-123")
    agg.apply_events([agg.initiate(), 
                      {"event_type": "ComplianceCheckCompleted", "payload": {}}])
    
    with pytest.raises(DomainError, match="Already completed"):
        agg.evaluate_reg_001_aml(False)