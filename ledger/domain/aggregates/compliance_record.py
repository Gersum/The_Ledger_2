from datetime import datetime, timezone

class DomainError(Exception):
    pass

class ComplianceRecordAggregate:
    def __init__(self, application_id: str):
        self.application_id = application_id
        self.is_started = False
        self.is_completed = False
        self.passed_rules = set()
        self.failed_rules = set()
        self.rules_evaluated = set()

    @classmethod
    async def load(cls, application_id: str, store) -> "ComplianceRecordAggregate":
        agg = cls(application_id)
        stream_id = f"compliance-{application_id}"
        events = []
        try:
            events = await store.load_stream(stream_id)
        except Exception:
            pass
        if events:
            agg.apply_events(events)
        return agg

    def apply_events(self, events: list[dict]):
        for e in events:
            et = e.get("event_type")
            if et == "ComplianceCheckInitiated":
                self.is_started = True
            elif et == "ComplianceRulePassed":
                self.passed_rules.add(e["payload"]["rule_id"])
                self.rules_evaluated.add(e["payload"]["rule_id"])
            elif et == "ComplianceRuleFailed":
                self.failed_rules.add(e["payload"]["rule_id"])
                self.rules_evaluated.add(e["payload"]["rule_id"])
            elif et == "ComplianceCheckCompleted":
                self.is_completed = True

    def initiate(self) -> dict:
        if self.is_started:
            raise DomainError("Already started")
        return {"event_type": "ComplianceCheckInitiated", "payload": {"application_id": self.application_id}}

    def complete(self) -> dict:
        if not self.is_started: 
            raise DomainError("Must initiate first")
        if self.is_completed: 
            raise DomainError("Already completed")
        
        verdict = "FAIL" if self.failed_rules else "PASS"
        return {"event_type": "ComplianceCheckCompleted", "payload": {"verdict": verdict, "passed": list(self.passed_rules), "failed": list(self.failed_rules)}}

    def evaluate_reg_001_aml(self, has_flags: bool) -> dict:
        self._ensure_evaluable("REG-001")
        if has_flags:
            return {"event_type": "ComplianceRuleFailed", "payload": {"rule_id": "REG-001", "reason": "Active AML flags found"}}
        return {"event_type": "ComplianceRulePassed", "payload": {"rule_id": "REG-001"}}

    def evaluate_reg_002_sanctions(self, failed_sanctions: bool) -> dict:
        self._ensure_evaluable("REG-002")
        if failed_sanctions:
            return {"event_type": "ComplianceRuleFailed", "payload": {"rule_id": "REG-002", "reason": "Failed Sanctions checks"}}
        return {"event_type": "ComplianceRulePassed", "payload": {"rule_id": "REG-002"}}

    def evaluate_reg_003_jurisdiction(self, state: str) -> dict:
        self._ensure_evaluable("REG-003")
        if state and state.upper() == "MT":
            return {"event_type": "ComplianceRuleFailed", "payload": {"rule_id": "REG-003", "reason": "Blocked jurisdiction MT"}}
        return {"event_type": "ComplianceRulePassed", "payload": {"rule_id": "REG-003"}}

    def evaluate_reg_004_business_entity(self, entity_type: str, revenue: float) -> dict:
        self._ensure_evaluable("REG-004")
        is_sp = "SOLE" in (entity_type or "").upper() or "SP" in (entity_type or "").upper()
        if is_sp and revenue > 250000:
            return {"event_type": "ComplianceRuleFailed", "payload": {"rule_id": "REG-004", "reason": "Sole Proprietor revenue > 250k remediation required"}}
        return {"event_type": "ComplianceRulePassed", "payload": {"rule_id": "REG-004"}}

    def evaluate_reg_005_foundation_year(self, year: int) -> dict:
        self._ensure_evaluable("REG-005")
        if year > 2022:
            return {"event_type": "ComplianceRuleFailed", "payload": {"rule_id": "REG-005", "reason": "Founded after 2022"}}
        return {"event_type": "ComplianceRulePassed", "payload": {"rule_id": "REG-005"}}

    def evaluate_reg_006_cra(self, lmi_zone: bool) -> dict:
        self._ensure_evaluable("REG-006")
        return {"event_type": "ComplianceRulePassed", "payload": {"rule_id": "REG-006", "note": f"LMI Zone: {lmi_zone}"}}

    def _ensure_evaluable(self, rule_id: str):
        if not self.is_started:
            raise DomainError("Must initiate first")
        if self.is_completed:
            raise DomainError("Already completed")
        if rule_id in self.rules_evaluated:
            raise DomainError(f"Rule {rule_id} already evaluated")