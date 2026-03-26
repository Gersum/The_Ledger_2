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

    def apply_events(self, events: list[dict]):
        for e in events:
            et = e.get("event_type")
            if et == "ComplianceCheckInitiated":
                self.is_started = True
            elif et == "ComplianceRulePassed":
                self.passed_rules.add(e["payload"]["rule_id"])
            elif et == "ComplianceRuleFailed":
                self.failed_rules.add(e["payload"]["rule_id"])
            elif et == "ComplianceCheckCompleted":
                self.is_completed = True

    def initiate(self) -> dict:
        if self.is_started:
            raise DomainError("Already started")
        return {"event_type": "ComplianceCheckInitiated", "payload": {"application_id": self.application_id}}

    def pass_rule(self, rule_id: str) -> dict:
        if not self.is_started: raise DomainError("Must initiate first")
        if self.is_completed: raise DomainError("Already completed")
        return {"event_type": "ComplianceRulePassed", "payload": {"rule_id": rule_id}}

    def fail_rule(self, rule_id: str) -> dict:
        if not self.is_started: raise DomainError("Must initiate first")
        if self.is_completed: raise DomainError("Already completed")
        return {"event_type": "ComplianceRuleFailed", "payload": {"rule_id": rule_id}}

    def complete(self) -> dict:
        if not self.is_started: raise DomainError("Must initiate first")
        if self.is_completed: raise DomainError("Already completed")
        verdict = "FAIL" if self.failed_rules else "PASS"
        return {"event_type": "ComplianceCheckCompleted", "payload": {"verdict": verdict, "passed": list(self.passed_rules), "failed": list(self.failed_rules)}}