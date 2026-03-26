class DomainError(Exception):
    pass

class AuditLedgerAggregate:
    def __init__(self, ledger_id: str):
        self.ledger_id = ledger_id
        self.chain_validated = False

    def apply_events(self, events: list[dict]):
        for e in events:
            if e.get("event_type") == "AuditIntegrityCheckRun":
                self.chain_validated = True

    def run_check(self, tamper_detected: bool) -> dict:
        return {"event_type": "AuditIntegrityCheckRun", "payload": {"tamper_detected": tamper_detected}}