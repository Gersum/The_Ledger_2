class DomainError(Exception):
    pass

class AuditLedgerAggregate:
    def __init__(self, ledger_id: str):
        self.ledger_id = ledger_id
        self.chain_validated = False

    @classmethod
    async def load(cls, ledger_id: str, store) -> "AuditLedgerAggregate":
        agg = cls(ledger_id)
        stream_id = f"audit-{ledger_id}"
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
            if e.get("event_type") == "AuditIntegrityCheckRun":
                self.chain_validated = True

    def run_check(self, tamper_detected: bool) -> dict:
        return {"event_type": "AuditIntegrityCheckRun", "payload": {"tamper_detected": tamper_detected}}