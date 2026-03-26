from src.models.agent_context import AgentContext
import uuid

def test_agent_cold_crash_recovery():
    # Simulate an agent crashing and a new agent reconstructing state from events
    session_id = str(uuid.uuid4())
    
    # Run 1: crashes midway
    ctx1 = AgentContext(session_id=session_id, application_id="app-1")
    ctx1.add_event("ScanDoc", "SUCCESS", "w9 loaded", 50)
    ctx1.add_event("ParseDoc", "PENDING", "parsing...", 10)
    # CRASH!
    
    # Run 2: recovers from the events that were stored
    recovered_events = ctx1.events # In real life, loaded from EventStore
    
    ctx2 = AgentContext(session_id=session_id, application_id="app-1")
    ctx2.events.extend(recovered_events) # Hydrate
    
    summary = ctx2.summarize()
    assert ctx2.needs_reconciliation is False
    assert "[SUCCESS] ScanDoc" in summary
    assert "[PENDING] ParseDoc" in summary
