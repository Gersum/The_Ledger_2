import pytest
import uuid
from src.models.agent_context import AgentContext, reconstruct_agent_context

class MockStore:
    async def load_stream(self, stream_id, **kwargs):
        # return a list directly, simulating the actual load_stream
        return [
            {"event_type": "AgentSessionStarted", "payload": {"application_id": "app-1", "node_name": "start"}},
            {"event_type": "AgentNodeStarted", "payload": {"node_name": "ScanDoc"}},
            {"event_type": "AgentNodeExecuted", "payload": {"node_name": "ScanDoc"}},
            {"event_type": "AgentNodeStarted", "payload": {"node_name": "ParseDoc"}}
        ]

@pytest.mark.asyncio
async def test_agent_cold_crash_recovery():
    session_id = str(uuid.uuid4())
    store = MockStore()
    
    ctx = await reconstruct_agent_context(session_id, store)
    
    assert ctx.application_id == "app-1"
    assert ctx.last_event_position == 3
    assert "ParseDoc" in ctx.pending_work
    assert ctx.needs_reconciliation is True
    
    summary = ctx.summarize()
    assert "Node started: ParseDoc" in summary
    assert "Node executed: ScanDoc" in summary