import pytest
import json
from src.mcp_server import MCPServer

class MockStoreForMCP:
    async def load_stream(self, stream_id, event_types=None):
        if stream_id in ("loan-123", "123"):
            return [{"event_type": "ApplicationSubmitted", "payload": {"application_id": "123", "requested_amount_usd": 100}}]
        elif stream_id == "missing":
            return []
        return []
            
    async def append(self, *args, **kwargs):
        pass
        
    async def stream_version(self, stream_id: str) -> int:
        return 1

@pytest.mark.asyncio
async def test_mcp_server_lists_and_reads():
    store = MockStoreForMCP()
    server = MCPServer(store)
    
    # List resources
    resources = server.list_resources()
    assert len(resources) >= 3
    assert resources[0]["uri"].startswith("ledger://")
    
    # Read resource properties
    stream_data = await server.read_resource("ledger://streams/loan-123")
    events = json.loads(stream_data)
    assert events[0]["event_type"] == "ApplicationSubmitted"
    
    # Read aggregate state
    agg_ready = await server.read_resource("ledger://aggregates/loan/123")
    state = json.loads(agg_ready)
    assert state["application_id"] == "123"
    assert "ApplicationState" in state["state"]
    
@pytest.mark.asyncio
async def test_mcp_server_tools():
    store = MockStoreForMCP()
    server = MCPServer(store)
    
    tools = server.list_tools()
    assert len(tools) >= 2
    
    # Call a tool
    res = await server.call_tool("submit_command", {
        "aggregate_id": "123", "command_type": "some_cmd", "payload": {}
    })
    
    assert res["status"] == "success"