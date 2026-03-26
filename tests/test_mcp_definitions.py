from src.mcp_definitions import MCPDefinitions

def test_mcp_definitions_expose_tools_and_resources():
    resources = MCPDefinitions.get_resources()
    assert len(resources) >= 3
    assert resources[0].uri.startswith("ledger://")
    
    tools = MCPDefinitions.get_tools()
    assert len(tools) >= 2
    assert any(t.name == "submit_command" for t in tools)
    assert any(t.name == "query_registry" for t in tools)
