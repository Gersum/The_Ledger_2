from typing import Dict, Any
from src.mcp_definitions import MCPDefinitions
import json

class MCPServer:
    def __init__(self, store):
        self.store = store
        self.resources = MCPDefinitions.get_resources()
        self.tools = MCPDefinitions.get_tools()
        
    def list_resources(self):
        return [r.model_dump() for r in self.resources]
        
    async def read_resource(self, uri: str) -> str:
        if uri.startswith("ledger://streams/"):
            stream_id = uri.split("/")[-1]
            events = await self.store.load_stream(stream_id)
            if not events:
                raise ValueError(f"Resource not found: {uri}")
            return json.dumps(events)
        elif uri.startswith("ledger://aggregates/loan/"):
            app_id = uri.split("/")[-1]
            from ledger.domain.aggregates.loan_application import LoanApplicationAggregate
            agg = await LoanApplicationAggregate.load(self.store, app_id)
            if not agg.application_id:
                raise ValueError(f"Resource not found: {uri}")
            return json.dumps({"application_id": agg.application_id, "state": str(agg.state)})
        raise ValueError(f"Unknown resource URI: {uri}")

    def list_tools(self):
        return [t.model_dump() for t in self.tools]
        
    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Any:
        if name == "submit_command":
            agg_id = arguments.get("aggregate_id")
            cmd_type = arguments.get("command_type")
            return {"status": "success", "message": f"Command {cmd_type} dispatched to {agg_id}"}
        elif name == "query_registry":
            comp_id = arguments.get("company_id")
            qtype = arguments.get("query_type")
            return {"company_id": comp_id, "data": {"type": qtype, "result": "mocked_data"}}
            
        raise ValueError(f"Unknown tool: {name}")