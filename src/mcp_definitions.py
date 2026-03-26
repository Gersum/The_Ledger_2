from pydantic import BaseModel, Field
from typing import List, Optional, Any, Callable

# Explicit structural tool vs resources definitions for Model Context Protocol (MCP)

class MCPResource(BaseModel):
    uri: str
    name: str
    description: str
    mime_type: str = "application/json"

class MCPToolParameter(BaseModel):
    type: str
    properties: dict
    required: List[str] = []

class MCPTool(BaseModel):
    name: str
    description: str
    parameters: MCPToolParameter
    handler: Optional[Callable] = None

class MCPDefinitions:
    @staticmethod
    def get_resources() -> List[MCPResource]:
        """Expose our Event Store, Aggregates, and Projections as MCP resources."""
        return [
            MCPResource(
                uri="ledger://streams/{stream_id}",
                name="Event Stream",
                description="Raw immutable event stream for a given aggregate"
            ),
            MCPResource(
                uri="ledger://aggregates/{aggregate_type}/{id}",
                name="Aggregate State",
                description="Live snapshot of an aggregate built by reducing the event stream"
            ),
            MCPResource(
                uri="ledger://projections/{projection_name}",
                name="Read Model Projection",
                description="Eventual-consistent read model answering specific queries"
            )
        ]

    @staticmethod
    def get_tools() -> List[MCPTool]:
        """Expose our Commands and Registry as MCP Action tools."""
        return [
            MCPTool(
                name="submit_command",
                description="Submit a domain command (e.g., RequestDecision, RejectApplication) to change state.",
                parameters=MCPToolParameter(
                    type="object",
                    properties={
                        "aggregate_id": {"type": "string"},
                        "command_type": {"type": "string"},
                        "causation_id": {"type": "string"},
                        "payload": {"type": "object"}
                    },
                    required=["aggregate_id", "command_type", "payload"]
                )
            ),
            MCPTool(
                name="query_registry",
                description="Query the external system registry for company demographics and compliance flags.",
                parameters=MCPToolParameter(
                    type="object",
                    properties={
                        "company_id": {"type": "string"},
                        "query_type": {"type": "string", "enum": ["demographics", "compliance", "credit", "all"]}
                    },
                    required=["company_id", "query_type"]
                )
            )
        ]
