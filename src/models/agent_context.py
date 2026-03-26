from datetime import datetime, timezone
from pydantic import BaseModel, Field
from typing import List

class EventMemory(BaseModel):
    event_type: str
    status: str
    content: str
    tokens: int
    timestamp: datetime = Field(default_factory=datetime.now)

class AgentContext(BaseModel):
    session_id: str
    application_id: str = ""
    state: str = "PENDING"
    needs_reconciliation: bool = False
    events: List[EventMemory] = []
    last_event_position: int = -1
    pending_work: List[str] = []
    
    def add_event(self, event_type: str, status: str, content: str, tokens: int):
        self.events.append(EventMemory(
            event_type=event_type, 
            status=status, 
            content=content, 
            tokens=tokens
        ))
        
    def summarize(self, max_tokens: int = 4000) -> str:
        retained = []
        sorted_events = sorted(self.events, key=lambda x: x.timestamp)
        
        last_3 = sorted_events[-3:] if len(sorted_events) >= 3 else sorted_events
        last_3_ids = {id(e) for e in last_3}
        
        for e in sorted_events:
            if id(e) in last_3_ids or e.status in ("PENDING", "ERROR"):
                if e not in retained:
                    retained.append(e)
                
        total_tokens = sum(e.tokens for e in retained)
        if total_tokens > max_tokens:
            self.needs_reconciliation = True
            
        return "\n".join([f"[{e.status}] {e.event_type}: {e.content}" for e in retained])

async def reconstruct_agent_context(session_id: str, store) -> AgentContext:
    ctx = AgentContext(session_id=session_id)
    stream_id = f"agent-{session_id}"
    
    events = []
    try:
        events = await store.load_stream(stream_id)
    except Exception:
        pass
        
    started_nodes = set()
    completed_nodes = set()
    
    for i, e in enumerate(events):
        ctx.last_event_position = i
        etype = e.get("event_type")
        payload = e.get("payload", {})
        
        if etype == "AgentNodeExecuted":
            completed_nodes.add(payload.get("node_name"))
            ctx.add_event(etype, "SUCCESS", f"Node executed: {payload.get('node_name')}", 100)
        elif etype == "AgentSessionStarted":
            ctx.application_id = payload.get("application_id", "")
        elif etype == "AgentNodeStarted":
            started_nodes.add(payload.get("node_name"))
            ctx.add_event(etype, "PENDING", f"Node started: {payload.get('node_name')}", 50)
            
    pending = started_nodes - completed_nodes
    ctx.pending_work = list(pending)
    
    if ctx.pending_work:
        ctx.needs_reconciliation = True
        
    return ctx