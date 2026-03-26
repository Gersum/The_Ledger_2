from datetime import datetime
from pydantic import BaseModel, Field
from typing import List

class EventMemory(BaseModel):
    event_type: str
    status: str
    content: str
    tokens: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class AgentContext(BaseModel):
    session_id: str
    application_id: str
    state: str = "PENDING"
    needs_reconciliation: bool = False
    events: List[EventMemory] = []
    
    def add_event(self, event_type: str, status: str, content: str, tokens: int):
        self.events.append(EventMemory(
            event_type=event_type, 
            status=status, 
            content=content, 
            tokens=tokens
        ))
        
    def summarize(self, max_tokens: int = 4000) -> str:
        # Token-aware summarization with last-3 and PENDING/ERROR verbatim retention
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
