from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AgentSessionAggregate:
    session_id: str
    agent_type: str | None = None
    application_id: str | None = None
    agent_id: str | None = None
    model_version: str | None = None
    context_source: str | None = None
    started: bool = False
    validated_inputs: bool = False
    nodes_executed: int = 0
    tool_calls: int = 0
    outputs_written: int = 0
    total_tokens_used: int = 0
    total_cost_usd: float = 0.0
    completed: bool = False
    failed: bool = False
    recovered_from_session_id: str | None = None
    version: int = 0
    events: list[dict] = field(default_factory=list)

    @classmethod
    async def load(cls, store, session_stream_id: str) -> "AgentSessionAggregate":
        aggregate = cls(session_id=session_stream_id.split("-")[-1])
        for event in await store.load_stream(session_stream_id):
            aggregate.apply(event)
        return aggregate

    def apply(self, event: dict) -> None:
        event_type = event.get("event_type")
        payload = dict(event.get("payload", {}))

        if event_type == "AgentSessionStarted":
            if self.started:
                raise ValueError("AgentSessionStarted can only occur once per session")
            if not payload.get("context_source"):
                raise ValueError("Gas Town context enforcement requires non-empty context_source")
            self.session_id = payload.get("session_id", self.session_id)
            self.agent_type = payload.get("agent_type")
            self.application_id = payload.get("application_id")
            self.agent_id = payload.get("agent_id")
            self.model_version = payload.get("model_version")
            self.context_source = payload.get("context_source")
            self.started = True

        elif event_type == "AgentInputValidated":
            self._require_started(event_type)
            self.validated_inputs = True

        elif event_type == "AgentInputValidationFailed":
            self._require_started(event_type)
            self.failed = True

        elif event_type == "AgentNodeExecuted":
            self._require_started(event_type)
            self.nodes_executed += 1
            self.total_tokens_used += int(payload.get("llm_tokens_input") or 0)
            self.total_tokens_used += int(payload.get("llm_tokens_output") or 0)
            self.total_cost_usd += float(payload.get("llm_cost_usd") or 0.0)

        elif event_type == "AgentToolCalled":
            self._require_started(event_type)
            self.tool_calls += 1

        elif event_type == "AgentOutputWritten":
            self._require_started(event_type)
            self.outputs_written += len(payload.get("events_written", []))
            if not self.model_version:
                raise ValueError("Session output requires a tracked model_version from AgentSessionStarted")

        elif event_type == "AgentSessionCompleted":
            self._require_started(event_type)
            self.completed = True
            self.total_tokens_used = int(payload.get("total_tokens_used") or self.total_tokens_used)
            self.total_cost_usd = float(payload.get("total_cost_usd") or self.total_cost_usd)

        elif event_type == "AgentSessionFailed":
            self._require_started(event_type)
            self.failed = True

        elif event_type == "AgentSessionRecovered":
            self._require_started(event_type)
            self.recovered_from_session_id = payload.get("recovered_from_session_id")

        self.events.append(event)
        self.version = event.get("stream_position", self.version + 1)

    def _require_started(self, event_type: str) -> None:
        if not self.started:
            raise ValueError(f"{event_type} cannot occur before AgentSessionStarted")
