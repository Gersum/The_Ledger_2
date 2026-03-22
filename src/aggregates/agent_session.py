from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any

from src.models.events import DomainError


@dataclass
class AgentSessionAggregate:
    session_id: str
    agent_type: str | None = None
    application_id: str | None = None
    agent_id: str | None = None
    model_version: str | None = None
    context_source: str | None = None
    context_declared: bool = False
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
    version: int = -1
    events: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    async def load(cls, store, session_stream_id: str) -> "AgentSessionAggregate":
        aggregate = cls(session_id=session_stream_id.split("-")[-1])
        for event in await store.load_stream(session_stream_id):
            aggregate.apply(event)
        return aggregate

    def apply(self, event: dict[str, Any]) -> None:
        event_type = event.get("event_type")
        if not event_type:
            raise DomainError("Event is missing event_type")

        payload = dict(event.get("payload", {}))
        handler_name = self._handler_name(event_type)
        handler = getattr(self, handler_name, None)
        if handler is None:
            raise DomainError(f"Unsupported agent session event type: {event_type}")

        handler(payload)
        self.events.append(event)
        self.version = self._next_version(event, self.version)

    def guard_context_declared(self) -> None:
        if not self.context_declared:
            raise DomainError("Agent session context has not been declared")

    def guard_model_version(self, declared_model_version: str) -> None:
        self.guard_context_declared()
        if self.model_version != declared_model_version:
            raise DomainError(
                f"Agent session model_version mismatch: expected {self.model_version}, got {declared_model_version}"
            )

    def _apply_agent_session_started(self, payload: dict[str, Any]) -> None:
        if self.started:
            raise DomainError("AgentSessionStarted can only occur once per session")
        context_source = payload.get("context_source")
        if not context_source:
            raise DomainError("Gas Town context enforcement requires non-empty context_source")
        self.session_id = payload.get("session_id", self.session_id)
        self.agent_type = payload.get("agent_type")
        self.application_id = payload.get("application_id")
        self.agent_id = payload.get("agent_id")
        self.model_version = payload.get("model_version")
        self.context_source = context_source
        self.context_declared = True
        self.started = True

    def _apply_agent_input_validated(self, _: dict[str, Any]) -> None:
        self._require_started("AgentInputValidated")
        self.validated_inputs = True

    def _apply_agent_input_validation_failed(self, _: dict[str, Any]) -> None:
        self._require_started("AgentInputValidationFailed")
        self.failed = True

    def _apply_agent_node_executed(self, payload: dict[str, Any]) -> None:
        self._require_started("AgentNodeExecuted")
        self.nodes_executed += 1
        self.total_tokens_used += int(payload.get("llm_tokens_input") or 0)
        self.total_tokens_used += int(payload.get("llm_tokens_output") or 0)
        self.total_cost_usd += float(payload.get("llm_cost_usd") or 0.0)

    def _apply_agent_tool_called(self, _: dict[str, Any]) -> None:
        self._require_started("AgentToolCalled")
        self.tool_calls += 1

    def _apply_agent_output_written(self, payload: dict[str, Any]) -> None:
        self._require_started("AgentOutputWritten")
        self.guard_context_declared()
        self.outputs_written += len(payload.get("events_written", []))
        if not self.model_version:
            raise DomainError("Session output requires a tracked model_version from AgentSessionStarted")

    def _apply_agent_session_completed(self, payload: dict[str, Any]) -> None:
        self._require_started("AgentSessionCompleted")
        self.completed = True
        self.total_tokens_used = int(payload.get("total_tokens_used") or self.total_tokens_used)
        self.total_cost_usd = float(payload.get("total_cost_usd") or self.total_cost_usd)

    def _apply_agent_session_failed(self, _: dict[str, Any]) -> None:
        self._require_started("AgentSessionFailed")
        self.failed = True

    def _apply_agent_session_recovered(self, payload: dict[str, Any]) -> None:
        self._require_started("AgentSessionRecovered")
        self.recovered_from_session_id = payload.get("recovered_from_session_id")

    def _require_started(self, event_type: str) -> None:
        if not self.started:
            raise DomainError(f"{event_type} cannot occur before AgentSessionStarted")

    @staticmethod
    def _next_version(event: dict[str, Any], current_version: int) -> int:
        stream_position = event.get("stream_position")
        if isinstance(stream_position, int):
            return stream_position
        return 1 if current_version == -1 else current_version + 1

    @staticmethod
    def _handler_name(event_type: str) -> str:
        snake = re.sub(r"(?<!^)(?=[A-Z])", "_", event_type).lower()
        return f"_apply_{snake}"
