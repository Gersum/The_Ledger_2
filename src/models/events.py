"""
Interim-facing event models and exception types.

This module keeps the canonical event catalogue in ``ledger.schema.events``
while exposing the submission-oriented base/envelope types expected under
``src/models/events.py``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from ledger.schema import events as event_catalog


@dataclass(eq=False)
class OptimisticConcurrencyError(Exception):
    stream_id: str
    expected: int
    actual: int

    def __post_init__(self) -> None:
        super().__init__(
            f"OCC on '{self.stream_id}': expected v{self.expected}, actual v{self.actual}"
        )


class DomainError(Exception):
    """Raised when a command violates domain rules before persistence."""


class BaseEvent(BaseModel):
    event_type: str
    event_version: int = 1

    def to_payload(self) -> dict[str, Any]:
        data = self.model_dump(mode="json")
        data.pop("event_type", None)
        data.pop("event_version", None)
        return data

    def to_store_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type,
            "event_version": self.event_version,
            "payload": self.to_payload(),
        }


class StoredEvent(BaseModel):
    event_id: UUID | str
    stream_id: str
    stream_position: int
    global_position: int | None = None
    event_type: str
    event_version: int = 1
    payload: dict[str, Any]
    metadata: dict[str, Any] = Field(default_factory=dict)
    recorded_at: datetime | None = None


class StreamMetadata(BaseModel):
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime | None = None
    archived_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


EVENT_REGISTRY = event_catalog.EVENT_REGISTRY


class ApplicationSubmitted(event_catalog.ApplicationSubmitted):
    pass


class DocumentUploadRequested(event_catalog.DocumentUploadRequested):
    pass


class DocumentUploaded(event_catalog.DocumentUploaded):
    pass


class CreditAnalysisRequested(event_catalog.CreditAnalysisRequested):
    pass


class FraudScreeningRequested(event_catalog.FraudScreeningRequested):
    pass


class ComplianceCheckRequested(event_catalog.ComplianceCheckRequested):
    pass


class DecisionRequested(event_catalog.DecisionRequested):
    pass


class DecisionGenerated(event_catalog.DecisionGenerated):
    pass


class HumanReviewRequested(event_catalog.HumanReviewRequested):
    pass


class HumanReviewCompleted(event_catalog.HumanReviewCompleted):
    pass


class ApplicationApproved(event_catalog.ApplicationApproved):
    pass


class ApplicationDeclined(event_catalog.ApplicationDeclined):
    pass


class AgentSessionStarted(event_catalog.AgentSessionStarted):
    pass


class AgentInputValidated(event_catalog.AgentInputValidated):
    pass


for _event_name in EVENT_REGISTRY:
    globals().setdefault(_event_name, getattr(event_catalog, _event_name))


__all__ = [
    "BaseEvent",
    *EVENT_REGISTRY.keys(),
    "DomainError",
    "OptimisticConcurrencyError",
    "StoredEvent",
    "StreamMetadata",
]
