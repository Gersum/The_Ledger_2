"""
Interim-facing event models module.

This file re-exports the canonical event catalogue from
`ledger.schema.events` so there is still a single source of truth for all
event types while matching the `src/models/events.py` artifact expected by the
interim submission checklist.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from ledger.event_store import OptimisticConcurrencyError
from ledger.schema.events import *  # noqa: F401,F403
from ledger.schema.events import EVENT_REGISTRY


class DomainError(Exception):
    """Raised when a command violates domain rules before persistence."""


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


__all__ = [
    *EVENT_REGISTRY.keys(),
    "DomainError",
    "OptimisticConcurrencyError",
    "StoredEvent",
    "StreamMetadata",
]
