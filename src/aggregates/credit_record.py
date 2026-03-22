from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class CreditRecordAggregate:
    application_id: str
    version: int = -1
    events: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    async def load(cls, store, application_id: str) -> "CreditRecordAggregate":
        aggregate = cls(application_id=application_id)
        for event in await store.load_stream(f"credit-{application_id}"):
            aggregate.events.append(event)
            stream_position = event.get("stream_position")
            aggregate.version = (
                stream_position
                if isinstance(stream_position, int)
                else (1 if aggregate.version == -1 else aggregate.version + 1)
            )
        return aggregate
