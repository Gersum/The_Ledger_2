from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import AsyncGenerator

from ledger.event_store import EventStore as LedgerEventStore

from src.models.events import StoredEvent
from src.models.events import StreamMetadata


class EventStore(LedgerEventStore):
    """Interim-facing wrapper over the canonical ledger EventStore."""

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        return await super().append(
            stream_id,
            events,
            expected_version,
            correlation_id=correlation_id,
            causation_id=causation_id,
            metadata=metadata,
        )

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
        event_types: list[str] | None = None,
    ) -> list[StoredEvent | dict]:
        return await super().load_stream(
            stream_id,
            from_position=from_position,
            to_position=to_position,
            event_types=event_types,
        )

    async def load_all(
        self,
        from_position: int = 0,
        batch_size: int = 500,
    ) -> AsyncGenerator[dict, None]:
        async for event in super().load_all(
            from_position=from_position,
            batch_size=batch_size,
        ):
            yield event

    async def stream_version(self, stream_id: str) -> int:
        return await super().stream_version(stream_id)

    async def archive_stream(self, stream_id: str, reason: str | None = None) -> StreamMetadata:
        if self._pool is None:
            raise RuntimeError("EventStore.connect() must be called before use")

        metadata = {"archived_reason": reason} if reason else {}
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE event_streams
                SET archived_at = $2,
                    metadata = metadata || $3::jsonb
                WHERE stream_id = $1
                RETURNING stream_id, aggregate_type, current_version, created_at, archived_at, metadata
                """,
                stream_id,
                datetime.now(timezone.utc),
                json.dumps(metadata),
            )
        if row is None:
            raise ValueError(f"Stream not found: {stream_id}")
        return StreamMetadata(**dict(row))

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata | None:
        if self._pool is None:
            raise RuntimeError("EventStore.connect() must be called before use")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT stream_id, aggregate_type, current_version, created_at, archived_at, metadata
                FROM event_streams
                WHERE stream_id = $1
                """,
                stream_id,
            )
        return StreamMetadata(**dict(row)) if row else None
