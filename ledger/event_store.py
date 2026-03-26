"""
ledger/event_store.py — PostgreSQL-backed EventStore
=====================================================
COMPLETION CHECKLIST (implement in order):
  [ ] Phase 1, Day 1: append() + stream_version()
  [ ] Phase 1, Day 1: load_stream()
  [ ] Phase 1, Day 2: load_all()  (needed for projection daemon)
  [ ] Phase 1, Day 2: get_event() (needed for causation chain)
  [ ] Phase 4:        UpcasterRegistry.upcast() integration in load_stream/load_all
"""
from __future__ import annotations
import json
from datetime import datetime
from typing import AsyncGenerator
from uuid import UUID
import asyncpg
from src.models.events import OptimisticConcurrencyError


class EventStore:
    """
    Append-only PostgreSQL event store. All agents and projections use this class.

    IMPLEMENT IN ORDER — see inline guides in each method:
      1. stream_version()   — simplest, needed immediately
      2. append()           — most critical; OCC correctness is the exam
      3. load_stream()      — needed for aggregate replay
      4. load_all()         — async generator, needed for projection daemon
      5. get_event()        — needed for causation chain audit

    VERSIONING CONVENTION:
      - missing stream => version = -1
      - first persisted event => stream_position = 1, stream_version() = 1
      - version N means N events are present in the stream
    """

    def __init__(self, db_url: str, upcaster_registry=None):
        self.db_url = db_url
        self.upcasters = upcaster_registry
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(self.db_url, min_size=2, max_size=10)

    async def close(self) -> None:
        if self._pool: await self._pool.close()

    async def stream_version(self, stream_id: str) -> int:
        """
        Returns current version, or -1 if stream doesn't exist.
        IMPLEMENT:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams WHERE stream_id = $1",
                    stream_id)
                return row["current_version"] if row else -1
        """
        if self._pool is None:
            raise RuntimeError("EventStore.connect() must be called before use")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
            return row["current_version"] if row else -1

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,    # -1 = new stream, N = exact current version
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        """
        Appends events atomically with OCC. Returns list of positions assigned.

        FULL IMPLEMENTATION GUIDE — copy, uncomment, and complete:

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Lock stream row (prevents concurrent appends)
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams "
                    "WHERE stream_id = $1 FOR UPDATE", stream_id)

                # 2. OCC check
                # Missing stream => actual = -1
                # Existing stream => actual = current_version (1-based event count)
                current = row["current_version"] if row else -1
                if current != expected_version:
                    raise OptimisticConcurrencyError(stream_id, expected_version, current)

                # 3. Create stream if new
                if row is None:
                    await conn.execute(
                        "INSERT INTO event_streams(stream_id, aggregate_type, current_version)"
                        " VALUES($1, $2, 0)",
                        stream_id, stream_id.split("-")[0])

                # 4. Insert each event
                positions = []
                meta = {**(metadata or {})}
                if causation_id: meta["causation_id"] = causation_id
                base_pos = 1 if current == -1 else current + 1
                for i, event in enumerate(events):
                    pos = base_pos + i
                    await conn.execute(
                        "INSERT INTO events(stream_id, stream_position, event_type,"
                        " event_version, payload, metadata, recorded_at)"
                        " VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7)",
                        stream_id, pos,
                        event["event_type"], event["event_version"],
                        json.dumps(event["payload"]),
                        json.dumps(meta),
                        datetime.utcnow())
                    positions.append(pos)

                # 5. Update stream version
                await conn.execute(
                    "UPDATE event_streams SET current_version=$1 WHERE stream_id=$2",
                    positions[-1], stream_id)
                return positions
        """
        if self._pool is None:
            raise RuntimeError("EventStore.connect() must be called before use")
        if not events:
            return []

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Lock the logical stream even when it does not exist yet.
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    stream_id,
                )

                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams WHERE stream_id = $1",
                    stream_id,
                )
                current = row["current_version"] if row else -1
                if current != expected_version:
                    raise OptimisticConcurrencyError(stream_id, expected_version, current)

                aggregate_type = stream_id.split("-", 1)[0]
                if row is None:
                    await conn.execute(
                        "INSERT INTO event_streams(stream_id, aggregate_type, current_version) "
                        "VALUES ($1, $2, 0)",
                        stream_id,
                        aggregate_type,
                    )

                positions: list[int] = []
                base_metadata = dict(metadata or {})
                if correlation_id is not None:
                    base_metadata["correlation_id"] = correlation_id
                if causation_id is not None:
                    base_metadata["causation_id"] = causation_id

                base_pos = 1 if current == -1 else current + 1
                for index, event in enumerate(events):
                    pos = base_pos + index
                    row = await conn.fetchrow(
                        """
                        INSERT INTO events(
                            stream_id,
                            stream_position,
                            event_type,
                            event_version,
                            payload,
                            metadata,
                            recorded_at
                        )
                        VALUES($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7)
                        RETURNING event_id, stream_position, payload, metadata
                        """,
                        stream_id,
                        pos,
                        event["event_type"],
                        event.get("event_version", 1),
                        json.dumps(event.get("payload", {}), default=str),
                        json.dumps(base_metadata, default=str),
                        datetime.utcnow(),
                    )
                    await conn.execute(
                        """
                        INSERT INTO outbox(event_id, destination, payload)
                        VALUES($1, $2, $3::jsonb)
                        """,
                        row["event_id"],
                        base_metadata.get("destination", "event_store"),
                        json.dumps(
                            {
                                "stream_id": stream_id,
                                "stream_position": row["stream_position"],
                                "event_type": event["event_type"],
                                "event_version": event.get("event_version", 1),
                                "payload": self._decode_json_value(row["payload"]),
                                "metadata": self._decode_json_value(row["metadata"]),
                            },
                            default=str,
                        ),
                    )
                    positions.append(pos)

                await conn.execute(
                    """
                    UPDATE event_streams
                    SET current_version = $1
                    WHERE stream_id = $2
                    """,
                    positions[-1],
                    stream_id,
                )
                return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
        event_types: list[str] | None = None,
    ) -> list[dict]:
        """
        Loads events from a stream in stream_position order.
        Applies upcasters if self.upcasters is set.
        """
        if self._pool is None:
            raise RuntimeError("EventStore.connect() must be called before use")

        query = (
            "SELECT event_id, stream_id, stream_position, global_position, "
            "event_type, event_version, payload, metadata, recorded_at "
            "FROM events WHERE stream_id = $1 AND stream_position >= $2"
        )
        params: list[object] = [stream_id, from_position]
        param_idx = 3

        if to_position is not None:
            query += f" AND stream_position <= ${param_idx}"
            params.append(to_position)
            param_idx += 1
            
        if event_types:
            query += f" AND event_type = ANY(${param_idx})"
            params.append(event_types)
            param_idx += 1

        query += " ORDER BY stream_position ASC"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        events = [self._row_to_event_dict(row) for row in rows]
        if self.upcasters:
            return [self.upcasters.upcast(event) for event in events]
        return events

    async def load_all(
        self, from_position: int = 0, batch_size: int = 500
    ) -> AsyncGenerator[dict, None]:
        if self._pool is None: raise RuntimeError()

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                query = """
                    SELECT event_id, stream_id, stream_position, global_position,
                           event_type, event_version, payload, metadata, recorded_at
                    FROM events
                    WHERE global_position > $1
                    ORDER BY global_position ASC
                """
                async for row in conn.cursor(query, from_position, prefetch=batch_size):
                    event = self._row_to_event_dict(row)
                    if self.upcasters:
                        event = self.upcasters.upcast(event)
                    yield event

    async def get_event(self, event_id: UUID) -> dict | None:
        """
        Loads one event by UUID. Used for causation chain lookups.

        IMPLEMENT:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM events WHERE event_id=$1", event_id)
                if not row: return None
                return {**dict(row), "payload": dict(row["payload"]),
                                      "metadata": dict(row["metadata"])}
        """
        if self._pool is None:
            raise RuntimeError("EventStore.connect() must be called before use")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT event_id, stream_id, stream_position, global_position,
                       event_type, event_version, payload, metadata, recorded_at
                FROM events
                WHERE event_id = $1
                """,
                event_id,
            )
        if row is None:
            return None

        event = self._row_to_event_dict(row)
        if self.upcasters:
            event = self.upcasters.upcast(event)
        return event

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        if self._pool is None:
            raise RuntimeError("EventStore.connect() must be called before use")

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO projection_checkpoints(projection_name, last_position, updated_at)
                VALUES($1, $2, NOW())
                ON CONFLICT (projection_name)
                DO UPDATE SET last_position = EXCLUDED.last_position, updated_at = NOW()
                """,
                projection_name,
                position,
            )

    async def load_checkpoint(self, projection_name: str) -> int:
        if self._pool is None:
            raise RuntimeError("EventStore.connect() must be called before use")

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT last_position
                FROM projection_checkpoints
                WHERE projection_name = $1
                """,
                projection_name,
            )
        return row["last_position"] if row else 0

    @staticmethod
    def _decode_json_value(value):
        if value is None:
            return {}
        if isinstance(value, str):
            return json.loads(value)
        return dict(value) if not isinstance(value, dict) else value

    def _row_to_event_dict(self, row) -> dict:
        keys = set(row.keys())
        return {
            "event_id": row["event_id"],
            "stream_id": row["stream_id"],
            "stream_position": row["stream_position"],
            "global_position": row["global_position"] if "global_position" in keys else None,
            "event_type": row["event_type"],
            "event_version": row["event_version"],
            "payload": self._decode_json_value(row["payload"]),
            "metadata": self._decode_json_value(row["metadata"]),
            "recorded_at": row["recorded_at"],
        }


# ─────────────────────────────────────────────────────────────────────────────
# UPCASTER REGISTRY — Phase 4
# ─────────────────────────────────────────────────────────────────────────────

class UpcasterRegistry:
    """
    Transforms old event versions to current versions on load.
    Upcasters are PURE functions — they never write to the database.

    REGISTER AN UPCASTER:
        registry = UpcasterRegistry()

        @registry.upcaster("CreditAnalysisCompleted", from_version=1, to_version=2)
        def upcast_credit_v1_v2(payload: dict) -> dict:
            # v2 adds model_versions dict
            payload.setdefault("model_versions", {})
            return payload

    REQUIRED FOR PHASE 4:
        - CreditAnalysisCompleted  v1 → v2  (adds model_versions: dict)
        - DecisionGenerated        v1 → v2  (adds model_versions: dict)

    IMMUTABILITY TEST (required artifact):
        registry.assert_upcaster_does_not_write_to_db(store, event)
        # Loads the event, upcasts it, re-loads it, confirms DB row unchanged.
    """

    def __init__(self):
        self._upcasters: dict[str, dict[int, callable]] = {}

    def upcaster(self, event_type: str, from_version: int, to_version: int):
        def decorator(fn):
            self._upcasters.setdefault(event_type, {})[from_version] = fn
            return fn
        return decorator

    def upcast(self, event: dict) -> dict:
        """Apply chain of upcasters until latest version reached."""
        et = event["event_type"]
        v = event.get("event_version", 1)
        chain = self._upcasters.get(et, {})
        while v in chain:
            event["payload"] = chain[v](dict(event["payload"]))
            v += 1
            event["event_version"] = v
        return event


# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY EVENT STORE — for Phase 1 tests only
# Identical interface to EventStore. Drop-in for tests; never use in production.
# ─────────────────────────────────────────────────────────────────────────────

import asyncio as _asyncio
from collections import defaultdict as _defaultdict
from datetime import datetime as _datetime
from uuid import uuid4 as _uuid4

class InMemoryEventStore:
    """
    Thread-safe (asyncio-safe) in-memory event store.
    Used exclusively in Phase 1 tests and conftest fixtures.
    Same interface as EventStore — swap one for the other with no code changes.

    Uses the same convention as the challenge brief and real Postgres tests:
      - missing stream => version = -1
      - first persisted event => stream_position = 1, version = 1
    """

    def __init__(self, upcaster_registry=None):
        self.upcasters = upcaster_registry
        # stream_id -> list of event dicts
        self._streams: dict[str, list[dict]] = _defaultdict(list)
        # stream_id -> current version (1-based event count, -1 if empty)
        self._versions: dict[str, int] = {}
        # global append log (ordered by insertion)
        self._global: list[dict] = []
        # projection checkpoints
        self._checkpoints: dict[str, int] = {}
        # asyncio lock per stream for OCC
        self._locks: dict[str, _asyncio.Lock] = _defaultdict(_asyncio.Lock)

    async def stream_version(self, stream_id: str) -> int:
        return self._versions.get(stream_id, -1)

    async def append(
        self,
        stream_id: str,
        events: list[dict],
        expected_version: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        metadata: dict | None = None,
    ) -> list[int]:
        async with self._locks[stream_id]:
            current = self._versions.get(stream_id, -1)
            if current != expected_version:
                raise OptimisticConcurrencyError(stream_id, expected_version, current)

            positions = []
            meta = {**(metadata or {})}
            if correlation_id:
                meta["correlation_id"] = correlation_id
            if causation_id:
                meta["causation_id"] = causation_id

            base_pos = 1 if current == -1 else current + 1
            for i, event in enumerate(events):
                pos = base_pos + i
                stored = {
                    "event_id": str(_uuid4()),
                    "stream_id": stream_id,
                    "stream_position": pos,
                    "global_position": len(self._global),
                    "event_type": event["event_type"],
                    "event_version": event.get("event_version", 1),
                    "payload": dict(event.get("payload", {})),
                    "metadata": meta,
                    "recorded_at": _datetime.utcnow().isoformat(),
                }
                self._streams[stream_id].append(stored)
                self._global.append(stored)
                positions.append(pos)

            self._versions[stream_id] = positions[-1]
            return positions

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
        event_types: list[str] | None = None,
    ) -> list[dict]:
        events = [
            e for e in self._streams.get(stream_id, [])
            if e["stream_position"] >= from_position
            and (to_position is None or e["stream_position"] <= to_position)
            and (event_types is None or e["event_type"] in event_types)
        ]
        events = sorted(events, key=lambda e: e["stream_position"])
        if self.upcasters:
            return [self.upcasters.upcast(dict(e)) for e in events]
        return events

    async def load_all(self, from_position: int = 0, batch_size: int = 500):
        # We simulate true streaming batches by yielding chunks
        events = [e for e in self._global if e["global_position"] >= from_position]
        for i in range(0, len(events), batch_size):
            batch = events[i : i + batch_size]
            for e in batch:
                event = dict(e)
                if self.upcasters:
                    event = self.upcasters.upcast(event)
                yield event

    async def get_event(self, event_id: str) -> dict | None:
        for e in self._global:
            if e["event_id"] == event_id:
                event = dict(e)
                if self.upcasters:
                    event = self.upcasters.upcast(event)
                return event
        return None

    async def save_checkpoint(self, projection_name: str, position: int) -> None:
        self._checkpoints[projection_name] = position

    async def load_checkpoint(self, projection_name: str) -> int:
        return self._checkpoints.get(projection_name, 0)
