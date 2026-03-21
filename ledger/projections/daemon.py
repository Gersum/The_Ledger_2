"""
ledger/projections/daemon.py
=============================
ProjectionDaemon — reads the global event stream and dispatches to projections.

CRITICAL: Checkpoint MUST be saved in same transaction as projection update.
If the checkpoint is not atomic, a crash causes reprocessing (projections must be idempotent).

Usage:
    daemon = ProjectionDaemon(store, pool, projections=[...])
    await daemon.run_once()        # one batch pass
    await daemon.run_forever()     # continuous loop
"""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ProjectionDaemon:
    """
    Fault-tolerant projection daemon.
    - Reads events in batches from store.load_all(from_position=checkpoint)
    - Dispatches each event to all registered projections
    - Saves checkpoint atomically with projection update (same transaction)
    - On restart: resumes from saved checkpoint (not position 0)
    - Exposes get_lag() per projection name for the health endpoint
    """

    PROJECTION_NAME = "projection_daemon"
    BATCH_SIZE = 100

    def __init__(self, store, pool, projections: list):
        self.store = store
        self.pool = pool
        self.projections = projections
        self._lag: dict[str, int] = {}   # per-projection lag (events behind)
        self._last_run: dict[str, str] = {}

    async def _ensure_checkpoint_table(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS projection_checkpoints (
                    projection_name TEXT PRIMARY KEY,
                    last_position   BIGINT NOT NULL DEFAULT 0,
                    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

    async def _load_checkpoint(self, conn, name: str) -> int:
        row = await conn.fetchrow(
            "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
            name,
        )
        return row["last_position"] if row else 0

    async def _save_checkpoint(self, conn, name: str, position: int) -> None:
        await conn.execute(
            """
            INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (projection_name) DO UPDATE SET
                last_position = EXCLUDED.last_position,
                updated_at = EXCLUDED.updated_at
            """,
            name,
            position,
            datetime.utcnow().isoformat(),
        )

    async def run_once(self) -> int:
        """
        Process one batch of events.
        Returns number of events processed.
        """
        await self._ensure_checkpoint_table()

        async with self.pool.acquire() as conn:
            checkpoint = await self._load_checkpoint(conn, self.PROJECTION_NAME)

        processed = 0
        last_position = checkpoint
        batch: list[dict] = []

        # Collect a batch
        async for event in self.store.load_all(from_position=checkpoint):
            batch.append(event)
            if len(batch) >= self.BATCH_SIZE:
                break

        if not batch:
            return 0

        # Process batch — each event in its own transaction for fault isolation
        for event in batch:
            try:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        for proj in self.projections:
                            if proj.can_handle(event["event_type"]):
                                await proj.handle(event, conn)
                        # Save checkpoint atomically with projection writes
                        pos = event.get("global_position") or last_position + 1
                        await self._save_checkpoint(conn, self.PROJECTION_NAME, pos)
                        last_position = pos
                processed += 1
            except Exception as exc:
                logger.error(
                    "Projection [%s] error at global_position=%s event_type=%s: %s",
                    proj.__class__.__name__,
                    event.get("global_position"),
                    event.get("event_type"),
                    exc,
                )
                # Skip bad event but do NOT advance checkpoint past it
                break

        self._last_run[self.PROJECTION_NAME] = datetime.utcnow().isoformat()
        return processed

    async def run_forever(self, poll_interval_s: float = 0.5) -> None:
        """Continuously poll the event store and dispatch to projections."""
        while True:
            try:
                n = await self.run_once()
                if n == 0:
                    # No new events — wait before polling again
                    await asyncio.sleep(poll_interval_s)
            except Exception as exc:
                logger.error("ProjectionDaemon error: %s", exc)
                await asyncio.sleep(poll_interval_s * 2)

    async def get_lag(self) -> dict:
        """
        Return lag for all tracked projections.
        Lag = (latest global_position) - (checkpoint position).
        Used by the health endpoint: ledger://ledger/health
        """
        async with self.pool.acquire() as conn:
            latest = await conn.fetchval("SELECT COALESCE(MAX(global_position), 0) FROM events") or 0
            checkpoint = await self._load_checkpoint(conn, self.PROJECTION_NAME)

        return {
            "projection_name": self.PROJECTION_NAME,
            "checkpoint_position": checkpoint,
            "latest_position": latest,
            "events_behind": max(0, latest - checkpoint),
            "last_run": self._last_run.get(self.PROJECTION_NAME),
            "status": (
                "OK" if (latest - checkpoint) <= 10
                else "WARNING" if (latest - checkpoint) <= 100
                else "CRITICAL"
            ),
        }

    async def initialize_tables(self) -> None:
        """Create all projection tables if they don't exist."""
        from ledger.projections.application_summary import CREATE_TABLE_SQL as APP_SQL
        from ledger.projections.agent_performance import CREATE_TABLE_SQL as PERF_SQL
        from ledger.projections.compliance_audit import CREATE_TABLE_SQL as COMP_SQL

        async with self.pool.acquire() as conn:
            await conn.execute(APP_SQL)
            await conn.execute(PERF_SQL)
            await conn.execute(COMP_SQL)
