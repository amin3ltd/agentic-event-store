"""
Projection Daemon

Implements the async projection daemon that:
- Continuously polls the events table
- Routes events to registered projections
- Maintains checkpoints for fault tolerance
- Exposes lag metrics
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import asyncpg

from ledger.event_store import EventStore, StoredEvent

logger = logging.getLogger(__name__)


class Projection:
    """Base class for projections."""
    
    @property
    def name(self) -> str:
        """Unique name for this projection."""
        raise NotImplementedError
    
    async def handle_event(self, event: StoredEvent) -> None:
        """Handle a single event."""
        raise NotImplementedError
    
    async def get_lag_ms(self, store: EventStore) -> int:
        """Get projection lag in milliseconds."""
        return 0
    
    async def rebuild(self, store: EventStore) -> None:
        """Rebuild projection from scratch."""
        raise NotImplementedError


@dataclass
class ProjectionDaemon:
    """
    Background daemon that processes events and updates projections.
    
    Features:
    - Fault-tolerant: skips bad events, logs errors, continues
    - Checkpoint management: resumes from last processed position
    - Lag measurement: exposes metrics for monitoring
    """
    
    store: EventStore
    projections: list[Projection]
    poll_interval_ms: int = 100
    batch_size: int = 100
    
    _running: bool = False
    
    async def run_forever(self) -> None:
        """Main daemon loop."""
        self._running = True
        logger.info(f"Starting projection daemon with {len(self.projections)} projections")
        
        while self._running:
            try:
                await self._process_batch()
            except Exception as e:
                logger.error(f"Error in projection daemon: {e}")
            
            await asyncio.sleep(self.poll_interval_ms / 1000)
    
    def stop(self) -> None:
        """Stop the daemon."""
        self._running = False
    
    async def _process_batch(self) -> None:
        """Process a batch of events."""
        # Get current global position
        current_global_pos = await self.store.get_global_position()
        
        # Get lowest checkpoint across all projections
        projection_map = {p.name: p for p in self.projections}
        
        for projection in self.projections:
            lag = await projection.get_lag_ms(self.store)
            if lag > 500:  # Warn if lag > 500ms
                logger.warning(
                    f"Projection '{projection.name}' lag: {lag}ms "
                    f"(global: {current_global_pos})"
                )
        
        # Load events from lowest checkpoint position
        lowest_checkpoint = await self._get_lowest_checkpoint()
        
        if lowest_checkpoint >= current_global_pos:
            return  # No new events
        
        # Process events
        async for event in self.store.load_all(
            from_global_position=lowest_checkpoint,
            batch_size=self.batch_size
        ):
            try:
                # Route to all subscribed projections
                for projection in self.projections:
                    await projection.handle_event(event)
                
                # Update checkpoint
                await self._update_checkpoint(event.global_position)
                
            except Exception as e:
                logger.error(
                    f"Error processing event {event.event_id} "
                    f"in projection: {e}"
                )
                # Continue processing other events
    
    async def _get_lowest_checkpoint(self) -> int:
        """Get the lowest checkpoint position across all projections."""
        if not self.projections:
            return 0
        
        min_checkpoint = float('inf')
        
        async with self.store._pool.acquire() as conn:
            for projection in self.projections:
                pos = await conn.fetchval(
                    """SELECT last_position 
                       FROM projection_checkpoints 
                       WHERE projection_name = $1""",
                    projection.name
                )
                if pos is None:
                    # Initialize checkpoint
                    await conn.execute(
                        """INSERT INTO projection_checkpoints (projection_name, last_position)
                           VALUES ($1, 0)""",
                        projection.name
                    )
                    pos = 0
                
                min_checkpoint = min(min_checkpoint, pos)
        
        return int(min_checkpoint)
    
    async def _update_checkpoint(self, position: int) -> None:
        """Update checkpoint for all projections."""
        async with self.store._pool.acquire() as conn:
            await conn.execute(
                """UPDATE projection_checkpoints 
                   SET last_position = $1, updated_at = NOW()
                   WHERE last_position < $1""",
                position
            )


# =============================================================================
# Projection Tables Setup
# =============================================================================

CREATE_APPLICATION_SUMMARY_TABLE = """
CREATE TABLE IF NOT EXISTS application_summary (
    application_id      TEXT PRIMARY KEY,
    state               TEXT NOT NULL,
    applicant_id        TEXT,
    requested_amount_usd    REAL,
    approved_amount_usd    REAL,
    risk_tier           TEXT,
    fraud_score         REAL,
    compliance_status   TEXT,
    decision            TEXT,
    agent_sessions_completed   TEXT[],
    last_event_type    TEXT,
    last_event_at       TIMESTAMPTZ,
    human_reviewer_id   TEXT,
    final_decision_at   TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

CREATE_AGENT_PERFORMANCE_LEDGER_TABLE = """
CREATE TABLE IF NOT EXISTS agent_performance_ledger (
    agent_id            TEXT,
    model_version       TEXT,
    analyses_completed  INTEGER DEFAULT 0,
    decisions_generated INTEGER DEFAULT 0,
    avg_confidence_score REAL DEFAULT 0,
    avg_duration_ms     REAL DEFAULT 0,
    approve_rate        REAL DEFAULT 0,
    decline_rate        REAL DEFAULT 0,
    refer_rate          REAL DEFAULT 0,
    human_override_rate REAL DEFAULT 0,
    first_seen_at       TIMESTAMPTZ,
    last_seen_at        TIMESTAMPTZ,
    PRIMARY KEY (agent_id, model_version)
);
"""

CREATE_COMPLIANCE_AUDIT_VIEW_TABLE = """
CREATE TABLE IF NOT EXISTS compliance_audit_view (
    id                  SERIAL PRIMARY KEY,
    application_id      TEXT NOT NULL,
    rule_id             TEXT NOT NULL,
    rule_version        TEXT,
    verdict             TEXT NOT NULL,
    evidence_hash       TEXT,
    failure_reason      TEXT,
    remediation_required TEXT,
    evaluated_at        TIMESTAMPTZ NOT NULL,
    -- For temporal queries
    valid_from          TIMESTAMPTZ NOT NULL,
    valid_to            TIMESTAMPTZ,
    -- Index for efficient temporal queries
    UNIQUE (application_id, rule_id, valid_from)
);
"""

CREATE_COMPLIANCE_TEMPORAL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_compliance_temporal 
ON compliance_audit_view (application_id, valid_from, valid_to);
"""


async def create_projection_tables(pool: asyncpg.Pool) -> None:
    """Create all projection tables."""
    async with pool.acquire() as conn:
        await conn.execute(CREATE_APPLICATION_SUMMARY_TABLE)
        await conn.execute(CREATE_AGENT_PERFORMANCE_LEDGER_TABLE)
        await conn.execute(CREATE_COMPLIANCE_AUDIT_VIEW_TABLE)
        await conn.execute(CREATE_COMPLIANCE_TEMPORAL_INDEX)
