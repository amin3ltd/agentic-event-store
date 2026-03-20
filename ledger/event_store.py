"""
Event Store Implementation

The core PostgreSQL-backed event store with async interface.
Implements optimistic concurrency control with row-level locking (FOR UPDATE)
and the outbox pattern for reliable event publishing.
"""

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator

import asyncpg

from ledger.events import BaseEvent, EVENT_REGISTRY
from ledger.schema import get_all_schema_sql


class OptimisticConcurrencyError(Exception):
    """Raised when a concurrent write conflict is detected."""
    def __init__(self, stream_id: str, expected_version: int, actual_version: int):
        self.stream_id = stream_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            f"Stream '{stream_id}' expected version {expected_version} but was {actual_version}"
        )


class StreamNotFoundError(Exception):
    """Raised when attempting to load a non-existent stream."""
    pass


@dataclass
class StoredEvent:
    """An event retrieved from the store."""
    event_id: uuid.UUID
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: dict[str, Any]
    metadata: dict[str, Any]
    recorded_at: datetime
    correlation_id: uuid.UUID | None
    causation_id: uuid.UUID | None


@dataclass
class StreamMetadata:
    """Metadata about a stream."""
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: datetime | None
    metadata: dict[str, Any]


class EventStore:
    """
    PostgreSQL-backed event store with async interface.
    
    Key features:
    - Append-only event storage with optimistic concurrency control
    - Stream-based organization with version tracking
    - Outbox pattern for reliable event publishing
    - Global and stream-level position tracking
    """
    
    def __init__(self, dsn: str):
        """
        Initialize the event store.
        
        Args:
            dsn: PostgreSQL connection string
        """
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None
    
    async def connect(self) -> None:
        """Establish connection pool to the database."""
        self._pool = await asyncpg.create_pool(self._dsn, min_size=2, max_size=10)
    
    async def disconnect(self) -> None:
        """Close all database connections."""
        if self._pool:
            await self._pool.close()
    
    async def initialize_schema(self) -> None:
        """Create all required tables and indexes."""
        async with self._pool.acquire() as conn:
            for sql in get_all_schema_sql():
                await conn.execute(sql)
    
    async def append(
        self,
        stream_id: str,
        events: list[BaseEvent],
        expected_version: int,
        correlation_id: uuid.UUID | None = None,
        causation_id: uuid.UUID | None = None,
    ) -> int:
        """
        Atomically append events to a stream.
        
        Uses optimistic concurrency control - raises OptimisticConcurrencyError
        if the stream's current version doesn't match expected_version.
        
        Args:
            stream_id: The stream to append to
            events: List of events to append
            expected_version: The version the stream should have (-1 for new stream)
            correlation_id: Optional correlation ID for causal chains
            causation_id: Optional causation ID for causal chains
            
        Returns:
            The new stream version after appending
            
        Raises:
            OptimisticConcurrencyError: If version mismatch detected
        """
        if not events:
            return await self.stream_version(stream_id)
        
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Get current stream version with row lock for optimistic concurrency
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams WHERE stream_id = $1 FOR UPDATE",
                    stream_id
                )
                current_version = row['current_version'] if row else None
                
                if current_version is None:
                    # New stream
                    if expected_version != -1:
                        raise OptimisticConcurrencyError(stream_id, expected_version, 0)
                    current_version = 0
                    
                    # Create stream record (extract aggregate type from first event)
                    aggregate_type = self._infer_aggregate_type(events[0])
                    try:
                        await conn.execute(
                            """INSERT INTO event_streams (stream_id, aggregate_type, current_version)
                               VALUES ($1, $2, 0)""",
                            stream_id, aggregate_type
                        )
                    except asyncpg.UniqueViolationError:
                        # Stream was created by another concurrent transaction
                        # Re-read the stream version and verify
                        row = await conn.fetchrow(
                            "SELECT current_version FROM event_streams WHERE stream_id = $1 FOR UPDATE",
                            stream_id
                        )
                        if row is None:
                            # Should not happen, but handle gracefully
                            raise
                        current_version = row['current_version']
                        if expected_version != -1 and current_version != expected_version:
                            raise OptimisticConcurrencyError(stream_id, expected_version, current_version)
                else:
                    # Existing stream - check version
                    if expected_version != -1 and current_version != expected_version:
                        raise OptimisticConcurrencyError(stream_id, expected_version, current_version)
                
                # Generate correlation_id if not provided
                if correlation_id is None:
                    correlation_id = uuid.uuid4()
                
                # Append each event
                new_version = current_version
                for event in events:
                    new_version += 1
                    event_id = uuid.uuid4()
                    
                    await conn.execute(
                        """INSERT INTO events 
                           (event_id, stream_id, stream_position, event_type, event_version, 
                            payload, metadata, correlation_id, causation_id)
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""",
                        event_id,
                        stream_id,
                        new_version,
                        event.event_type,
                        event.event_version,
                        self._serialize_payload(event.to_payload()),
                        self._serialize_payload({}),
                        correlation_id,
                        causation_id
                    )
                    
                    # Add to outbox (Outbox Pattern)
                    await conn.execute(
                        """INSERT INTO outbox (event_id, destination, payload)
                           VALUES ($1, $2, $3)""",
                        event_id,
                        f"stream:{stream_id}",
                        self._serialize_payload(event.to_payload())
                    )
                
                # Update stream version
                await conn.execute(
                    "UPDATE event_streams SET current_version = $1 WHERE stream_id = $2",
                    new_version, stream_id
                )
                
                return new_version
    
    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> list[StoredEvent]:
        """
        Load all events from a stream.
        
        Args:
            stream_id: The stream to load
            from_position: Starting position (0-based)
            to_position: Ending position (None for all remaining)
            
        Returns:
            List of stored events in stream order
            
        Raises:
            StreamNotFoundError: If stream doesn't exist
        """
        async with self._pool.acquire() as conn:
            # Check if stream exists
            exists = await conn.fetchval(
                "SELECT 1 FROM event_streams WHERE stream_id = $1",
                stream_id
            )
            if not exists:
                raise StreamNotFoundError(f"Stream '{stream_id}' not found")
            
            # Build query
            if to_position is not None:
                rows = await conn.fetch(
                    """SELECT event_id, stream_id, stream_position, global_position,
                              event_type, event_version, payload, metadata, 
                              recorded_at, correlation_id, causation_id
                       FROM events
                       WHERE stream_id = $1 
                         AND stream_position > $2 
                         AND stream_position <= $3
                       ORDER BY stream_position""",
                    stream_id, from_position, to_position
                )
            else:
                rows = await conn.fetch(
                    """SELECT event_id, stream_id, stream_position, global_position,
                              event_type, event_version, payload, metadata,
                              recorded_at, correlation_id, causation_id
                       FROM events
                       WHERE stream_id = $1 AND stream_position > $2
                       ORDER BY stream_position""",
                    stream_id, from_position
                )
            
            return [self._row_to_stored_event(row) for row in rows]
    
    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: list[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncIterator[StoredEvent]:
        """
        Load events from all streams, optionally filtered by type.
        
        This is an async generator - use `async for` to iterate.
        
        Args:
            from_global_position: Starting global position
            event_types: Optional list of event types to filter
            batch_size: Number of events per batch
            
        Yields:
            StoredEvent objects in global position order
        """
        async with self._pool.acquire() as conn:
            position = from_global_position
            
            while True:
                if event_types:
                    rows = await conn.fetch(
                        """SELECT event_id, stream_id, stream_position, global_position,
                                  event_type, event_version, payload, metadata,
                                  recorded_at, correlation_id, causation_id
                           FROM events
                           WHERE global_position > $1 
                             AND event_type = ANY($2)
                           ORDER BY global_position
                           LIMIT $3""",
                        position, event_types, batch_size
                    )
                else:
                    rows = await conn.fetch(
                        """SELECT event_id, stream_id, stream_position, global_position,
                                  event_type, event_version, payload, metadata,
                                  recorded_at, correlation_id, causation_id
                           FROM events
                           WHERE global_position > $1
                           ORDER BY global_position
                           LIMIT $2""",
                        position, batch_size
                    )
                
                if not rows:
                    break
                
                for row in rows:
                    yield self._row_to_stored_event(row)
                    position = row['global_position']
    
    async def stream_version(self, stream_id: str) -> int:
        """
        Get the current version of a stream.
        
        Args:
            stream_id: The stream to check
            
        Returns:
            Current version (0 if stream doesn't exist)
        """
        async with self._pool.acquire() as conn:
            version = await conn.fetchval(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id
            )
            return version if version is not None else 0
    
    async def archive_stream(self, stream_id: str) -> None:
        """
        Archive a stream (mark as no longer active).
        
        Args:
            stream_id: The stream to archive
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE event_streams SET archived_at = NOW() WHERE stream_id = $1",
                stream_id
            )
    
    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        """
        Get metadata about a stream.
        
        Args:
            stream_id: The stream to query
            
        Returns:
            StreamMetadata object
            
        Raises:
            StreamNotFoundError: If stream doesn't exist
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT stream_id, aggregate_type, current_version, 
                          created_at, archived_at, metadata
                   FROM event_streams WHERE stream_id = $1""",
                stream_id
            )
            if not row:
                raise StreamNotFoundError(f"Stream '{stream_id}' not found")
            
            return StreamMetadata(
                stream_id=row['stream_id'],
                aggregate_type=row['aggregate_type'],
                current_version=row['current_version'],
                created_at=row['created_at'],
                archived_at=row['archived_at'],
                metadata=row['metadata']
            )
    
    async def get_global_position(self) -> int:
        """Get the current global position (highest event ID)."""
        async with self._pool.acquire() as conn:
            pos = await conn.fetchval(
                "SELECT MAX(global_position) FROM events"
            )
            return pos if pos is not None else 0
    
    def _infer_aggregate_type(self, event: BaseEvent) -> str:
        """Infer aggregate type from event type."""
        event_type = event.event_type
        
        # Map event types to aggregate types
        if event_type.startswith("Application") or event_type in [
            "CreditAnalysisRequested", "FraudScreeningRequested", 
            "ComplianceCheckRequested", "DecisionRequested", "DecisionGenerated",
            "HumanReviewRequested", "HumanReviewCompleted"
        ]:
            return "LoanApplication"
        elif event_type.startswith("Agent"):
            return "AgentSession"
        elif event_type.startswith("Credit"):
            return "CreditRecord"
        elif event_type.startswith("Fraud"):
            return "FraudScreening"
        elif event_type.startswith("Compliance"):
            return "ComplianceRecord"
        elif event_type.startswith("Package") or event_type.startswith("Document"):
            return "DocumentPackage"
        elif event_type.startswith("Audit"):
            return "AuditLedger"
        
        return "Unknown"
    
    def _serialize_payload(self, data: Any) -> str:
        """
        Serialize data to JSON string with proper type validation.
        
        Args:
            data: The data to serialize
            
        Returns:
            JSON string representation
            
        Raises:
            TypeError: If data contains unsupported types
        """
        if data is None:
            return "{}"
        
        # Validate that data is a basic type or dict/list of basic types
        self._validate_serializable(data)
        return json.dumps(data, default=str)
    
    def _validate_serializable(self, data: Any) -> None:
        """
        Recursively validate that data contains only JSON-serializable types.
        
        Args:
            data: The data to validate
            
        Raises:
            TypeError: If data contains non-serializable types
        """
        if data is None or isinstance(data, (bool, int, float, str)):
            return
        elif isinstance(data, (list, tuple)):
            for item in data:
                self._validate_serializable(item)
        elif isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str):
                    raise TypeError(f"Dict keys must be strings, got {type(key)}")
                self._validate_serializable(value)
        elif isinstance(data, (datetime, uuid.UUID)):
            # These are handled by json.dumps default=str
            return
        else:
            raise TypeError(f"Unsupported type for JSON serialization: {type(data)}")
    
    def _row_to_stored_event(self, row: asyncpg.Record) -> StoredEvent:
        """Convert a database row to a StoredEvent."""
        # Parse JSON payload and metadata back to dicts
        payload = row['payload']
        if isinstance(payload, str):
            payload = json.loads(payload)
        
        metadata = row['metadata']
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        
        return StoredEvent(
            event_id=row['event_id'],
            stream_id=row['stream_id'],
            stream_position=row['stream_position'],
            global_position=row['global_position'],
            event_type=row['event_type'],
            event_version=row['event_version'],
            payload=payload,
            metadata=metadata,
            recorded_at=row['recorded_at'],
            correlation_id=row['correlation_id'],
            causation_id=row['causation_id']
        )


# Factory function for creating EventStore instances
async def create_event_store(dsn: str) -> EventStore:
    """
    Create and initialize an event store.
    
    Args:
        dsn: PostgreSQL connection string
        
    Returns:
        Initialized EventStore instance
    """
    store = EventStore(dsn)
    await store.connect()
    await store.initialize_schema()
    return store
