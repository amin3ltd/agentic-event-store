"""
The Ledger - Agentic Event Store

A production-quality event sourcing infrastructure for multi-agent AI systems.
"""

from ledger.event_store import EventStore, OptimisticConcurrencyError, StreamNotFoundError, StoredEvent, StreamMetadata, create_event_store
from ledger.events import BaseEvent, EVENT_REGISTRY
from ledger import schema

__all__ = [
    "EventStore",
    "OptimisticConcurrencyError", 
    "StreamNotFoundError",
    "StoredEvent",
    "StreamMetadata",
    "create_event_store",
    "BaseEvent",
    "EVENT_REGISTRY",
    "schema",
]

__version__ = "1.0.0"
