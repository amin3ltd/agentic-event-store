"""
Test Suite for Event Store Core Functionality

Tests the fundamental event store operations including:
- Append events
- Load stream
- Optimistic concurrency control
- Stream metadata
"""

import asyncio
import pytest
import uuid
from datetime import datetime, timezone

from ledger.event_store import EventStore, OptimisticConcurrencyError, StreamNotFoundError
from ledger.events import (
    ApplicationSubmitted,
    CreditAnalysisRequested,
    CreditAnalysisCompleted,
    DecisionGenerated,
)


# Test configuration
TEST_DSN = "postgresql://postgres:postgres@localhost:5432/apex_ledger_test"


@pytest.fixture
async def event_store() -> EventStore:
    """Create a test event store."""
    store = EventStore(TEST_DSN)
    await store.connect()
    
    # Initialize schema
    await store.initialize_schema()
    
    yield store
    
    await store.disconnect()


@pytest.fixture
async def sample_stream(event_store: EventStore) -> str:
    """Create a sample loan application stream."""
    stream_id = f"loan-test-{uuid.uuid4()}"
    application_id = "TEST-001"
    
    events = [
        ApplicationSubmitted(
            application_id=application_id,
            applicant_id="company-123",
            requested_amount_usd=100000.0,
            loan_purpose="expansion",
            submission_channel="online"
        ),
        CreditAnalysisRequested(
            application_id=application_id,
            assigned_agent_id="agent-1"
        ),
    ]
    
    await event_store.append(stream_id, events, expected_version=-1)
    return stream_id


class TestEventStoreBasics:
    """Tests for basic event store operations."""
    
    @pytest.mark.asyncio
    async def test_append_first_event(self, event_store: EventStore):
        """Test appending first event to a new stream."""
        stream_id = f"loan-test-{uuid.uuid4()}"
        
        event = ApplicationSubmitted(
            application_id="APP-001",
            applicant_id="company-1",
            requested_amount_usd=50000.0,
            loan_purpose="equipment",
            submission_channel="online"
        )
        
        # expected_version=-1 means "stream should not exist"
        await event_store.append(stream_id, [event], expected_version=-1)
        
        # Verify the stream was created
        loaded = await event_store.load_stream(stream_id)
        
        assert len(loaded) == 1
        assert loaded[0].event_type == "ApplicationSubmitted"
        assert loaded[0].stream_position == 1
    
    @pytest.mark.asyncio
    async def test_load_stream(self, event_store: EventStore, sample_stream: str):
        """Test loading a stream with multiple events."""
        events = await event_store.load_stream(sample_stream)
        
        assert len(events) == 2
        assert events[0].event_type == "ApplicationSubmitted"
        assert events[1].event_type == "CreditAnalysisRequested"
        assert events[0].stream_position == 1
        assert events[1].stream_position == 2
    
    @pytest.mark.asyncio
    async def test_load_stream_not_found(self, event_store: EventStore):
        """Test loading a non-existent stream raises error."""
        with pytest.raises(StreamNotFoundError):
            await event_store.load_stream(f"nonexistent-{uuid.uuid4()}")
    
    @pytest.mark.asyncio
    async def test_append_to_existing_stream(self, event_store: EventStore, sample_stream: str):
        """Test appending events to an existing stream."""
        new_event = DecisionGenerated(
            application_id="TEST-001",
            orchestrator_agent_id="orchestrator-1",
            recommendation="APPROVE",
            confidence_score=0.92,
            contributing_agent_sessions=["session-1", "session-2"],
            decision_basis_summary="Low risk profile",
            model_versions={"credit": "v2.1", "fraud": "v1.5"}
        )
        
        # Current version is 2, so we expect version 2
        await event_store.append(sample_stream, [new_event], expected_version=2)
        
        events = await event_store.load_stream(sample_stream)
        assert len(events) == 3
        assert events[-1].event_type == "DecisionGenerated"
    
    @pytest.mark.asyncio
    async def test_get_stream_metadata(self, event_store: EventStore, sample_stream: str):
        """Test retrieving stream metadata."""
        metadata = await event_store.get_stream_metadata(sample_stream)
        
        assert metadata.stream_id == sample_stream
        assert metadata.aggregate_type == "LoanApplication"
        assert metadata.current_version == 2


class TestOptimisticConcurrency:
    """Tests for optimistic concurrency control."""
    
    @pytest.mark.asyncio
    async def test_concurrent_append_conflict(self, event_store: EventStore, sample_stream: str):
        """Test that concurrent appends to the same version cause conflict."""
        # Create two events to append concurrently
        event1 = DecisionGenerated(
            application_id="TEST-001",
            orchestrator_agent_id="orchestrator-1",
            recommendation="APPROVE",
            confidence_score=0.90,
        )
        
        event2 = DecisionGenerated(
            application_id="TEST-001", 
            orchestrator_agent_id="orchestrator-2",
            recommendation="DECLINE",
            confidence_score=0.85,
        )
        
        # Both try to append at version 2
        # First one should succeed, second should fail
        await event_store.append(sample_stream, [event1], expected_version=2)
        
        with pytest.raises(OptimisticConcurrencyError) as exc_info:
            await event_store.append(sample_stream, [event2], expected_version=2)
        
        assert exc_info.value.expected_version == 2
        assert exc_info.value.actual_version == 3
    
    @pytest.mark.asyncio
    async def test_retry_after_conflict(self, event_store: EventStore, sample_stream: str):
        """Test retry mechanism after concurrency conflict."""
        event1 = DecisionGenerated(
            application_id="TEST-001",
            orchestrator_agent_id="orchestrator-1",
            recommendation="APPROVE",
            confidence_score=0.90,
        )
        
        event2 = DecisionGenerated(
            application_id="TEST-001",
            orchestrator_agent_id="orchestrator-2", 
            recommendation="DECLINE",
            confidence_score=0.85,
        )
        
        # First append succeeds
        await event_store.append(sample_stream, [event1], expected_version=2)
        
        # Second append fails - needs retry
        # In real code, you'd reload the stream and retry
        with pytest.raises(OptimisticConcurrencyError):
            await event_store.append(sample_stream, [event2], expected_version=2)
        
        # Retry with correct version
        await event_store.append(sample_stream, [event2], expected_version=3)
        
        # Verify both events are in the stream
        events = await event_store.load_stream(sample_stream)
        assert len(events) == 4


class TestEventTypes:
    """Tests for event type handling."""
    
    @pytest.mark.asyncio
    async def test_event_versioning(self, event_store: EventStore):
        """Test that event versions are stored correctly."""
        stream_id = f"loan-test-{uuid.uuid4()}"
        
        # DecisionGenerated v2
        event = DecisionGenerated(
            application_id="APP-001",
            orchestrator_agent_id="orchestrator-1",
            recommendation="APPROVE",
            confidence_score=0.95,
        )
        
        assert event.event_version == 2
        
        await event_store.append(stream_id, [event], expected_version=-1)
        
        loaded = await event_store.load_stream(stream_id)
        assert loaded[0].event_version == 2
    
    @pytest.mark.asyncio
    async def test_event_timestamps(self, event_store: EventStore):
        """Test that event timestamps are recorded."""
        stream_id = f"loan-test-{uuid.uuid4()}"
        
        before = datetime.now(timezone.utc)
        
        event = ApplicationSubmitted(
            application_id="APP-001",
            applicant_id="company-1",
            requested_amount_usd=10000.0,
            loan_purpose="test",
            submission_channel="online"
        )
        
        await event_store.append(stream_id, [event], expected_version=-1)
        
        after = datetime.now(timezone.utc)
        
        loaded = await event_store.load_stream(stream_id)
        
        assert loaded[0].recorded_at is not None
        assert loaded[0].recorded_at >= before.replace(tzinfo=timezone.utc)
        assert loaded[0].recorded_at <= after.replace(tzinfo=timezone.utc)


class TestCausationCorrelation:
    """Tests for correlation and causation IDs."""
    
    @pytest.mark.asyncio
    async def test_correlation_id(self, event_store: EventStore):
        """Test that correlation IDs are preserved."""
        correlation_id = uuid.uuid4()
        
        stream_id = f"loan-test-{uuid.uuid4()}"
        
        event = ApplicationSubmitted(
            application_id="APP-001",
            applicant_id="company-1",
            requested_amount_usd=10000.0,
            loan_purpose="test",
            submission_channel="online"
        )
        
        await event_store.append(
            stream_id, 
            [event], 
            expected_version=-1,
            correlation_id=correlation_id
        )
        
        loaded = await event_store.load_stream(stream_id)
        assert loaded[0].correlation_id == correlation_id
