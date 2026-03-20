"""
Test: Optimistic Concurrency Control - The Double-Decision Test

This is the most critical test in Phase 1. Two AI agents simultaneously attempt 
to append a CreditAnalysisCompleted event to the same loan application stream. 
Both read the stream at version 3 and pass expected_version=3 to their append call. 
Exactly one must succeed. The other must receive OptimisticConcurrencyError and 
retry after reloading the stream.

This test represents two fraud-detection agents simultaneously flagging the same 
application. Without optimistic concurrency, both flags are applied and the 
application's state becomes inconsistent.
"""

import asyncio
import pytest
import uuid

from ledger.event_store import EventStore, OptimisticConcurrencyError, create_event_store
from ledger.events import ApplicationSubmitted, CreditAnalysisRequested, CreditAnalysisCompleted


# Test configuration
TEST_DSN = "postgresql://postgres:postgres@localhost:5432/apex_ledger_test"


@pytest.fixture
async def event_store() -> EventStore:
    """Create a test event store."""
    store = await create_event_store(TEST_DSN)
    yield store
    await store.disconnect()


@pytest.fixture
async def setup_stream(event_store: EventStore) -> str:
    """Set up a loan application stream with 3 events."""
    stream_id = f"loan-test-{uuid.uuid4()}"
    application_id = "TEST-001"
    
    # Create initial events to bring stream to version 3
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
        CreditAnalysisCompleted(
            application_id=application_id,
            agent_id="agent-1",
            session_id="session-1",
            model_version="v2.3",
            confidence_score=0.85,
            risk_tier="LOW",
            recommended_limit_usd=100000.0,
            analysis_duration_ms=1500,
            input_data_hash="abc123"
        ),
    ]
    
    await event_store.append(stream_id, events, expected_version=-1)
    return stream_id


class TestOptimisticConcurrency:
    """Tests for optimistic concurrency control."""
    
    @pytest.mark.asyncio
    async def test_double_decision_only_one_succeeds(
        self, event_store: EventStore, setup_stream: str
    ):
        """
        Test that exactly one of two concurrent appends succeeds.
        
        This is the core OCC test: Two agents read version 3, both try to append
        with expected_version=3. Only one should succeed.
        """
        stream_id = setup_stream
        application_id = "TEST-001"
        
        # Verify initial state
        initial_version = await event_store.stream_version(stream_id)
        assert initial_version == 3, f"Expected version 3, got {initial_version}"
        
        # Define the concurrent append tasks
        async def agent_a_attempt():
            """Agent A tries to append CreditAnalysisCompleted."""
            events = [
                CreditAnalysisCompleted(
                    application_id=application_id,
                    agent_id="agent-a",
                    session_id="session-a",
                    model_version="v2.4",
                    confidence_score=0.90,
                    risk_tier="LOW",
                    recommended_limit_usd=95000.0,
                    analysis_duration_ms=1800,
                    input_data_hash="def456"
                )
            ]
            return await event_store.append(stream_id, events, expected_version=3)
        
        async def agent_b_attempt():
            """Agent B tries to append CreditAnalysisCompleted."""
            events = [
                CreditAnalysisCompleted(
                    application_id=application_id,
                    agent_id="agent-b",
                    session_id="session-b",
                    model_version="v2.4",
                    confidence_score=0.88,
                    risk_tier="LOW",
                    recommended_limit_usd=98000.0,
                    analysis_duration_ms=1650,
                    input_data_hash="ghi789"
                )
            ]
            return await event_store.append(stream_id, events, expected_version=3)
        
        # Run both tasks concurrently
        results: dict[str, int | Exception] = {}
        
        async def run_with_name(name: str, coro):
            try:
                result = await coro
                results[name] = result
            except OptimisticConcurrencyError as e:
                results[name] = e
            except Exception as e:
                results[name] = e
        
        # Execute both concurrently
        await asyncio.gather(
            run_with_name("agent_a", agent_a_attempt()),
            run_with_name("agent_b", agent_b_attempt())
        )
        
        # Analyze results
        success_count = 0
        occ_error_count = 0
        
        for name, result in results.items():
            if isinstance(result, int):
                success_count += 1
                print(f"{name} succeeded with version {result}")
            elif isinstance(result, OptimisticConcurrencyError):
                occ_error_count += 1
                print(f"{name} got OCC error: {result}")
            else:
                pytest.fail(f"{name} got unexpected error: {result}")
        
        # Assertions
        assert success_count == 1, f"Expected exactly 1 success, got {success_count}"
        assert occ_error_count == 1, f"Expected exactly 1 OCC error, got {occ_error_count}"
        
        # Verify final stream state
        final_version = await event_store.stream_version(stream_id)
        assert final_version == 4, f"Expected final version 4, got {final_version}"
        
        # Verify the winning event is in the stream
        events = await event_store.load_stream(stream_id, from_position=3)
        assert len(events) == 1, f"Expected 1 event at position 4, got {len(events)}"
        assert events[0].event_type == "CreditAnalysisCompleted"
    
    @pytest.mark.asyncio
    async def test_losing_agent_must_retry(
        self, event_store: EventStore, setup_stream: str
    ):
        """
        Test that the losing agent receives OCC error and can retry.
        
        After receiving OptimisticConcurrencyError, the losing agent should:
        1. Reload the stream to get the new version
        2. Retry the operation with the correct expected_version
        """
        stream_id = setup_stream
        application_id = "TEST-001"
        
        # First, establish which agent wins by controlling timing
        # Agent A will win by going first
        events_a = [
            CreditAnalysisCompleted(
                application_id=application_id,
                agent_id="agent-a",
                session_id="session-a",
                model_version="v2.4",
                confidence_score=0.90,
                risk_tier="LOW",
                recommended_limit_usd=95000.0,
                analysis_duration_ms=1800,
                input_data_hash="def456"
            )
        ]
        
        # Agent A succeeds
        await event_store.append(stream_id, events_a, expected_version=3)
        
        # Now Agent B tries with the old expected_version - should fail
        events_b = [
            CreditAnalysisCompleted(
                application_id=application_id,
                agent_id="agent-b",
                session_id="session-b",
                model_version="v2.4",
                confidence_score=0.88,
                risk_tier="LOW",
                recommended_limit_usd=98000.0,
                analysis_duration_ms=1650,
                input_data_hash="ghi789"
            )
        ]
        
        with pytest.raises(OptimisticConcurrencyError) as exc_info:
            await event_store.append(stream_id, events_b, expected_version=3)
        
        assert exc_info.value.expected_version == 3
        assert exc_info.value.actual_version == 4
        
        # Agent B retries with correct version
        # After reload, the version is 4, so Agent B should append at position 5
        new_version = await event_store.append(stream_id, events_b, expected_version=4)
        assert new_version == 5
        
        # Verify both events are in the stream
        all_events = await event_store.load_stream(stream_id)
        assert len(all_events) == 5  # 3 initial + 2 new
    
    @pytest.mark.asyncio
    async def test_version_mismatch_on_nonexistent_stream(
        self, event_store: EventStore
    ):
        """
        Test that appending with wrong version to nonexistent stream fails.
        """
        stream_id = f"loan-nonexistent-{uuid.uuid4()}"
        
        events = [
            ApplicationSubmitted(
                application_id="NEW-001",
                applicant_id="company-new",
                requested_amount_usd=50000.0,
                loan_purpose="startup",
                submission_channel="online"
            )
        ]
        
        # Trying to append to nonexistent stream with version 0 (not -1) should fail
        with pytest.raises(OptimisticConcurrencyError):
            await event_store.append(stream_id, events, expected_version=0)
    
    @pytest.mark.asyncio
    async def test_new_stream_requires_minus_one(
        self, event_store: EventStore
    ):
        """
        Test that creating a new stream requires expected_version=-1.
        """
        stream_id = f"loan-new-{uuid.uuid4()}"
        
        events = [
            ApplicationSubmitted(
                application_id="NEW-002",
                applicant_id="company-new-2",
                requested_amount_usd=75000.0,
                loan_purpose="equipment",
                submission_channel="online"
            )
        ]
        
        # Should succeed with -1
        version = await event_store.append(stream_id, events, expected_version=-1)
        assert version == 1
        
        # Verify stream was created
        version_check = await event_store.stream_version(stream_id)
        assert version_check == 1


class TestEventStoreBasicOperations:
    """Tests for basic event store operations."""
    
    @pytest.mark.asyncio
    async def test_append_and_load(self, event_store: EventStore):
        """Test basic append and load operations."""
        stream_id = f"loan-basic-{uuid.uuid4()}"
        
        events = [
            ApplicationSubmitted(
                application_id="BASIC-001",
                applicant_id="company-basic",
                requested_amount_usd=100000.0,
                loan_purpose="expansion",
                submission_channel="online"
            )
        ]
        
        # Append
        version = await event_store.append(stream_id, events, expected_version=-1)
        assert version == 1
        
        # Load
        loaded = await event_store.load_stream(stream_id)
        assert len(loaded) == 1
        assert loaded[0].event_type == "ApplicationSubmitted"
        assert loaded[0].payload["application_id"] == "BASIC-001"
    
    @pytest.mark.asyncio
    async def test_load_nonexistent_stream_raises_error(self, event_store: EventStore):
        """Test that loading a nonexistent stream raises StreamNotFoundError."""
        from ledger.event_store import StreamNotFoundError
        
        with pytest.raises(StreamNotFoundError):
            await event_store.load_stream("nonexistent-stream")
    
    @pytest.mark.asyncio
    async def test_stream_version_nonexistent(self, event_store: EventStore):
        """Test stream_version returns 0 for nonexistent streams."""
        version = await event_store.stream_version("nonexistent-stream")
        assert version == 0
