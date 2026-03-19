"""
Application Summary Projection

Read-optimized view of every loan application's current state.
Stored as a PostgreSQL table (one row per application).
"""

from ledger.event_store import EventStore, StoredEvent
from ledger.projections.base import Projection, create_projection_tables


class ApplicationSummaryProjection(Projection):
    """
    Projection that maintains application summary data.
    
    Tracks:
    - Current state
    - Applicant info
    - Amounts (requested/approved)
    - Risk data
    - Compliance status
    - Decision
    """
    
    @property
    def name(self) -> str:
        return "application_summary"
    
    async def initialize(self, pool) -> None:
        """Create projection table."""
        await create_projection_tables(pool)
    
    async def handle_event(self, event: StoredEvent) -> None:
        """Update summary based on event."""
        if not event.stream_id.startswith("loan-"):
            return
        
        application_id = event.payload.get("application_id", "")
        if not application_id:
            return
        
        async with event.store._pool.acquire() as conn:
            if event.event_type == "ApplicationSubmitted":
                await conn.execute(
                    """INSERT INTO application_summary 
                       (application_id, state, applicant_id, requested_amount_usd, 
                        last_event_type, last_event_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5, $6, NOW())
                       ON CONFLICT (application_id) DO UPDATE SET
                       state = EXCLUDED.state,
                       applicant_id = EXCLUDED.applicant_id,
                       requested_amount_usd = EXCLUDED.requested_amount_usd,
                       last_event_type = EXCLUDED.last_event_type,
                       last_event_at = EXCLUDED.last_event_at,
                       updated_at = NOW()""",
                    application_id,
                    "SUBMITTED",
                    event.payload.get("applicant_id"),
                    event.payload.get("requested_amount_usd"),
                    event.event_type,
                    event.recorded_at
                )
            
            elif event.event_type == "CreditAnalysisCompleted":
                await conn.execute(
                    """UPDATE application_summary SET
                       state = 'CREDIT_COMPLETE',
                       risk_tier = $2,
                       last_event_type = $3,
                       last_event_at = $4,
                       updated_at = NOW()
                       WHERE application_id = $1""",
                    application_id,
                    event.payload.get("risk_tier"),
                    event.event_type,
                    event.recorded_at
                )
            
            elif event.event_type == "FraudScreeningCompleted":
                await conn.execute(
                    """UPDATE application_summary SET
                       state = 'FRAUD_COMPLETE',
                       fraud_score = $2,
                       last_event_type = $3,
                       last_event_at = $4,
                       updated_at = NOW()
                       WHERE application_id = $1""",
                    application_id,
                    event.payload.get("fraud_score"),
                    event.event_type,
                    event.recorded_at
                )
            
            elif event.event_type == "ComplianceCheckCompleted":
                await conn.execute(
                    """UPDATE application_summary SET
                       state = 'COMPLIANCE_COMPLETE',
                       compliance_status = $2,
                       last_event_type = $3,
                       last_event_at = $4,
                       updated_at = NOW()
                       WHERE application_id = $1""",
                    application_id,
                    event.payload.get("overall_verdict"),
                    event.event_type,
                    event.recorded_at
                )
            
            elif event.event_type == "DecisionGenerated":
                await conn.execute(
                    """UPDATE application_summary SET
                       state = 'PENDING_DECISION',
                       decision = $2,
                       last_event_type = $3,
                       last_event_at = $4,
                       updated_at = NOW()
                       WHERE application_id = $1""",
                    application_id,
                    event.payload.get("recommendation"),
                    event.event_type,
                    event.recorded_at
                )
            
            elif event.event_type == "HumanReviewCompleted":
                await conn.execute(
                    """UPDATE application_summary SET
                       state = $2,
                       human_reviewer_id = $3,
                       final_decision_at = $4,
                       last_event_type = $5,
                       last_event_at = $6,
                       updated_at = NOW()
                       WHERE application_id = $1""",
                    application_id,
                    "FINAL_APPROVED" if event.payload.get("final_decision") == "APPROVE" 
                        else "FINAL_DECLINED",
                    event.payload.get("reviewer_id"),
                    event.recorded_at,
                    event.event_type,
                    event.recorded_at
                )
            
            elif event.event_type == "ApplicationApproved":
                await conn.execute(
                    """UPDATE application_summary SET
                       state = 'FINAL_APPROVED',
                       approved_amount_usd = $2,
                       last_event_type = $3,
                       last_event_at = $4,
                       updated_at = NOW()
                       WHERE application_id = $1""",
                    application_id,
                    event.payload.get("approved_amount_usd"),
                    event.event_type,
                    event.recorded_at
                )
            
            elif event.event_type == "ApplicationDeclined":
                await conn.execute(
                    """UPDATE application_summary SET
                       state = 'FINAL_DECLINED',
                       last_event_type = $2,
                       last_event_at = $3,
                       updated_at = NOW()
                       WHERE application_id = $1""",
                    application_id,
                    event.event_type,
                    event.recorded_at
                )
    
    async def get_lag_ms(self, store: EventStore) -> int:
        """Calculate projection lag."""
        import time
        current_global = await store.get_global_position()
        
        async with store._pool.acquire() as conn:
            checkpoint = await conn.fetchval(
                """SELECT last_position FROM projection_checkpoints 
                   WHERE projection_name = $1""",
                self.name
            )
        
        # Simple lag calculation - in production would use timestamps
        if checkpoint is None:
            return 0
        
        lag_events = current_global - checkpoint
        # Estimate: ~1ms per event processing
        return lag_events
    
    async def rebuild(self, store: EventStore) -> None:
        """Rebuild projection from scratch."""
        async with store._pool.acquire() as conn:
            await conn.execute("TRUNCATE TABLE application_summary")
        
        # Replay all events
        async for event in store.load_all():
            await self.handle_event(event)
    
    async def get_application(self, store: EventStore, application_id: str) -> dict:
        """Get application summary by ID."""
        async with store._pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM application_summary WHERE application_id = $1",
                application_id
            )
    
    async def get_all_applications(
        self, 
        store: EventStore, 
        state: str = None,
        limit: int = 100
    ) -> list[dict]:
        """Get all applications, optionally filtered by state."""
        async with store._pool.acquire() as conn:
            if state:
                return await conn.fetch(
                    """SELECT * FROM application_summary 
                       WHERE state = $1 ORDER BY updated_at DESC LIMIT $2""",
                    state, limit
                )
            return await conn.fetch(
                """SELECT * FROM application_summary 
                   ORDER BY updated_at DESC LIMIT $1""",
                limit
            )
