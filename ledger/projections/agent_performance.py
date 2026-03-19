"""
Agent Performance Ledger Projection

Aggregated performance metrics per AI agent model version.
Enables questions like: "Has agent v2.3 been making systematically 
different decisions than v2.2?"
"""

from ledger.event_store import EventStore, StoredEvent
from ledger.projections.base import Projection


class AgentPerformanceLedgerProjection(Projection):
    """
    Projection that tracks agent performance metrics.
    
    Metrics:
    - analyses_completed
    - decisions_generated
    - avg_confidence_score
    - avg_duration_ms
    - approve_rate, decline_rate, refer_rate
    - human_override_rate
    """
    
    @property
    def name(self) -> str:
        return "agent_performance_ledger"
    
    async def handle_event(self, event: StoredEvent) -> None:
        """Update performance metrics based on event."""
        if not event.stream_id.startswith("agent-"):
            return
        
        agent_id = event.payload.get("agent_id", "")
        model_version = event.payload.get("model_version", "")
        
        if not agent_id or not model_version:
            return
        
        async with event.store._pool.acquire() as conn:
            if event.event_type == "CreditAnalysisCompleted":
                # Increment analyses completed
                await conn.execute(
                    """INSERT INTO agent_performance_ledger 
                       (agent_id, model_version, analyses_completed, 
                        avg_confidence_score, avg_duration_ms, first_seen_at, last_seen_at)
                       VALUES ($1, $2, 1, $3, $4, NOW(), NOW())
                       ON CONFLICT (agent_id, model_version) DO UPDATE SET
                       analyses_completed = agent_performance_ledger.analyses_completed + 1,
                       avg_confidence_score = (
                           (agent_performance_ledger.avg_confidence_score * agent_performance_ledger.analyses_completed) + $3
                       ) / (agent_performance_ledger.analyses_completed + 1),
                       avg_duration_ms = (
                           (agent_performance_ledger.avg_duration_ms * agent_performance_ledger.analyses_completed) + $4
                       ) / (agent_performance_ledger.analyses_completed + 1),
                       last_seen_at = NOW()""",
                    agent_id,
                    model_version,
                    event.payload.get("confidence_score", 0),
                    event.payload.get("analysis_duration_ms", 0)
                )
            
            elif event.event_type == "DecisionGenerated":
                # Track decision rates
                recommendation = event.payload.get("recommendation", "REFER")
                
                # Get current rates
                current = await conn.fetchrow(
                    """SELECT decisions_generated, approve_rate, decline_rate, refer_rate
                       FROM agent_performance_ledger
                       WHERE agent_id = $1 AND model_version = $2""",
                    agent_id, model_version
                )
                
                if current:
                    decisions = current["decisions_generated"] + 1
                    if recommendation == "APPROVE":
                        new_approve = current["approve_rate"] * current["decisions_generated"] + 1
                        new_decline = current["decline_rate"] * current["decisions_generated"]
                        new_refer = current["refer_rate"] * current["decisions_generated"]
                    elif recommendation == "DECLINE":
                        new_approve = current["approve_rate"] * current["decisions_generated"]
                        new_decline = current["decline_rate"] * current["decisions_generated"] + 1
                        new_refer = current["refer_rate"] * current["decisions_generated"]
                    else:  # REFER
                        new_approve = current["approve_rate"] * current["decisions_generated"]
                        new_decline = current["decline_rate"] * current["decisions_generated"]
                        new_refer = current["refer_rate"] * current["decisions_generated"] + 1
                    
                    await conn.execute(
                        """UPDATE agent_performance_ledger SET
                           decisions_generated = $3,
                           approve_rate = $4 / $3,
                           decline_rate = $5 / $3,
                           refer_rate = $6 / $3,
                           avg_confidence_score = (
                               (agent_performance_ledger.avg_confidence_score * agent_performance_ledger.decisions_generated) + $7
                           ) / (agent_performance_ledger.decisions_generated + 1),
                           last_seen_at = NOW()
                           WHERE agent_id = $1 AND model_version = $2""",
                        agent_id,
                        model_version,
                        decisions,
                        new_approve,
                        new_decline,
                        new_refer,
                        event.payload.get("confidence_score", 0)
                    )
            
            elif event.event_type == "HumanReviewCompleted":
                # Track human override rate
                if event.payload.get("override", False):
                    current = await conn.fetchrow(
                        """SELECT decisions_generated, human_override_rate
                           FROM agent_performance_ledger
                           WHERE agent_id = $1 AND model_version = $2""",
                        agent_id, model_version
                    )
                    
                    if current and current["decisions_generated"] > 0:
                        overrides = current["human_override_rate"] * current["decisions_generated"] + 1
                        await conn.execute(
                            """UPDATE agent_performance_ledger SET
                               human_override_rate = $3 / decisions_generated,
                               last_seen_at = NOW()
                               WHERE agent_id = $1 AND model_version = $2""",
                            agent_id,
                            model_version,
                            overrides
                        )
    
    async def get_lag_ms(self, store: EventStore) -> int:
        """Calculate projection lag."""
        current_global = await store.get_global_position()
        
        async with store._pool.acquire() as conn:
            checkpoint = await conn.fetchval(
                """SELECT last_position FROM projection_checkpoints 
                   WHERE projection_name = $1""",
                self.name
            )
        
        if checkpoint is None:
            return 0
        
        return current_global - checkpoint
    
    async def rebuild(self, store: EventStore) -> None:
        """Rebuild projection from scratch."""
        async with store._pool.acquire() as conn:
            await conn.execute("TRUNCATE TABLE agent_performance_ledger")
        
        async for event in store.load_all():
            await self.handle_event(event)
    
    async def get_agent_metrics(
        self, 
        store: EventStore, 
        agent_id: str, 
        model_version: str = None
    ) -> list[dict]:
        """Get performance metrics for an agent."""
        async with store._pool.acquire() as conn:
            if model_version:
                return await conn.fetch(
                    """SELECT * FROM agent_performance_ledger 
                       WHERE agent_id = $1 AND model_version = $2""",
                    agent_id, model_version
                )
            return await conn.fetch(
                """SELECT * FROM agent_performance_ledger 
                   WHERE agent_id = $1 ORDER BY last_seen_at DESC""",
                agent_id
            )
    
    async def compare_versions(
        self, 
        store: EventStore, 
        agent_id: str, 
        version_a: str, 
        version_b: str
    ) -> dict:
        """Compare performance between two model versions."""
        async with store._pool.acquire() as conn:
            a = await conn.fetchrow(
                """SELECT * FROM agent_performance_ledger 
                   WHERE agent_id = $1 AND model_version = $2""",
                agent_id, version_a
            )
            b = await conn.fetchrow(
                """SELECT * FROM agent_performance_ledger 
                   WHERE agent_id = $1 AND model_version = $2""",
                agent_id, version_b
            )
            
            if not a or not b:
                return {"error": "One or both versions not found"}
            
            return {
                "version_a": dict(a),
                "version_b": dict(b),
                "differences": {
                    "confidence_delta": b["avg_confidence_score"] - a["avg_confidence_score"],
                    "approve_rate_delta": b["approve_rate"] - a["approve_rate"],
                    "decline_rate_delta": b["decline_rate"] - a["decline_rate"],
                    "refer_rate_delta": b["refer_rate"] - a["refer_rate"],
                    "override_rate_delta": b["human_override_rate"] - a["human_override_rate"],
                }
            }
