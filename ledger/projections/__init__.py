"""
CQRS Projections

Read-optimized views built from the event stream:
- ApplicationSummaryProjection
- AgentPerformanceLedgerProjection
- ComplianceAuditViewProjection
- ProjectionDaemon
"""

from ledger.projections.base import (
    Projection,
    ProjectionDaemon,
    create_projection_tables,
)
from ledger.projections.application_summary import ApplicationSummaryProjection
from ledger.projections.agent_performance import AgentPerformanceLedgerProjection
from ledger.projections.compliance_audit import ComplianceAuditViewProjection

__all__ = [
    "Projection",
    "ProjectionDaemon",
    "create_projection_tables",
    "ApplicationSummaryProjection",
    "AgentPerformanceLedgerProjection",
    "ComplianceAuditViewProjection",
]
