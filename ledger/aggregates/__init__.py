"""
Domain Aggregates

Implements the domain logic for The Ledger:
- LoanApplication aggregate
- AgentSession aggregate
- Command handlers
"""

from ledger.aggregates.loan_application import (
    LoanApplicationAggregate,
    ApplicationState,
    DomainError as LoanDomainError,
    handle_credit_analysis_completed,
    handle_decision_generated,
)
from ledger.aggregates.agent_session import (
    AgentSessionAggregate,
    DomainError as AgentDomainError,
    handle_agent_session_started,
    handle_agent_context_loaded,
    handle_agent_node_executed,
    handle_agent_tool_called,
)

__all__ = [
    "LoanApplicationAggregate",
    "ApplicationState",
    "LoanDomainError",
    "handle_credit_analysis_completed",
    "handle_decision_generated",
    "AgentSessionAggregate",
    "AgentDomainError",
    "handle_agent_session_started",
    "handle_agent_context_loaded",
    "handle_agent_node_executed",
    "handle_agent_tool_called",
]
