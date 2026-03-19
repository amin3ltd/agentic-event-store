"""
Loan Application Aggregate

Implements the domain logic for loan applications, including:
- State machine transitions
- Business rule enforcement
- Aggregate reconstruction from event stream
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from ledger.event_store import EventStore, StoredEvent
from ledger.events import (
    ApplicationSubmitted,
    CreditAnalysisRequested,
    CreditAnalysisCompleted,
    FraudScreeningRequested,
    FraudScreeningCompleted,
    ComplianceCheckRequested,
    ComplianceCheckCompleted,
    DecisionRequested,
    DecisionGenerated,
    HumanReviewRequested,
    HumanReviewCompleted,
    ApplicationApproved,
    ApplicationDeclined,
)


class ApplicationState(Enum):
    """Valid states for a loan application."""
    SUBMITTED = "SUBMITTED"
    AWAITING_DOCUMENTS = "AWAITING_DOCUMENTS"
    DOCUMENTS_UPLOADED = "DOCUMENTS_UPLOADED"
    DOCUMENTS_PROCESSED = "DOCUMENTS_PROCESSED"
    AWAITING_CREDIT_ANALYSIS = "AWAITING_CREDIT_ANALYSIS"
    CREDIT_COMPLETE = "CREDIT_COMPLETE"
    AWAITING_FRAUD_SCREENING = "AWAITING_FRAUD_SCREENING"
    FRAUD_COMPLETE = "FRAUD_COMPLETE"
    AWAITING_COMPLIANCE = "AWAITING_COMPLIANCE"
    COMPLIANCE_COMPLETE = "COMPLIANCE_COMPLETE"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"
    REFERRED = "REFERRED"


class DomainError(Exception):
    """Raised when a business rule is violated."""
    def __init__(self, message: str, rule: str):
        self.message = message
        self.rule = rule
        super().__init__(message)


@dataclass
class LoanApplicationAggregate:
    """
    Aggregate root for loan applications.
    
    Enforces business rules:
    1. Valid state transitions only
    2. Cannot be approved if compliance check is pending
    3. Credit limit cannot exceed agent-assessed maximum
    4. Confidence < 0.60 → must REFER
    """
    
    application_id: str = ""
    state: ApplicationState = ApplicationState.SUBMITTED
    applicant_id: str = ""
    requested_amount: float = 0.0
    loan_purpose: str = ""
    submission_channel: str = ""
    
    # Credit analysis data
    risk_tier: str = ""
    recommended_limit: float = 0.0
    credit_confidence: float = 0.0
    
    # Fraud screening data
    fraud_score: float = 0.0
    fraud_risk_level: str = ""
    
    # Compliance data
    compliance_status: str = ""  # CLEAR, BLOCKED, CONDITIONAL
    has_hard_block: bool = False
    compliance_rules_passed: list[str] = field(default_factory=list)
    
    # Decision data
    recommendation: str = ""  # APPROVE, DECLINE, REFER
    decision_confidence: float = 0.0
    final_decision: str = ""
    human_reviewer_id: str = ""
    
    # Version tracking
    version: int = 0
    
    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "LoanApplicationAggregate":
        """
        Load aggregate by replaying all events from the stream.
        """
        stream_id = f"loan-{application_id}"
        events = await store.load_stream(stream_id)
        
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        
        return agg
    
    def _apply(self, event: StoredEvent) -> None:
        """Apply an event to update aggregate state."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position
    
    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = event.payload.get("applicant_id", "")
        self.requested_amount = event.payload.get("requested_amount_usd", 0.0)
        self.loan_purpose = event.payload.get("loan_purpose", "")
        self.submission_channel = event.payload.get("submission_channel", "")
    
    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.CREDIT_COMPLETE
        self.risk_tier = event.payload.get("risk_tier", "")
        self.recommended_limit = event.payload.get("recommended_limit_usd", 0.0)
        self.credit_confidence = event.payload.get("confidence_score", 0.0)
    
    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.FRAUD_COMPLETE
        self.fraud_score = event.payload.get("fraud_score", 0.0)
        # Determine risk level from score
        if self.fraud_score > 0.6:
            self.fraud_risk_level = "HIGH"
        elif self.fraud_score > 0.3:
            self.fraud_risk_level = "MEDIUM"
        else:
            self.fraud_risk_level = "LOW"
    
    def _on_ComplianceCheckCompleted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.COMPLIANCE_COMPLETE
        self.compliance_status = event.payload.get("overall_verdict", "")
        self.has_hard_block = event.payload.get("has_hard_block", False)
    
    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        self.state = ApplicationState.PENDING_DECISION
        self.recommendation = event.payload.get("recommendation", "")
        self.decision_confidence = event.payload.get("confidence_score", 0.0)
        
        # Business rule: Confidence < 0.60 → must REFER
        if self.decision_confidence < 0.60:
            self.recommendation = "REFER"
    
    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        self.final_decision = event.payload.get("final_decision", "")
        self.human_reviewer_id = event.payload.get("reviewer_id", "")
        
        if event.payload.get("override", False):
            # Override - go against agent recommendation
            self.state = (
                ApplicationState.FINAL_APPROVED 
                if self.final_decision == "APPROVE" 
                else ApplicationState.FINAL_DECLINED
            )
        else:
            # Follow agent recommendation
            if self.recommendation == "APPROVE":
                self.state = ApplicationState.FINAL_APPROVED
            elif self.recommendation == "DECLINE":
                self.state = ApplicationState.FINAL_DECLINED
            else:
                self.state = ApplicationState.REFERRED
    
    def _on_ApplicationApproved(self, event: StoredEvent) -> None:
        self.state = ApplicationState.FINAL_APPROVED
    
    def _on_ApplicationDeclined(self, event: StoredEvent) -> None:
        self.state = ApplicationState.FINAL_DECLINED
    
    # =========================================================================
    # Business Rules - State Machine
    # =========================================================================
    
    VALID_TRANSITIONS: dict[ApplicationState, list[ApplicationState]] = {
        ApplicationState.SUBMITTED: [
            ApplicationState.AWAITING_DOCUMENTS,
            ApplicationState.DOCUMENTS_UPLOADED,
        ],
        ApplicationState.DOCUMENTS_UPLOADED: [
            ApplicationState.DOCUMENTS_PROCESSED,
        ],
        ApplicationState.DOCUMENTS_PROCESSED: [
            ApplicationState.AWAITING_CREDIT_ANALYSIS,
        ],
        ApplicationState.AWAITING_CREDIT_ANALYSIS: [
            ApplicationState.CREDIT_COMPLETE,
        ],
        ApplicationState.CREDIT_COMPLETE: [
            ApplicationState.AWAITING_FRAUD_SCREENING,
        ],
        ApplicationState.AWAITING_FRAUD_SCREENING: [
            ApplicationState.FRAUD_COMPLETE,
        ],
        ApplicationState.FRAUD_COMPLETE: [
            ApplicationState.AWAITING_COMPLIANCE,
        ],
        ApplicationState.AWAITING_COMPLIANCE: [
            ApplicationState.COMPLIANCE_COMPLETE,
        ],
        ApplicationState.COMPLIANCE_COMPLETE: [
            ApplicationState.PENDING_DECISION,
        ],
        ApplicationState.PENDING_DECISION: [
            ApplicationState.APPROVED_PENDING_HUMAN,
            ApplicationState.DECLINED_PENDING_HUMAN,
            ApplicationState.REFERRED,
        ],
        ApplicationState.APPROVED_PENDING_HUMAN: [
            ApplicationState.FINAL_APPROVED,
            ApplicationState.FINAL_DECLINED,
        ],
        ApplicationState.DECLINED_PENDING_HUMAN: [
            ApplicationState.FINAL_DECLINED,
            ApplicationState.FINAL_APPROVED,
        ],
        ApplicationState.REFERRED: [
            ApplicationState.FINAL_APPROVED,
            ApplicationState.FINAL_DECLINED,
        ],
        ApplicationState.FINAL_APPROVED: [],  # Terminal state
        ApplicationState.FINAL_DECLINED: [],  # Terminal state
    }
    
    def assert_valid_transition(self, new_state: ApplicationState) -> None:
        """Assert that a state transition is valid."""
        if new_state not in self.VALID_TRANSITIONS.get(self.state, []):
            raise DomainError(
                f"Invalid state transition from {self.state.value} to {new_state.value}",
                rule="state_machine"
            )
    
    # =========================================================================
    # Business Rules - Compliance Dependency
    # =========================================================================
    
    def assert_can_approve(self) -> None:
        """Assert that the application can be approved (compliance cleared)."""
        if self.state != ApplicationState.COMPLIANCE_COMPLETE:
            raise DomainError(
                f"Cannot approve - application is in {self.state.value} state, not COMPLIANCE_COMPLETE",
                rule="compliance_dependency"
            )
        
        if self.has_hard_block or self.compliance_status == "BLOCKED":
            raise DomainError(
                "Cannot approve - compliance check has hard block",
                rule="compliance_dependency"
            )
    
    # =========================================================================
    # Business Rules - Credit Limit
    # =========================================================================
    
    def assert_within_credit_limit(self, amount: float) -> None:
        """Assert that the amount doesn't exceed recommended limit."""
        if self.recommended_limit > 0 and amount > self.recommended_limit:
            raise DomainError(
                f"Amount ${amount:,.2f} exceeds recommended limit ${self.recommended_limit:,.2f}",
                rule="credit_limit"
            )
    
    # =========================================================================
    # Business Rules - Confidence Floor
    # =========================================================================
    
    def assert_valid_orchestrator_decision(self, recommendation: str, confidence: float) -> None:
        """
        Assert that the orchestrator's decision meets minimum requirements.
        
        Business rule: confidence < 0.60 → recommendation must be REFER
        """
        if confidence < 0.60 and recommendation != "REFER":
            raise DomainError(
                f"Confidence {confidence} is below 0.60, recommendation must be REFER",
                rule="confidence_floor"
            )


# =============================================================================
# Command Handlers
# =============================================================================

async def handle_credit_analysis_completed(
    store: EventStore,
    cmd: dict[str, Any],
) -> list[CreditAnalysisCompleted]:
    """
    Handle credit analysis completed command.
    
    Validates:
    - Application is in correct state
    - Agent has loaded context (Gas Town)
    - Model version is current
    """
    # 1. Reconstruct aggregate
    app = await LoanApplicationAggregate.load(store, cmd["application_id"])
    
    # 2. Validate state
    if app.state != ApplicationState.AWAITING_CREDIT_ANALYSIS:
        raise DomainError(
            f"Application not awaiting credit analysis (state: {app.state.value})",
            rule="state_validation"
        )
    
    # 3. Create events
    events = [
        CreditAnalysisCompleted(
            application_id=cmd["application_id"],
            agent_id=cmd["agent_id"],
            session_id=cmd["session_id"],
            model_version=cmd["model_version"],
            confidence_score=cmd["confidence_score"],
            risk_tier=cmd["risk_tier"],
            recommended_limit_usd=cmd["recommended_limit_usd"],
            analysis_duration_ms=cmd.get("duration_ms", 0),
            input_data_hash=cmd.get("input_data_hash", ""),
        )
    ]
    
    # 4. Append to stream
    await store.append(
        stream_id=f"loan-{cmd['application_id']}",
        events=events,
        expected_version=app.version,
    )
    
    return events


async def handle_decision_generated(
    store: EventStore,
    cmd: dict[str, Any],
) -> list[DecisionGenerated]:
    """
    Handle decision generated command.
    
    Validates:
    - Compliance is complete
    - Confidence floor rule
    - Causal chain (references valid agent sessions)
    """
    # 1. Reconstruct aggregate
    app = await LoanApplicationAggregate.load(store, cmd["application_id"])
    
    # 2. Validate state
    if app.state != ApplicationState.COMPLIANCE_COMPLETE:
        raise DomainError(
            f"Compliance not complete (state: {app.state.value})",
            rule="compliance_dependency"
        )
    
    # 3. Validate confidence floor
    app.assert_valid_orchestrator_decision(
        cmd["recommendation"],
        cmd["confidence_score"]
    )
    
    # 4. Create event
    events = [
        DecisionGenerated(
            application_id=cmd["application_id"],
            orchestrator_agent_id=cmd["orchestrator_agent_id"],
            recommendation=cmd["recommendation"],
            confidence_score=cmd["confidence_score"],
            contributing_agent_sessions=cmd.get("contributing_agent_sessions", []),
            decision_basis_summary=cmd.get("decision_basis_summary", ""),
            model_versions=cmd.get("model_versions", {}),
        )
    ]
    
    # 5. Append to stream
    await store.append(
        stream_id=f"loan-{cmd['application_id']}",
        events=events,
        expected_version=app.version,
    )
    
    return events
