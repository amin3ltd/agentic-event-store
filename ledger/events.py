"""
Event Type Definitions

This module defines all event types used in The Ledger.
Each event represents a fact that occurred in the system.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
import uuid


@dataclass
class BaseEvent:
    """Base class for all events."""
    event_type: str = field(init=False)
    
    def to_payload(self) -> dict[str, Any]:
        """Convert event to dictionary for storage."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
    @property
    def event_version(self) -> int:
        """Event schema version - override in subclasses for versioning."""
        return 1


# =============================================================================
# Loan Application Events
# =============================================================================

@dataclass
class ApplicationSubmitted(BaseEvent):
    """A new loan application is submitted."""
    event_type: str = "ApplicationSubmitted"
    application_id: str = ""
    applicant_id: str = ""
    requested_amount_usd: float = 0.0
    loan_purpose: str = ""
    submission_channel: str = ""
    submitted_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class CreditAnalysisRequested(BaseEvent):
    """Credit analysis is requested for an application."""
    event_type: str = "CreditAnalysisRequested"
    application_id: str = ""
    assigned_agent_id: str = ""
    requested_at: datetime = field(default_factory=datetime.utcnow)
    priority: str = "normal"


@dataclass
class FraudScreeningRequested(BaseEvent):
    """Fraud screening is requested for an application."""
    event_type: str = "FraudScreeningRequested"
    application_id: str = ""
    assigned_agent_id: str = ""
    requested_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ComplianceCheckRequested(BaseEvent):
    """Compliance check is requested for an application."""
    event_type: str = "ComplianceCheckRequested"
    application_id: str = ""
    regulation_set_version: str = ""
    checks_required: list[str] = field(default_factory=list)


@dataclass
class DecisionRequested(BaseEvent):
    """A decision is requested from the orchestrator."""
    event_type: str = "DecisionRequested"
    application_id: str = ""
    requested_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class DecisionGenerated(BaseEvent):
    """The orchestrator generates a decision recommendation."""
    event_type: str = "DecisionGenerated"
    event_version: int = 2
    application_id: str = ""
    orchestrator_agent_id: str = ""
    recommendation: str = ""  # APPROVE, DECLINE, REFER
    confidence_score: float = 0.0
    contributing_agent_sessions: list[str] = field(default_factory=list)
    decision_basis_summary: str = ""
    model_versions: dict[str, str] = field(default_factory=dict)


@dataclass
class HumanReviewRequested(BaseEvent):
    """An application is referred for human review."""
    event_type: str = "HumanReviewRequested"
    application_id: str = ""
    reason: str = ""
    requested_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class HumanReviewCompleted(BaseEvent):
    """A human reviewer completes their review."""
    event_type: str = "HumanReviewCompleted"
    application_id: str = ""
    reviewer_id: str = ""
    override: bool = False
    final_decision: str = ""  # APPROVE, DECLINE
    override_reason: str = ""


@dataclass
class ApplicationApproved(BaseEvent):
    """An application is approved."""
    event_type: str = "ApplicationApproved"
    application_id: str = ""
    approved_amount_usd: float = 0.0
    interest_rate: float = 0.0
    conditions: list[str] = field(default_factory=list)
    approved_by: str = ""
    effective_date: datetime = None


@dataclass
class ApplicationDeclined(BaseEvent):
    """An application is declined."""
    event_type: str = "ApplicationDeclined"
    application_id: str = ""
    decline_reasons: list[str] = field(default_factory=list)
    declined_by: str = ""
    adverse_action_notice_required: bool = False


# =============================================================================
# Agent Session Events
# =============================================================================

@dataclass
class AgentSessionStarted(BaseEvent):
    """An agent session begins (Gas Town pattern - first event)."""
    event_type: str = "AgentSessionStarted"
    session_id: str = ""
    agent_type: str = ""
    model_version: str = ""
    context_source: str = "fresh"  # or "prior_session_replay:{id}"
    context_token_count: int = 0


@dataclass
class AgentInputValidated(BaseEvent):
    """Agent validated its input parameters."""
    event_type: str = "AgentInputValidated"
    session_id: str = ""
    agent_type: str = ""
    inputs_validated: list[str] = field(default_factory=list)
    validation_duration_ms: int = 0


@dataclass
class AgentInputValidationFailed(BaseEvent):
    """Agent input validation failed."""
    event_type: str = "AgentInputValidationFailed"
    session_id: str = ""
    agent_type: str = ""
    missing_inputs: list[str] = field(default_factory=list)
    validation_errors: list[str] = field(default_factory=list)


@dataclass
class AgentNodeExecuted(BaseEvent):
    """A LangGraph node completed execution."""
    event_type: str = "AgentNodeExecuted"
    session_id: str = ""
    agent_type: str = ""
    node_name: str = ""
    node_sequence: int = 0
    input_keys: list[str] = field(default_factory=list)
    output_keys: list[str] = field(default_factory=list)
    llm_called: bool = False
    llm_tokens_input: int = 0
    llm_tokens_output: int = 0
    llm_cost_usd: float = 0.0
    duration_ms: int = 0


@dataclass
class AgentToolCalled(BaseEvent):
    """An agent called a tool (registry query, event store query)."""
    event_type: str = "AgentToolCalled"
    session_id: str = ""
    agent_type: str = ""
    tool_name: str = ""
    tool_input_summary: str = ""
    tool_output_summary: str = ""
    tool_duration_ms: int = 0


@dataclass
class AgentOutputWritten(BaseEvent):
    """Agent wrote output events to the store."""
    event_type: str = "AgentOutputWritten"
    session_id: str = ""
    agent_type: str = ""
    events_written: list[dict] = field(default_factory=list)
    output_summary: str = ""


@dataclass
class AgentSessionCompleted(BaseEvent):
    """An agent session completed successfully."""
    event_type: str = "AgentSessionCompleted"
    session_id: str = ""
    agent_type: str = ""
    total_nodes_executed: int = 0
    total_llm_calls: int = 0
    total_tokens_used: int = 0
    total_cost_usd: float = 0.0
    next_agent_triggered: str = ""


@dataclass
class AgentSessionFailed(BaseEvent):
    """An agent session failed."""
    event_type: str = "AgentSessionFailed"
    session_id: str = ""
    agent_type: str = ""
    error_type: str = ""
    error_message: str = ""
    last_successful_node: str = ""
    recoverable: bool = False


@dataclass
class AgentSessionRecovered(BaseEvent):
    """A session recovered from a previous failure."""
    event_type: str = "AgentSessionRecovered"
    session_id: str = ""
    agent_type: str = ""
    recovered_from_session_id: str = ""
    recovery_point: str = ""


@dataclass
class AgentContextLoaded(BaseEvent):
    """Agent loaded context (Gas Town pattern - for replay)."""
    event_type: str = "AgentContextLoaded"
    agent_id: str = ""
    session_id: str = ""
    context_source: str = ""
    event_replay_from_position: int = 0
    context_token_count: int = 0
    model_version: str = ""


# =============================================================================
# Credit Record Events
# =============================================================================

@dataclass
class CreditRecordOpened(BaseEvent):
    """A credit record is opened for an application."""
    event_type: str = "CreditRecordOpened"
    application_id: str = ""
    agent_session_id: str = ""


@dataclass
class HistoricalProfileConsumed(BaseEvent):
    """Historical financial profile was consumed from registry."""
    event_type: str = "HistoricalProfileConsumed"
    application_id: str = ""
    agent_session_id: str = ""
    company_id: str = ""
    fiscal_years: list[int] = field(default_factory=list)


@dataclass
class ExtractedFactsConsumed(BaseEvent):
    """Extracted financial facts were consumed from document processing."""
    event_type: str = "ExtractedFactsConsumed"
    application_id: str = ""
    agent_session_id: str = ""
    document_package_id: str = ""


@dataclass
class CreditAnalysisCompleted(BaseEvent):
    """Credit analysis is complete."""
    event_type: str = "CreditAnalysisCompleted"
    event_version: int = 2
    application_id: str = ""
    agent_id: str = ""
    session_id: str = ""
    model_version: str = ""
    confidence_score: float = 0.0
    risk_tier: str = ""  # LOW, MEDIUM, HIGH
    recommended_limit_usd: float = 0.0
    analysis_duration_ms: int = 0
    input_data_hash: str = ""


@dataclass
class CreditAnalysisDeferred(BaseEvent):
    """Credit analysis was deferred."""
    event_type: str = "CreditAnalysisDeferred"
    application_id: str = ""
    agent_session_id: str = ""
    reason: str = ""


# =============================================================================
# Fraud Screening Events
# =============================================================================

@dataclass
class FraudScreeningInitiated(BaseEvent):
    """Fraud screening is initiated."""
    event_type: str = "FraudScreeningInitiated"
    application_id: str = ""
    agent_session_id: str = ""


@dataclass
class FraudAnomalyDetected(BaseEvent):
    """A fraud anomaly is detected."""
    event_type: str = "FraudAnomalyDetected"
    application_id: str = ""
    agent_session_id: str = ""
    anomaly_type: str = ""
    description: str = ""
    severity: float = 0.0  # 0.0 - 1.0
    evidence: str = ""


@dataclass
class FraudScreeningCompleted(BaseEvent):
    """Fraud screening is complete."""
    event_type: str = "FraudScreeningCompleted"
    application_id: str = ""
    agent_id: str = ""
    session_id: str = ""
    fraud_score: float = 0.0  # 0.0 - 1.0
    anomaly_flags: list[str] = field(default_factory=list)
    screening_model_version: str = ""
    input_data_hash: str = ""


# =============================================================================
# Compliance Events
# =============================================================================

@dataclass
class ComplianceCheckInitiated(BaseEvent):
    """Compliance check is initiated."""
    event_type: str = "ComplianceCheckInitiated"
    application_id: str = ""
    agent_session_id: str = ""
    regulation_set_version: str = ""


@dataclass
class ComplianceRulePassed(BaseEvent):
    """A compliance rule passed."""
    event_type: str = "ComplianceRulePassed"
    application_id: str = ""
    rule_id: str = ""
    rule_version: str = ""
    evaluation_timestamp: datetime = field(default_factory=datetime.utcnow)
    evidence_hash: str = ""


@dataclass
class ComplianceRuleFailed(BaseEvent):
    """A compliance rule failed."""
    event_type: str = "ComplianceRuleFailed"
    application_id: str = ""
    rule_id: str = ""
    rule_version: str = ""
    failure_reason: str = ""
    remediation_required: str = ""


@dataclass
class ComplianceRuleNoted(BaseEvent):
    """A compliance rule was noted (informational)."""
    event_type: str = "ComplianceRuleNoted"
    application_id: str = ""
    rule_id: str = ""
    note_type: str = ""
    note: str = ""


@dataclass
class ComplianceCheckCompleted(BaseEvent):
    """Compliance check is complete."""
    event_type: str = "ComplianceCheckCompleted"
    application_id: str = ""
    agent_session_id: str = ""
    overall_verdict: str = ""  # CLEAR, BLOCKED, CONDITIONAL
    has_hard_block: bool = False
    rules_evaluated: int = 0


# =============================================================================
# Document Package Events
# =============================================================================

@dataclass
class PackageCreated(BaseEvent):
    """A document package is created for an application."""
    event_type: str = "PackageCreated"
    package_id: str = ""
    application_id: str = ""


@dataclass
class DocumentAdded(BaseEvent):
    """A document is added to the package."""
    event_type: str = "DocumentAdded"
    package_id: str = ""
    document_id: str = ""
    document_type: str = ""  # INCOME_STATEMENT, BALANCE_SHEET, etc.
    file_path: str = ""


@dataclass
class DocumentFormatValidated(BaseEvent):
    """Document format was validated."""
    event_type: str = "DocumentFormatValidated"
    package_id: str = ""
    document_id: str = ""
    is_valid: bool = False
    format_notes: str = ""


@dataclass
class ExtractionStarted(BaseEvent):
    """Document extraction started."""
    event_type: str = "ExtractionStarted"
    package_id: str = ""
    document_id: str = ""
    extraction_method: str = ""


@dataclass
class ExtractionCompleted(BaseEvent):
    """Document extraction completed."""
    event_type: str = "ExtractionCompleted"
    package_id: str = ""
    document_id: str = ""
    financial_facts: dict = field(default_factory=dict)
    extraction_confidence: float = 0.0


@dataclass
class QualityAssessmentCompleted(BaseEvent):
    """Document quality assessment completed."""
    event_type: str = "QualityAssessmentCompleted"
    package_id: str = ""
    overall_confidence: float = 0.0
    is_coherent: bool = False
    anomalies: list[str] = field(default_factory=list)
    critical_missing_fields: list[str] = field(default_factory=list)


@dataclass
class PackageReadyForAnalysis(BaseEvent):
    """Document package is ready for agent analysis."""
    event_type: str = "PackageReadyForAnalysis"
    package_id: str = ""
    application_id: str = ""


# =============================================================================
# Audit Ledger Events
# =============================================================================

@dataclass
class AuditIntegrityCheckRun(BaseEvent):
    """An audit integrity check was run."""
    event_type: str = "AuditIntegrityCheckRun"
    entity_id: str = ""
    entity_type: str = ""
    check_timestamp: datetime = field(default_factory=datetime.utcnow)
    events_verified_count: int = 0
    integrity_hash: str = ""
    previous_hash: str = ""


# Event type registry for validation
EVENT_REGISTRY: dict[str, type[BaseEvent]] = {
    # Loan Application
    "ApplicationSubmitted": ApplicationSubmitted,
    "CreditAnalysisRequested": CreditAnalysisRequested,
    "FraudScreeningRequested": FraudScreeningRequested,
    "ComplianceCheckRequested": ComplianceCheckRequested,
    "DecisionRequested": DecisionRequested,
    "DecisionGenerated": DecisionGenerated,
    "HumanReviewRequested": HumanReviewRequested,
    "HumanReviewCompleted": HumanReviewCompleted,
    "ApplicationApproved": ApplicationApproved,
    "ApplicationDeclined": ApplicationDeclined,
    
    # Agent Session
    "AgentSessionStarted": AgentSessionStarted,
    "AgentInputValidated": AgentInputValidated,
    "AgentInputValidationFailed": AgentInputValidationFailed,
    "AgentNodeExecuted": AgentNodeExecuted,
    "AgentToolCalled": AgentToolCalled,
    "AgentOutputWritten": AgentOutputWritten,
    "AgentSessionCompleted": AgentSessionCompleted,
    "AgentSessionFailed": AgentSessionFailed,
    "AgentSessionRecovered": AgentSessionRecovered,
    "AgentContextLoaded": AgentContextLoaded,
    
    # Credit Record
    "CreditRecordOpened": CreditRecordOpened,
    "HistoricalProfileConsumed": HistoricalProfileConsumed,
    "ExtractedFactsConsumed": ExtractedFactsConsumed,
    "CreditAnalysisCompleted": CreditAnalysisCompleted,
    "CreditAnalysisDeferred": CreditAnalysisDeferred,
    
    # Fraud Screening
    "FraudScreeningInitiated": FraudScreeningInitiated,
    "FraudAnomalyDetected": FraudAnomalyDetected,
    "FraudScreeningCompleted": FraudScreeningCompleted,
    
    # Compliance
    "ComplianceCheckInitiated": ComplianceCheckInitiated,
    "ComplianceRulePassed": ComplianceRulePassed,
    "ComplianceRuleFailed": ComplianceRuleFailed,
    "ComplianceRuleNoted": ComplianceRuleNoted,
    "ComplianceCheckCompleted": ComplianceCheckCompleted,
    
    # Document Package
    "PackageCreated": PackageCreated,
    "DocumentAdded": DocumentAdded,
    "DocumentFormatValidated": DocumentFormatValidated,
    "ExtractionStarted": ExtractionStarted,
    "ExtractionCompleted": ExtractionCompleted,
    "QualityAssessmentCompleted": QualityAssessmentCompleted,
    "PackageReadyForAnalysis": PackageReadyForAnalysis,
    
    # Audit
    "AuditIntegrityCheckRun": AuditIntegrityCheckRun,
}
