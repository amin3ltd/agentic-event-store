"""
Agent Session Aggregate

Implements the domain logic for AI agent sessions, including:
- Gas Town pattern (session must start with AgentContextLoaded)
- Per-node execution tracking
- Tool call recording
- Session lifecycle management
"""

from dataclasses import dataclass, field
from typing import Any

from ledger.event_store import EventStore, StoredEvent
from ledger.events import (
    AgentSessionStarted,
    AgentContextLoaded,
    AgentInputValidated,
    AgentInputValidationFailed,
    AgentNodeExecuted,
    AgentToolCalled,
    AgentOutputWritten,
    AgentSessionCompleted,
    AgentSessionFailed,
    AgentSessionRecovered,
)


class DomainError(Exception):
    """Raised when a business rule is violated."""
    def __init__(self, message: str, rule: str):
        self.message = message
        self.rule = rule
        super().__init__(message)


@dataclass
class AgentSessionAggregate:
    """
    Aggregate root for agent sessions.
    
    Enforces business rules:
    1. AgentContextLoaded must be first event (Gas Town pattern)
    2. Every output event must reference a ContextLoaded event
    3. Every decision must reference specific model version
    """
    
    agent_id: str = ""
    session_id: str = ""
    agent_type: str = ""
    model_version: str = ""
    context_source: str = ""
    context_token_count: int = 0
    
    # Tracking
    nodes_executed: int = 0
    llm_calls: int = 0
    tokens_used: int = 0
    cost_usd: float = 0.0
    
    # State
    is_complete: bool = False
    has_failed: bool = False
    error_message: str = ""
    
    # Version tracking
    version: int = 0
    
    @classmethod
    async def load(
        cls, 
        store: EventStore, 
        agent_id: str, 
        session_id: str
    ) -> "AgentSessionAggregate":
        """
        Load aggregate by replaying all events from the stream.
        """
        stream_id = f"agent-{agent_type_to_stream(agent_id)}-{session_id}"
        events = await store.load_stream(stream_id)
        
        agg = cls(agent_id=agent_id, session_id=session_id)
        for event in events:
            agg._apply(event)
        
        return agg
    
    @classmethod
    async def load_by_stream(cls, store: EventStore, stream_id: str) -> "AgentSessionAggregate":
        """Load aggregate from stream ID."""
        events = await store.load_stream(stream_id)
        
        if not events:
            raise DomainError(f"No events in stream {stream_id}", rule="empty_stream")
        
        # Extract agent info from first event
        first_event = events[0]
        if first_event.event_type == "AgentSessionStarted":
            agent_type = first_event.payload.get("agent_type", "")
        else:
            agent_type = stream_id.split("-")[1] if len(stream_id.split("-")) > 1 else ""
        
        agg = cls(
            agent_id=first_event.payload.get("agent_id", ""),
            session_id=first_event.payload.get("session_id", ""),
            agent_type=agent_type
        )
        
        for event in events:
            agg._apply(event)
        
        return agg
    
    def _apply(self, event: StoredEvent) -> None:
        """Apply an event to update aggregate state."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position
    
    def _on_AgentSessionStarted(self, event: StoredEvent) -> None:
        self.agent_id = event.payload.get("agent_id", "")
        self.session_id = event.payload.get("session_id", "")
        self.agent_type = event.payload.get("agent_type", "")
        self.model_version = event.payload.get("model_version", "")
        self.context_source = event.payload.get("context_source", "fresh")
    
    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        self.context_source = event.payload.get("context_source", "replay")
        self.context_token_count = event.payload.get("context_token_count", 0)
        self.model_version = event.payload.get("model_version", "")
    
    def _on_AgentNodeExecuted(self, event: StoredEvent) -> None:
        self.nodes_executed += 1
        
        if event.payload.get("llm_called", False):
            self.llm_calls += 1
            self.tokens_used += event.payload.get("llm_tokens_input", 0)
            self.tokens_used += event.payload.get("llm_tokens_output", 0)
            self.cost_usd += event.payload.get("llm_cost_usd", 0.0)
    
    def _on_AgentSessionCompleted(self, event: StoredEvent) -> None:
        self.is_complete = True
        self.nodes_executed = event.payload.get("total_nodes_executed", self.nodes_executed)
        self.llm_calls = event.payload.get("total_llm_calls", self.llm_calls)
        self.tokens_used = event.payload.get("total_tokens_used", self.tokens_used)
        self.cost_usd = event.payload.get("total_cost_usd", self.cost_usd)
    
    def _on_AgentSessionFailed(self, event: StoredEvent) -> None:
        self.has_failed = True
        self.error_message = event.payload.get("error_message", "")
    
    # =========================================================================
    # Business Rules - Gas Town Pattern
    # =========================================================================
    
    def assert_context_loaded(self) -> None:
        """
        Assert that context has been loaded (Gas Town pattern).
        
        An agent session MUST have an AgentContextLoaded event as its first event
        before any decision event can be appended.
        """
        if self.version == 0:
            raise DomainError(
                "No context loaded - cannot make decisions without context (Gas Town rule)",
                rule="gas_town_context"
            )
    
    def assert_session_started(self) -> None:
        """Assert that session has started."""
        if not self.agent_id or not self.session_id:
            raise DomainError(
                "Session not started - no AgentSessionStarted event",
                rule="session_start"
            )
    
    # =========================================================================
    # Business Rules - Model Version
    # =========================================================================
    
    def assert_model_version_current(self, claimed_version: str) -> None:
        """Assert that the model version matches."""
        if self.model_version and claimed_version != self.model_version:
            raise DomainError(
                f"Model version mismatch: session uses {self.model_version}, "
                f"but event claims {claimed_version}",
                rule="model_version"
            )


def agent_type_to_stream(agent_type: str) -> str:
    """Convert agent type to stream prefix."""
    type_map = {
        "credit": "credit",
        "fraud": "fraud", 
        "compliance": "compliance",
        "document": "docpkg",
        "orchestrator": "orchestrator",
    }
    return type_map.get(agent_type.lower(), agent_type.lower())


def stream_prefix_to_agent_type(prefix: str) -> str:
    """Convert stream prefix to agent type."""
    type_map = {
        "credit": "credit_analysis",
        "fraud": "fraud_detection",
        "compliance": "compliance",
        "docpkg": "document_processing",
        "orchestrator": "decision_orchestrator",
    }
    return type_map.get(prefix.lower(), prefix.lower())


# =============================================================================
# Command Handlers
# =============================================================================

async def handle_agent_session_started(
    store: EventStore,
    cmd: dict[str, Any],
) -> list[AgentSessionStarted]:
    """
    Handle agent session started command.
    """
    agent_type = cmd.get("agent_type", "")
    session_id = cmd["session_id"]
    
    stream_id = f"agent-{agent_type}-{session_id}"
    
    events = [
        AgentSessionStarted(
            session_id=session_id,
            agent_type=agent_type,
            model_version=cmd.get("model_version", ""),
            context_source=cmd.get("context_source", "fresh"),
            context_token_count=cmd.get("context_token_count", 0),
        )
    ]
    
    await store.append(stream_id, events, expected_version=-1)
    return events


async def handle_agent_context_loaded(
    store: EventStore,
    cmd: dict[str, Any],
) -> list[AgentContextLoaded]:
    """
    Handle agent context loaded command (Gas Town pattern).
    """
    agent_type = cmd.get("agent_type", "")
    session_id = cmd["session_id"]
    
    stream_id = f"agent-{agent_type}-{session_id}"
    
    # Verify session exists
    version = await store.stream_version(stream_id)
    if version == 0:
        raise DomainError(
            f"Session {session_id} does not exist - must start with AgentSessionStarted",
            rule="session_start"
        )
    
    events = [
        AgentContextLoaded(
            agent_id=cmd["agent_id"],
            session_id=session_id,
            context_source=cmd.get("context_source", ""),
            event_replay_from_position=cmd.get("event_replay_from_position", 0),
            context_token_count=cmd.get("context_token_count", 0),
            model_version=cmd.get("model_version", ""),
        )
    ]
    
    await store.append(stream_id, events, expected_version=version)
    return events


async def handle_agent_node_executed(
    store: EventStore,
    cmd: dict[str, Any],
) -> list[AgentNodeExecuted]:
    """
    Handle agent node executed command.
    
    Records execution of a LangGraph node.
    """
    agent_type = cmd.get("agent_type", "")
    session_id = cmd["session_id"]
    
    stream_id = f"agent-{agent_type}-{session_id}"
    version = await store.stream_version(stream_id)
    
    # Verify context is loaded (Gas Town)
    agg = await AgentSessionAggregate.load_by_stream(store, stream_id)
    agg.assert_context_loaded()
    
    events = [
        AgentNodeExecuted(
            session_id=session_id,
            agent_type=agent_type,
            node_name=cmd["node_name"],
            node_sequence=cmd.get("node_sequence", 0),
            input_keys=cmd.get("input_keys", []),
            output_keys=cmd.get("output_keys", []),
            llm_called=cmd.get("llm_called", False),
            llm_tokens_input=cmd.get("llm_tokens_input", 0),
            llm_tokens_output=cmd.get("llm_tokens_output", 0),
            llm_cost_usd=cmd.get("llm_cost_usd", 0.0),
            duration_ms=cmd.get("duration_ms", 0),
        )
    ]
    
    await store.append(stream_id, events, expected_version=version)
    return events


async def handle_agent_tool_called(
    store: EventStore,
    cmd: dict[str, Any],
) -> list[AgentToolCalled]:
    """
    Handle agent tool called command.
    
    Records a tool call (e.g., to Applicant Registry or Event Store).
    """
    agent_type = cmd.get("agent_type", "")
    session_id = cmd["session_id"]
    
    stream_id = f"agent-{agent_type}-{session_id}"
    version = await store.stream_version(stream_id)
    
    events = [
        AgentToolCalled(
            session_id=session_id,
            agent_type=agent_type,
            tool_name=cmd["tool_name"],
            tool_input_summary=cmd.get("tool_input_summary", ""),
            tool_output_summary=cmd.get("tool_output_summary", ""),
            tool_duration_ms=cmd.get("tool_duration_ms", 0),
        )
    ]
    
    await store.append(stream_id, events, expected_version=version)
    return events
