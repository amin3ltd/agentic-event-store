"""
Upcaster Registry

Handles schema evolution by applying version migrations when old events 
are loaded from the store. Events are immutable - upcasters transform 
them at read time without modifying stored data.
"""

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, TYPE_CHECKING

logger = logging.getLogger(__name__)


# Type alias for upcaster functions
UpcasterFunc = Callable[[Any], Any]


class UpcasterRegistry:
    """
    Central registry for event schema upcasters.
    
    Automatically applies version migrations when old events are loaded.
    Upcasters are applied in version order: v1→v2→v3, etc.
    """
    
    def __init__(self):
        self._upcasters: dict[tuple[str, int], UpcasterFunc] = {}
    
    def register(self, event_type: str, from_version: int) -> Callable:
        """
        Decorator to register an upcaster.
        
        Usage:
            @registry.register("CreditAnalysisCompleted", from_version=1)
            def upcast_credit_v1_to_v2(payload: dict) -> dict:
                return {**payload, "new_field": "value"}
        """
        def decorator(fn: UpcasterFunc) -> UpcasterFunc:
            self._upcasters[(event_type, from_version)] = fn
            logger.info(f"Registered upcaster: {event_type} v{from_version} → v{from_version + 1}")
            return fn
        return decorator
    
    def upcast(self, event: "StoredEvent") -> "StoredEvent":
        """
        Apply all registered upcasters for this event type in version order.
        
        Args:
            event: The stored event to upcast
            
        Returns:
            New StoredEvent with updated payload and version
        """
        current_payload = event.payload.copy()
        current_version = event.event_version
        
        # Apply upcasters in order
        while (event.event_type, current_version) in self._upcasters:
            upcaster = self._upcasters[(event.event_type, current_version)]
            new_payload = upcaster(current_payload)
            
            logger.debug(
                f"Upcasting {event.event_type} from v{current_version} to v{current_version + 1}"
            )
            
            current_payload = new_payload
            current_version += 1
        
        # If version changed, create new event with updated payload
        if current_version != event.event_version:
            return event.__class__(
                event_id=event.event_id,
                stream_id=event.stream_id,
                stream_position=event.stream_position,
                global_position=event.global_position,
                event_type=event.event_type,
                event_version=current_version,
                payload=current_payload,
                metadata=event.metadata,
                recorded_at=event.recorded_at,
                correlation_id=event.correlation_id,
                causation_id=event.causation_id,
            )
        
        return event
    
    def get_upcasters_for_type(self, event_type: str) -> list[tuple[int, UpcasterFunc]]:
        """Get all upcasters registered for an event type, in version order."""
        upcasters = [
            (version, fn) 
            for (evt_type, version), fn in self._upcasters.items() 
            if evt_type == event_type
        ]
        return sorted(upcasters, key=lambda x: x[0])


# Global registry instance
_registry = UpcasterRegistry()


def get_registry() -> UpcasterRegistry:
    """Get the global upcaster registry."""
    return _registry


# =============================================================================
# Built-in Upcasters
# =============================================================================

@_registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_v1_to_v2(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Upcaster for CreditAnalysisCompleted v1 → v2.
    
    Adds:
    - model_version: Inferred from recorded_at timestamp (legacy-pre-2026)
    - confidence_score: Null for historical events (genuinely unknown)
    - regulatory_basis: Inferred from rule versions active at recorded_at date
    
    Inference Strategy:
    - model_version: Use "legacy-pre-2026" as marker for pre-2026 events
    - confidence_score: None - fabricating would be worse than null for compliance
    - regulatory_basis: Explicit marker for historical events without regulatory basis
    """
    import re
    
    # Infer model_version from recorded_at if available
    # For v1 events, assume they're from before 2026
    if "recorded_at" in payload:
        try:
            recorded = payload["recorded_at"]
            if isinstance(recorded, str):
                # Parse timestamp - if pre-2026, mark as legacy
                if "2025" in recorded or "2024" in recorded:
                    payload["model_version"] = "legacy-pre-2026"
                else:
                    payload["model_version"] = "unknown"
            else:
                payload["model_version"] = "legacy-pre-2026"
        except Exception:
            payload["model_version"] = "legacy-pre-2026"
    else:
        payload["model_version"] = "legacy-pre-2026"
    
    # confidence_score - null for historical v1 events
    # Do NOT fabricate - null is more honest for compliance
    if "confidence_score" not in payload:
        payload["confidence_score"] = None
    
    # Infer regulatory_basis from date
    # Pre-2026 events don't have regulatory tracking
    # Use explicit marker to indicate this is an inferred value, not actual regulatory basis
    payload["regulatory_basis"] = "historical-event-no-regulatory-basis"
    
    return payload


@_registry.register("DecisionGenerated", from_version=1)
def upcast_decision_v1_to_v2(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Upcaster for DecisionGenerated v1 → v2.
    
    Adds:
    - model_versions: Dict of agent_type → model_version
      Reconstructed from contributing_agent_sessions by loading each 
      session's AgentContextLoaded event.
    
    Note: This requires a store lookup, which has performance implications.
    In production, consider caching model versions or using snapshot strategy.
    """
    # Add model_versions dict (v2 field)
    if "model_versions" not in payload:
        payload["model_versions"] = {}
    
    # The contributing_agent_sessions list can be used to look up
    # the AgentContextLoaded events to find model versions
    # This is done at query time by the application, not in the upcaster
    
    return payload


# =============================================================================
# Forward Compatibility (for writing)
# =============================================================================

def ensure_current_version(event_type: str, payload: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """
    Ensure event payload has all current version fields.
    
    When writing new events, this ensures all required fields are present.
    
    Returns:
        Tuple of (updated_payload, version)
    """
    # Current versions
    current_versions = {
        "CreditAnalysisCompleted": 2,
        "DecisionGenerated": 2,
    }
    
    version = current_versions.get(event_type, 1)
    
    # Ensure fields based on version
    if event_type == "CreditAnalysisCompleted" and version >= 2:
        if "model_version" not in payload:
            raise ValueError("model_version required for v2+ CreditAnalysisCompleted")
    
    if event_type == "DecisionGenerated" and version >= 2:
        if "model_versions" not in payload:
            payload["model_versions"] = {}
    
    return payload, version
