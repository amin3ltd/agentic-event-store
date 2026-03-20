"""
Cryptographic Audit Chain

Implements hash chain construction for tamper detection.
Each event's hash includes the previous event's hash, creating an immutable chain.
"""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Union


@dataclass
class AuditChain:
    """
    Maintains cryptographic integrity chain for audit trail.
    
    Each event's integrity hash includes:
    - The event's payload (JSON canonicalized)
    - The previous event's hash
    - Timestamp
    
    This creates an immutable chain - any tampering with historical events
    will break the chain and be detectable.
    """
    
    @staticmethod
    def compute_event_hash(
        payload: dict[str, Any],
        previous_hash: str,
        timestamp: Union[datetime, str, None] = None
    ) -> str:
        """
        Compute SHA-256 hash for an event.
        
        Args:
            payload: Event payload
            previous_hash: Hash of previous event in chain
            timestamp: Optional timestamp. Can be datetime, ISO string, or None.
                       If None, uses a fixed epoch timestamp for deterministic hashing.
            
        Returns:
            Hex-encoded SHA-256 hash
        """
        if timestamp is None:
            # For audit chain integrity, timestamp must be explicitly provided
            # to ensure deterministic hash computation. Use datetime.now(timezone.utc)
            # for current time, or provide a specific timestamp for deterministic testing.
            raise ValueError(
                "timestamp cannot be None for audit chain hashing. "
                "Provide an explicit timestamp (datetime.now(timezone.utc) for current time "
                "or a specific datetime for deterministic hashing)."
            )
        elif isinstance(timestamp, str):
            # Parse ISO format string
            timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        
        # Canonicalize payload (deterministic JSON)
        canonical_payload = json.dumps(payload, sort_keys=True, default=str)
        
        # Create hash input
        hash_input = f"{canonical_payload}|{previous_hash}|{timestamp.isoformat()}"
        
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
    @staticmethod
    def verify_event_chain(events: list[dict[str, Any]]) -> tuple[bool, list[str]]:
        """
        Verify integrity of an event chain.
        
        Args:
            events: List of events with payload and metadata
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        previous_hash = "genesis"  # Initial hash
        
        for i, event in enumerate(events):
            # Get stored hash from metadata
            stored_hash = event.get("metadata", {}).get("integrity_hash", "")
            event_timestamp = event.get("recorded_at")
            
            if event_timestamp is None:
                errors.append(
                    f"Event {i} (position {event.get('stream_position')}): "
                    f"missing recorded_at timestamp for hash verification"
                )
                continue
            
            # Compute expected hash
            payload = event.get("payload", {})
            expected_hash = AuditChain.compute_event_hash(
                payload, 
                previous_hash,
                event_timestamp
            )
            
            # Verify
            if stored_hash != expected_hash:
                errors.append(
                    f"Event {i} (position {event.get('stream_position')}): "
                    f"hash mismatch. Expected {expected_hash[:16]}..., "
                    f"got {stored_hash[:16]}..."
                )
            
            # Move to next
            previous_hash = stored_hash if stored_hash else expected_hash
        
        return len(errors) == 0, errors
    
    @staticmethod
    def verify_stream_integrity(
        stream_events: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """
        Verify integrity of a stream and return verification report.
        
        Returns:
            Dictionary with verification results
        """
        is_valid, errors = AuditChain.verify_event_chain(stream_events)
        
        return {
            "is_valid": is_valid,
            "events_checked": len(stream_events),
            "first_event_hash": stream_events[0].get("metadata", {}).get("integrity_hash", "") if stream_events else None,
            "last_event_hash": stream_events[-1].get("metadata", {}).get("integrity_hash", "") if stream_events else None,
            "errors": errors,
            "verified_at": datetime.now(timezone.utc).isoformat()
        }


class RegulatoryPackage:
    """
    Generates compliance packages for regulatory examination.
    
    Produces a complete, verifiable package that includes:
    - All events in chronological order
    - Integrity verification
    - Chain of custody documentation
    """
    
    @staticmethod
    def generate_package(
        events: list[dict[str, Any]],
        application_id: str,
        regulator_name: str
    ) -> dict[str, Any]:
        """
        Generate a regulatory compliance package.
        
        Args:
            events: All events for the application
            application_id: The application ID
            regulator_name: Name of requesting regulator
            
        Returns:
            Complete regulatory package
        """
        # Verify chain
        verification = AuditChain.verify_stream_integrity(events)
        
        package = {
            "package_id": f"REG-{application_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
            "application_id": application_id,
            "regulator": regulator_name,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "events_count": len(events),
            "integrity_verification": verification,
            "events": events,
            "chain_custody": {
                "generated_by": "The Ledger",
                "generation_method": "SHA-256 hash chain",
                "genesis_hash": "genesis" if not events else events[0].get("metadata", {}).get("integrity_hash", ""),
            }
        }
        
        return package
    
    @staticmethod
    def generate_temporal_package(
        events: list[dict[str, Any]],
        application_id: str,
        as_of_timestamp: datetime,
        regulator_name: str
    ) -> dict[str, Any]:
        """
        Generate a regulatory package as of a specific point in time.
        
        This simulates what the system looked like at a historical moment,
        useful for regulatory examinations of past decisions.
        """
        # Normalize as_of_timestamp to datetime
        if isinstance(as_of_timestamp, str):
            as_of_timestamp = datetime.fromisoformat(as_of_timestamp.replace('Z', '+00:00'))
        
        # Filter events to only those that existed at the timestamp
        def get_event_time(event):
            recorded = event.get("recorded_at")
            if recorded is None:
                return datetime.min
            if isinstance(recorded, str):
                return datetime.fromisoformat(recorded.replace('Z', '+00:00'))
            return recorded
        
        historical_events = [
            e for e in events 
            if get_event_time(e) <= as_of_timestamp
        ]
        
        package = RegulatoryPackage.generate_package(
            historical_events, 
            application_id, 
            regulator_name
        )
        
        package["as_of_timestamp"] = as_of_timestamp.isoformat()
        package["note"] = (
            f"This package represents the state of application {application_id} "
            f"as of {as_of_timestamp.isoformat()}. "
            f"Events after this timestamp are excluded."
        )
        
        return package
