"""
Test Suite for Integrity and Audit Chain

Tests cryptographic hash chain construction and verification.
"""

import pytest
from datetime import datetime, timezone
import hashlib
import json

from ledger.integrity import AuditChain, RegulatoryPackage


class TestAuditChain:
    """Tests for AuditChain hash chain functionality."""
    
    def test_compute_event_hash_basic(self):
        """Test basic hash computation."""
        payload = {
            "application_id": "APP-001",
            "applicant_id": "company-123",
            "requested_amount_usd": 100000.0,
        }
        previous_hash = "genesis"
        
        result = AuditChain.compute_event_hash(payload, previous_hash)
        
        # Verify it's a valid SHA-256 hash (64 hex characters)
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)
    
    def test_compute_event_hash_deterministic(self):
        """Test that the same inputs produce the same hash."""
        payload = {"key": "value", "number": 42}
        previous_hash = "test-hash"
        
        result1 = AuditChain.compute_event_hash(payload, previous_hash)
        result2 = AuditChain.compute_event_hash(payload, previous_hash)
        
        assert result1 == result2
    
    def test_compute_event_hash_different_payloads(self):
        """Test that different payloads produce different hashes."""
        payload1 = {"application_id": "APP-001"}
        payload2 = {"application_id": "APP-002"}
        previous_hash = "genesis"
        
        result1 = AuditChain.compute_event_hash(payload1, previous_hash)
        result2 = AuditChain.compute_event_hash(payload2, previous_hash)
        
        assert result1 != result2
    
    def test_compute_event_hash_different_previous_hashes(self):
        """Test that different previous hashes produce different results."""
        payload = {"application_id": "APP-001"}
        previous1 = "hash-1"
        previous2 = "hash-2"
        
        result1 = AuditChain.compute_event_hash(payload, previous1)
        result2 = AuditChain.compute_event_hash(payload, previous2)
        
        assert result1 != result2
    
    def test_verify_event_chain_valid(self):
        """Test verification of a valid event chain."""
        events = []
        previous_hash = "genesis"
        
        # Create a valid chain of 3 events
        for i in range(3):
            payload = {"event_number": i, "data": f"event-{i}"}
            event_hash = AuditChain.compute_event_hash(payload, previous_hash)
            
            events.append({
                "payload": payload,
                "metadata": {"integrity_hash": event_hash},
                "stream_position": i + 1,
                "recorded_at": datetime.now(timezone.utc).isoformat()
            })
            
            previous_hash = event_hash
        
        is_valid, errors = AuditChain.verify_event_chain(events)
        
        assert is_valid is True
        assert len(errors) == 0
    
    def test_verify_event_chain_tampered(self):
        """Test detection of tampered events."""
        events = []
        previous_hash = "genesis"
        
        # Create a valid chain
        for i in range(3):
            payload = {"event_number": i, "data": f"event-{i}"}
            event_hash = AuditChain.compute_event_hash(payload, previous_hash)
            
            events.append({
                "payload": payload,
                "metadata": {"integrity_hash": event_hash},
                "stream_position": i + 1,
            })
            
            previous_hash = event_hash
        
        # Tamper with the second event
        events[1]["payload"]["data"] = "tampered-data"
        
        is_valid, errors = AuditChain.verify_event_chain(events)
        
        assert is_valid is False
        assert len(errors) == 1
        assert "hash mismatch" in errors[0].lower()
    
    def test_verify_event_chain_broken_link(self):
        """Test detection of broken chain links."""
        events = []
        previous_hash = "genesis"
        
        # Create first two events in chain
        for i in range(2):
            payload = {"event_number": i, "data": f"event-{i}"}
            event_hash = AuditChain.compute_event_hash(payload, previous_hash)
            
            events.append({
                "payload": payload,
                "metadata": {"integrity_hash": event_hash},
                "stream_position": i + 1,
            })
            
            previous_hash = event_hash
        
        # Add third event but with wrong previous hash
        payload = {"event_number": 2, "data": "event-2"}
        event_hash = AuditChain.compute_event_hash(payload, "wrong-previous-hash")
        
        events.append({
            "payload": payload,
            "metadata": {"integrity_hash": event_hash},
            "stream_position": 3,
        })
        
        is_valid, errors = AuditChain.verify_event_chain(events)
        
        assert is_valid is False
        assert len(errors) >= 1
    
    def test_verify_stream_integrity(self):
        """Test stream integrity verification returns complete report."""
        events = []
        previous_hash = "genesis"
        
        for i in range(3):
            payload = {"application_id": f"APP-00{i}"}
            event_hash = AuditChain.compute_event_hash(payload, previous_hash)
            
            events.append({
                "payload": payload,
                "metadata": {"integrity_hash": event_hash},
                "recorded_at": datetime.now(timezone.utc).isoformat()
            })
            
            previous_hash = event_hash
        
        result = AuditChain.verify_stream_integrity(events)
        
        assert result["is_valid"] is True
        assert result["events_checked"] == 3
        assert result["first_event_hash"] is not None
        assert result["last_event_hash"] is not None
        assert result["verified_at"] is not None
        assert len(result["errors"]) == 0
    
    def test_verify_stream_integrity_empty(self):
        """Test verification of empty stream."""
        result = AuditChain.verify_stream_integrity([])
        
        assert result["is_valid"] is True
        assert result["events_checked"] == 0
        assert result["first_event_hash"] is None
        assert result["last_event_hash"] is None


class TestRegulatoryPackage:
    """Tests for regulatory package generation."""
    
    def test_generate_package_basic(self):
        """Test basic package generation."""
        events = []
        previous_hash = "genesis"
        
        for i in range(2):
            payload = {"application_id": "APP-001", "step": i}
            event_hash = AuditChain.compute_event_hash(payload, previous_hash)
            
            events.append({
                "payload": payload,
                "metadata": {"integrity_hash": event_hash},
                "recorded_at": datetime.now(timezone.utc).isoformat()
            })
            
            previous_hash = event_hash
        
        package = RegulatoryPackage.generate_package(
            events=events,
            application_id="APP-001",
            regulator_name="FCA"
        )
        
        assert "package_id" in package
        assert package["application_id"] == "APP-001"
        assert package["regulator"] == "FCA"
        assert package["events_count"] == 2
        assert "integrity_verification" in package
        assert "chain_custody" in package
        assert "generated_at" in package
    
    def test_generate_package_includes_events(self):
        """Test that package includes all events."""
        events = [
            {"payload": {"data": "event1"}, "metadata": {}},
            {"payload": {"data": "event2"}, "metadata": {}},
        ]
        
        package = RegulatoryPackage.generate_package(
            events=events,
            application_id="APP-001",
            regulator_name="SEC"
        )
        
        assert package["events"] == events
    
    def test_generate_temporal_package(self):
        """Test temporal package generation."""
        events = []
        previous_hash = "genesis"
        
        base_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        for i in range(3):
            payload = {"application_id": "APP-001", "step": i}
            event_hash = AuditChain.compute_event_hash(payload, previous_hash)
            
            events.append({
                "payload": payload,
                "metadata": {"integrity_hash": event_hash},
                "recorded_at": base_time.isoformat()
            })
            
            previous_hash = event_hash
        
        as_of = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        package = RegulatoryPackage.generate_temporal_package(
            events=events,
            application_id="APP-001",
            as_of_timestamp=as_of,
            regulator_name="FED"
        )
        
        assert "as_of_timestamp" in package
        assert "note" in package
        assert "temporal" in package["note"].lower()
    
    def test_package_id_format(self):
        """Test that package ID follows expected format."""
        events = []
        previous_hash = "genesis"
        
        payload = {"application_id": "APP-001"}
        event_hash = AuditChain.compute_event_hash(payload, previous_hash)
        
        events.append({
            "payload": payload,
            "metadata": {"integrity_hash": event_hash},
            "recorded_at": datetime.now(timezone.utc).isoformat()
        })
        
        package = RegulatoryPackage.generate_package(
            events=events,
            application_id="APP-001",
            regulator_name="TEST"
        )
        
        # Package ID should start with REG-
        assert package["package_id"].startswith("REG-")
        assert "APP-001" in package["package_id"]
