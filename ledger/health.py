"""
Health Check and Monitoring Endpoints

Provides health checks, readiness checks, and metrics for the event store.
Designed for Kubernetes probes and monitoring systems.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import asyncpg

from ledger.config import get_settings


logger = logging.getLogger(__name__)


@dataclass
class HealthStatus:
    """Health check result."""
    status: str  # "healthy", "degraded", "unhealthy"
    checks: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class HealthChecker:
    """Performs health checks on system components."""
    
    def __init__(self, pool: Optional[asyncpg.Pool] = None):
        self._pool = pool
    
    def set_pool(self, pool: asyncpg.Pool) -> None:
        """Set the database connection pool."""
        self._pool = pool
    
    async def check_database(self) -> dict[str, Any]:
        """
        Check database connectivity and basic operations.
        
        Returns:
            Dictionary with check results
        """
        if self._pool is None:
            return {
                "status": "unhealthy",
                "error": "No database connection pool"
            }
        
        try:
            start = time.time()
            async with self._pool.acquire() as conn:
                # Test basic connectivity
                result = await conn.fetchval("SELECT 1")
                
                # Get database info
                db_version = await conn.fetchval("SELECT version()")
                
                # Check event store tables exist
                tables_exist = await conn.fetch("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('events', 'event_streams', 'projection_checkpoints', 'outbox')
                """)
                
                required_tables = {"events", "event_streams", "projection_checkpoints", "outbox"}
                found_tables = {row["table_name"] for row in tables_exist}
                missing_tables = required_tables - found_tables
                
                latency_ms = (time.time() - start) * 1000
                
                if missing_tables:
                    return {
                        "status": "degraded",
                        "latency_ms": round(latency_ms, 2),
                        "error": f"Missing tables: {missing_tables}",
                        "db_version": db_version[:50]
                    }
                
                return {
                    "status": "healthy",
                    "latency_ms": round(latency_ms, 2),
                    "tables_found": len(found_tables),
                    "db_version": db_version[:50]
                }
                
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    async def check_event_store(self) -> dict[str, Any]:
        """Check event store specific metrics."""
        if self._pool is None:
            return {"status": "unhealthy", "error": "No database connection"}
        
        try:
            async with self._pool.acquire() as conn:
                # Get event counts
                event_count = await conn.fetchval("SELECT COUNT(*) FROM events")
                stream_count = await conn.fetchval("SELECT COUNT(*) FROM event_streams")
                
                # Get latest event
                latest = await conn.fetchrow("""
                    SELECT event_type, recorded_at 
                    FROM events 
                    ORDER BY global_position DESC 
                    LIMIT 1
                """)
                
                return {
                    "status": "healthy",
                    "total_events": event_count,
                    "total_streams": stream_count,
                    "latest_event": latest["event_type"] if latest else None,
                    "latest_recorded_at": latest["recorded_at"].isoformat() if latest else None
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    async def check_projections(self) -> dict[str, Any]:
        """Check projection checkpoints."""
        if self._pool is None:
            return {"status": "unhealthy", "error": "No database connection"}
        
        try:
            async with self._pool.acquire() as conn:
                checkpoints = await conn.fetch("""
                    SELECT projection_name, last_position, updated_at 
                    FROM projection_checkpoints
                """)
                
                return {
                    "status": "healthy",
                    "projections": [
                        {
                            "name": row["projection_name"],
                            "position": row["last_position"],
                            "updated_at": row["updated_at"].isoformat()
                        }
                        for row in checkpoints
                    ]
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    async def get_full_health(self) -> HealthStatus:
        """
        Get comprehensive health status.
        
        Returns:
            HealthStatus with all checks
        """
        checks = {}
        overall_status = "healthy"
        
        # Database check
        db_check = await self.check_database()
        checks["database"] = db_check
        if db_check["status"] == "unhealthy":
            overall_status = "unhealthy"
        elif db_check["status"] == "degraded":
            overall_status = "degraded"
        
        # Event store check
        es_check = await self.check_event_store()
        checks["event_store"] = es_check
        
        # Projections check
        proj_check = await self.check_projections()
        checks["projections"] = proj_check
        
        return HealthStatus(
            status=overall_status,
            checks=checks
        )


# Global health checker
_health_checker: Optional[HealthChecker] = None


def get_health_checker(pool: Optional[asyncpg.Pool] = None) -> HealthChecker:
    """Get the global health checker instance."""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker(pool)
    elif pool is not None:
        _health_checker.set_pool(pool)
    return _health_checker
