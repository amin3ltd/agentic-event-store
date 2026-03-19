"""
Configuration Management

Loads configuration from config.yaml and environment variables.
Supports 12-factor app methodology with environment variable overrides.
"""

import os
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml


logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration."""
    host: str = "localhost"
    port: int = 5432
    name: str = "apex_ledger"
    user: str = "postgres"
    password: str = "postgres"
    min_size: int = 2
    max_size: int = 10
    connection_timeout: int = 30

    @property
    def dsn(self) -> str:
        """Generate PostgreSQL connection string."""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )


@dataclass
class EventStoreConfig:
    """Event store configuration."""
    snapshot_interval: int = 100
    max_stream_load: int = 1000
    outbox_poll_interval_ms: int = 100


@dataclass
class ProjectionDaemonConfig:
    """Projection daemon configuration."""
    poll_interval_ms: int = 100
    batch_size: int = 100
    checkpoint_interval: int = 50


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "json"
    output: str = "stdout"


@dataclass
class HealthConfig:
    """Health check configuration."""
    enabled: bool = True
    port: int = 8080


@dataclass
class ServerConfig:
    """Server configuration."""
    host: str = "0.0.0.0"
    port: int = 8765
    workers: int = 4


@dataclass
class Settings:
    """Main application settings."""
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    event_store: EventStoreConfig = field(default_factory=EventStoreConfig)
    projection_daemon: ProjectionDaemonConfig = field(default_factory=ProjectionDaemonConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    health: HealthConfig = field(default_factory=HealthConfig)
    server: ServerConfig = field(default_factory=ServerConfig)

    @classmethod
    def from_yaml(cls, config_path: Optional[Path] = None) -> "Settings":
        """
        Load settings from YAML config file with environment variable overrides.
        
        Args:
            config_path: Path to config.yaml file. Defaults to ./config.yaml
            
        Returns:
            Settings instance
        """
        if config_path is None:
            # Try to find config.yaml in current directory or parent
            config_path = Path(__file__).parent.parent / "config.yaml"
        
        settings = cls()
        
        if config_path.exists():
            with open(config_path, "r") as f:
                config_data = yaml.safe_load(f) or {}
            
            # Override with environment variables
            settings._load_from_dict(config_data)
        
        # Always allow environment variable overrides
        settings._apply_env_overrides()
        
        return settings
    
    def _load_from_dict(self, data: dict[str, Any]) -> None:
        """Load settings from dictionary."""
        if "database" in data:
            db_data = data["database"]
            self.database.host = db_data.get("host", self.database.host)
            self.database.port = db_data.get("port", self.database.port)
            self.database.name = db_data.get("name", self.database.name)
            self.database.user = db_data.get("user", self.database.user)
            self.database.password = db_data.get("password", self.database.password)
            if "pool" in db_data:
                self.database.min_size = db_data["pool"].get("min_size", self.database.min_size)
                self.database.max_size = db_data["pool"].get("max_size", self.database.max_size)
                self.database.connection_timeout = db_data["pool"].get("connection_timeout", self.database.connection_timeout)
        
        if "event_store" in data:
            es_data = data["event_store"]
            self.event_store.snapshot_interval = es_data.get("snapshot_interval", self.event_store.snapshot_interval)
            self.event_store.max_stream_load = es_data.get("max_stream_load", self.event_store.max_stream_load)
            self.event_store.outbox_poll_interval_ms = es_data.get("outbox_poll_interval_ms", self.event_store.outbox_poll_interval_ms)
        
        if "projection_daemon" in data:
            pd_data = data["projection_daemon"]
            self.projection_daemon.poll_interval_ms = pd_data.get("poll_interval_ms", self.projection_daemon.poll_interval_ms)
            self.projection_daemon.batch_size = pd_data.get("batch_size", self.projection_daemon.batch_size)
            self.projection_daemon.checkpoint_interval = pd_data.get("checkpoint_interval", self.projection_daemon.checkpoint_interval)
        
        if "logging" in data:
            log_data = data["logging"]
            self.logging.level = log_data.get("level", self.logging.level)
            self.logging.format = log_data.get("format", self.logging.format)
            self.logging.output = log_data.get("output", self.logging.output)
        
        if "health" in data:
            health_data = data["health"]
            self.health.enabled = health_data.get("enabled", self.health.enabled)
            self.health.port = health_data.get("port", self.health.port)
        
        if "server" in data:
            server_data = data["server"]
            self.server.host = server_data.get("host", self.server.host)
            self.server.port = server_data.get("port", self.server.port)
            self.server.workers = server_data.get("workers", self.server.workers)
    
    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides."""
        # Database
        self.database.host = os.environ.get("DB_HOST", self.database.host)
        self.database.port = int(os.environ.get("DB_PORT", str(self.database.port)))
        self.database.name = os.environ.get("DB_NAME", self.database.name)
        self.database.user = os.environ.get("DB_USER", self.database.user)
        self.database.password = os.environ.get("DB_PASSWORD", self.database.password)
        self.database.min_size = int(os.environ.get("DB_POOL_MIN", str(self.database.min_size)))
        self.database.max_size = int(os.environ.get("DB_POOL_MAX", str(self.database.max_size)))
        
        # Event Store
        self.event_store.snapshot_interval = int(os.environ.get("EVENT_STORE_SNAPSHOT_INTERVAL", str(self.event_store.snapshot_interval)))
        self.event_store.max_stream_load = int(os.environ.get("EVENT_STORE_MAX_STREAM_LOAD", str(self.event_store.max_stream_load)))
        self.event_store.outbox_poll_interval_ms = int(os.environ.get("EVENT_STORE_OUTBOX_POLL_MS", str(self.event_store.outbox_poll_interval_ms)))
        
        # Projection Daemon
        self.projection_daemon.poll_interval_ms = int(os.environ.get("PROJECTION_POLL_MS", str(self.projection_daemon.poll_interval_ms)))
        self.projection_daemon.batch_size = int(os.environ.get("PROJECTION_BATCH_SIZE", str(self.projection_daemon.batch_size)))
        
        # Logging
        self.logging.level = os.environ.get("LOG_LEVEL", self.logging.level)
        self.logging.format = os.environ.get("LOG_FORMAT", self.logging.format)
        
        # Health
        self.health.enabled = os.environ.get("HEALTH_ENABLED", str(self.health.enabled)).lower() == "true"
        self.health.port = int(os.environ.get("HEALTH_PORT", str(self.health.port)))
        
        # Server
        self.server.host = os.environ.get("SERVER_HOST", self.server.host)
        self.server.port = int(os.environ.get("SERVER_PORT", str(self.server.port)))


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the global settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings.from_yaml()
    return _settings


def setup_logging() -> None:
    """Configure logging based on settings."""
    settings = get_settings()
    
    log_level = getattr(logging, settings.logging.level.upper(), logging.INFO)
    
    # Simple format for now - can be extended to JSON
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    logger.info(f"Logging configured at {settings.logging.level} level")
