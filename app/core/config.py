"""
Configuration Management - Production-Grade Settings
Centralizes all configuration with environment variable support.
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ServerConfig:
    """Server configuration with sensible production defaults."""
    
    # Network settings
    host: str = "0.0.0.0"
    port: int = 8888
    
    # Connection limits
    max_connections: int = 10000
    max_connections_per_ip: int = 100
    connection_timeout: float = 300.0  # 5 minutes
    
    # Rate limiting
    max_messages_per_minute: int = 60
    max_joins_per_minute: int = 10
    rate_limit_burst: int = 10
    
    # Resource limits
    max_frame_size: int = 10 * 1024 * 1024  # 10MB
    max_message_length: int = 10000  # 10KB text
    max_username_length: int = 50
    max_room_name_length: int = 100
    max_rooms_per_user: int = 50
    max_room_size: int = 1000  # Max users per room
    
    # Buffer settings
    send_buffer_size: int = 64 * 1024  # 64KB
    recv_buffer_size: int = 64 * 1024
    write_buffer_high_water: int = 256 * 1024  # Backpressure threshold
    
    # Timeouts
    auth_timeout: float = 30.0
    idle_timeout: float = 600.0  # 10 minutes
    shutdown_timeout: float = 30.0
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "json"  # or "text"
    
    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 9090
    health_check_interval: float = 30.0
    
    # Security
    enable_ssl: bool = False
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "ServerConfig":
        """Load configuration from environment variables."""
        return cls(
            host=os.getenv("CHAT_HOST", "0.0.0.0"),
            port=int(os.getenv("CHAT_PORT", "8888")),
            max_connections=int(os.getenv("MAX_CONNECTIONS", "10000")),
            max_connections_per_ip=int(os.getenv("MAX_CONNECTIONS_PER_IP", "100")),
            connection_timeout=float(os.getenv("CONNECTION_TIMEOUT", "300.0")),
            max_messages_per_minute=int(os.getenv("MAX_MESSAGES_PER_MINUTE", "60")),
            max_joins_per_minute=int(os.getenv("MAX_JOINS_PER_MINUTE", "10")),
            max_frame_size=int(os.getenv("MAX_FRAME_SIZE", str(10 * 1024 * 1024))),
            max_message_length=int(os.getenv("MAX_MESSAGE_LENGTH", "10000")),
            max_username_length=int(os.getenv("MAX_USERNAME_LENGTH", "50")),
            max_room_name_length=int(os.getenv("MAX_ROOM_NAME_LENGTH", "100")),
            max_rooms_per_user=int(os.getenv("MAX_ROOMS_PER_USER", "50")),
            max_room_size=int(os.getenv("MAX_ROOM_SIZE", "1000")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_format=os.getenv("LOG_FORMAT", "json"),
            enable_metrics=os.getenv("ENABLE_METRICS", "true").lower() == "true",
            metrics_port=int(os.getenv("METRICS_PORT", "9090")),
            enable_ssl=os.getenv("ENABLE_SSL", "false").lower() == "true",
            ssl_certfile=os.getenv("SSL_CERTFILE"),
            ssl_keyfile=os.getenv("SSL_KEYFILE"),
        )


@dataclass
class ClientConfig:
    """Client configuration."""
    
    host: str = "127.0.0.1"
    port: int = 8888
    
    # Reconnection
    auto_reconnect: bool = True
    max_reconnect_attempts: int = 5
    reconnect_delay: float = 2.0
    reconnect_backoff: float = 2.0  # Exponential backoff multiplier
    
    # Timeouts
    connect_timeout: float = 10.0
    read_timeout: float = 300.0
    
    # Buffering
    message_queue_size: int = 1000
    
    @classmethod
    def from_env(cls) -> "ClientConfig":
        """Load client configuration from environment variables."""
        return cls(
            host=os.getenv("CHAT_HOST", "127.0.0.1"),
            port=int(os.getenv("CHAT_PORT", "8888")),
            auto_reconnect=os.getenv("AUTO_RECONNECT", "true").lower() == "true",
            max_reconnect_attempts=int(os.getenv("MAX_RECONNECT_ATTEMPTS", "5")),
            reconnect_delay=float(os.getenv("RECONNECT_DELAY", "2.0")),
            connect_timeout=float(os.getenv("CONNECT_TIMEOUT", "10.0")),
        )


# Global config instances
_server_config: Optional[ServerConfig] = None
_client_config: Optional[ClientConfig] = None


def get_server_config() -> ServerConfig:
    """Get or create server configuration."""
    global _server_config
    if _server_config is None:
        _server_config = ServerConfig.from_env()
    return _server_config


def get_client_config() -> ClientConfig:
    """Get or create client configuration."""
    global _client_config
    if _client_config is None:
        _client_config = ClientConfig.from_env()
    return _client_config


def set_server_config(config: ServerConfig) -> None:
    """Override server configuration (for testing)."""
    global _server_config
    _server_config = config


def set_client_config(config: ClientConfig) -> None:
    """Override client configuration (for testing)."""
    global _client_config
    _client_config = config
