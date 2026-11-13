"""
Configuration Management
Every server setting, with environment-variable overrides.

Environment variables use the CHAT_ prefix (CHAT_PORT, CHAT_MAX_CONNECTIONS, ...),
which is what docker-compose.yml and .env.example set. The unprefixed names an
earlier version read (MAX_CONNECTIONS, ...) and CHAT_SERVER_HOST / CHAT_SERVER_PORT
are still accepted as fallbacks.
"""
import os
from dataclasses import dataclass
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


def _env(names: tuple, default: T, cast: Callable[[str], T]) -> T:
    """First set variable among `names`, converted with `cast`.

    A malformed value raises ValueError naming the variable, instead of the
    server starting with a setting nobody asked for.
    """
    for name in names:
        raw = os.getenv(name)
        if raw is None or raw.strip() == "":
            continue
        # Allow trailing comments copied from .env files: "65536  # 64 KB".
        raw = raw.split("#", 1)[0].strip()
        try:
            return cast(raw)
        except ValueError as exc:
            raise ValueError(f"Invalid value for {name}: {raw!r}") from exc
    return default


def _bool(raw: str) -> bool:
    value = raw.lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(raw)


@dataclass
class ServerConfig:
    """Server configuration with production defaults."""

    # Network
    host: str = "0.0.0.0"
    port: int = 8888

    # Connection limits
    max_connections: int = 10000
    max_connections_per_ip: int = 100
    connection_timeout: float = 300.0  # idle seconds before a client is dropped

    # Rate limiting (token buckets). Each message must pass BOTH the sender's
    # bucket and the bucket of the IP it connects from, so opening many
    # connections or switching usernames does not multiply the allowance.
    enable_rate_limiting: bool = True
    max_messages_per_minute: int = 60  # per user
    max_messages_per_minute_per_ip: int = 300  # shared by every connection from one IP
    max_joins_per_minute: int = 10
    rate_limit_burst: int = 10

    # Protocol limits. A frame carries one JSON command; 128 KiB fits the
    # longest allowed message even when every character is JSON-escaped
    # (10,000 characters x up to 12 bytes), with room for the envelope.
    max_frame_size: int = 128 * 1024
    max_message_length: int = 10000  # characters
    max_username_length: int = 50
    max_room_name_length: int = 100

    # Room limits
    max_rooms_per_user: int = 50
    max_room_size: int = 1000

    # Timeouts (seconds)
    auth_timeout: float = 30.0
    # A recipient that cannot accept a frame within this time is disconnected,
    # so one stalled client cannot hold up a broadcast to everyone else.
    write_timeout: float = 10.0
    shutdown_timeout: float = 30.0

    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = None  # also log to this file when set

    # Metrics / health HTTP endpoint
    enable_metrics: bool = True
    metrics_port: int = 9090
    health_check_interval: float = 30.0

    @classmethod
    def from_env(cls) -> "ServerConfig":
        """Load configuration from environment variables (CHAT_* names first)."""
        d = cls()
        return cls(
            host=_env(("CHAT_HOST", "CHAT_SERVER_HOST"), d.host, str),
            port=_env(("CHAT_PORT", "CHAT_SERVER_PORT"), d.port, int),
            max_connections=_env(("CHAT_MAX_CONNECTIONS", "MAX_CONNECTIONS"), d.max_connections, int),
            max_connections_per_ip=_env(
                ("CHAT_MAX_CONNECTIONS_PER_IP", "MAX_CONNECTIONS_PER_IP"), d.max_connections_per_ip, int
            ),
            connection_timeout=_env(
                ("CHAT_READ_TIMEOUT", "CHAT_CONNECTION_TIMEOUT", "CONNECTION_TIMEOUT"),
                d.connection_timeout, float,
            ),
            enable_rate_limiting=_env(("CHAT_ENABLE_RATE_LIMITING",), d.enable_rate_limiting, _bool),
            max_messages_per_minute=_env(
                ("CHAT_MAX_MESSAGES_PER_MINUTE", "MAX_MESSAGES_PER_MINUTE"), d.max_messages_per_minute, int
            ),
            max_messages_per_minute_per_ip=_env(
                ("CHAT_MAX_MESSAGES_PER_MINUTE_PER_IP",), d.max_messages_per_minute_per_ip, int
            ),
            max_joins_per_minute=_env(
                ("CHAT_MAX_JOINS_PER_MINUTE", "MAX_JOINS_PER_MINUTE"), d.max_joins_per_minute, int
            ),
            rate_limit_burst=_env(("CHAT_RATE_LIMIT_BURST",), d.rate_limit_burst, int),
            max_frame_size=_env(("CHAT_MAX_FRAME_SIZE", "MAX_FRAME_SIZE"), d.max_frame_size, int),
            max_message_length=_env(
                ("CHAT_MAX_MESSAGE_LENGTH", "MAX_MESSAGE_LENGTH"), d.max_message_length, int
            ),
            max_username_length=_env(
                ("CHAT_MAX_USERNAME_LENGTH", "MAX_USERNAME_LENGTH"), d.max_username_length, int
            ),
            max_room_name_length=_env(
                ("CHAT_MAX_ROOM_NAME_LENGTH", "MAX_ROOM_NAME_LENGTH"), d.max_room_name_length, int
            ),
            max_rooms_per_user=_env(
                ("CHAT_MAX_ROOMS_PER_USER", "MAX_ROOMS_PER_USER"), d.max_rooms_per_user, int
            ),
            max_room_size=_env(("CHAT_MAX_ROOM_SIZE", "MAX_ROOM_SIZE"), d.max_room_size, int),
            auth_timeout=_env(("CHAT_AUTH_TIMEOUT",), d.auth_timeout, float),
            write_timeout=_env(("CHAT_WRITE_TIMEOUT",), d.write_timeout, float),
            shutdown_timeout=_env(("CHAT_SHUTDOWN_TIMEOUT",), d.shutdown_timeout, float),
            log_level=_env(("CHAT_LOG_LEVEL", "LOG_LEVEL"), d.log_level, str),
            log_file=_env(("CHAT_LOG_FILE",), d.log_file, str),
            enable_metrics=_env(("CHAT_ENABLE_METRICS", "ENABLE_METRICS"), d.enable_metrics, _bool),
            metrics_port=_env(("CHAT_METRICS_PORT", "METRICS_PORT"), d.metrics_port, int),
            health_check_interval=_env(("CHAT_HEALTH_CHECK_INTERVAL",), d.health_check_interval, float),
        )


@dataclass
class ClientConfig:
    """Client configuration."""

    host: str = "127.0.0.1"
    port: int = 8888
    auto_reconnect: bool = True
    max_reconnect_attempts: int = 5
    reconnect_delay: float = 2.0
    reconnect_backoff: float = 2.0  # exponential backoff multiplier
    connect_timeout: float = 10.0
    read_timeout: float = 300.0
    message_queue_size: int = 1000

    @classmethod
    def from_env(cls) -> "ClientConfig":
        """Load client configuration from environment variables."""
        d = cls()
        return cls(
            host=_env(("CHAT_CLIENT_HOST",), d.host, str),
            port=_env(("CHAT_PORT",), d.port, int),
            auto_reconnect=_env(("CHAT_AUTO_RECONNECT", "AUTO_RECONNECT"), d.auto_reconnect, _bool),
            max_reconnect_attempts=_env(
                ("CHAT_MAX_RECONNECT_ATTEMPTS", "MAX_RECONNECT_ATTEMPTS"), d.max_reconnect_attempts, int
            ),
            reconnect_delay=_env(("CHAT_RECONNECT_DELAY", "RECONNECT_DELAY"), d.reconnect_delay, float),
            reconnect_backoff=_env(("CHAT_RECONNECT_BACKOFF",), d.reconnect_backoff, float),
            connect_timeout=_env(("CHAT_CONNECT_TIMEOUT", "CONNECT_TIMEOUT"), d.connect_timeout, float),
            message_queue_size=_env(("CHAT_MESSAGE_QUEUE_SIZE",), d.message_queue_size, int),
        )


# Global configuration instances
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
