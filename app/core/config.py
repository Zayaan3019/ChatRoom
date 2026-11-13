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


def _read_secret(value: Optional[str], file_var: str) -> Optional[str]:
    """A secret given directly, or read from the file named by `file_var`
    (the Docker-secrets pattern: the value never appears in the environment)."""
    if value:
        return value
    path = os.getenv(file_var)
    if path:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip() or None
        except OSError as exc:
            raise ValueError(f"Cannot read {file_var}={path!r}: {exc}") from exc
    return None


def _with_redis_password(url: Optional[str]) -> Optional[str]:
    """Insert CHAT_REDIS_PASSWORD(_FILE) into a Redis URL that has none."""
    if not url:
        return url
    password = _read_secret(os.getenv("CHAT_REDIS_PASSWORD"), "CHAT_REDIS_PASSWORD_FILE")
    if not password or "@" in url.split("//", 1)[-1]:
        return url
    from urllib.parse import quote
    scheme, rest = url.split("//", 1)
    return f"{scheme}//:{quote(password, safe='')}@{rest}"


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

    # Authentication. "open": AUTH claims any free username (development).
    # "token": AUTH carries a signed JWT (HS256) whose `sub` is the username.
    # "password": AUTH carries a username and password checked against a
    # scrypt-hashed user file. Both real modes require TLS unless
    # allow_plaintext_auth is set, because they put credentials on the wire.
    auth_mode: str = "open"
    jwt_secret: Optional[str] = None
    jwt_issuer: Optional[str] = None
    jwt_audience: Optional[str] = "chat"
    users_file: Optional[str] = None
    max_auth_attempts_per_minute_per_ip: int = 10
    allow_plaintext_auth: bool = False

    # TLS for the chat port (PEM files). The metrics port stays plain HTTP and
    # belongs on a private network.
    tls_cert: Optional[str] = None
    tls_key: Optional[str] = None

    # Behind a TCP load balancer: read the real client address from a PROXY
    # protocol header (v1 or v2), accepted only from these proxy addresses.
    # Without it every per-IP limit would see only the balancer's address.
    proxy_protocol: bool = False
    trusted_proxies: Optional[str] = None   # "10.0.0.0/8,lb.internal,nginx"

    # Cluster mode. With a Redis URL, rooms span every replica connected to
    # the same Redis and messages are stored in Redis Streams. Without it the
    # server runs alone and keeps recent history in memory.
    redis_url: Optional[str] = None
    redis_namespace: str = "chat"
    replica_id: Optional[str] = None
    lease_seconds: float = 30.0

    # Message history
    history_on_join: int = 0             # messages replayed on JOIN when the client does not say (clients send "history": n)
    history_page_max: int = 100          # largest HISTORY page
    history_resume_max: int = 500        # largest JOIN-with-`since` replay
    history_per_room: int = 10000        # Redis stream length per room
    history_ttl_days: float = 30.0       # an idle room's log expires after this
    memory_history_per_room: int = 200   # single-process mode
    memory_history_bytes: int = 64 * 1024 * 1024

    def validate(self) -> "ServerConfig":
        if self.auth_mode not in ("open", "token", "password"):
            raise ValueError(f"CHAT_AUTH_MODE must be open, token or password, not {self.auth_mode!r}")
        if self.auth_mode == "token" and not self.jwt_secret:
            raise ValueError("CHAT_AUTH_MODE=token needs CHAT_JWT_SECRET or CHAT_JWT_SECRET_FILE")
        if self.auth_mode == "token" and len(self.jwt_secret.encode()) < 32:
            raise ValueError("The JWT secret must be at least 32 bytes")
        if self.auth_mode == "password" and not self.users_file:
            raise ValueError("CHAT_AUTH_MODE=password needs CHAT_USERS_FILE")
        if bool(self.tls_cert) != bool(self.tls_key):
            raise ValueError("Set both CHAT_TLS_CERT and CHAT_TLS_KEY, or neither")
        if self.proxy_protocol:
            from ..server.proxy_protocol import split_spec
            try:
                if not any(split_spec(self.trusted_proxies)):
                    raise ValueError("empty")
            except ValueError as exc:
                raise ValueError(
                    "CHAT_PROXY_PROTOCOL=true needs CHAT_TRUSTED_PROXIES (addresses or CIDRs "
                    f"of the load balancers): {exc}"
                ) from exc
            if self.tls_cert:
                raise ValueError(
                    "Use either CHAT_PROXY_PROTOCOL (TLS ends at the load balancer) or "
                    "CHAT_TLS_CERT (TLS ends here), not both"
                )
        if self.auth_mode != "open" and not self.tls_cert and not self.allow_plaintext_auth:
            raise ValueError(
                f"CHAT_AUTH_MODE={self.auth_mode} sends credentials, so it requires TLS "
                "(CHAT_TLS_CERT/CHAT_TLS_KEY). Set CHAT_ALLOW_PLAINTEXT_AUTH=true only when "
                "TLS is terminated in front of the server on a trusted network."
            )
        return self

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
            auth_mode=_env(("CHAT_AUTH_MODE",), d.auth_mode, lambda v: v.lower()),
            jwt_secret=_read_secret(os.getenv("CHAT_JWT_SECRET"), "CHAT_JWT_SECRET_FILE"),
            jwt_issuer=_env(("CHAT_JWT_ISSUER",), d.jwt_issuer, str),
            jwt_audience=_env(("CHAT_JWT_AUDIENCE",), d.jwt_audience, str),
            users_file=_env(("CHAT_USERS_FILE",), d.users_file, str),
            max_auth_attempts_per_minute_per_ip=_env(
                ("CHAT_MAX_AUTH_ATTEMPTS_PER_MINUTE_PER_IP",), d.max_auth_attempts_per_minute_per_ip, int
            ),
            allow_plaintext_auth=_env(("CHAT_ALLOW_PLAINTEXT_AUTH",), d.allow_plaintext_auth, _bool),
            tls_cert=_env(("CHAT_TLS_CERT",), d.tls_cert, str),
            tls_key=_env(("CHAT_TLS_KEY",), d.tls_key, str),
            proxy_protocol=_env(("CHAT_PROXY_PROTOCOL",), d.proxy_protocol, _bool),
            trusted_proxies=_env(("CHAT_TRUSTED_PROXIES",), d.trusted_proxies, str),
            redis_url=_with_redis_password(_env(("CHAT_REDIS_URL",), d.redis_url, str)),
            redis_namespace=_env(("CHAT_REDIS_NAMESPACE",), d.redis_namespace, str),
            replica_id=_env(("CHAT_REPLICA_ID",), d.replica_id, str),
            lease_seconds=_env(("CHAT_LEASE_SECONDS",), d.lease_seconds, float),
            history_on_join=_env(("CHAT_HISTORY_ON_JOIN",), d.history_on_join, int),
            history_page_max=_env(("CHAT_HISTORY_PAGE_MAX",), d.history_page_max, int),
            history_resume_max=_env(("CHAT_HISTORY_RESUME_MAX",), d.history_resume_max, int),
            history_per_room=_env(("CHAT_HISTORY_PER_ROOM",), d.history_per_room, int),
            history_ttl_days=_env(("CHAT_HISTORY_TTL_DAYS",), d.history_ttl_days, float),
            memory_history_per_room=_env(("CHAT_MEMORY_HISTORY_PER_ROOM",), d.memory_history_per_room, int),
            memory_history_bytes=_env(("CHAT_MEMORY_HISTORY_BYTES",), d.memory_history_bytes, int),
        ).validate()


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
    tls: bool = False
    tls_ca: Optional[str] = None          # CA bundle that signed the server cert
    tls_server_name: Optional[str] = None  # name to verify (default: host)
    token: Optional[str] = None

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
            tls=_env(("CHAT_CLIENT_TLS",), d.tls, _bool),
            tls_ca=_env(("CHAT_CLIENT_TLS_CA",), d.tls_ca, str),
            tls_server_name=_env(("CHAT_CLIENT_TLS_SERVER_NAME",), d.tls_server_name, str),
            token=_read_secret(os.getenv("CHAT_CLIENT_TOKEN"), "CHAT_CLIENT_TOKEN_FILE"),
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
