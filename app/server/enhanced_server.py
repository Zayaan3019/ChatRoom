"""
Production server: the chat protocol plus rate limiting, connection limits,
Prometheus metrics, a health endpoint and graceful shutdown.
"""
import asyncio
import json
import logging
import signal
import socket
import ssl
from asyncio import StreamReader, StreamWriter
from typing import Optional, Set

from ..core.config import ServerConfig, get_server_config
from ..core.metrics import MetricsCollector, get_metrics
from ..core.rate_limiter import ConnectionLimiter, RateLimiter
from .cluster import Cluster, LocalCluster, RedisCluster
from .connection import ClientConnection
from .proxy_protocol import ProxyProtocolError, TrustedProxies
from .proxy_protocol import read_header as read_proxy_header
from .room_manager import RoomManager

logger = logging.getLogger(__name__)


def listen_options() -> dict:
    """Socket options for start_server. SO_REUSEPORT does not exist on
    Windows, where asking for it makes start_server raise."""
    options = {"reuse_address": True}
    if hasattr(socket, "SO_REUSEPORT"):
        options["reuse_port"] = True
    return options


def build_tls_context(cert: str, key: str) -> ssl.SSLContext:
    """Server-side TLS: TLS 1.2 minimum, the library's modern cipher
    defaults, no client certificate required."""
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile=cert, keyfile=key)
    return ctx


def build_cluster(config: ServerConfig) -> Cluster:
    if config.redis_url:
        return RedisCluster(
            config.redis_url,
            namespace=config.redis_namespace,
            history_per_room=config.history_per_room,
            history_ttl_seconds=config.history_ttl_days * 86400,
            lease_seconds=config.lease_seconds,
            replica_id=config.replica_id,
        )
    return LocalCluster(history_per_room=config.memory_history_per_room,
                        max_bytes=config.memory_history_bytes)


class EnhancedChatServer:
    """
    Async TCP chat server.

    - Rate limiting: per-user and per-IP token buckets for messages and joins
    - Connection limits: global and per IP
    - Slow clients: a recipient that cannot take a frame within write_timeout
      is disconnected, so it cannot hold up a broadcast
    - Metrics on :metrics_port  (/metrics Prometheus, /health, /stats)
    - Graceful shutdown on SIGTERM/SIGINT
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        config: Optional[ServerConfig] = None,
        metrics: Optional[MetricsCollector] = None,
        cluster: Optional[Cluster] = None,
    ) -> None:
        self.config = (config or get_server_config()).validate()
        self.host = host if host is not None else self.config.host
        self.port = port if port is not None else self.config.port

        self.cluster = cluster or build_cluster(self.config)
        self.room_manager = RoomManager(self.cluster)
        self.tls_context: Optional[ssl.SSLContext] = None
        self.trusted_proxies = TrustedProxies(self.config.trusted_proxies if self.config.proxy_protocol else None)
        if self.config.tls_cert:
            self.tls_context = build_tls_context(self.config.tls_cert, self.config.tls_key)
        self.rate_limiter: Optional[RateLimiter] = None
        if self.config.enable_rate_limiting:
            self.rate_limiter = RateLimiter(
                messages_per_minute=self.config.max_messages_per_minute,
                joins_per_minute=self.config.max_joins_per_minute,
                burst=self.config.rate_limit_burst,
                messages_per_minute_per_ip=self.config.max_messages_per_minute_per_ip,
                auth_attempts_per_minute_per_ip=self.config.max_auth_attempts_per_minute_per_ip,
            )
        self.connection_limiter = ConnectionLimiter(
            max_total=self.config.max_connections,
            max_per_ip=self.config.max_connections_per_ip,
        )
        self.metrics = metrics or get_metrics()
        self.metrics.set_rooms_provider(lambda: len(self.room_manager))
        self.metrics.set_cluster_provider(self.cluster.stats)

        self.server: Optional[asyncio.AbstractServer] = None
        self.metrics_port: Optional[int] = None  # actual port once listening
        self._metrics_server: Optional[asyncio.AbstractServer] = None
        self._connections: Set[ClientConnection] = set()
        self._connection_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        self._shutting_down = False
        self._health_check_task: Optional[asyncio.Task] = None
        self.ready = asyncio.Event()  # set once both listeners are up

    # ----------------------------------------------------------- lifecycle

    async def start(self) -> None:
        self._register_signals()
        # The shared backend first: a replica that cannot reach Redis must
        # not accept clients it cannot serve.
        await self.cluster.start()
        if self.config.enable_metrics:
            await self._start_metrics_server()
        self.server = await asyncio.start_server(
            self._handle_client, self.host, self.port,
            ssl=self.tls_context, ssl_handshake_timeout=10.0 if self.tls_context else None,
            **listen_options(),
        )
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        addrs = ", ".join(str(sock.getsockname()) for sock in self.server.sockets)
        logger.info(f"Chat server listening on {addrs}")
        if self.metrics_port:
            logger.info(f"Metrics: http://{self.host}:{self.metrics_port}/metrics  health: /health")
        logger.info(
            f"Security: auth={self.config.auth_mode}, tls={'on' if self.tls_context else 'off'}"
            f"{', proxy protocol from ' + self.config.trusted_proxies if self.config.proxy_protocol else ''}; "
            f"cluster={'redis' if self.cluster.shared else 'single process'}"
        )
        logger.info(
            f"Limits: max_connections={self.config.max_connections}, "
            f"rate_limiting={'on' if self.rate_limiter else 'off'} "
            f"({self.config.max_messages_per_minute}/min per user, "
            f"{self.config.max_messages_per_minute_per_ip}/min per IP)"
        )
        self.ready.set()

        async with self.server:
            try:
                await self._shutdown_event.wait()
            except asyncio.CancelledError:
                logger.info("Server task cancelled")
        logger.info("Server stopped")

    async def _handle_client(self, reader: StreamReader, writer: StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        if self.config.proxy_protocol:
            addr = await self._client_address(reader, writer, addr)
            if addr is None:
                return
        ip_address = addr[0] if addr else "unknown"

        if self._shutting_down or not await self.connection_limiter.acquire(ip_address):
            logger.warning(f"Connection refused for {ip_address} (limit or shutdown)")
            await self.metrics.increment_failed_connections()
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return

        await self.metrics.increment_connections()
        connection = ClientConnection(
            reader, writer, self.room_manager, addr,
            rate_limiter=self.rate_limiter, metrics=self.metrics, config=self.config,
        )
        self._connections.add(connection)
        task = asyncio.current_task()
        if task is not None:
            self._connection_tasks.add(task)
        try:
            await connection.handle()
        finally:
            # Runs however handle() ends, including cancellation at shutdown.
            self._connections.discard(connection)
            if task is not None:
                self._connection_tasks.discard(task)
            await self.connection_limiter.release(ip_address)
            await self.metrics.decrement_connections()

    async def _client_address(self, reader: StreamReader, writer: StreamWriter, peer):
        """The real client address from the PROXY header, or None (and the
        socket closed) if the peer is not a trusted proxy or the header is bad."""
        peer_ip = peer[0] if peer else None
        try:
            if not await self.trusted_proxies.contains(peer_ip):
                raise ProxyProtocolError(f"{peer_ip} is not a trusted proxy")
            client = await read_proxy_header(reader, timeout=5.0)
        except ProxyProtocolError as e:
            logger.warning(f"Connection refused: {e}")
            await self.metrics.increment_failed_connections()
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return None
        # "PROXY UNKNOWN" / v2 LOCAL: the balancer's own health probe.
        return client or peer

    # ------------------------------------------------------- HTTP endpoint

    async def _start_metrics_server(self) -> None:
        async def respond(writer: StreamWriter, status: str, content_type: str, body: str) -> None:
            payload = body.encode("utf-8")
            writer.write(
                (
                    f"HTTP/1.1 {status}\r\n"
                    f"Content-Type: {content_type}\r\n"
                    f"Content-Length: {len(payload)}\r\n"
                    "Connection: close\r\n\r\n"
                ).encode("ascii")
                + payload
            )
            await writer.drain()

        async def handle_http(reader: StreamReader, writer: StreamWriter) -> None:
            try:
                request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                while True:  # consume headers
                    line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                    if line in (b"\r\n", b"\n", b""):
                        break
                parts = request_line.decode("latin-1").split()
                method = parts[0] if parts else ""
                path = parts[1].split("?", 1)[0] if len(parts) > 1 else ""

                if method not in ("GET", "HEAD"):
                    await respond(writer, "405 Method Not Allowed", "text/plain", "GET only\n")
                elif path == "/metrics":
                    await respond(writer, "200 OK", "text/plain; version=0.0.4; charset=utf-8",
                                  await self.metrics.get_prometheus_format())
                elif path == "/health":
                    ok = self.is_healthy()
                    await respond(writer, "200 OK" if ok else "503 Service Unavailable",
                                  "application/json",
                                  json.dumps({"status": "healthy" if ok else "unhealthy"}) + "\n")
                elif path == "/stats":
                    await respond(writer, "200 OK", "application/json",
                                  json.dumps(self.get_stats(), indent=2) + "\n")
                elif path == "/":
                    await respond(writer, "200 OK", "text/plain",
                                  "Chat server\n  /metrics  Prometheus metrics\n  /health   health check\n"
                                  "  /stats    JSON statistics\n")
                else:
                    await respond(writer, "404 Not Found", "text/plain", "Not found\n")
            except Exception as e:
                logger.debug(f"Metrics request error: {e!r}")
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

        self._metrics_server = await asyncio.start_server(handle_http, self.host, self.config.metrics_port)
        self.metrics_port = self._metrics_server.sockets[0].getsockname()[1]
        logger.info(f"Metrics server on port {self.metrics_port}")

    def is_healthy(self) -> bool:
        """Computed on every request. A cached flag refreshed by a timer
        reported 503 for the first interval after start-up, before the
        listener existed."""
        return (
            not self._shutting_down
            and self.server is not None
            and self.server.is_serving()
            and len(self._connections) < self.config.max_connections
            and self.cluster.healthy
        )

    async def _health_check_loop(self) -> None:
        """Logs transitions between healthy and unhealthy."""
        last = None
        while not self._shutdown_event.is_set():
            try:
                now = self.is_healthy()
                if now != last and last is not None:
                    logger.warning(f"Health changed: {'healthy' if now else 'UNHEALTHY'}")
                last = now
                await asyncio.sleep(self.config.health_check_interval)
            except asyncio.CancelledError:
                break

    # ----------------------------------------------------------- shutdown

    def _register_signals(self) -> None:
        try:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._shutdown(s)))
        except (NotImplementedError, RuntimeError):
            logger.warning("Signal handlers not supported on this platform")

    async def _shutdown(self, sig: signal.Signals) -> None:
        if self._shutting_down:
            return
        self._shutting_down = True
        logger.info(f"Received {sig.name}, shutting down")

        if self.server:
            self.server.close()
        if self._metrics_server:
            self._metrics_server.close()
        if self._health_check_task:
            self._health_check_task.cancel()

        # Tell every client, then close it, so handlers finish now instead of
        # waiting out their idle timeout.
        connections = list(self._connections)
        logger.info(f"Closing {len(connections)} connections")
        for conn in connections:
            try:
                await asyncio.wait_for(conn.send({"type": "BYE", "message": "Server shutting down"}), 1.0)
            except Exception:
                pass
            conn._detach()

        tasks = [t for t in self._connection_tasks if not t.done()]
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=self.config.shutdown_timeout)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending, timeout=2.0)

        for srv in (self.server, self._metrics_server):
            if srv:
                try:
                    await asyncio.wait_for(srv.wait_closed(), timeout=2.0)
                except Exception:
                    pass

        try:
            await asyncio.wait_for(self.cluster.close(), timeout=5.0)
        except Exception as e:
            logger.warning(f"Cluster shutdown: {e!r}")

        m = await self.metrics.get_metrics()
        logger.info(
            f"Final metrics: connections={m.total_connections}, messages={m.total_messages}, "
            f"errors={m.protocol_errors + m.timeout_errors + m.disconnect_errors}"
        )
        self._shutdown_event.set()

    async def stop(self) -> None:
        """Programmatic shutdown."""
        await self._shutdown(signal.SIGTERM)

    def get_stats(self) -> dict:
        stats = {
            "server": {
                "healthy": self.is_healthy(),
                "active_connections": len(self._connections),
                "max_connections": self.config.max_connections,
            },
            "connection_limiter": self.connection_limiter.get_stats(),
            "rooms": self.room_manager.get_stats(),
            "security": {"auth_mode": self.config.auth_mode, "tls": self.tls_context is not None},
        }
        stats["rate_limiter"] = self.rate_limiter.get_stats() if self.rate_limiter else {"enabled": False}
        return stats


async def run_server(host: Optional[str] = None, port: Optional[int] = None) -> None:
    """Run the production server (config from the environment)."""
    server = EnhancedChatServer(host, port)
    await server.start()
