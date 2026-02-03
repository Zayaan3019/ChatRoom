"""
Production-Grade Enhanced Server
Includes all enterprise features: rate limiting, metrics, health checks, backpressure.
"""
import asyncio
import logging
import signal
from typing import Optional, Set, Dict
from asyncio import StreamReader, StreamWriter
from collections import defaultdict

from .connection import ClientConnection
from .room_manager import RoomManager
from ..core.config import get_server_config
from ..core.rate_limiter import RateLimiter, ConnectionLimiter
from ..core.metrics import get_metrics

logger = logging.getLogger(__name__)


class EnhancedChatServer:
    """
    Production-grade async TCP chat server with enterprise features.
    
    Features:
        - Rate limiting (per-user and per-IP)
        - Connection limits (global and per-IP)
        - Metrics collection (Prometheus-compatible)
        - Health checks
        - Backpressure handling
        - Graceful shutdown
        - Resource monitoring
    """
    
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None) -> None:
        config = get_server_config()
        
        self.host = host or config.host
        self.port = port or config.port
        self.config = config
        
        # Core components
        self.room_manager = RoomManager()
        self.rate_limiter = RateLimiter(
            messages_per_minute=config.max_messages_per_minute,
            joins_per_minute=config.max_joins_per_minute,
            burst=config.rate_limit_burst
        )
        self.connection_limiter = ConnectionLimiter(
            max_total=config.max_connections,
            max_per_ip=config.max_connections_per_ip
        )
        self.metrics = get_metrics()
        
        self.server: Optional[asyncio.AbstractServer] = None
        
        # Track active connection tasks
        self._connection_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        
        # Health check
        self._healthy = True
        self._health_check_task: Optional[asyncio.Task] = None
        
        # Metrics HTTP server
        self._metrics_server: Optional[asyncio.AbstractServer] = None
    
    async def start(self) -> None:
        """
        Start the server with all production features.
        """
        # Register signal handlers
        self._register_signals()
        
        # Start metrics HTTP server
        if self.config.enable_metrics:
            await self._start_metrics_server()
        
        # Start health check task
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        
        # Start TCP server
        self.server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            reuse_address=True,
            reuse_port=True,
        )
        
        addrs = ", ".join(str(sock.getsockname()) for sock in self.server.sockets)
        logger.info(f"🚀 Enhanced Chat Server listening on {addrs}")
        logger.info(f"📊 Metrics endpoint: http://{self.host}:{self.config.metrics_port}/metrics")
        logger.info(f"❤️  Health check endpoint: http://{self.host}:{self.config.metrics_port}/health")
        logger.info(f"⚙️  Config: max_connections={self.config.max_connections}, "
                   f"rate_limit={self.config.max_messages_per_minute}/min")
        
        # Keep server running
        async with self.server:
            try:
                await self._shutdown_event.wait()
            except asyncio.CancelledError:
                logger.info("Server task cancelled")
        
        logger.info("Server stopped")
    
    async def _handle_client(
        self, 
        reader: StreamReader, 
        writer: StreamWriter
    ) -> None:
        """
        Handle new client connection with rate limiting and metrics.
        """
        addr = writer.get_extra_info("peername")
        ip_address = addr[0] if addr else "unknown"
        
        # Check connection limits
        if not await self.connection_limiter.acquire(ip_address):
            logger.warning(f"Connection limit exceeded for {ip_address}")
            await self.metrics.increment_failed_connections()
            writer.close()
            await writer.wait_closed()
            return
        
        # Record connection
        await self.metrics.increment_connections()
        
        try:
            # Create connection wrapper
            connection = ClientConnection(
                reader, writer, self.room_manager, addr,
                rate_limiter=self.rate_limiter,
                metrics=self.metrics,
                config=self.config
            )
            
            # Spawn handler task
            task = asyncio.create_task(connection.handle())
            self._connection_tasks.add(task)
            
            # Cleanup on completion
            def done_callback(t):
                self._connection_tasks.discard(t)
                asyncio.create_task(self._cleanup_connection(ip_address))
            
            task.add_done_callback(done_callback)
            
            logger.debug(f"Active connections: {len(self._connection_tasks)}")
        
        except Exception as e:
            logger.error(f"Error handling client {addr}: {e}", exc_info=True)
            await self.connection_limiter.release(ip_address)
            await self.metrics.decrement_connections()
    
    async def _cleanup_connection(self, ip_address: str) -> None:
        """Cleanup after connection closes."""
        await self.connection_limiter.release(ip_address)
        await self.metrics.decrement_connections()
    
    async def _start_metrics_server(self) -> None:
        """Start HTTP server for metrics and health checks."""
        async def handle_metrics_request(reader: StreamReader, writer: StreamWriter) -> None:
            try:
                # Read HTTP request
                request_line = await reader.readline()
                request = request_line.decode().strip()
                
                # Read headers (we don't need them but must consume)
                while True:
                    line = await reader.readline()
                    if line == b'\r\n' or line == b'\n' or not line:
                        break
                
                # Route request
                if b'/metrics' in request_line:
                    prometheus_text = await self.metrics.get_prometheus_format()
                    response = (
                        b"HTTP/1.1 200 OK\r\n"
                        b"Content-Type: text/plain; version=0.0.4\r\n"
                        f"Content-Length: {len(prometheus_text)}\r\n"
                        b"\r\n"
                        + prometheus_text.encode()
                    )
                elif b'/health' in request_line:
                    health_status = "healthy" if self._healthy else "unhealthy"
                    status_code = "200 OK" if self._healthy else "503 Service Unavailable"
                    body = f'{{"status": "{health_status}"}}\n'
                    response = (
                        f"HTTP/1.1 {status_code}\r\n"
                        f"Content-Type: application/json\r\n"
                        f"Content-Length: {len(body)}\r\n"
                        f"\r\n"
                        f"{body}"
                    ).encode()
                elif b'/stats' in request_line:
                    import json
                    stats = self.get_stats()
                    body = json.dumps(stats, indent=2) + "\n"
                    response = (
                        f"HTTP/1.1 200 OK\r\n"
                        f"Content-Type: application/json\r\n"
                        f"Content-Length: {len(body)}\r\n"
                        f"\r\n"
                        f"{body}"
                    ).encode()
                else:
                    body = "Chat Server Metrics\n\nEndpoints:\n  /metrics - Prometheus metrics\n  /health - Health check\n  /stats - JSON statistics\n"
                    response = (
                        f"HTTP/1.1 200 OK\r\n"
                        f"Content-Type: text/plain\r\n"
                        f"Content-Length: {len(body)}\r\n"
                        f"\r\n"
                        f"{body}"
                    ).encode()
                
                writer.write(response)
                await writer.drain()
            
            except Exception as e:
                logger.error(f"Metrics server error: {e}")
            
            finally:
                writer.close()
                await writer.wait_closed()
        
        self._metrics_server = await asyncio.start_server(
            handle_metrics_request,
            self.host,
            self.config.metrics_port
        )
        
        logger.info(f"📊 Metrics server started on port {self.config.metrics_port}")
    
    async def _health_check_loop(self) -> None:
        """Periodic health check."""
        while not self._shutdown_event.is_set():
            try:
                # Simple health checks
                self._healthy = (
                    len(self._connection_tasks) < self.config.max_connections and
                    self.server is not None
                )
                
                await asyncio.sleep(self.config.health_check_interval)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                self._healthy = False
    
    def _register_signals(self) -> None:
        """Register signal handlers for graceful shutdown."""
        try:
            loop = asyncio.get_running_loop()
            
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self._shutdown(s))
                )
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform")
    
    async def _shutdown(self, sig: signal.Signals) -> None:
        """
        Graceful shutdown with timeout.
        """
        logger.info(f"Received signal {sig.name}, initiating shutdown...")
        
        # Mark as unhealthy
        self._healthy = False
        
        # Stop accepting new connections
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        # Stop metrics server
        if self._metrics_server:
            self._metrics_server.close()
            await self._metrics_server.wait_closed()
        
        # Cancel health check
        if self._health_check_task:
            self._health_check_task.cancel()
        
        logger.info(f"Closing {len(self._connection_tasks)} active connections...")
        
        # Give connections time to finish gracefully
        if self._connection_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._connection_tasks, return_exceptions=True),
                    timeout=self.config.shutdown_timeout
                )
            except asyncio.TimeoutError:
                logger.warning(f"Shutdown timeout, force closing {len(self._connection_tasks)} connections")
                
                # Force cancel remaining tasks
                for task in self._connection_tasks:
                    if not task.done():
                        task.cancel()
        
        # Print final statistics
        metrics = await self.metrics.get_metrics()
        logger.info(f"Final metrics: connections={metrics.total_connections}, "
                   f"messages={metrics.total_messages}, "
                   f"errors={metrics.protocol_errors + metrics.timeout_errors}")
        
        # Signal main loop to exit
        self._shutdown_event.set()
    
    async def stop(self) -> None:
        """Programmatic shutdown."""
        await self._shutdown(signal.SIGTERM)
    
    def get_stats(self) -> dict:
        """Get comprehensive server statistics."""
        return {
            "server": {
                "healthy": self._healthy,
                "active_connections": len(self._connection_tasks),
                "max_connections": self.config.max_connections,
            },
            "rate_limiter": self.rate_limiter.get_stats(),
            "connection_limiter": self.connection_limiter.get_stats(),
            "rooms": self.room_manager.get_stats(),
        }


async def run_server(host: str = "0.0.0.0", port: int = 8888) -> None:
    """
    Run the enhanced server.
    """
    server = EnhancedChatServer(host, port)
    await server.start()
