"""
Metrics and Observability - Prometheus-style Metrics
Production-grade monitoring and alerting support.
"""
import asyncio
import time
from typing import Dict, Optional, Set
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Metrics:
    """System-wide metrics collector."""
    
    # Connection metrics
    total_connections: int = 0
    active_connections: int = 0
    failed_connections: int = 0
    connections_rate: float = 0.0
    
    # Message metrics
    total_messages: int = 0
    messages_per_second: float = 0.0
    failed_messages: int = 0
    
    # Room metrics
    total_rooms: int = 0
    active_rooms: int = 0
    
    # Performance metrics
    avg_message_latency_ms: float = 0.0
    p95_message_latency_ms: float = 0.0
    p99_message_latency_ms: float = 0.0
    
    # Rate limiting
    rate_limited_requests: int = 0
    
    # Error metrics
    protocol_errors: int = 0
    timeout_errors: int = 0
    disconnect_errors: int = 0
    
    # Resource metrics
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    # Uptime
    server_start_time: datetime = field(default_factory=datetime.now)
    
    def uptime_seconds(self) -> float:
        """Calculate server uptime."""
        return (datetime.now() - self.server_start_time).total_seconds()


class MetricsCollector:
    """
    Collects and aggregates metrics for monitoring.
    Thread-safe for asyncio.
    """
    
    def __init__(self) -> None:
        self.metrics = Metrics()
        self._lock = asyncio.Lock()
        
        # Latency tracking (circular buffer)
        self._latencies: list = []
        self._max_latency_samples = 10000
        
        # Rate tracking
        self._message_timestamps: list = []
        self._connection_timestamps: list = []
        self._window_size = 60.0  # 1 minute window
        
        # Per-room metrics
        self._room_message_counts: Dict[str, int] = defaultdict(int)
        self._room_user_counts: Dict[str, int] = defaultdict(int)
    
    async def increment_connections(self) -> None:
        """Record new connection."""
        async with self._lock:
            self.metrics.total_connections += 1
            self.metrics.active_connections += 1
            self._connection_timestamps.append(time.time())
            await self._update_connection_rate()
    
    async def decrement_connections(self) -> None:
        """Record connection close."""
        async with self._lock:
            self.metrics.active_connections = max(0, self.metrics.active_connections - 1)
    
    async def increment_failed_connections(self) -> None:
        """Record failed connection attempt."""
        async with self._lock:
            self.metrics.failed_connections += 1
    
    async def record_message(self, latency_ms: float) -> None:
        """Record message sent with latency."""
        async with self._lock:
            self.metrics.total_messages += 1
            self._message_timestamps.append(time.time())
            
            # Track latency
            self._latencies.append(latency_ms)
            if len(self._latencies) > self._max_latency_samples:
                self._latencies.pop(0)
            
            # Update rates and percentiles
            await self._update_message_rate()
            await self._update_latency_percentiles()
    
    async def increment_failed_messages(self) -> None:
        """Record failed message."""
        async with self._lock:
            self.metrics.failed_messages += 1
    
    async def record_room_metrics(self, room_name: str, user_count: int) -> None:
        """Update room statistics."""
        async with self._lock:
            self._room_user_counts[room_name] = user_count
            self.metrics.active_rooms = len([c for c in self._room_user_counts.values() if c > 0])
    
    async def increment_room_message(self, room_name: str) -> None:
        """Record message in specific room."""
        async with self._lock:
            self._room_message_counts[room_name] += 1
    
    async def increment_rate_limited(self) -> None:
        """Record rate-limited request."""
        async with self._lock:
            self.metrics.rate_limited_requests += 1
    
    async def increment_protocol_errors(self) -> None:
        """Record protocol error."""
        async with self._lock:
            self.metrics.protocol_errors += 1
    
    async def increment_timeout_errors(self) -> None:
        """Record timeout error."""
        async with self._lock:
            self.metrics.timeout_errors += 1
    
    async def increment_disconnect_errors(self) -> None:
        """Record disconnect error."""
        async with self._lock:
            self.metrics.disconnect_errors += 1
    
    async def _update_connection_rate(self) -> None:
        """Calculate connections per second."""
        now = time.time()
        cutoff = now - self._window_size
        
        # Remove old timestamps
        self._connection_timestamps = [ts for ts in self._connection_timestamps if ts > cutoff]
        
        # Calculate rate
        if self._connection_timestamps:
            elapsed = now - self._connection_timestamps[0]
            if elapsed > 0:
                self.metrics.connections_rate = len(self._connection_timestamps) / elapsed
    
    async def _update_message_rate(self) -> None:
        """Calculate messages per second."""
        now = time.time()
        cutoff = now - self._window_size
        
        # Remove old timestamps
        self._message_timestamps = [ts for ts in self._message_timestamps if ts > cutoff]
        
        # Calculate rate
        if self._message_timestamps:
            elapsed = now - self._message_timestamps[0]
            if elapsed > 0:
                self.metrics.messages_per_second = len(self._message_timestamps) / elapsed
    
    async def _update_latency_percentiles(self) -> None:
        """Calculate latency percentiles."""
        if not self._latencies:
            return
        
        sorted_latencies = sorted(self._latencies)
        count = len(sorted_latencies)
        
        # Calculate percentiles
        self.metrics.avg_message_latency_ms = sum(sorted_latencies) / count
        
        p95_idx = int(count * 0.95)
        self.metrics.p95_message_latency_ms = sorted_latencies[min(p95_idx, count - 1)]
        
        p99_idx = int(count * 0.99)
        self.metrics.p99_message_latency_ms = sorted_latencies[min(p99_idx, count - 1)]
    
    async def get_metrics(self) -> Metrics:
        """Get current metrics snapshot."""
        async with self._lock:
            return self.metrics
    
    async def get_prometheus_format(self) -> str:
        """
        Export metrics in Prometheus text format.
        
        Returns:
            Metrics in Prometheus exposition format
        """
        async with self._lock:
            lines = [
                "# HELP chat_connections_total Total number of connections",
                "# TYPE chat_connections_total counter",
                f"chat_connections_total {self.metrics.total_connections}",
                "",
                "# HELP chat_connections_active Currently active connections",
                "# TYPE chat_connections_active gauge",
                f"chat_connections_active {self.metrics.active_connections}",
                "",
                "# HELP chat_connections_failed_total Failed connection attempts",
                "# TYPE chat_connections_failed_total counter",
                f"chat_connections_failed_total {self.metrics.failed_connections}",
                "",
                "# HELP chat_messages_total Total messages sent",
                "# TYPE chat_messages_total counter",
                f"chat_messages_total {self.metrics.total_messages}",
                "",
                "# HELP chat_messages_per_second Current message rate",
                "# TYPE chat_messages_per_second gauge",
                f"chat_messages_per_second {self.metrics.messages_per_second:.2f}",
                "",
                "# HELP chat_message_latency_ms Message latency in milliseconds",
                "# TYPE chat_message_latency_ms summary",
                f"chat_message_latency_ms{{quantile=\"0.5\"}} {self.metrics.avg_message_latency_ms:.2f}",
                f"chat_message_latency_ms{{quantile=\"0.95\"}} {self.metrics.p95_message_latency_ms:.2f}",
                f"chat_message_latency_ms{{quantile=\"0.99\"}} {self.metrics.p99_message_latency_ms:.2f}",
                "",
                "# HELP chat_rooms_active Currently active rooms",
                "# TYPE chat_rooms_active gauge",
                f"chat_rooms_active {self.metrics.active_rooms}",
                "",
                "# HELP chat_rate_limited_requests_total Rate limited requests",
                "# TYPE chat_rate_limited_requests_total counter",
                f"chat_rate_limited_requests_total {self.metrics.rate_limited_requests}",
                "",
                "# HELP chat_errors_total Total errors by type",
                "# TYPE chat_errors_total counter",
                f"chat_errors_total{{type=\"protocol\"}} {self.metrics.protocol_errors}",
                f"chat_errors_total{{type=\"timeout\"}} {self.metrics.timeout_errors}",
                f"chat_errors_total{{type=\"disconnect\"}} {self.metrics.disconnect_errors}",
                "",
                "# HELP chat_uptime_seconds Server uptime in seconds",
                "# TYPE chat_uptime_seconds counter",
                f"chat_uptime_seconds {self.metrics.uptime_seconds():.0f}",
                ""
            ]
            
            return "\n".join(lines)
    
    async def get_room_stats(self) -> Dict:
        """Get per-room statistics."""
        async with self._lock:
            return {
                "rooms": {
                    room: {
                        "users": self._room_user_counts.get(room, 0),
                        "messages": self._room_message_counts.get(room, 0)
                    }
                    for room in set(list(self._room_user_counts.keys()) + list(self._room_message_counts.keys()))
                }
            }


# Global metrics instance
_metrics: Optional[MetricsCollector] = None


def get_metrics() -> MetricsCollector:
    """Get global metrics collector."""
    global _metrics
    if _metrics is None:
        _metrics = MetricsCollector()
    return _metrics
