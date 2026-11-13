"""
Metrics and Observability - Prometheus text exposition.

Recording a message is O(1): the latency goes into a bounded ring buffer and a
timestamp into a deque. Percentiles and rates are computed only when /metrics
is scraped, not on every message.
"""
import math
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Deque, Dict, Optional, Sequence


@dataclass
class Metrics:
    """Point-in-time snapshot of the server's metrics."""

    # Connections
    total_connections: int = 0
    active_connections: int = 0
    failed_connections: int = 0
    connections_rate: float = 0.0

    # Messages
    total_messages: int = 0
    messages_per_second: float = 0.0
    failed_messages: int = 0

    # Rooms
    active_rooms: int = 0

    # Latency (handling time of a MSG command, ms), over the recent window
    p50_message_latency_ms: float = 0.0
    p95_message_latency_ms: float = 0.0
    p99_message_latency_ms: float = 0.0
    avg_message_latency_ms: float = 0.0

    # Rate limiting and errors
    rate_limited_requests: int = 0
    protocol_errors: int = 0
    timeout_errors: int = 0
    disconnect_errors: int = 0
    slow_client_disconnects: int = 0
    auth_failures: int = 0
    backplane_errors: int = 0  # operations refused because Redis was unreachable
    history_messages_sent: int = 0

    server_start_time: datetime = field(default_factory=datetime.now)

    def uptime_seconds(self) -> float:
        return (datetime.now() - self.server_start_time).total_seconds()


def percentile(sorted_values: Sequence[float], q: float) -> float:
    """Nearest-rank percentile of already-sorted values (q in [0, 1])."""
    if not sorted_values:
        return 0.0
    rank = max(1, math.ceil(q * len(sorted_values)))
    return float(sorted_values[min(rank, len(sorted_values)) - 1])


class MetricsCollector:
    """
    Collects server metrics. All methods run on the event loop and none of
    them await internally, so updates are atomic without locks. They stay
    `async` so existing callers can keep awaiting them.
    """

    LATENCY_SAMPLES = 10000  # ring buffer for percentiles
    RATE_WINDOW_SECONDS = 60.0

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self.metrics = Metrics()
        self._latencies: Deque[float] = deque(maxlen=self.LATENCY_SAMPLES)
        self._latency_sum_ms = 0.0  # all-time, for the summary's _sum
        self._message_times: Deque[float] = deque()
        self._connection_times: Deque[float] = deque()
        self._room_message_counts: Dict[str, int] = {}
        self._room_user_counts: Dict[str, int] = {}
        # When set, the live room count comes from the room manager itself,
        # which is the source of truth.
        self._rooms_provider: Optional[Callable[[], int]] = None
        self._cluster_provider: Optional[Callable[[], Dict]] = None

    # ---- recording -------------------------------------------------------

    def set_rooms_provider(self, provider: Callable[[], int]) -> None:
        self._rooms_provider = provider

    def set_cluster_provider(self, provider: Callable[[], Dict]) -> None:
        self._cluster_provider = provider

    async def increment_connections(self) -> None:
        self.metrics.total_connections += 1
        self.metrics.active_connections += 1
        self._connection_times.append(self._clock())

    async def decrement_connections(self) -> None:
        self.metrics.active_connections = max(0, self.metrics.active_connections - 1)

    async def increment_failed_connections(self) -> None:
        self.metrics.failed_connections += 1

    async def record_message(self, latency_ms: float) -> None:
        self.metrics.total_messages += 1
        self._message_times.append(self._clock())
        self._latencies.append(latency_ms)
        self._latency_sum_ms += latency_ms

    async def increment_failed_messages(self) -> None:
        self.metrics.failed_messages += 1

    async def record_room_metrics(self, room_name: str, user_count: int) -> None:
        """Record a room's member count after a join or leave. Empty rooms are
        forgotten, so the gauge goes down and the map cannot grow forever."""
        if user_count > 0:
            self._room_user_counts[room_name] = user_count
        else:
            self._room_user_counts.pop(room_name, None)
            self._room_message_counts.pop(room_name, None)
        self.metrics.active_rooms = len(self._room_user_counts)

    async def increment_room_message(self, room_name: str) -> None:
        self._room_message_counts[room_name] = self._room_message_counts.get(room_name, 0) + 1

    async def increment_rate_limited(self) -> None:
        self.metrics.rate_limited_requests += 1

    async def increment_protocol_errors(self) -> None:
        self.metrics.protocol_errors += 1

    async def increment_timeout_errors(self) -> None:
        self.metrics.timeout_errors += 1

    async def increment_disconnect_errors(self) -> None:
        self.metrics.disconnect_errors += 1

    async def increment_slow_client_disconnects(self) -> None:
        self.metrics.slow_client_disconnects += 1

    async def increment_auth_failures(self) -> None:
        self.metrics.auth_failures += 1

    async def increment_backplane_errors(self) -> None:
        self.metrics.backplane_errors += 1

    async def add_history_sent(self, count: int) -> None:
        self.metrics.history_messages_sent += count

    # ---- reading ---------------------------------------------------------

    @staticmethod
    def _rate(times: Deque[float], now: float, window: float) -> float:
        while times and times[0] <= now - window:
            times.popleft()
        if not times:
            return 0.0
        # Divide by the full window once it has filled, so a single event
        # does not read as an enormous instantaneous rate.
        span = max(now - times[0], min(window, 1.0))
        return len(times) / span

    def _refresh(self) -> Metrics:
        now = self._clock()
        m = self.metrics
        m.messages_per_second = self._rate(self._message_times, now, self.RATE_WINDOW_SECONDS)
        m.connections_rate = self._rate(self._connection_times, now, self.RATE_WINDOW_SECONDS)
        ordered = sorted(self._latencies)
        m.p50_message_latency_ms = percentile(ordered, 0.50)
        m.p95_message_latency_ms = percentile(ordered, 0.95)
        m.p99_message_latency_ms = percentile(ordered, 0.99)
        m.avg_message_latency_ms = (sum(ordered) / len(ordered)) if ordered else 0.0
        if self._rooms_provider is not None:
            m.active_rooms = self._rooms_provider()
        return m

    async def get_metrics(self) -> Metrics:
        return self._refresh()

    async def get_prometheus_format(self) -> str:
        m = self._refresh()

        def block(name: str, kind: str, help_text: str, *samples: str) -> list:
            return [f"# HELP {name} {help_text}", f"# TYPE {name} {kind}", *samples]

        lines = []
        lines += block("chat_connections_total", "counter", "Connections accepted.",
                       f"chat_connections_total {m.total_connections}")
        lines += block("chat_connections_active", "gauge", "Currently open connections.",
                       f"chat_connections_active {m.active_connections}")
        lines += block("chat_connections_failed_total", "counter", "Connections refused by limits.",
                       f"chat_connections_failed_total {m.failed_connections}")
        lines += block("chat_messages_total", "counter", "Chat messages broadcast.",
                       f"chat_messages_total {m.total_messages}")
        lines += block("chat_messages_failed_total", "counter", "Chat messages refused.",
                       f"chat_messages_failed_total {m.failed_messages}")
        lines += block("chat_messages_per_second", "gauge", "Message rate over the last minute.",
                       f"chat_messages_per_second {m.messages_per_second:.3f}")
        lines += block(
            "chat_message_latency_ms", "summary",
            "Time to handle a MSG command including the broadcast, in ms "
            f"(quantiles over the last {self.LATENCY_SAMPLES} messages).",
            f'chat_message_latency_ms{{quantile="0.5"}} {m.p50_message_latency_ms:.3f}',
            f'chat_message_latency_ms{{quantile="0.95"}} {m.p95_message_latency_ms:.3f}',
            f'chat_message_latency_ms{{quantile="0.99"}} {m.p99_message_latency_ms:.3f}',
            f"chat_message_latency_ms_sum {self._latency_sum_ms:.3f}",
            f"chat_message_latency_ms_count {m.total_messages}",
        )
        lines += block("chat_rooms_active", "gauge", "Rooms with at least one member.",
                       f"chat_rooms_active {m.active_rooms}")
        lines += block("chat_rate_limited_requests_total", "counter", "Requests refused by rate limits.",
                       f"chat_rate_limited_requests_total {m.rate_limited_requests}")
        lines += block(
            "chat_errors_total", "counter", "Connection errors by type.",
            f'chat_errors_total{{type="protocol"}} {m.protocol_errors}',
            f'chat_errors_total{{type="timeout"}} {m.timeout_errors}',
            f'chat_errors_total{{type="disconnect"}} {m.disconnect_errors}',
            f'chat_errors_total{{type="slow_client"}} {m.slow_client_disconnects}',
            f'chat_errors_total{{type="auth"}} {m.auth_failures}',
            f'chat_errors_total{{type="backplane"}} {m.backplane_errors}',
        )
        lines += block("chat_history_messages_sent_total", "counter",
                       "Stored messages replayed to clients (JOIN, resume, HISTORY).",
                       f"chat_history_messages_sent_total {m.history_messages_sent}")
        if self._cluster_provider is not None:
            c = self._cluster_provider()
            lines += block("chat_backplane_up", "gauge",
                           "1 when the shared backend (Redis) is reachable, or in single-process mode.",
                           f"chat_backplane_up {1 if c.get('healthy') else 0}")
            if c.get("mode") == "redis":
                lines += block("chat_backplane_reconnects_total", "counter",
                               "Subscriber reconnections to Redis.",
                               f"chat_backplane_reconnects_total {c.get('subscriber_reconnects', 0)}")
                lines += block("chat_backplane_repaired_messages_total", "counter",
                               "Messages recovered from the stream after a subscriber gap.",
                               f"chat_backplane_repaired_messages_total {c.get('repaired_messages', 0)}")
                lines += block("chat_backplane_resyncs_total", "counter",
                               "Gaps too long to replay; clients were told to resync.",
                               f"chat_backplane_resyncs_total {c.get('resyncs', 0)}")
        lines += block("chat_uptime_seconds", "gauge", "Seconds since the server started.",
                       f"chat_uptime_seconds {m.uptime_seconds():.0f}")
        return "\n".join(lines) + "\n"

    async def get_room_stats(self) -> Dict:
        rooms = set(self._room_user_counts) | set(self._room_message_counts)
        return {
            "rooms": {
                room: {
                    "users": self._room_user_counts.get(room, 0),
                    "messages": self._room_message_counts.get(room, 0),
                }
                for room in rooms
            }
        }


_metrics: Optional[MetricsCollector] = None


def get_metrics() -> MetricsCollector:
    """Process-wide metrics collector."""
    global _metrics
    if _metrics is None:
        _metrics = MetricsCollector()
    return _metrics


def reset_metrics() -> MetricsCollector:
    """Fresh collector (servers and tests that need isolated counters)."""
    global _metrics
    _metrics = MetricsCollector()
    return _metrics
