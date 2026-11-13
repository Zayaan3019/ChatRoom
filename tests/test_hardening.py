"""
Regression tests for the connection lifecycle, limits, validation, metrics,
configuration and the production server's HTTP endpoint.
"""
import asyncio
import time

import pytest

from app.core.config import ServerConfig
from app.core.exceptions import DisconnectError
from app.core.metrics import MetricsCollector, percentile
from app.core.protocol import read_frame, write_frame
from app.core.rate_limiter import RateLimiter, TokenBucket
from app.core.validation import InputValidator
from app.server.connection import ClientConnection
from app.server.enhanced_server import EnhancedChatServer
from app.server.room_manager import RoomManager
from app.server.server import ChatServer


# ---------------------------------------------------------------- helpers

class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


async def connect(port, username):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    await write_frame(writer, {"type": "AUTH", "username": username})
    reply = await asyncio.wait_for(read_frame(reader), 2)
    return reader, writer, reply


async def join(reader, writer, room):
    await write_frame(writer, {"type": "JOIN", "room": room})
    return await asyncio.wait_for(read_frame(reader), 2)


async def expect_eof(reader, timeout=2.0):
    """The server closed this connection."""
    data = await asyncio.wait_for(reader.read(), timeout)
    return data == b""


async def close(writer):
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


@pytest.fixture
async def basic_server():
    srv = ChatServer(host="127.0.0.1", port=0)
    task = asyncio.create_task(srv.start())
    for _ in range(50):
        if srv.server is not None:
            break
        await asyncio.sleep(0.02)
    srv.test_port = srv.server.sockets[0].getsockname()[1]
    yield srv
    await srv.stop()
    try:
        await asyncio.wait_for(task, 3)
    except asyncio.TimeoutError:
        task.cancel()


def make_config(**overrides):
    base = dict(host="127.0.0.1", port=0, metrics_port=0, health_check_interval=0.05,
                shutdown_timeout=3.0)
    base.update(overrides)
    return ServerConfig(**base)


@pytest.fixture
async def prod_server():
    servers = []

    async def start(**overrides):
        srv = EnhancedChatServer(config=make_config(**overrides), metrics=MetricsCollector())
        task = asyncio.create_task(srv.start())
        await asyncio.wait_for(srv.ready.wait(), 3)
        srv.test_port = srv.server.sockets[0].getsockname()[1]
        servers.append((srv, task))
        return srv

    yield start
    for srv, task in servers:
        await srv.stop()
        try:
            await asyncio.wait_for(task, 5)
        except asyncio.TimeoutError:
            task.cancel()


async def http_get(port, path):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(), 3)
    await close(writer)
    head, _, body = raw.partition(b"\r\n\r\n")
    return int(head.split()[1]), body.decode()


# ------------------------------------------------- connection lifecycle

async def test_quit_removes_member_notifies_room_and_closes_socket(basic_server):
    rm = basic_server.room_manager
    ra, wa, _ = await connect(basic_server.test_port, "alice")
    rb, wb, _ = await connect(basic_server.test_port, "bob")
    await join(ra, wa, "lobby")
    await join(rb, wb, "lobby")
    assert (await read_frame(ra))["type"] == "USER_JOINED"

    await write_frame(wa, {"type": "QUIT"})
    assert (await read_frame(ra))["type"] == "BYE"
    assert await expect_eof(ra), "server must close the socket after QUIT"

    left = await asyncio.wait_for(read_frame(rb), 2)
    assert left == {"type": "USER_LEFT", "username": "alice", "room": "lobby"}
    assert [c.username for c in rm.get_room_members("lobby")] == ["bob"]
    assert rm.get_stats()["total_connections"] == 1  # alice's name released
    await close(wb)


async def test_abrupt_disconnect_leaves_no_ghost(basic_server):
    rm = basic_server.room_manager
    ra, wa, _ = await connect(basic_server.test_port, "carol")
    await join(ra, wa, "r1")
    wa.transport.abort()  # vanish without QUIT
    for _ in range(100):
        if not rm.get_room_members("r1"):
            break
        await asyncio.sleep(0.02)
    assert not rm.get_room_members("r1")
    assert len(rm) == 0


class BrokenWriter:
    """A StreamWriter whose peer has gone away."""

    def __init__(self, error=ConnectionResetError, hang=False):
        self.error, self.hang = error, hang
        self.transport = self
        self.aborted = False

    def write(self, data):
        pass

    async def drain(self):
        if self.hang:
            await asyncio.sleep(3600)
        raise self.error()

    def abort(self):
        self.aborted = True

    def is_closing(self):
        return self.aborted

    def close(self):
        self.aborted = True

    async def wait_closed(self):
        return None

    def get_extra_info(self, name):
        return ("10.0.0.9", 1)


async def test_failed_send_detaches_member_immediately():
    rm = RoomManager()
    metrics = MetricsCollector()
    dead = ClientConnection(None, BrokenWriter(), rm, ("10.0.0.9", 1), metrics=metrics,
                            config=ServerConfig())
    dead.username = "dead"
    rm.join("r", dead)
    sender = ClientConnection(None, BrokenWriter(), rm, ("10.0.0.8", 1), metrics=metrics,
                              config=ServerConfig())
    sender.username = "sender"

    delivered = await rm.broadcast("r", sender, {"type": "MSG", "text": "hi"}, exclude_sender=True)
    assert delivered == 0
    assert not rm.is_member("r", dead), "a failed send must remove the member at once"
    assert dead.writer.aborted and dead.closed

    await dead.close()  # the read loop's finally: must still clean up
    await dead.close()  # and be idempotent
    assert len(rm) == 0


async def test_slow_client_is_disconnected_without_stalling_broadcast():
    rm = RoomManager()
    metrics = MetricsCollector()
    cfg = ServerConfig(write_timeout=0.2)
    slow = ClientConnection(None, BrokenWriter(hang=True), rm, ("10.0.0.9", 1), metrics=metrics, config=cfg)
    slow.username = "slow"
    rm.join("r", slow)

    sent = []

    class Fast:
        username = "fast"

        async def send(self, message):
            sent.append(message)

    fast = Fast()
    rm.join("r", fast)
    started = time.perf_counter()
    delivered = await rm.broadcast("r", None, {"type": "MSG", "text": "x"}, exclude_sender=False)
    assert time.perf_counter() - started < 1.5
    assert delivered == 1 and sent  # the fast member still got it
    assert not rm.is_member("r", slow)
    assert metrics.metrics.slow_client_disconnects == 1


# ----------------------------------------------------- protocol commands

async def test_duplicate_username_rejected_until_released(basic_server):
    ra, wa, ok = await connect(basic_server.test_port, "dana")
    assert ok["type"] == "AUTH_OK"
    rb, wb, err = await connect(basic_server.test_port, "DANA")
    assert err["type"] == "ERROR" and "already in use" in err["message"]
    assert await expect_eof(rb)
    await close(wb)

    await close(wa)
    for _ in range(100):
        if basic_server.room_manager.get_stats()["total_connections"] == 0:
            break
        await asyncio.sleep(0.02)
    rc, wc, ok = await connect(basic_server.test_port, "dana")
    assert ok["type"] == "AUTH_OK"
    await close(wc)


async def test_leave_of_unjoined_room_is_refused_and_not_broadcast(basic_server):
    ra, wa, _ = await connect(basic_server.test_port, "erin")
    rb, wb, _ = await connect(basic_server.test_port, "frank")
    await join(ra, wa, "secret")

    await write_frame(wb, {"type": "LEAVE", "room": "secret"})
    reply = await read_frame(rb)
    assert reply["type"] == "ERROR" and "not in room" in reply["message"]
    with pytest.raises(asyncio.TimeoutError):  # erin hears nothing
        await asyncio.wait_for(read_frame(ra), 0.3)
    await close(wa)
    await close(wb)


async def test_join_is_idempotent(basic_server):
    ra, wa, _ = await connect(basic_server.test_port, "gus")
    rb, wb, _ = await connect(basic_server.test_port, "hal")
    await join(ra, wa, "r")
    await join(rb, wb, "r")
    assert (await read_frame(ra))["username"] == "hal"
    again = await join(rb, wb, "r")
    assert again == {"type": "JOINED", "room": "r", "members": 2}
    with pytest.raises(asyncio.TimeoutError):  # no second USER_JOINED
        await asyncio.wait_for(read_frame(ra), 0.3)
    await close(wa)
    await close(wb)


async def test_malformed_fields_get_errors_not_crashes(basic_server):
    ra, wa, _ = await connect(basic_server.test_port, "ivy")
    await write_frame(wa, {"type": "JOIN", "room": 42})
    assert (await read_frame(ra))["type"] == "ERROR"
    await write_frame(wa, {"type": ["MSG"]})
    assert (await read_frame(ra))["type"] == "ERROR"
    await write_frame(wa, {"type": "MSG", "room": "r", "text": None})
    assert (await read_frame(ra))["type"] == "ERROR"
    # still connected and working
    assert (await join(ra, wa, "r"))["type"] == "JOINED"
    await close(wa)


async def test_ping_gets_pong(basic_server):
    ra, wa, _ = await connect(basic_server.test_port, "pinger")
    await write_frame(wa, {"type": "PING"})
    assert (await read_frame(ra)) == {"type": "PONG"}
    await close(wa)


async def test_non_object_frame_is_a_protocol_error(basic_server):
    ra, wa, _ = await connect(basic_server.test_port, "jo")
    body = b"[1, 2, 3]"
    wa.write(len(body).to_bytes(4, "big") + body)
    await wa.drain()
    reply = await read_frame(ra)
    assert reply["type"] == "ERROR" and "JSON object" in reply["message"]
    assert await expect_eof(ra)
    await close(wa)


def test_default_frame_limit_is_small_but_fits_the_longest_message():
    cfg = ServerConfig()
    assert cfg.max_frame_size == 128 * 1024
    import json
    worst = json.dumps({"type": "MSG", "room": "r" * cfg.max_room_name_length,
                        "text": "\U0001F600" * cfg.max_message_length,
                        "username": "u" * cfg.max_username_length, "timestamp": "x" * 32},
                       separators=(",", ":"))
    assert len(worst.encode()) < cfg.max_frame_size


# ----------------------------------------------------------- rate limits

async def test_per_ip_bucket_spans_usernames_and_refusals_cost_nothing():
    clock = FakeClock()
    rl = RateLimiter(messages_per_minute=60, burst=2, messages_per_minute_per_ip=60, clock=clock)
    # The IP bucket's burst equals the user's here (same rate): 2 per IP.
    assert await rl.check_message("a", "1.2.3.4")
    assert await rl.check_message("b", "1.2.3.4")
    assert not await rl.check_message("c", "1.2.3.4"), "new usernames must not reset the IP bucket"
    # The refusal above must not have spent c's own token.
    assert await rl.check_message("c", "5.6.7.8")
    assert await rl.check_message("c", "5.6.7.8")
    assert not await rl.check_message("c", "5.6.7.8")
    clock.now += 1.0  # 1 token/s refills
    assert await rl.check_message("c", "5.6.7.8")


async def test_idle_buckets_are_pruned_without_resetting_active_ones():
    clock = FakeClock()
    rl = RateLimiter(messages_per_minute=60, burst=3, clock=clock)
    for user in ("u1", "u2"):
        await rl.check_message(user, "9.9.9.9")
    await rl.check_message("u2", "9.9.9.9")
    clock.now += 1.5  # u1 (1 spent) is full again; u2 (2 spent) is not
    dropped = rl.prune()
    assert dropped >= 1
    stats = rl.get_stats()
    assert stats["tracked_users"] == 1  # only u2 remains
    assert TokenBucket(1.0, 3, clock).is_full()


async def test_server_enforces_per_user_limit(prod_server):
    srv = await prod_server(rate_limit_burst=3, max_messages_per_minute=6)
    r, w, _ = await connect(srv.test_port, "spammer")
    await join(r, w, "r")
    results = []
    for i in range(5):
        await write_frame(w, {"type": "MSG", "room": "r", "text": f"m{i}"})
        results.append((await read_frame(r))["type"])
    assert results == ["MSG", "MSG", "MSG", "ERROR", "ERROR"]
    status, body = await http_get(srv.metrics_port, "/metrics")
    assert "chat_rate_limited_requests_total 2" in body
    await close(w)


# ------------------------------------------------------------ validation

@pytest.mark.parametrize("payload", [
    "<script>alert(1)</script>", "<img src=x onerror=alert(1)>", "&#x3C;script>",
    "jav&#x09;ascript:alert(1)", "＜script＞", "<svg onload=alert(1)>",
    "java​script:alert(1)", "<a href=data:text/html;base64,xx>", "&amp;lt;script&amp;gt;",
])
def test_xss_payloads_rejected(payload):
    ok, _ = InputValidator.validate_message_text(payload)
    assert not ok


@pytest.mark.parametrize("text", [
    "money = 5", "x < y and y > z", "I love javascript", "online=true", "<3 you",
    "if a<b then c=d", "formula: x = y",
])
def test_ordinary_text_allowed(text):
    ok, _ = InputValidator.validate_message_text(text)
    assert ok


# --------------------------------------------------------------- metrics

async def test_percentiles_are_real_and_rooms_gauge_goes_down():
    m = MetricsCollector()
    for latency in [1, 1, 1, 1, 100]:
        await m.record_message(latency)
    snap = await m.get_metrics()
    assert snap.p50_message_latency_ms == 1  # the median, not the mean (20.8)
    assert snap.p99_message_latency_ms == 100
    assert percentile([1, 2, 3, 4], 0.5) == 2

    await m.record_room_metrics("a", 2)
    await m.record_room_metrics("b", 1)
    assert (await m.get_metrics()).active_rooms == 2
    await m.record_room_metrics("b", 0)
    assert (await m.get_metrics()).active_rooms == 1

    text = await m.get_prometheus_format()
    assert "chat_message_latency_ms_count 5" in text
    assert "chat_message_latency_ms_sum 104.000" in text


# ---------------------------------------------------------- configuration

def test_config_reads_chat_prefixed_names_with_legacy_fallback(monkeypatch):
    monkeypatch.setenv("CHAT_PORT", "7000")
    monkeypatch.setenv("CHAT_SERVER_HOST", "127.0.0.2")  # legacy compose name
    monkeypatch.setenv("CHAT_MAX_FRAME_SIZE", "65536  # 64 KB")
    monkeypatch.setenv("MAX_CONNECTIONS", "123")  # legacy unprefixed name
    monkeypatch.setenv("CHAT_ENABLE_RATE_LIMITING", "false")
    cfg = ServerConfig.from_env()
    assert (cfg.port, cfg.host, cfg.max_frame_size, cfg.max_connections) == (7000, "127.0.0.2", 65536, 123)
    assert cfg.enable_rate_limiting is False

    monkeypatch.setenv("CHAT_MAX_CONNECTIONS", "lots")
    with pytest.raises(ValueError, match="CHAT_MAX_CONNECTIONS"):
        ServerConfig.from_env()


# ---------------------------------------------------- production server

async def test_http_endpoints_and_rooms_gauge(prod_server):
    srv = await prod_server()
    r, w, _ = await connect(srv.test_port, "kim")
    await join(r, w, "metrics-room")

    status, body = await http_get(srv.metrics_port, "/health")
    assert status == 200 and "healthy" in body
    status, body = await http_get(srv.metrics_port, "/metrics")
    assert status == 200 and "chat_rooms_active 1" in body and "chat_connections_active 1" in body
    status, _ = await http_get(srv.metrics_port, "/metricsfoo")
    assert status == 404
    status, body = await http_get(srv.metrics_port, "/stats")
    assert status == 200 and '"active_connections": 1' in body

    await close(w)
    for _ in range(100):
        _, body = await http_get(srv.metrics_port, "/metrics")
        if "chat_rooms_active 0" in body:
            break
        await asyncio.sleep(0.02)
    assert "chat_rooms_active 0" in body and "chat_connections_active 0" in body


async def test_per_ip_connection_cap(prod_server):
    srv = await prod_server(max_connections_per_ip=2)
    conns = [await connect(srv.test_port, f"c{i}") for i in range(2)]
    reader, writer = await asyncio.open_connection("127.0.0.1", srv.test_port)
    assert await expect_eof(reader), "third connection from one IP is refused"
    await close(writer)
    for _, w, _ in conns:
        await close(w)


async def test_shutdown_is_prompt_with_idle_clients(prod_server):
    srv = await prod_server()
    clients = [await connect(srv.test_port, f"idle{i}") for i in range(5)]
    started = time.perf_counter()
    await srv.stop()
    assert time.perf_counter() - started < 3.0  # not the 300 s idle timeout
    for reader, writer, _ in clients:
        frame = await asyncio.wait_for(read_frame(reader), 2)
        assert frame["type"] == "BYE"
        await close(writer)
