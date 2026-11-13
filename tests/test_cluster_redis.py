"""
Two chat servers sharing one Redis: rooms, usernames, presence and history
across replicas, gap repair after a lost subscriber connection, and
behaviour while Redis is unreachable.

Needs a real Redis; skipped unless CHAT_TEST_REDIS_URL is set, e.g.

    docker run -d --name chat-test-redis -p 127.0.0.1:6390:6379 redis:7.4-alpine \
        redis-server --requirepass testpw
    CHAT_TEST_REDIS_URL=redis://:testpw@127.0.0.1:6390/0 python -m pytest tests/test_cluster_redis.py
"""
import asyncio
import os
import urllib.parse
import uuid

import pytest

from app.server.cluster import RedisCluster, parse_message_id
from tests.support import drain, start_server, stop_server, texts, user, Client

REDIS_URL = os.getenv("CHAT_TEST_REDIS_URL")
pytestmark = pytest.mark.skipif(not REDIS_URL, reason="set CHAT_TEST_REDIS_URL to run against Redis")


def new_cluster(ns, url=None, **kwargs):
    return RedisCluster(url or REDIS_URL, namespace=ns, replica_id=f"r-{uuid.uuid4().hex[:6]}", **kwargs)


@pytest.fixture
async def servers():
    """start(ns, **cluster_kwargs) -> a running server; all stopped at the end."""
    started = []

    async def start(ns, url=None, **cluster_kwargs):
        srv = await start_server(cluster=new_cluster(ns, url, **cluster_kwargs), enable_rate_limiting=False)
        started.append(srv)
        return srv

    yield start
    for srv in started:
        if not srv._task.done():
            await stop_server(srv)


@pytest.fixture
def ns():
    return f"test-{uuid.uuid4().hex[:8]}"


async def until(predicate, timeout=5.0):
    for _ in range(int(timeout / 0.02)):
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


async def http_status(port, path="/health"):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    await writer.drain()
    status = (await asyncio.wait_for(reader.readline(), 3)).split()[1]
    writer.close()
    return int(status)


async def test_rooms_span_replicas(servers, ns):
    a, b = await servers(ns), await servers(ns)
    alice, bob = await user(a.test_port, "alice"), await user(b.test_port, "bob")
    await alice.join("lobby")
    joined = await bob.join("lobby")
    assert joined["members"] == 2, joined                                   # presence counted across replicas
    assert (await alice.recv())["type"] == "USER_JOINED"
    sent = await alice.say("lobby", "hello from A")
    got = await bob.recv()
    assert got["text"] == "hello from A" and got["id"] == sent["id"]
    await bob.say("lobby", "hello from B")
    assert (await alice.recv())["text"] == "hello from B"
    await alice.close()
    await bob.close()


async def test_every_replica_sees_one_global_order(servers, ns):
    a, b = await servers(ns), await servers(ns)
    writers = [await user(a.test_port, "w1"), await user(b.test_port, "w2")]
    readers = [await user(a.test_port, "r1"), await user(b.test_port, "r2")]
    for c in writers + readers:
        await c.join("busy")
    await asyncio.sleep(0.2)
    for c in readers:
        await drain(c)

    async def burst(c, tag):
        for i in range(20):
            await c.send({"type": "MSG", "room": "busy", "text": f"{tag}{i}"})

    await asyncio.gather(burst(writers[0], "a"), burst(writers[1], "b"))
    seen = []
    for c in readers:
        frames = []
        while len(texts(frames)) < 40:
            frames.append(await c.recv())
        seen.append([(f["id"], f["text"]) for f in frames if f.get("type") == "MSG"])
    assert seen[0] == seen[1]                                       # identical order on both replicas
    ids = [parse_message_id(mid) for mid, _ in seen[0]]
    assert ids == sorted(ids) and len(set(ids)) == 40
    for c in writers + readers:
        await c.close()


async def test_usernames_are_unique_across_replicas(servers, ns):
    a, b = await servers(ns), await servers(ns)
    alice = await user(a.test_port, "alice")
    for name in ("alice", "ALICE"):
        c = await Client.open(b.test_port)
        reply = await c.login(username=name)
        assert reply["type"] == "ERROR" and "already in use" in reply["message"]
        await c.close()
    await alice.close()
    # Logging out frees the name everywhere.
    assert await until(lambda: not a._connections)
    again = await user(b.test_port, "alice")
    await again.close()


async def test_a_crashed_replica_frees_its_names_when_the_lease_expires(servers, ns):
    a = await servers(ns, lease_seconds=1.0)
    b = await servers(ns, lease_seconds=1.0)
    crashed_session = await user(a.test_port, "alice")  # keep a reference: GC would close it
    a.cluster._heartbeat.cancel()                   # A "crashes": it stops renewing
    c = await Client.open(b.test_port)
    assert "already in use" in (await c.login(username="alice"))["message"]
    await asyncio.sleep(1.3)
    taken = await user(b.test_port, "alice")
    # When A finally shuts down it must not delete the lease B now holds
    # (release is compare-and-delete).
    await stop_server(a)
    owner = await b.cluster.redis.execute("GET", f"{ns}:user:alice")
    assert owner is not None and owner.decode().startswith(b.cluster.replica_id)
    await taken.close()


async def test_history_survives_a_server_restart(servers, ns):
    a = await servers(ns)
    alice = await user(a.test_port, "alice")
    await alice.join("log")
    for i in range(3):
        await alice.say("log", f"kept{i}")
    await alice.close()
    await stop_server(a)

    fresh = await servers(ns)
    bob = await user(fresh.test_port, "bob")
    await bob.send({"type": "JOIN", "room": "log", "history": 10})
    frames = await bob.recv_until("HISTORY_END")
    assert texts(frames) == ["kept0", "kept1", "kept2"]
    await bob.close()


async def test_messages_missed_during_a_subscriber_drop_are_repaired(servers, ns):
    a, b = await servers(ns), await servers(ns)
    alice, bob = await user(a.test_port, "alice"), await user(b.test_port, "bob")
    await alice.join("r")
    await bob.join("r")
    await drain(alice)
    await alice.say("r", "before")
    assert (await bob.recv())["text"] == "before"

    pubsub = b.cluster.pubsub
    pubsub.hold_reconnect = asyncio.Event()
    pubsub.drop_connection()
    assert await until(lambda: not pubsub.connected)
    for i in range(3):
        await alice.say("r", f"missed{i}")          # B is not subscribed right now
    pubsub.hold_reconnect.set()
    frames = []
    while len(texts(frames)) < 3:
        frames.append(await bob.recv(5))
    await alice.say("r", "after")
    frames += await drain(bob, 0.5)
    assert texts(frames) == ["missed0", "missed1", "missed2", "after"]  # in order, once each
    assert b.cluster.repaired_messages == 3 and pubsub.reconnects == 1
    await alice.close()
    await bob.close()


async def test_a_long_drop_tells_clients_to_resync(servers, ns):
    a = await servers(ns)
    b = await servers(ns, repair_limit=2)
    alice, bob = await user(a.test_port, "alice"), await user(b.test_port, "bob")
    await alice.join("r")
    await bob.join("r")
    await drain(alice)
    pubsub = b.cluster.pubsub
    pubsub.hold_reconnect = asyncio.Event()
    pubsub.drop_connection()
    assert await until(lambda: not pubsub.connected)
    for i in range(5):
        await alice.say("r", f"m{i}")
    pubsub.hold_reconnect.set()
    first = await bob.recv(5)
    assert first == {"type": "RESYNC", "room": "r"}
    await alice.say("r", "live again")
    assert texts(await drain(bob, 0.5)) == ["live again"]
    assert b.cluster.resyncs == 1
    await alice.close()
    await bob.close()


class Cuttable:
    """A TCP relay to Redis that can be cut, to simulate an outage."""

    def __init__(self, host, port):
        self.target = (host, port)
        self.down = False
        self.writers = []

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def _handle(self, reader, writer):
        if self.down:
            writer.transport.abort()
            return
        up_reader, up_writer = await asyncio.open_connection(*self.target)
        self.writers += [writer, up_writer]

        async def pipe(src, dst):
            try:
                while data := await src.read(65536):
                    dst.write(data)
                    await dst.drain()
            except Exception:
                pass
            finally:
                dst.transport.abort()

        await asyncio.gather(pipe(reader, up_writer), pipe(up_reader, writer))

    def cut(self):
        self.down = True
        for w in self.writers:
            w.transport.abort()
        self.writers.clear()

    async def stop(self):
        self.cut()
        self.server.close()
        await self.server.wait_closed()


async def test_redis_outage_is_reported_and_recovers(servers, ns):
    parts = urllib.parse.urlsplit(REDIS_URL)
    relay = await Cuttable(parts.hostname, parts.port or 6379).start()
    via_relay = REDIS_URL.replace(f"{parts.hostname}:{parts.port}", f"127.0.0.1:{relay.port}")
    srv = await servers(ns, url=via_relay)
    alice = await user(srv.test_port, "alice")
    await alice.join("r")
    assert await http_status(srv.metrics_port) == 200

    relay.cut()
    assert await until(lambda: not srv.cluster.healthy)
    assert await http_status(srv.metrics_port) == 503              # load balancers stop sending users here
    await alice.send({"type": "MSG", "room": "r", "text": "lost?"})
    reply = await alice.recv()
    assert reply == {"type": "ERROR", "message": "Message not sent: service temporarily unavailable"}
    assert srv.metrics.metrics.backplane_errors >= 1

    relay.down = False
    assert await until(lambda: srv.cluster.healthy, timeout=10)
    sent = await alice.say("r", "back")
    assert sent["id"]
    assert await http_status(srv.metrics_port) == 200
    await alice.close()
    await relay.stop()
