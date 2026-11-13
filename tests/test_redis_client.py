"""
The Redis client's wire protocol and failure handling, against an in-process
fake server (no Redis needed). tests/test_cluster_redis.py runs against a real
Redis.
"""
import asyncio

import pytest

from app.core.redis_client import (PubSub, Redis, RedisConnectionError, RedisError, Script,
                                   encode_command, parse_url, read_reply)


def reader_with(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


def test_encode_command():
    assert encode_command(["SET", "k", b"v\r\n", 5, 1.5]) == (
        b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$3\r\nv\r\n\r\n$1\r\n5\r\n$3\r\n1.5\r\n")
    assert encode_command(["ECHO", "é"]) == b"*2\r\n$4\r\nECHO\r\n$2\r\n\xc3\xa9\r\n"
    with pytest.raises(TypeError):
        encode_command(["SET", "k", None])
    with pytest.raises(TypeError):
        encode_command(["SET", "k", True])


async def test_read_reply_types():
    data = (b"+OK\r\n-ERR boom\r\n:42\r\n$5\r\nhe\r\nl\r\n$-1\r\n*-1\r\n"
            b"*3\r\n:1\r\n-NOSCRIPT x\r\n*1\r\n$1\r\na\r\n")
    reader = reader_with(data)
    assert await read_reply(reader) == "OK"
    err = await read_reply(reader)
    assert isinstance(err, RedisError) and str(err) == "ERR boom"   # returned, not raised
    assert await read_reply(reader) == 42
    assert await read_reply(reader) == b"he\r\nl"                    # binary-safe bulk string
    assert await read_reply(reader) is None
    assert await read_reply(reader) is None
    nested = await read_reply(reader)
    assert nested[0] == 1 and isinstance(nested[1], RedisError) and nested[2] == [b"a"]
    with pytest.raises(RedisConnectionError):
        await read_reply(reader)                                    # EOF
    with pytest.raises(RedisConnectionError):
        await read_reply(reader_with(b"?huh\r\n"))
    with pytest.raises(RedisConnectionError):
        await read_reply(reader_with(b"+no line end"))


def test_parse_url():
    u = parse_url("rediss://user:p%40ss@cache.internal:6380/2")
    assert (u.host, u.port, u.db, u.username, u.password, u.tls) == ("cache.internal", 6380, 2, "user", "p@ss", True)
    u = parse_url("redis://:secret@redis")
    assert (u.port, u.db, u.username, u.password, u.tls) == (6379, 0, None, "secret", False)
    with pytest.raises(ValueError):
        parse_url("http://redis")


class FakeRedis:
    """Speaks just enough RESP: PING, ECHO, AUTH, EVALSHA (NOSCRIPT), EVAL,
    HANG (never answers), SUBSCRIBE and PUBLISH."""

    def __init__(self, password=None):
        self.password = password
        self.commands = []
        self.subscribers = {}
        self.server = None
        self.connections = []

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        for w in self.connections:
            w.transport.abort()
        self.server.close()
        await self.server.wait_closed()

    def kick_all(self):
        for w in self.connections:
            w.transport.abort()
        self.connections.clear()

    async def _handle(self, reader, writer):
        self.connections.append(writer)
        authed = self.password is None
        try:
            while True:
                command = await read_reply(reader)
                name = command[0].decode().upper()
                args = command[1:]
                self.commands.append([name] + [a.decode() for a in args])
                if name == "AUTH":
                    authed = args[-1].decode() == self.password
                    writer.write(b"+OK\r\n" if authed else b"-WRONGPASS invalid\r\n")
                elif not authed:
                    writer.write(b"-NOAUTH Authentication required.\r\n")
                elif name == "PING":
                    writer.write(b"+PONG\r\n")
                elif name == "ECHO":
                    writer.write(b"$%d\r\n%s\r\n" % (len(args[0]), args[0]))
                elif name == "EVALSHA":
                    writer.write(b"-NOSCRIPT No matching script.\r\n")
                elif name == "EVAL":
                    writer.write(b":7\r\n")
                elif name == "HANG":
                    pass
                elif name == "SUBSCRIBE":
                    for ch in args:
                        self.subscribers.setdefault(ch, []).append(writer)
                        writer.write(b"*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:1\r\n" % (len(ch), ch))
                elif name == "PUBLISH":
                    ch, data = args
                    for sub in self.subscribers.get(ch, []):
                        if not sub.is_closing():
                            sub.write(b"*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n"
                                      % (len(ch), ch, len(data), data))
                    writer.write(b":1\r\n")
                else:
                    writer.write(b"-ERR unknown command\r\n")
                await writer.drain()
        except (RedisConnectionError, ConnectionError, asyncio.IncompleteReadError):
            pass


@pytest.fixture
async def fake():
    server = await FakeRedis(password="pw").start()
    yield server
    await server.stop()


async def test_pipelined_replies_stay_in_order(fake):
    redis = Redis(f"redis://:pw@127.0.0.1:{fake.port}")
    results = await asyncio.gather(*(redis.execute("ECHO", f"v{i}") for i in range(200)))
    assert results == [f"v{i}".encode() for i in range(200)]
    replies = await redis.pipeline([["PING"], ["NOPE"], ["ECHO", "x"]])
    assert replies[0] == "PONG" and isinstance(replies[1], RedisError) and replies[2] == b"x"
    with pytest.raises(RedisError, match="unknown command"):
        await redis.execute("NOPE")
    assert fake.commands[0] == ["AUTH", "pw"]                       # password sent on connect
    await redis.close()


async def test_wrong_password_and_unreachable_server_fail_cleanly(fake):
    redis = Redis(f"redis://:wrong@127.0.0.1:{fake.port}")
    with pytest.raises(RedisError, match="WRONGPASS"):
        await redis.execute("PING")
    await redis.close()
    unused = Redis("redis://127.0.0.1:1", connect_timeout=0.5)
    with pytest.raises(RedisConnectionError):
        await unused.execute("PING")
    await unused.close()


async def test_a_hung_server_times_out_and_the_client_reconnects(fake):
    redis = Redis(f"redis://:pw@127.0.0.1:{fake.port}", command_timeout=0.3)
    assert await redis.ping()
    behind = asyncio.ensure_future(redis.execute("PING"))
    with pytest.raises(RedisConnectionError, match="timed out"):
        await asyncio.gather(redis.execute("HANG"), behind)
    assert not redis.connected
    assert await redis.ping()                                        # a fresh connection
    await redis.close()


async def test_script_falls_back_to_eval_after_noscript(fake):
    redis = Redis(f"redis://:pw@127.0.0.1:{fake.port}")
    script = Script("return 7")
    assert await script(redis, ["k"], ["a"]) == 7
    names = [c[0] for c in fake.commands]
    assert names[-2:] == ["EVALSHA", "EVAL"] and fake.commands[-1][1] == "return 7"
    await redis.close()


async def test_subscriber_reconnects_resubscribes_and_reports_it(fake):
    got, events = [], []
    resubscribed = asyncio.Event()

    async def on_disconnect():
        events.append("down")

    async def on_resubscribed():
        events.append("up")
        resubscribed.set()

    sub = PubSub(f"redis://:pw@127.0.0.1:{fake.port}", lambda ch, data: got.append((ch, data)),
                 on_disconnect=on_disconnect, on_resubscribed=on_resubscribed)
    await sub.start(wait=2)
    await sub.subscribe("room:a")
    publisher = Redis(f"redis://:pw@127.0.0.1:{fake.port}")
    await publisher.execute("PUBLISH", "room:a", "one")

    sub.hold_reconnect = asyncio.Event()
    sub.drop_connection()
    for _ in range(100):
        if events:
            break
        await asyncio.sleep(0.01)
    assert events == ["down"] and not sub.connected
    sub.hold_reconnect.set()
    await asyncio.wait_for(resubscribed.wait(), 3)
    await publisher.execute("PUBLISH", "room:a", "two")
    for _ in range(100):
        if len(got) == 2:
            break
        await asyncio.sleep(0.01)
    assert got == [("room:a", b"one"), ("room:a", b"two")] and events == ["down", "up"]
    assert sub.reconnects == 1
    await publisher.close()
    await sub.close()
