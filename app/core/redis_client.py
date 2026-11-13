"""
A small asyncio Redis client (RESP2), standard library only.

The chat server needs a handful of commands -- SET/GET/DEL, sorted sets,
streams, Lua scripts and PUBLISH -- plus a subscriber connection. Pulling in
a third-party client for that would make the server's only dependency a
network library, so this module speaks the protocol directly.

Two connection types:

* ``Redis``   -- request/response. Commands are pipelined on one socket:
                 Redis answers in order, so a FIFO of futures matches each
                 reply to its command. A broken socket fails every pending
                 command and the next command reconnects.
* ``PubSub``  -- a dedicated subscriber socket (Redis forbids normal
                 commands on it). It reconnects by itself with backoff,
                 re-subscribes every channel, and reports both events so the
                 caller can repair anything published while it was away.
"""
import asyncio
import collections
import hashlib
import logging
import ssl
import urllib.parse
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Sequence, Set

logger = logging.getLogger(__name__)


class RedisError(Exception):
    """An error reply from the server (e.g. WRONGTYPE, NOSCRIPT)."""


class RedisConnectionError(RedisError):
    """The connection failed, closed, or timed out."""


# --------------------------------------------------------------------- URLs

@dataclass(frozen=True)
class RedisURL:
    host: str = "127.0.0.1"
    port: int = 6379
    db: int = 0
    username: Optional[str] = None
    password: Optional[str] = None
    tls: bool = False


def parse_url(url: str) -> RedisURL:
    """redis://[[user]:password@]host[:port][/db]   (rediss:// = TLS)."""
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("redis", "rediss"):
        raise ValueError(f"Unsupported Redis URL scheme: {parts.scheme!r}")
    db_text = parts.path.lstrip("/")
    return RedisURL(
        host=parts.hostname or "127.0.0.1",
        port=parts.port or 6379,
        db=int(db_text) if db_text else 0,
        username=urllib.parse.unquote(parts.username) if parts.username else None,
        password=urllib.parse.unquote(parts.password) if parts.password else None,
        tls=parts.scheme == "rediss",
    )


# ----------------------------------------------------------------- protocol

def encode_command(args: Sequence[Any]) -> bytes:
    """One command as a RESP array of bulk strings."""
    out = [b"*%d\r\n" % len(args)]
    for arg in args:
        if isinstance(arg, bytes):
            data = arg
        elif isinstance(arg, str):
            data = arg.encode("utf-8")
        elif isinstance(arg, (int, float)) and not isinstance(arg, bool):
            data = str(arg).encode("ascii")
        else:
            raise TypeError(f"Unsupported Redis argument type: {type(arg).__name__}")
        out.append(b"$%d\r\n" % len(data))
        out.append(data)
        out.append(b"\r\n")
    return b"".join(out)


async def read_reply(reader: asyncio.StreamReader) -> Any:
    """Read one RESP2 reply.

    Error replies are *returned* as RedisError instances rather than raised,
    because they can appear inside arrays (e.g. a script's result); the
    caller decides whether to raise.
    """
    line = await reader.readline()
    if not line:
        raise RedisConnectionError("Connection closed by Redis")
    if not line.endswith(b"\r\n"):
        raise RedisConnectionError("Truncated reply from Redis")
    kind, body = line[:1], line[1:-2]
    if kind == b"+":
        return body.decode("utf-8")
    if kind == b"-":
        return RedisError(body.decode("utf-8", "replace"))
    if kind == b":":
        return int(body)
    if kind == b"$":
        size = int(body)
        if size < 0:
            return None
        data = await reader.readexactly(size + 2)
        return data[:-2]
    if kind == b"*":
        count = int(body)
        if count < 0:
            return None
        return [await read_reply(reader) for _ in range(count)]
    raise RedisConnectionError(f"Unexpected reply type {kind!r} from Redis")


def _raise_if_error(reply: Any) -> Any:
    if isinstance(reply, RedisError):
        raise reply
    return reply


async def _open(url: RedisURL, timeout: float, ssl_context: Optional[ssl.SSLContext]):
    ctx = None
    if url.tls:
        ctx = ssl_context or ssl.create_default_context()
    try:
        return await asyncio.wait_for(
            asyncio.open_connection(url.host, url.port, ssl=ctx), timeout=timeout
        )
    except (OSError, asyncio.TimeoutError) as e:
        raise RedisConnectionError(f"Cannot connect to Redis at {url.host}:{url.port}: {e!r}") from e


def _handshake_commands(url: RedisURL, select_db: bool = True) -> List[List[Any]]:
    commands: List[List[Any]] = []
    if url.password is not None:
        if url.username:
            commands.append(["AUTH", url.username, url.password])
        else:
            commands.append(["AUTH", url.password])
    if select_db and url.db:
        commands.append(["SELECT", url.db])
    return commands


# --------------------------------------------------------- command client

class Redis:
    """Pipelined request/response client. Safe to share between tasks."""

    def __init__(
        self,
        url: str,
        *,
        connect_timeout: float = 3.0,
        command_timeout: float = 5.0,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        self.url = parse_url(url)
        self.connect_timeout = connect_timeout
        self.command_timeout = command_timeout
        self._ssl_context = ssl_context
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._pending: Deque[asyncio.Future] = collections.deque()
        self._reader_task: Optional[asyncio.Task] = None
        self._connect_lock = asyncio.Lock()
        self._closed = False

    @property
    def connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    async def _ensure_connected(self) -> None:
        if self.connected:
            return
        if self._closed:
            raise RedisConnectionError("Client is closed")
        async with self._connect_lock:
            if self.connected:
                return
            reader, writer = await _open(self.url, self.connect_timeout, self._ssl_context)
            self._reader, self._writer = reader, writer
            self._reader_task = asyncio.create_task(self._read_loop(reader))
            for command in _handshake_commands(self.url):
                reply = await self._send(command)
                _raise_if_error(reply)

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                reply = await read_reply(reader)
                if not self._pending:
                    raise RedisConnectionError("Reply with no pending command")
                future = self._pending.popleft()
                if not future.done():
                    future.set_result(reply)
        except asyncio.CancelledError:
            self._fail_all(RedisConnectionError("Client closed"))
            raise
        except Exception as e:
            self._fail_all(e if isinstance(e, RedisConnectionError) else RedisConnectionError(repr(e)))

    def _fail_all(self, error: Exception) -> None:
        writer, self._writer, self._reader = self._writer, None, None
        if writer is not None:
            writer.transport.abort()
        while self._pending:
            future = self._pending.popleft()
            if not future.done():
                future.set_exception(error)

    async def _send(self, command: Sequence[Any]) -> Any:
        writer = self._writer
        if writer is None:
            raise RedisConnectionError("Not connected")
        future = asyncio.get_running_loop().create_future()
        # Appending the future and writing the bytes happen with no await in
        # between, so the FIFO order of futures always matches the wire.
        self._pending.append(future)
        writer.write(encode_command(command))
        try:
            return await asyncio.wait_for(asyncio.shield(future), timeout=self.command_timeout)
        except asyncio.TimeoutError:
            # A server that stopped answering: drop the socket so every
            # command behind this one fails now instead of timing out too.
            self._fail_all(RedisConnectionError("Redis command timed out"))
            raise RedisConnectionError("Redis command timed out") from None

    async def execute(self, *args: Any) -> Any:
        """Run one command; raises RedisError for an error reply."""
        await self._ensure_connected()
        return _raise_if_error(await self._send(args))

    async def pipeline(self, commands: Sequence[Sequence[Any]]) -> List[Any]:
        """Send several commands in one write; replies in order (errors
        returned in place, not raised)."""
        await self._ensure_connected()
        writer = self._writer
        if writer is None:
            raise RedisConnectionError("Not connected")
        loop = asyncio.get_running_loop()
        futures = []
        payload = []
        for command in commands:
            future = loop.create_future()
            self._pending.append(future)
            futures.append(future)
            payload.append(encode_command(command))
        writer.write(b"".join(payload))
        try:
            return list(
                await asyncio.wait_for(asyncio.shield(asyncio.gather(*futures)), timeout=self.command_timeout)
            )
        except asyncio.TimeoutError:
            self._fail_all(RedisConnectionError("Redis pipeline timed out"))
            raise RedisConnectionError("Redis pipeline timed out") from None

    async def ping(self) -> bool:
        try:
            return await self.execute("PING") == "PONG"
        except RedisError:
            return False

    async def close(self) -> None:
        self._closed = True
        task, self._reader_task = self._reader_task, None
        if self._writer is not None:
            self._writer.close()
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._fail_all(RedisConnectionError("Client closed"))


class Script:
    """A Lua script run with EVALSHA, falling back to EVAL once after a
    restart (or failover) empties Redis's script cache."""

    def __init__(self, source: str) -> None:
        self.source = source
        self.sha = hashlib.sha1(source.encode("utf-8")).hexdigest()

    async def __call__(self, redis: Redis, keys: Sequence[Any] = (), args: Sequence[Any] = ()) -> Any:
        try:
            return await redis.execute("EVALSHA", self.sha, len(keys), *keys, *args)
        except RedisError as e:
            if isinstance(e, RedisConnectionError) or not str(e).startswith("NOSCRIPT"):
                raise
        return await redis.execute("EVAL", self.source, len(keys), *keys, *args)


# --------------------------------------------------------------- subscriber

MessageHandler = Callable[[str, bytes], None]
EventHandler = Callable[[], Awaitable[None]]


class PubSub:
    """A self-healing subscriber connection.

    ``on_message(channel, data)`` is called synchronously, in the order Redis
    delivered the messages. ``on_disconnect`` fires when the socket is lost,
    ``on_resubscribed`` once every channel is subscribed again after a
    reconnect: anything published between the two was missed and must be
    recovered from durable storage by the caller.
    """

    def __init__(
        self,
        url: str,
        on_message: MessageHandler,
        *,
        on_disconnect: Optional[EventHandler] = None,
        on_resubscribed: Optional[EventHandler] = None,
        connect_timeout: float = 3.0,
        ping_interval: float = 5.0,
        confirm_timeout: float = 5.0,
        max_backoff: float = 5.0,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        self.url = parse_url(url)
        self._on_message = on_message
        self._on_disconnect = on_disconnect
        self._on_resubscribed = on_resubscribed
        self.connect_timeout = connect_timeout
        self.ping_interval = ping_interval
        self.confirm_timeout = confirm_timeout
        self.max_backoff = max_backoff
        self._ssl_context = ssl_context

        self._channels: Set[str] = set()
        self._waiters: Dict[str, Deque[asyncio.Future]] = collections.defaultdict(collections.deque)
        self._writer: Optional[asyncio.StreamWriter] = None
        self._task: Optional[asyncio.Task] = None
        self._connected = asyncio.Event()
        self._closed = False
        self._last_reply = 0.0
        self.reconnects = 0
        self.hold_reconnect: Optional[asyncio.Event] = None  # tests: pause reconnection

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    @property
    def channels(self) -> Set[str]:
        return set(self._channels)

    async def start(self, wait: float = 0.0) -> None:
        """Start the connection loop; optionally wait for the first connect."""
        if self._task is None:
            self._task = asyncio.create_task(self._run())
        if wait:
            try:
                await asyncio.wait_for(self._connected.wait(), timeout=wait)
            except asyncio.TimeoutError:
                raise RedisConnectionError("Redis subscriber did not connect in time") from None

    async def subscribe(self, channel: str) -> None:
        """Subscribe and wait until Redis confirms, so nothing published
        after this returns can be missed."""
        self._channels.add(channel)
        writer = self._writer
        if writer is None or not self.connected:
            raise RedisConnectionError("Redis subscriber is not connected")
        future = asyncio.get_running_loop().create_future()
        self._waiters[channel].append(future)
        writer.write(encode_command(["SUBSCRIBE", channel]))
        try:
            await asyncio.wait_for(future, timeout=self.confirm_timeout)
        except asyncio.TimeoutError:
            self._channels.discard(channel)
            raise RedisConnectionError(f"SUBSCRIBE {channel} not confirmed") from None
        except RedisError:
            self._channels.discard(channel)
            raise

    async def unsubscribe(self, channel: str) -> None:
        self._channels.discard(channel)
        writer = self._writer
        if writer is not None and self.connected:
            writer.write(encode_command(["UNSUBSCRIBE", channel]))

    def drop_connection(self) -> None:
        """Abort the socket (tests and operational use); the loop reconnects."""
        if self._writer is not None:
            self._writer.transport.abort()

    async def _run(self) -> None:
        backoff = 0.05
        first = True
        while not self._closed:
            if self.hold_reconnect is not None and not first:
                await self.hold_reconnect.wait()
            try:
                reader, writer = await _open(self.url, self.connect_timeout, self._ssl_context)
            except RedisConnectionError as e:
                logger.warning(f"Redis subscriber: {e}; retrying in {backoff:.2f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff)
                continue
            backoff = 0.05
            self._writer = writer
            try:
                await self._session(reader, writer, resubscribe=not first)
            except asyncio.CancelledError:
                writer.transport.abort()
                raise
            except Exception as e:
                logger.warning(f"Redis subscriber connection lost: {e!r}")
            finally:
                was_connected = self._connected.is_set()
                self._connected.clear()
                self._writer = None
                writer.transport.abort()
                for waiters in self._waiters.values():
                    while waiters:
                        w = waiters.popleft()
                        if not w.done():
                            w.set_exception(RedisConnectionError("Subscriber disconnected"))
                if was_connected and not self._closed and self._on_disconnect:
                    try:
                        await self._on_disconnect()
                    except Exception:
                        logger.exception("on_disconnect handler failed")
            first = False
            if not self._closed:
                self.reconnects += 1
                await asyncio.sleep(backoff)

    async def _session(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, resubscribe: bool) -> None:
        loop = asyncio.get_running_loop()
        # Handshake replies are plain (+OK), read before entering push mode.
        for command in _handshake_commands(self.url, select_db=False):
            writer.write(encode_command(command))
            _raise_if_error(await asyncio.wait_for(read_reply(reader), timeout=self.connect_timeout))

        pending_resubscribe = set(self._channels) if resubscribe else set()
        if pending_resubscribe:
            writer.write(encode_command(["SUBSCRIBE", *sorted(pending_resubscribe)]))
        self._connected.set()
        if resubscribe and not pending_resubscribe and self._on_resubscribed:
            loop.create_task(self._on_resubscribed())

        self._last_reply = loop.time()
        pinger = loop.create_task(self._keepalive(writer))
        try:
            while True:
                reply = await read_reply(reader)
                self._last_reply = loop.time()
                if isinstance(reply, RedisError):
                    raise reply
                if not isinstance(reply, list) or not reply:
                    continue
                kind = reply[0]
                if kind == b"message" and len(reply) == 3:
                    channel = reply[1].decode("utf-8")
                    if channel in self._channels:
                        try:
                            self._on_message(channel, reply[2])
                        except Exception:
                            logger.exception("Pub/sub message handler failed")
                elif kind == b"subscribe":
                    channel = reply[1].decode("utf-8")
                    waiters = self._waiters.get(channel)
                    if waiters:
                        w = waiters.popleft()
                        if not w.done():
                            w.set_result(None)
                    if channel in pending_resubscribe:
                        pending_resubscribe.discard(channel)
                        if not pending_resubscribe and self._on_resubscribed:
                            loop.create_task(self._on_resubscribed())
                # unsubscribe confirmations and pongs need no action
        finally:
            pinger.cancel()

    async def _keepalive(self, writer: asyncio.StreamWriter) -> None:
        """PING while subscribed; a connection that stops answering is
        dropped, which catches half-open sockets a read would wait on forever."""
        loop = asyncio.get_running_loop()
        try:
            while True:
                await asyncio.sleep(self.ping_interval)
                if loop.time() - self._last_reply > self.ping_interval * 3:
                    logger.warning("Redis subscriber unresponsive; reconnecting")
                    writer.transport.abort()
                    return
                writer.write(encode_command(["PING"]))
        except asyncio.CancelledError:
            pass

    async def close(self) -> None:
        self._closed = True
        if self.hold_reconnect is not None:
            self.hold_reconnect.set()
        task, self._task = self._task, None
        if self._writer is not None:
            self._writer.transport.abort()
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
