"""
Where messages go after a connection sends them: one process, or many.

``LocalCluster``  -- a single server process. Messages are delivered in
                     process; recent history is kept in memory (bounded,
                     lost on restart).

``RedisCluster``  -- several server processes (replicas) sharing rooms
                     through Redis:

  * **Backplane.** Each replica subscribes to a pub/sub channel per room
    that has a local member. A message is published once and every
    subscribed replica delivers it to its own members.
  * **Storage and ordering.** Chat messages are appended to a Redis Stream
    per room (``XADD``) *and* published, in one Lua script. Redis runs a
    script atomically, so the stream order and the publish order are the
    same order: the stream ID (``<ms>-<seq>``) is a per-room sequence
    number every replica agrees on.
  * **Gap repair.** Pub/sub is fire-and-forget: a replica whose subscriber
    connection drops misses whatever is published meanwhile. Each replica
    remembers the last ID it delivered per room. After reconnecting it
    reads the stream from that ID and delivers what it missed, buffering
    live messages meanwhile and dropping duplicates by ID.
  * **Usernames.** A name is a Redis key with a lease (``SET NX PX``),
    refreshed while the session lives. A crashed replica's names free
    themselves when the lease expires.
  * **Presence.** A sorted set per room (member -> lease expiry) gives a
    member count across all replicas.

Messages that are not chat (USER_JOINED, USER_LEFT) are published without
being stored: they describe the present, not history.
"""
import asyncio
import collections
import json
import logging
import os
import socket
import time
import uuid
from typing import Any, Awaitable, Callable, Deque, Dict, Iterable, List, Optional, Tuple

from ..core.redis_client import PubSub, Redis, RedisError, Script

logger = logging.getLogger(__name__)

MessageId = Tuple[int, int]

# deliver(room, msg_id, message, origin) -- origin is the connection key of
# the sender to exclude, or None to deliver to every member.
Deliver = Callable[[str, Optional[str], Dict[str, Any], Optional[str]], Awaitable[None]]


class ClusterUnavailable(Exception):
    """The shared backend cannot be reached; the operation did not happen."""


def parse_message_id(text: Any) -> Optional[MessageId]:
    """'1731490000000-3' -> (1731490000000, 3); None if malformed."""
    if not isinstance(text, str) or len(text) > 41:
        return None
    ms, sep, seq = text.partition("-")
    if not sep or not ms.isdigit() or not seq.isdigit():
        return None
    return int(ms), int(seq)


def format_message_id(mid: MessageId) -> str:
    return f"{mid[0]}-{mid[1]}"


class Cluster:
    """Interface shared by the local and Redis implementations."""

    replica_id: str = "local"
    healthy: bool = True
    shared: bool = False  # True when rooms span processes

    def bind(self, deliver: Deliver, local_count: Callable[[str], int],
             memberships: Callable[[], Iterable[Tuple[str, str]]],
             on_lease_lost: Callable[[str], None]) -> None:
        self._deliver = deliver
        self._local_count = local_count
        self._memberships = memberships
        self._on_lease_lost = on_lease_lost

    async def start(self) -> None: ...
    async def close(self) -> None: ...

    async def claim_username(self, name: str, owner: str) -> bool: return True
    async def release_username(self, name: str, owner: str) -> None: ...

    async def subscribe(self, room: str) -> None: ...
    async def unsubscribe(self, room: str) -> None: ...

    async def presence_join(self, room: str, username: str) -> None: ...
    async def presence_leave(self, room: str, username: str) -> None: ...

    async def member_count(self, room: str) -> int:
        return self._local_count(room)

    async def publish(self, room: str, message: Dict[str, Any], *, persist: bool,
                      origin: Optional[str] = None) -> Optional[str]:
        raise NotImplementedError

    async def history(self, room: str, *, before: Optional[str] = None, after: Optional[str] = None,
                      limit: int = 50) -> Tuple[List[Tuple[str, Dict[str, Any]]], bool]:
        """Messages in chronological order, and whether more exist beyond
        the returned page (older for ``before``/latest, newer for ``after``)."""
        raise NotImplementedError

    def stats(self) -> Dict[str, Any]:
        return {"mode": "local" if not self.shared else "redis", "replica": self.replica_id,
                "healthy": self.healthy}


# ------------------------------------------------------------------- local

class LocalCluster(Cluster):
    """Single process. History is an in-memory log per room, bounded by
    messages per room and by total bytes (oldest messages of the least
    recently active rooms go first). It does not survive a restart."""

    def __init__(self, history_per_room: int = 200, max_bytes: int = 64 * 1024 * 1024) -> None:
        self.history_per_room = history_per_room
        self.max_bytes = max_bytes
        self._logs: "collections.OrderedDict[str, Deque[Tuple[MessageId, Dict[str, Any], int]]]" = (
            collections.OrderedDict()
        )
        self._bytes = 0
        self._last_id: MessageId = (0, 0)

    def _next_id(self) -> MessageId:
        # Same shape as a Redis stream ID: milliseconds, then a sequence
        # within the millisecond. Strictly increasing even if the clock
        # steps backwards.
        ms = int(time.time() * 1000)
        last_ms, last_seq = self._last_id
        self._last_id = (ms, 0) if ms > last_ms else (last_ms, last_seq + 1)
        return self._last_id

    @staticmethod
    def _size(message: Dict[str, Any]) -> int:
        return 64 + sum(len(v) for v in message.values() if isinstance(v, str))

    def _store(self, room: str, mid: MessageId, message: Dict[str, Any]) -> None:
        log = self._logs.get(room)
        if log is None:
            log = self._logs[room] = collections.deque()
        else:
            self._logs.move_to_end(room)
        size = self._size(message)
        log.append((mid, message, size))
        self._bytes += size
        if len(log) > self.history_per_room:
            self._bytes -= log.popleft()[2]
        while self._bytes > self.max_bytes and self._logs:
            oldest_room, oldest_log = next(iter(self._logs.items()))
            self._bytes -= oldest_log.popleft()[2]
            if not oldest_log:
                del self._logs[oldest_room]

    async def publish(self, room, message, *, persist, origin=None):
        msg_id = None
        if persist and self.history_per_room > 0:
            mid = self._next_id()
            self._store(room, mid, message)
            msg_id = format_message_id(mid)
        await self._deliver(room, msg_id, message, origin)
        return msg_id

    async def history(self, room, *, before=None, after=None, limit=50):
        log = self._logs.get(room)
        if not log or limit <= 0:
            return [], False
        entries = [(m, msg) for m, msg, _ in log]
        if after is not None:
            bound = parse_message_id(after) or (0, 0)
            newer = [e for e in entries if e[0] > bound]
            page = newer[:limit]
            return [(format_message_id(m), msg) for m, msg in page], len(newer) > limit
        if before is not None:
            bound = parse_message_id(before) or (0, 0)
            entries = [e for e in entries if e[0] < bound]
        page = entries[-limit:]
        return [(format_message_id(m), msg) for m, msg in page], len(entries) > limit

    def stats(self):
        return {"mode": "local", "replica": self.replica_id, "healthy": True,
                "history_rooms": len(self._logs), "history_bytes": self._bytes}


# ------------------------------------------------------------------- redis

# KEYS[1] stream   ARGV: channel, envelope, persist, maxlen, ttl_ms
_PUBLISH = Script("""
local id = '-'
if ARGV[3] == '1' then
  id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[4], '*', 'e', ARGV[2])
  redis.call('PEXPIRE', KEYS[1], ARGV[5])
end
redis.call('PUBLISH', ARGV[1], id .. '|' .. ARGV[2])
return id
""")

# Refresh a lease only if we still hold it.  KEYS[1] lease  ARGV: owner, ttl_ms
_REFRESH = Script("""
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
""")

# Delete a lease only if we hold it (never someone else's).
_RELEASE = Script("""
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
""")

# Live members: drop expired entries, then count.  ARGV: now_ms
_COUNT = Script("""
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
return redis.call('ZCARD', KEYS[1])
""")


class _RoomState:
    __slots__ = ("refs", "last", "mode", "buffer", "lock", "queue", "worker")

    def __init__(self) -> None:
        self.refs = 0
        self.last: Optional[MessageId] = None
        self.mode = "init"  # init -> live; live -> repair -> live
        self.buffer: List[Tuple[Optional[MessageId], Dict[str, Any], Optional[str]]] = []
        self.lock = asyncio.Lock()
        self.queue: "asyncio.Queue" = asyncio.Queue()
        self.worker: Optional[asyncio.Task] = None


class RedisCluster(Cluster):
    shared = True

    def __init__(
        self,
        url: str,
        *,
        namespace: str = "chat",
        history_per_room: int = 10000,
        history_ttl_seconds: int = 30 * 86400,
        lease_seconds: float = 30.0,
        repair_limit: int = 1000,
        replica_id: Optional[str] = None,
    ) -> None:
        self.url = url
        self.ns = namespace
        self.history_per_room = max(1, history_per_room)
        self.history_ttl_ms = int(history_ttl_seconds * 1000)
        self.lease_ms = int(lease_seconds * 1000)
        self.repair_limit = repair_limit
        self.replica_id = replica_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"

        self.redis = Redis(url)
        self.pubsub = PubSub(url, self._on_pubsub_message,
                             on_disconnect=self._on_pubsub_disconnect,
                             on_resubscribed=self._on_pubsub_resubscribed)
        self._rooms: Dict[str, _RoomState] = {}
        self._leases: Dict[str, str] = {}  # casefolded name -> owner
        self._heartbeat: Optional[asyncio.Task] = None
        self._redis_ok = True
        self.resyncs = 0  # rooms whose gap was larger than repair_limit
        self.repaired_messages = 0

    # keys
    def _log_key(self, room: str) -> str: return f"{self.ns}:room:{room}:log"
    def _channel(self, room: str) -> str: return f"{self.ns}:room:{room}"
    def _lease_key(self, name: str) -> str: return f"{self.ns}:user:{name.casefold()}"
    def _presence_key(self, room: str) -> str: return f"{self.ns}:presence:{room}"

    def _room_of(self, channel: str) -> str:
        return channel[len(self.ns) + len(":room:"):]

    @property
    def healthy(self) -> bool:
        return self.pubsub.connected and self._redis_ok

    async def _call(self, coro):
        """Run a Redis operation; map transport failures to ClusterUnavailable."""
        try:
            result = await coro
            self._redis_ok = True
            return result
        except RedisError as e:
            if not self.redis.connected:
                self._redis_ok = False
            raise ClusterUnavailable(str(e)) from e

    # lifecycle
    async def start(self, wait: float = 10.0) -> None:
        deadline = asyncio.get_running_loop().time() + wait
        while True:
            try:
                await self._call(self.redis.execute("PING"))
                break
            except ClusterUnavailable as e:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                logger.warning(f"Waiting for Redis: {e}")
                await asyncio.sleep(0.5)
        await self.pubsub.start(wait=max(1.0, deadline - asyncio.get_running_loop().time()))
        self._heartbeat = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"Redis cluster mode: replica {self.replica_id}, namespace '{self.ns}'")

    async def close(self) -> None:
        if self._heartbeat:
            self._heartbeat.cancel()
        for state in list(self._rooms.values()):
            if state.worker:
                state.worker.cancel()
        # Free our names now rather than waiting for the leases to expire.
        for key, owner in list(self._leases.items()):
            try:
                await _RELEASE(self.redis, [self._lease_key(key)], [owner])
            except RedisError:
                pass
        self._leases.clear()
        await self.pubsub.close()
        await self.redis.close()

    # usernames
    async def claim_username(self, name, owner):
        reply = await self._call(self.redis.execute(
            "SET", self._lease_key(name), owner, "NX", "PX", self.lease_ms))
        if reply == "OK":
            self._leases[name.casefold()] = owner
            return True
        return False

    async def release_username(self, name, owner):
        self._leases.pop(name.casefold(), None)
        await self._call(_RELEASE(self.redis, [self._lease_key(name)], [owner]))

    async def _heartbeat_loop(self) -> None:
        interval = self.lease_ms / 3000.0
        while True:
            await asyncio.sleep(interval)
            try:
                await self._refresh_leases()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Lease refresh failed: {e!r}")

    async def _refresh_leases(self) -> None:
        now = int(time.time() * 1000)
        commands: List[List[Any]] = []
        names = list(self._leases.items())
        for key, owner in names:
            commands.append(["EVALSHA", _REFRESH.sha, 1, self._lease_key(key), owner, self.lease_ms])
        memberships = list(self._memberships())
        for room, username in memberships:
            commands.append(["ZADD", self._presence_key(room), now + self.lease_ms, username])
        if not commands:
            return
        try:
            replies = await self.redis.pipeline(commands)
            self._redis_ok = True
        except RedisError as e:
            self._redis_ok = self.redis.connected
            raise ClusterUnavailable(str(e)) from e
        for (key, owner), reply in zip(names, replies):
            if isinstance(reply, RedisError) and str(reply).startswith("NOSCRIPT"):
                reply = await self._call(_REFRESH(self.redis, [self._lease_key(key)], [owner, self.lease_ms]))
            if reply == 1:
                continue
            # Lease gone (expired during an outage, or Redis restarted without
            # persistence). Take it back if free; otherwise the name now
            # belongs to someone else and our session must end.
            again = await self._call(self.redis.execute(
                "SET", self._lease_key(key), owner, "NX", "PX", self.lease_ms))
            if again != "OK":
                self._leases.pop(key, None)
                logger.warning(f"Lost the lease on username '{key}'")
                self._on_lease_lost(key)

    # presence
    async def presence_join(self, room, username):
        now = int(time.time() * 1000)
        await self._call(self.redis.pipeline([
            ["ZADD", self._presence_key(room), now + self.lease_ms, username],
            ["PEXPIRE", self._presence_key(room), self.lease_ms * 4],
        ]))

    async def presence_leave(self, room, username):
        await self._call(self.redis.execute("ZREM", self._presence_key(room), username))

    async def member_count(self, room):
        now = int(time.time() * 1000)
        return int(await self._call(_COUNT(self.redis, [self._presence_key(room)], [now])))

    # subscriptions
    async def subscribe(self, room):
        while True:
            state = self._rooms.setdefault(room, _RoomState())
            async with state.lock:
                if self._rooms.get(room) is not state:
                    continue  # dropped while we waited for the lock; use the new one
                await self._subscribe_locked(room, state)
                return

    async def _subscribe_locked(self, room: str, state: _RoomState) -> None:
        state.refs += 1
        if state.refs > 1:
            return
        state.mode = "init"
        state.worker = asyncio.create_task(self._room_worker(room, state))
        try:
            await self._call(self.pubsub.subscribe(self._channel(room)))
            tail = await self._call(self.redis.execute("XREVRANGE", self._log_key(room), "+", "-", "COUNT", 1))
        except ClusterUnavailable:
            state.refs -= 1
            if state.refs == 0:
                await self._drop_room(room, state)
            raise
        tail_id = parse_message_id(tail[0][0].decode()) if tail else None
        # Anything that arrived while initialising was delivered as-is
        # (none of it can have been seen here before); from now on
        # deliveries are de-duplicated against the newest ID seen.
        candidates = [m for m in (state.last, tail_id) if m is not None]
        state.last = max(candidates) if candidates else (0, 0)
        if state.mode == "init":
            state.mode = "live"

    async def unsubscribe(self, room):
        state = self._rooms.get(room)
        if state is None:
            return
        async with state.lock:
            state.refs -= 1
            if state.refs > 0:
                return
            await self._drop_room(room, state)

    async def _drop_room(self, room: str, state: _RoomState) -> None:
        if self._rooms.get(room) is state:
            del self._rooms[room]
        try:
            await self.pubsub.unsubscribe(self._channel(room))
        except RedisError:
            pass
        if state.worker:
            # Let already-queued deliveries finish (e.g. our own USER_LEFT).
            state.queue.put_nowait(None)

    async def _room_worker(self, room: str, state: _RoomState) -> None:
        """Delivers one room's messages in order, so a slow fan-out in one
        room never holds up the subscriber connection or other rooms."""
        while True:
            item = await state.queue.get()
            if item is None:
                return
            msg_id, message, origin = item
            try:
                await self._deliver(room, msg_id, message, origin)
            except Exception:
                logger.exception(f"Delivery to room '{room}' failed")

    def _enqueue(self, room: str, state: _RoomState, mid: Optional[MessageId],
                 message: Dict[str, Any], origin: Optional[str]) -> None:
        if mid is not None:
            if state.mode == "live" and state.last is not None and mid <= state.last:
                return  # duplicate of something already delivered
            if state.last is None or mid > state.last:
                state.last = mid
        state.queue.put_nowait((format_message_id(mid) if mid else None, message, origin))

    @staticmethod
    def _decode(data: bytes) -> Tuple[Optional[MessageId], Dict[str, Any], Optional[str]]:
        text = data.decode("utf-8")
        raw_id, _, payload = text.partition("|")
        envelope = json.loads(payload)
        return (parse_message_id(raw_id) if raw_id != "-" else None,
                envelope["m"], envelope.get("o"))

    def _on_pubsub_message(self, channel: str, data: bytes) -> None:
        room = self._room_of(channel)
        state = self._rooms.get(room)
        if state is None:
            return
        try:
            mid, message, origin = self._decode(data)
        except (ValueError, KeyError, UnicodeDecodeError):
            logger.warning(f"Malformed backplane message on {channel}")
            return
        if state.mode == "repair":
            state.buffer.append((mid, message, origin))
        else:
            self._enqueue(room, state, mid, message, origin)

    async def _on_pubsub_disconnect(self) -> None:
        for state in self._rooms.values():
            state.mode = "repair"

    async def _on_pubsub_resubscribed(self) -> None:
        for room, state in list(self._rooms.items()):
            try:
                await self._repair(room, state)
            except Exception as e:
                logger.warning(f"Gap repair for '{room}' failed: {e!r}; will retry on next reconnect")

    async def _repair(self, room: str, state: _RoomState) -> None:
        """Deliver what was published while the subscriber was away."""
        if state.mode != "repair":
            return
        start = f"({format_message_id(state.last)}" if state.last else "-"
        entries = await self._call(self.redis.execute(
            "XRANGE", self._log_key(room), start, "+", "COUNT", self.repair_limit))
        if len(entries) >= self.repair_limit:
            # Too far behind to replay in full: tell clients to page with
            # HISTORY, and continue from the newest message.
            self.resyncs += 1
            state.queue.put_nowait((None, {"type": "RESYNC", "room": room}, None))
            tail = await self._call(self.redis.execute("XREVRANGE", self._log_key(room), "+", "-", "COUNT", 1))
            if tail:
                state.last = parse_message_id(tail[0][0].decode())
        else:
            for raw_id, fields in entries:
                envelope = json.loads(_field(fields, b"e"))
                mid = parse_message_id(raw_id.decode())
                self.repaired_messages += 1
                self._enqueue(room, state, mid, envelope["m"], envelope.get("o"))
        buffered, state.buffer = state.buffer, []
        state.mode = "live"
        for mid, message, origin in buffered:
            self._enqueue(room, state, mid, message, origin)
        logger.info(f"Room '{room}': recovered after reconnect ({len(entries)} from the log)")

    # messages
    async def publish(self, room, message, *, persist, origin=None):
        envelope = json.dumps({"m": message, "o": origin}, separators=(",", ":"))
        reply = await self._call(_PUBLISH(
            self.redis, [self._log_key(room)],
            [self._channel(room), envelope, "1" if persist else "0",
             self.history_per_room, self.history_ttl_ms],
        ))
        reply = reply.decode() if isinstance(reply, bytes) else reply
        return None if reply == "-" else reply

    async def history(self, room, *, before=None, after=None, limit=50):
        if limit <= 0:
            return [], False
        key = self._log_key(room)
        if after is not None:
            entries = await self._call(self.redis.execute(
                "XRANGE", key, f"({after}", "+", "COUNT", limit + 1))
            more = len(entries) > limit
            entries = entries[:limit]
        else:
            end = f"({before}" if before is not None else "+"
            entries = await self._call(self.redis.execute(
                "XREVRANGE", key, end, "-", "COUNT", limit + 1))
            more = len(entries) > limit
            entries = list(reversed(entries[:limit]))
        out = []
        for raw_id, fields in entries:
            envelope = json.loads(_field(fields, b"e"))
            out.append((raw_id.decode(), envelope["m"]))
        return out, more

    def stats(self):
        return {
            "mode": "redis",
            "replica": self.replica_id,
            "healthy": self.healthy,
            "subscribed_rooms": len(self._rooms),
            "subscriber_reconnects": self.pubsub.reconnects,
            "repaired_messages": self.repaired_messages,
            "resyncs": self.resyncs,
        }


def _field(fields: List[bytes], name: bytes) -> bytes:
    """Value of one field in a stream entry's flat [k1, v1, k2, v2] list."""
    for i in range(0, len(fields) - 1, 2):
        if fields[i] == name:
            return fields[i + 1]
    raise KeyError(name)
