"""
Client Connection Handler
Wraps one TCP connection and runs its message loop.

Lifecycle flags
---------------
Two separate flags, because one flag doing both jobs caused ghost members:

* ``_closing``    -- the connection is going away: stop reading and stop
                     sending. Set by QUIT, by a failed or timed-out send,
                     or when close() starts.
* ``_cleaned_up`` -- close() has finished its work: left every room,
                     released the username, closed the socket. close() runs
                     that work exactly once, whatever set ``_closing``.

History replay without gaps or duplicates
-----------------------------------------
On JOIN the connection becomes a member (so live messages start flowing)
*before* it reads the room's history. Live messages that arrive while the
history is being sent are held in a per-room buffer; afterwards the buffer
is flushed, skipping any message whose ID was already sent as history.
"""
import asyncio
import logging
import time
from asyncio import StreamReader, StreamWriter
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from ..core.auth import AuthError, UserStore, verify_token
from ..core.config import ServerConfig, get_server_config
from ..core.exceptions import DisconnectError, ProtocolError
from ..core.metrics import MetricsCollector, get_metrics
from ..core.protocol import encode_frame, read_frame
from ..core.rate_limiter import RateLimiter
from ..core.validation import InputValidator
from .cluster import ClusterUnavailable, parse_message_id
from .room_manager import RoomManager

logger = logging.getLogger(__name__)

_user_stores: Dict[str, UserStore] = {}


def _user_store(path: str) -> UserStore:
    store = _user_stores.get(path)
    if store is None:
        store = _user_stores[path] = UserStore(path)
    return store


def _field(frame: Dict[str, Any], key: str) -> str:
    """A string field, stripped; anything else (missing, number, list) -> ''."""
    value = frame.get(key)
    return value.strip() if isinstance(value, str) else ""


def _is_clean_eof(error: DisconnectError) -> bool:
    """True when the peer closed between frames (a normal disconnect),
    False when it vanished part-way through a frame."""
    original = getattr(error, "original_error", None)
    return isinstance(original, asyncio.IncompleteReadError) and not original.partial


class ClientConnection:
    """
    One TCP client.

    Responsibilities:
        - authenticate (first frame must be AUTH: an open claim, a signed
          token, or a password, depending on the server's auth mode)
        - process commands (JOIN, MSG, LEAVE, HISTORY, LIST_ROOMS, STATS, PING, QUIT)
        - write responses; wait (with a deadline) only when the socket's
          send buffer is past its high-water mark
        - clean up exactly once on any kind of exit
    """

    _id_counter: int = 0

    def __init__(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        room_manager: RoomManager,
        addr: Optional[tuple],
        rate_limiter: Optional[RateLimiter] = None,
        metrics: Optional[MetricsCollector] = None,
        config: Optional[ServerConfig] = None,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.room_manager = room_manager
        self.addr = addr
        self.ip: Optional[str] = addr[0] if addr else None
        self.rate_limiter = rate_limiter
        self.metrics = metrics or get_metrics()
        self.config = config or get_server_config()

        self.id: Optional[int] = None
        self.key: str = ""  # "<replica>:<id>", unique across the cluster
        self.username: Optional[str] = None
        self.connected_at: datetime = datetime.now()
        self.message_count: int = 0

        self._closing: bool = False
        self._cleaned_up: bool = False
        self._departed_rooms: List[str] = []  # rooms left on detach, for USER_LEFT
        self._subscribed: Set[str] = set()     # rooms this connection holds a cluster subscription for
        self._replay: Dict[str, List[Tuple[Optional[str], bytes]]] = {}
        self._name_leased: bool = False

    @property
    def closed(self) -> bool:
        return self._closing

    @property
    def cluster(self):
        return self.room_manager.cluster

    @classmethod
    def _next_id(cls) -> int:
        cls._id_counter += 1
        return cls._id_counter

    # ------------------------------------------------------------------ loop

    async def handle(self) -> None:
        """Authenticate, then read and process frames until the client leaves."""
        self.id = self._next_id()
        self.key = f"{self.cluster.replica_id}:{self.id}"
        logger.info(f"[Conn #{self.id}] New connection from {self.addr}")
        try:
            await self._authenticate()
            while not self._closing:
                try:
                    frame = await asyncio.wait_for(
                        read_frame(self.reader), timeout=self.config.connection_timeout
                    )
                except asyncio.TimeoutError:
                    logger.info(f"[{self.username}] Idle timeout")
                    await self.metrics.increment_timeout_errors()
                    await self._try_send_error("Connection timeout due to inactivity")
                    break
                await self._process_message(frame)

        except DisconnectError as e:
            if not _is_clean_eof(e) and not self._closing:
                await self.metrics.increment_disconnect_errors()
            logger.info(f"[{self.username}] Disconnected: {e}")
        except asyncio.TimeoutError:
            # Only _authenticate() can raise this here.
            await self.metrics.increment_timeout_errors()
            await self._try_send_error("Authentication timeout")
        except ProtocolError as e:
            await self.metrics.increment_protocol_errors()
            logger.warning(f"[{self.username}] Protocol error: {e}")
            await self._try_send_error(str(e))
        except Exception as e:  # never let one client's bug escape the task
            logger.error(f"[{self.username}] Unexpected error: {e}", exc_info=True)
        finally:
            await self.close()

    # ------------------------------------------------------------ identity

    async def _authenticate(self) -> None:
        """First frame: AUTH. What it must carry depends on the auth mode:
        open {"username"}, token {"token"}, password {"username", "password"}."""
        frame = await asyncio.wait_for(read_frame(self.reader), timeout=self.config.auth_timeout)
        if frame.get("type") != "AUTH":
            raise ProtocolError("First message must be AUTH")
        if self.rate_limiter and not await self.rate_limiter.check_auth(self.ip):
            await self.metrics.increment_rate_limited()
            raise ProtocolError("Too many login attempts; try again later")

        username = await self._verify_identity(frame)

        if not self.room_manager.claim_username(username, self):
            raise ProtocolError(f"Username '{username}' is already in use")
        try:
            leased = await self.cluster.claim_username(username, self.key)
        except ClusterUnavailable:
            self.room_manager.release_username(username, self)
            await self.metrics.increment_backplane_errors()
            raise ProtocolError("Service temporarily unavailable; try again shortly")
        if not leased:
            self.room_manager.release_username(username, self)
            raise ProtocolError(f"Username '{username}' is already in use")
        self._name_leased = True

        self.username = username
        logger.info(f"[Conn #{self.id}] Authenticated as '{self.username}' ({self.config.auth_mode})")
        await self.send({"type": "AUTH_OK", "username": username, "message": f"Welcome, {self.username}!"})

    async def _verify_identity(self, frame: Dict[str, Any]) -> str:
        mode = self.config.auth_mode
        if mode == "token":
            try:
                claims = verify_token(
                    frame.get("token"), self.config.jwt_secret,
                    issuer=self.config.jwt_issuer, audience=self.config.jwt_audience,
                )
            except AuthError as e:
                await self._auth_failed(e)
            return claims["sub"]

        username = _field(frame, "username")
        is_valid, error_msg = InputValidator.validate_username(username, self.config.max_username_length)
        if not is_valid:
            raise ProtocolError(error_msg)

        if mode == "password":
            password = frame.get("password")
            store = _user_store(self.config.users_file)
            # scrypt costs ~50 ms of CPU: run it off the event loop.
            ok = await asyncio.get_running_loop().run_in_executor(
                None, store.check, username, password if isinstance(password, str) else ""
            )
            if not ok:
                await self._auth_failed(AuthError(f"bad password for {username!r}"))
        return username

    async def _auth_failed(self, error: Exception) -> None:
        await self.metrics.increment_auth_failures()
        logger.info(f"[Conn #{self.id}] Authentication failed from {self.ip}: {error}")
        # One message for every failure, so it reveals nothing about which
        # part was wrong or whether the account exists.
        raise ProtocolError("Invalid credentials")

    # ------------------------------------------------------------ dispatch

    async def _process_message(self, frame: Dict[str, Any]) -> None:
        msg_type = frame.get("type")
        if not isinstance(msg_type, str) or not msg_type:
            await self.send_error("Missing 'type' field")
            return

        self.message_count += 1
        handler = {
            "JOIN": self._handle_join,
            "MSG": self._handle_message,
            "LEAVE": self._handle_leave,
            "HISTORY": self._handle_history,
            "LIST_ROOMS": self._handle_list_rooms,
            "STATS": self._handle_stats,
            "QUIT": self._handle_quit,
            "PING": self._handle_ping,
        }.get(msg_type)
        if handler:
            await handler(frame)
        else:
            await self.send_error(f"Unknown message type: {msg_type}")

    # -------------------------------------------------------------- commands

    async def _rate_limited(self, action: str) -> bool:
        """True (and an error sent) if this user or its IP is over the limit."""
        if not self.rate_limiter:
            return False
        if action == "join":
            allowed = await self.rate_limiter.check_join(self.username, self.ip)
        else:
            allowed = await self.rate_limiter.check_message(self.username, self.ip)
        if allowed:
            return False
        await self.metrics.increment_rate_limited()
        if action == "message":
            await self.metrics.increment_failed_messages()
        await self.send_error(
            "Rate limit exceeded for JOIN. Please slow down." if action == "join"
            else "Rate limit exceeded. Please slow down."
        )
        return True

    async def _member_count(self, room: str) -> int:
        try:
            return await self.cluster.member_count(room)
        except ClusterUnavailable:
            return self.room_manager.local_member_count(room)

    async def _handle_join(self, frame: Dict) -> None:
        room = _field(frame, "room")
        is_valid, error_msg = InputValidator.validate_room_name(room, self.config.max_room_name_length)
        if not is_valid:
            await self.send_error(error_msg)
            return
        since = frame.get("since")
        if since is not None and parse_message_id(since) is None:
            await self.send_error("Invalid 'since': expected a message id like '1731490000000-0'")
            return
        history = frame.get("history", self.config.history_on_join)
        if not isinstance(history, int) or isinstance(history, bool) or history < 0:
            await self.send_error("Invalid 'history': expected a non-negative integer")
            return

        if self.room_manager.is_member(room, self):
            # Idempotent: confirm, but don't announce the user a second time.
            await self.send({"type": "JOINED", "room": room, "members": await self._member_count(room)})
            return

        if await self._rate_limited("join"):
            return
        if len(self.room_manager.get_user_rooms(self)) >= self.config.max_rooms_per_user:
            await self.send_error(f"Cannot join more than {self.config.max_rooms_per_user} rooms")
            return
        if self.room_manager.local_member_count(room) >= self.config.max_room_size:
            await self.send_error(f"Room is full (max {self.config.max_room_size} members)")
            return

        # Subscribe first: once this returns, nothing published to the room
        # can be missed by this replica.
        try:
            await self.cluster.subscribe(room)
        except ClusterUnavailable:
            await self.metrics.increment_backplane_errors()
            await self.send_error("Rooms are temporarily unavailable; try again shortly")
            return
        self._subscribed.add(room)

        replay = since is not None or history > 0
        # Hold the room's live traffic until JOINED (and any history) has
        # been sent, so the client never sees room messages before the
        # confirmation, or live messages mixed into its history.
        self._replay[room] = []
        newest: Optional[str] = None
        try:
            local_count = self.room_manager.join(room, self)
            try:
                await self.cluster.presence_join(room, self.username)
            except ClusterUnavailable:
                await self.metrics.increment_backplane_errors()
            await self.send({"type": "JOINED", "room": room, "members": await self._member_count(room)})
            await self.metrics.record_room_metrics(room, local_count)
            await self._publish_presence(room, "USER_JOINED")
            if replay:
                newest = await self._send_history(room, after=since, limit=(
                    self.config.history_resume_max if since is not None
                    else min(history, self.config.history_page_max)))
        finally:
            await self._flush_replay(room, newest)

    async def _handle_message(self, frame: Dict) -> None:
        start = time.perf_counter()
        room = _field(frame, "room")
        text = _field(frame, "text")

        is_valid, error_msg = InputValidator.validate_room_name(room, self.config.max_room_name_length)
        if not is_valid:
            await self.send_error(error_msg)
            return
        is_valid, error_msg = InputValidator.validate_message_text(text, self.config.max_message_length)
        if not is_valid:
            await self.send_error(error_msg)
            return
        text = InputValidator.sanitize_text(text)
        if not text:
            await self.send_error("Message cannot be empty")
            return

        if not self.room_manager.is_member(room, self):
            await self.send_error(f"You are not in room '{room}'")
            return
        if await self._rate_limited("message"):
            return

        try:
            msg_id = await self.room_manager.publish(
                room, self,
                {
                    "type": "MSG",
                    "room": room,
                    "username": self.username,
                    "text": text,
                    "timestamp": datetime.now().isoformat(),
                },
                persist=True,
                exclude_sender=False,  # the sender's copy (with its id) confirms delivery
            )
        except ClusterUnavailable:
            await self.metrics.increment_backplane_errors()
            await self.metrics.increment_failed_messages()
            await self.send_error("Message not sent: service temporarily unavailable")
            return
        latency_ms = (time.perf_counter() - start) * 1000
        await self.metrics.record_message(latency_ms)
        await self.metrics.increment_room_message(room)
        logger.debug(f"[{self.username}] '{room}' <- {msg_id} ({latency_ms:.2f} ms)")

    async def _handle_leave(self, frame: Dict) -> None:
        room = _field(frame, "room")
        is_valid, error_msg = InputValidator.validate_room_name(room, self.config.max_room_name_length)
        if not is_valid:
            await self.send_error(error_msg)
            return
        # Only a member can leave; otherwise anyone could post USER_LEFT
        # notices into rooms they were never in.
        if not self.room_manager.is_member(room, self):
            await self.send_error(f"You are not in room '{room}'")
            return

        local_remaining = self.room_manager.leave(room, self)
        await self.metrics.record_room_metrics(room, local_remaining)
        try:
            await self.cluster.presence_leave(room, self.username)
        except ClusterUnavailable:
            await self.metrics.increment_backplane_errors()
        await self.send({"type": "LEFT", "room": room, "remaining": await self._member_count(room)})
        await self._publish_presence(room, "USER_LEFT")
        await self._release_subscription(room)

    async def _handle_history(self, frame: Dict) -> None:
        """HISTORY {room, before?: id, limit?: n} -> older messages, oldest
        first, then HISTORY_END {more}. Members only."""
        room = _field(frame, "room")
        is_valid, error_msg = InputValidator.validate_room_name(room, self.config.max_room_name_length)
        if not is_valid:
            await self.send_error(error_msg)
            return
        if not self.room_manager.is_member(room, self):
            await self.send_error(f"You are not in room '{room}'")
            return
        before = frame.get("before")
        if before is not None and parse_message_id(before) is None:
            await self.send_error("Invalid 'before': expected a message id")
            return
        limit = frame.get("limit", self.config.history_page_max)
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            await self.send_error("Invalid 'limit': expected a positive integer")
            return
        await self._send_history(room, before=before, limit=min(limit, self.config.history_page_max))

    async def _handle_list_rooms(self, frame: Dict) -> None:
        await self.send({"type": "ROOM_LIST", "rooms": sorted(self.room_manager.get_user_rooms(self))})

    async def _handle_stats(self, frame: Dict) -> None:
        await self.send({"type": "STATS", "data": self.room_manager.get_stats()})

    async def _handle_ping(self, frame: Dict) -> None:
        """Heartbeat from the client (enhanced_client sends one every 30 s).
        Any frame resets the idle timeout, so this keeps the connection open."""
        await self.send({"type": "PONG"})

    async def _handle_quit(self, frame: Dict) -> None:
        await self.send({"type": "BYE", "message": "Goodbye!"})
        self._closing = True  # the loop exits; close() then does the cleanup

    # ------------------------------------------------------------- history

    async def _send_history(self, room: str, *, after: Optional[str] = None,
                            before: Optional[str] = None, limit: int) -> Optional[str]:
        """Send stored messages (each marked "history": true), then
        HISTORY_END. Returns the newest ID sent."""
        error = None
        gap = False
        try:
            entries, more = await self.cluster.history(room, after=after, before=before, limit=limit)
            if after is not None and more:
                # Too much was missed to replay. Send the newest page instead,
                # so the replay joins up with the live messages that follow;
                # the skipped ones stay reachable with HISTORY {before}.
                entries, _ = await self.cluster.history(room, limit=limit)
                gap = True
        except ClusterUnavailable:
            await self.metrics.increment_backplane_errors()
            entries, more, error = [], False, "History temporarily unavailable"
        newest = None
        for msg_id, message in entries:
            out = dict(message)
            out["id"] = msg_id
            out["history"] = True
            await self.send(out)
            newest = msg_id
        end: Dict[str, Any] = {"type": "HISTORY_END", "room": room, "count": len(entries), "more": more}
        if gap:
            end["gap"] = True
        if error:
            end["error"] = error
        await self.send(end)
        if entries:
            await self.metrics.add_history_sent(len(entries))
        return newest

    async def _flush_replay(self, room: str, newest_sent: Optional[str]) -> None:
        """Send live messages held during a history replay, minus any the
        history already contained.

        The held list stays registered while it drains, so anything that
        arrives during a send queues behind the older frames instead of
        overtaking them; it is unregistered only once empty (no await
        between the check and the removal)."""
        buffered = self._replay.get(room)
        if buffered is None:
            return
        newest = parse_message_id(newest_sent) if newest_sent else None
        try:
            while buffered:
                msg_id, frame = buffered.pop(0)
                if newest is not None and msg_id is not None:
                    mid = parse_message_id(msg_id)
                    if mid is not None and mid <= newest:
                        continue  # already sent as history
                await self.send_bytes(frame)
        finally:
            if self._replay.get(room) is buffered:
                del self._replay[room]

    # ------------------------------------------------------------- cluster

    async def _publish_presence(self, room: str, kind: str) -> None:
        try:
            await self.room_manager.publish(
                room, self, {"type": kind, "username": self.username, "room": room},
                persist=False, exclude_sender=True,
            )
        except ClusterUnavailable:
            await self.metrics.increment_backplane_errors()

    async def _release_subscription(self, room: str) -> None:
        if room in self._subscribed:
            self._subscribed.discard(room)
            try:
                await self.cluster.unsubscribe(room)
            except ClusterUnavailable:
                pass

    async def kick(self, reason: str) -> None:
        """End this session from the server side (e.g. its name was taken
        over after a lease was lost)."""
        await self._try_send_error(reason)
        self._detach()

    # ------------------------------------------------------------------- I/O

    def deliver_nowait(self, room: str, msg_id: Optional[str], frame: bytes) -> bool:
        """A published message for one of our rooms (the room manager's fast
        path). Queues it without awaiting; held back while that room's JOIN
        or history replay is in progress. Returns False when the socket
        buffer is past its high-water mark, so the caller must then await
        wait_drained(). Raises (and detaches) if the connection is broken."""
        held = self._replay.get(room)
        if held is not None:
            held.append((msg_id, frame))
            return True
        return self.write_nowait(frame)

    async def deliver(self, room: str, msg_id: Optional[str], frame: bytes) -> None:
        if not self.deliver_nowait(room, msg_id, frame):
            await self.wait_drained()

    def write_nowait(self, frame: bytes) -> bool:
        """Queue one encoded frame. True if nothing more is needed; False if
        the client is behind and wait_drained() must be awaited."""
        if self._closing:
            return True
        try:
            transport = getattr(self.writer, "transport", None)
            if transport is not None and getattr(transport, "is_closing", lambda: False)():
                raise DisconnectError("connection is closing")
            self.writer.write(frame)
        except Exception as e:
            logger.info(f"[{self.username}] Send failed: {e!r}")
            self._detach()
            raise
        return not self._must_wait()

    async def wait_drained(self) -> None:
        """Wait (at most write_timeout) for a backed-up client to catch up;
        a client that does not is disconnected as a slow client."""
        try:
            await asyncio.wait_for(self.writer.drain(), timeout=self.config.write_timeout)
        except asyncio.TimeoutError:
            logger.warning(f"[{self.username}] Slow client: write timed out; disconnecting")
            await self.metrics.increment_slow_client_disconnects()
            self._detach()
            raise DisconnectError("write timed out (slow client)")
        except Exception as e:
            logger.info(f"[{self.username}] Send failed: {e!r}")
            self._detach()
            raise

    async def send(self, message: Dict[str, Any]) -> None:
        """Send one message (see send_bytes)."""
        if self._closing:
            return
        await self.send_bytes(encode_frame(message))

    def _must_wait(self) -> bool:
        """True when the socket's send buffer is past its high-water mark,
        i.e. the client is not keeping up and we must wait for it."""
        transport = getattr(self.writer, "transport", None)
        try:
            size = transport.get_write_buffer_size()
            _, high = transport.get_write_buffer_limits()
        except (AttributeError, TypeError):
            return True  # unknown writer: always use the checked, timed path
        return size > high

    async def send_bytes(self, frame: bytes) -> None:
        """Send one already-encoded frame.

        The write is queued immediately. Only when the client's buffer is
        backed up do we wait for it to drain -- and then for at most
        write_timeout. A send that fails or times out detaches this
        connection and re-raises, so callers (broadcast) count the failure.
        """
        if self._closing:
            return
        if not self.write_nowait(frame):
            await self.wait_drained()

    async def send_error(self, error_message: str) -> None:
        await self.send({"type": "ERROR", "message": error_message})

    async def _try_send_error(self, error_message: str) -> None:
        try:
            await self.send_error(error_message)
        except Exception:
            pass

    def _detach(self) -> None:
        """Synchronously take this connection out of service: no more sends,
        out of every room (so broadcasts stop targeting it), and the socket
        aborted, which wakes the read loop so close() runs."""
        self._closing = True
        if not self._departed_rooms:
            self._departed_rooms = sorted(self.room_manager.get_user_rooms(self))
        self.room_manager.leave_all(self)
        transport = getattr(self.writer, "transport", None)
        if transport is not None and not transport.is_closing():
            transport.abort()

    async def close(self) -> None:
        """Release everything this connection holds. Runs its work exactly
        once, however the connection ended (QUIT, EOF, error, slow client)."""
        self._closing = True
        if self._cleaned_up:
            return
        self._cleaned_up = True

        rooms = self._departed_rooms or sorted(self.room_manager.get_user_rooms(self))
        self.room_manager.leave_all(self)
        self.room_manager.release_username(self.username, self)
        self._replay.clear()

        # Tell the people still in each room (on every replica), then give
        # back this connection's presence entries, subscriptions and name.
        for room in rooms:
            await self.metrics.record_room_metrics(room, self.room_manager.local_member_count(room))
            if self.username:
                try:
                    await self.cluster.presence_leave(room, self.username)
                except ClusterUnavailable:
                    pass
                await self._publish_presence(room, "USER_LEFT")
        for room in list(self._subscribed):
            await self._release_subscription(room)
        if self._name_leased and self.username:
            self._name_leased = False
            try:
                await self.cluster.release_username(self.username, self.key)
            except ClusterUnavailable:
                pass  # the lease expires on its own

        try:
            if not self.writer.is_closing():
                self.writer.close()
            await asyncio.wait_for(self.writer.wait_closed(), timeout=2.0)
        except Exception as e:  # includes the timeout of a peer that never reads
            transport = getattr(self.writer, "transport", None)
            if transport is not None:
                transport.abort()
            logger.debug(f"[{self.username}] Close: {e!r}")

        duration = (datetime.now() - self.connected_at).total_seconds()
        logger.info(
            f"[{self.username}] Connection closed (duration: {duration:.1f}s, messages: {self.message_count})"
        )

    def __str__(self) -> str:
        return f"Connection(id={self.id}, user={self.username}, addr={self.addr})"
