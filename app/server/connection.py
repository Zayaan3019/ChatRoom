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

Earlier, QUIT and a failed send set the single ``_closed`` flag, and close()
returned early on seeing it -- so the user stayed in every room and the
socket was never closed by the server.
"""
import asyncio
import logging
import time
from asyncio import StreamReader, StreamWriter
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..core.config import ServerConfig, get_server_config
from ..core.exceptions import DisconnectError, ProtocolError
from ..core.metrics import MetricsCollector, get_metrics
from ..core.protocol import encode_frame, read_frame
from ..core.rate_limiter import RateLimiter
from ..core.validation import InputValidator
from .room_manager import RoomManager

logger = logging.getLogger(__name__)


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
        - authenticate (first frame must be AUTH with an unused username)
        - process commands (JOIN, MSG, LEAVE, LIST_ROOMS, STATS, PING, QUIT)
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
        self.username: Optional[str] = None
        self.connected_at: datetime = datetime.now()
        self.message_count: int = 0

        self._closing: bool = False
        self._cleaned_up: bool = False
        self._departed_rooms: List[str] = []  # rooms left on detach, for USER_LEFT

    @property
    def closed(self) -> bool:
        return self._closing

    @classmethod
    def _next_id(cls) -> int:
        cls._id_counter += 1
        return cls._id_counter

    # ------------------------------------------------------------------ loop

    async def handle(self) -> None:
        """Authenticate, then read and process frames until the client leaves."""
        self.id = self._next_id()
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

    async def _authenticate(self) -> None:
        """First frame: {"type": "AUTH", "username": "alice"}."""
        frame = await asyncio.wait_for(read_frame(self.reader), timeout=self.config.auth_timeout)
        if frame.get("type") != "AUTH":
            raise ProtocolError("First message must be AUTH")

        username = _field(frame, "username")
        is_valid, error_msg = InputValidator.validate_username(username, self.config.max_username_length)
        if not is_valid:
            raise ProtocolError(error_msg)
        if not self.room_manager.claim_username(username, self):
            raise ProtocolError(f"Username '{username}' is already in use")

        self.username = username
        logger.info(f"[Conn #{self.id}] Authenticated as '{self.username}'")
        await self.send({"type": "AUTH_OK", "message": f"Welcome, {self.username}!"})

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

    async def _handle_join(self, frame: Dict) -> None:
        room = _field(frame, "room")
        is_valid, error_msg = InputValidator.validate_room_name(room, self.config.max_room_name_length)
        if not is_valid:
            await self.send_error(error_msg)
            return

        if self.room_manager.is_member(room, self):
            # Idempotent: confirm, but don't announce the user a second time.
            await self.send({"type": "JOINED", "room": room,
                             "members": len(self.room_manager.get_room_members(room))})
            return

        if await self._rate_limited("join"):
            return
        if len(self.room_manager.get_user_rooms(self)) >= self.config.max_rooms_per_user:
            await self.send_error(f"Cannot join more than {self.config.max_rooms_per_user} rooms")
            return
        if len(self.room_manager.get_room_members(room)) >= self.config.max_room_size:
            await self.send_error(f"Room is full (max {self.config.max_room_size} members)")
            return

        member_count = self.room_manager.join(room, self)
        await self.send({"type": "JOINED", "room": room, "members": member_count})
        await self.metrics.record_room_metrics(room, member_count)
        await self.room_manager.broadcast(
            room, self, {"type": "USER_JOINED", "username": self.username, "room": room},
            exclude_sender=True,
        )

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

        recipients = await self.room_manager.broadcast(
            room, self,
            {
                "type": "MSG",
                "room": room,
                "username": self.username,
                "text": text,
                "timestamp": datetime.now().isoformat(),
            },
            exclude_sender=False,  # the sender's copy is its delivery confirmation
        )
        latency_ms = (time.perf_counter() - start) * 1000
        await self.metrics.record_message(latency_ms)
        await self.metrics.increment_room_message(room)
        logger.debug(f"[{self.username}] '{room}' -> {recipients} recipients ({latency_ms:.2f} ms)")

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

        remaining = self.room_manager.leave(room, self)
        await self.metrics.record_room_metrics(room, remaining)
        await self.send({"type": "LEFT", "room": room, "remaining": remaining})
        if remaining > 0:
            await self.room_manager.broadcast(
                room, self, {"type": "USER_LEFT", "username": self.username, "room": room},
                exclude_sender=True,
            )

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

    # ------------------------------------------------------------------- I/O

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
        try:
            transport = getattr(self.writer, "transport", None)
            if transport is not None and getattr(transport, "is_closing", lambda: False)():
                raise DisconnectError("connection is closing")
            self.writer.write(frame)
            if self._must_wait():
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

        # Tell the people still in each room.
        for room in rooms:
            remaining = len(self.room_manager.get_room_members(room))
            await self.metrics.record_room_metrics(room, remaining)
            if remaining and self.username:
                await self.room_manager.broadcast(
                    room, self, {"type": "USER_LEFT", "username": self.username, "room": room},
                    exclude_sender=True,
                )

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

    __repr__ = __str__
