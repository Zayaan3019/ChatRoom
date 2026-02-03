"""
Client Connection Handler
Wraps a TCP connection and implements the message processing loop.
"""
import asyncio
import logging
from typing import Optional, Dict, Any
from asyncio import StreamReader, StreamWriter
from datetime import datetime

from ..core.protocol import read_frame, write_frame
from ..core.exceptions import DisconnectError, ProtocolError
from ..core.validation import InputValidator
from ..core.config import ServerConfig, get_server_config
from ..core.rate_limiter import RateLimiter
from ..core.metrics import MetricsCollector, get_metrics
from .room_manager import RoomManager

logger = logging.getLogger(__name__)


class ClientConnection:
    """
    Represents a single TCP client connection.
    
    Responsibilities:
        - Read frames from client
        - Process commands (JOIN, MSG, QUIT)
        - Write responses back to client
        - Graceful cleanup on disconnect
    """
    
    # Class-level connection counter
    _id_counter: int = 0
    _counter_lock = asyncio.Lock()
    
    def __init__(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        room_manager: RoomManager,
        addr: tuple,
        rate_limiter: Optional[RateLimiter] = None,
        metrics: Optional[MetricsCollector] = None,
        config: Optional[ServerConfig] = None
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.room_manager = room_manager
        self.addr = addr
        self.rate_limiter = rate_limiter
        self.metrics = metrics or get_metrics()
        self.config = config or get_server_config()
        
        # Connection metadata
        self.id: Optional[int] = None
        self.username: Optional[str] = None
        self.connected_at: datetime = datetime.now()
        self.message_count: int = 0
        
        self._closed: bool = False
    
    @classmethod
    async def _get_next_id(cls) -> int:
        """Thread-safe connection ID generator."""
        async with cls._counter_lock:
            cls._id_counter += 1
            return cls._id_counter
    
    async def handle(self) -> None:
        """
        Main connection loop - processes incoming messages.
        
        Flow:
            1. Wait for authentication (first message must set username)
            2. Loop: read_frame -> process_command -> send_response
            3. On error/disconnect: cleanup and exit
        """
        self.id = await self._get_next_id()
        peername = self.writer.get_extra_info("peername")
        logger.info(f"[Conn #{self.id}] New connection from {peername}")
        
        try:
            # Step 1: Authentication - first message must be AUTH
            await self._authenticate()
            
            # Step 2: Main message loop
            while not self._closed:
                try:
                    # Read next frame with timeout
                    frame = await asyncio.wait_for(
                        read_frame(self.reader),
                        timeout=300.0  # 5 minute idle timeout
                    )
                    
                    # Process command
                    await self._process_message(frame)
                    
                except asyncio.TimeoutError:
                    logger.warning(f"[{self.username}] Idle timeout")
                    await self.send_error("Connection timeout due to inactivity")
                    break
                    
        except DisconnectError as e:
            logger.info(f"[{self.username}] Disconnected: {e}")
        except ProtocolError as e:
            logger.error(f"[{self.username}] Protocol error: {e}")
            try:
                await self.send_error(str(e))
            except:
                pass
        except Exception as e:
            logger.error(f"[{self.username}] Unexpected error: {e}", exc_info=True)
        finally:
            await self.close()
    
    async def _authenticate(self) -> None:
        """
        Handle initial authentication with validation.
        Expects: {"type": "AUTH", "username": "alice"}
        """
        frame = await asyncio.wait_for(read_frame(self.reader), timeout=self.config.auth_timeout)
        
        if frame.get("type") != "AUTH":
            raise ProtocolError("First message must be AUTH")
        
        username = frame.get("username", "").strip()
        
        # Validate username
        is_valid, error_msg = InputValidator.validate_username(username, self.config.max_username_length)
        if not is_valid:
            raise ProtocolError(error_msg)
        
        self.username = username
        logger.info(f"[Conn #{self.id}] Authenticated as '{self.username}'")
        
        await self.send({
            "type": "AUTH_OK",
            "message": f"Welcome, {self.username}!"
        })
    
    async def _process_message(self, frame: Dict[str, Any]) -> None:
        """
        Route message to appropriate handler based on type.
        
        Supported Commands:
            - JOIN: Join a chat room
            - MSG: Send message to current room
            - LEAVE: Leave a room
            - LIST_ROOMS: Get list of rooms user is in
            - STATS: Get server statistics
            - QUIT: Graceful disconnect
        """
        msg_type = frame.get("type")
        
        if not msg_type:
            await self.send_error("Missing 'type' field")
            return
        
        self.message_count += 1
        
        # Command routing
        handlers = {
            "JOIN": self._handle_join,
            "MSG": self._handle_message,
            "LEAVE": self._handle_leave,
            "LIST_ROOMS": self._handle_list_rooms,
            "STATS": self._handle_stats,
            "QUIT": self._handle_quit,
        }
        
        handler = handlers.get(msg_type)
        if handler:
            await handler(frame)
        else:
            await self.send_error(f"Unknown message type: {msg_type}")
    
    async def _handle_join(self, frame: Dict) -> None:
        """Handle JOIN command with rate limiting and validation."""
        room = frame.get("room", "").strip()
        
        # Validate room name
        is_valid, error_msg = InputValidator.validate_room_name(room, self.config.max_room_name_length)
        if not is_valid:
            await self.send_error(error_msg)
            return
        
        # Rate limiting
        if self.rate_limiter and not await self.rate_limiter.check_join(self.username):
            await self.send_error("Rate limit exceeded for JOIN. Please slow down.")
            await self.metrics.increment_rate_limited()
            return
        
        # Check room limit
        user_rooms = self.room_manager.get_user_rooms(self)
        if len(user_rooms) >= self.config.max_rooms_per_user:
            await self.send_error(f"Cannot join more than {self.config.max_rooms_per_user} rooms")
            return
        
        # Check room size
        room_members = self.room_manager.get_room_members(room)
        if len(room_members) >= self.config.max_room_size:
            await self.send_error(f"Room is full (max {self.config.max_room_size} members)")
            return
        
        member_count = self.room_manager.join(room, self)
        
        await self.send({
            "type": "JOINED",
            "room": room,
            "members": member_count
        })
        
        # Update metrics
        await self.metrics.record_room_metrics(room, member_count)
        
        # Notify others in room
        await self.room_manager.broadcast(
            room,
            self,
            {
                "type": "USER_JOINED",
                "username": self.username,
                "room": room
            },
            exclude_sender=True
        )
    
    async def _handle_message(self, frame: Dict) -> None:
        """Handle MSG command with rate limiting and validation."""
        import time
        start_time = time.time()
        
        room = frame.get("room", "").strip()
        text = frame.get("text", "").strip()
        
        # Validate room name
        is_valid, error_msg = InputValidator.validate_room_name(room, self.config.max_room_name_length)
        if not is_valid:
            await self.send_error(error_msg)
            return
        
        # Validate message text
        is_valid, error_msg = InputValidator.validate_message_text(text, self.config.max_message_length)
        if not is_valid:
            await self.send_error(error_msg)
            return
        
        # Sanitize text
        text = InputValidator.sanitize_text(text)
        
        # Rate limiting
        if self.rate_limiter and not await self.rate_limiter.check_message(self.username):
            await self.send_error("Rate limit exceeded. Please slow down.")
            await self.metrics.increment_rate_limited()
            await self.metrics.increment_failed_messages()
            return
        
        # Check if user is in room
        user_rooms = self.room_manager.get_user_rooms(self)
        if room not in user_rooms:
            await self.send_error(f"You are not in room '{room}'")
            return
        
        # Broadcast to room
        recipients = await self.room_manager.broadcast(
            room,
            self,
            {
                "type": "MSG",
                "room": room,
                "username": self.username,
                "text": text,
                "timestamp": datetime.now().isoformat()
            },
            exclude_sender=False  # Sender also receives confirmation
        )
        
        # Record metrics
        latency_ms = (time.time() - start_time) * 1000
        await self.metrics.record_message(latency_ms)
        await self.metrics.increment_room_message(room)
        
        logger.debug(f"[{self.username}] Message in '{room}' -> {recipients} recipients ({latency_ms:.2f}ms)")
    
    async def _handle_leave(self, frame: Dict) -> None:
        """Handle LEAVE command."""
        room = frame.get("room", "").strip()
        
        if not room:
            await self.send_error("Missing 'room' field")
            return
        
        remaining = self.room_manager.leave(room, self)
        
        await self.send({
            "type": "LEFT",
            "room": room,
            "remaining": remaining
        })
        
        # Notify others
        if remaining > 0:
            await self.room_manager.broadcast(
                room,
                self,
                {
                    "type": "USER_LEFT",
                    "username": self.username,
                    "room": room
                },
                exclude_sender=True
            )
    
    async def _handle_list_rooms(self, frame: Dict) -> None:
        """Handle LIST_ROOMS command."""
        rooms = list(self.room_manager.get_user_rooms(self))
        
        await self.send({
            "type": "ROOM_LIST",
            "rooms": rooms
        })
    
    async def _handle_stats(self, frame: Dict) -> None:
        """Handle STATS command - server statistics."""
        stats = self.room_manager.get_stats()
        
        await self.send({
            "type": "STATS",
            "data": stats
        })
    
    async def _handle_quit(self, frame: Dict) -> None:
        """Handle QUIT command - graceful disconnect."""
        await self.send({
            "type": "BYE",
            "message": "Goodbye!"
        })
        self._closed = True
    
    async def send(self, message: Dict[str, Any]) -> None:
        """
        Send a message to this client.
        
        Args:
            message: Dictionary to serialize and send
        """
        if self._closed:
            return
        
        try:
            await write_frame(self.writer, message)
        except Exception as e:
            logger.error(f"[{self.username}] Send failed: {e}")
            self._closed = True
            raise
    
    async def send_error(self, error_message: str) -> None:
        """Send an error message to client."""
        await self.send({
            "type": "ERROR",
            "message": error_message
        })
    
    async def close(self) -> None:
        """Cleanup connection resources."""
        if self._closed:
            return
        
        self._closed = True
        
        # Remove from all rooms
        self.room_manager.leave_all(self)
        
        # Close socket
        try:
            if not self.writer.is_closing():
                self.writer.close()
                await self.writer.wait_closed()
        except Exception as e:
            logger.debug(f"[{self.username}] Close error: {e}")
        
        duration = (datetime.now() - self.connected_at).total_seconds()
        logger.info(
            f"[{self.username}] Connection closed "
            f"(duration: {duration:.1f}s, messages: {self.message_count})"
        )
    
    def __str__(self) -> str:
        return f"Connection(id={self.id}, user={self.username}, addr={self.addr})"
    
    def __repr__(self) -> str:
        return self.__str__()
