"""
Pub/Sub Room Manager - Message Routing Engine
Manages chat rooms and broadcasts messages to subscribers.
"""
import asyncio
import logging
from typing import Dict, Set, Optional, TYPE_CHECKING
from collections import defaultdict

from ..core.protocol import encode_frame
from .cluster import Cluster, LocalCluster

if TYPE_CHECKING:
    from .connection import ClientConnection

logger = logging.getLogger(__name__)


class RoomManager:
    """
    Publish-Subscribe message routing system.
    
    Architecture:
        - Rooms are "topics" (channels)
        - Connections are "subscribers"
        - Broadcasting is O(N) where N = subscribers in room
        
    Thread-Safety:
        Single-threaded asyncio - no locks needed
    """
    
    def __init__(self, cluster: Optional[Cluster] = None) -> None:
        # room_name -> set of connections
        self._rooms: Dict[str, Set["ClientConnection"]] = defaultdict(set)
        
        # connection -> set of room names (for cleanup)
        self._user_rooms: Dict["ClientConnection", Set[str]] = defaultdict(set)
        
        self._stats_lock = asyncio.Lock()
        self._message_count: int = 0

        # username -> its connection. One live connection per name, so
        # USER_JOINED/MSG attribution and per-user rate limits mean something.
        self._usernames: Dict[str, "ClientConnection"] = {}

        # Where published messages go: in-process (default) or through Redis
        # to every replica. Either way they come back through deliver_local.
        self.cluster: Cluster = cluster or LocalCluster()
        self.cluster.bind(self.deliver_local, self.local_member_count,
                          self._memberships, self._on_lease_lost)

    # ------------------------------------------------------------- cluster

    def local_member_count(self, room_name: str) -> int:
        return len(self._rooms.get(room_name, ()))

    def _memberships(self):
        for room, members in self._rooms.items():
            for conn in members:
                if conn.username:
                    yield room, conn.username

    def _on_lease_lost(self, name_key: str) -> None:
        """Another session now holds this name (our lease lapsed during a
        Redis outage and was taken). End the local session."""
        conn = self._usernames.get(name_key)
        if conn is not None and hasattr(conn, "kick"):
            asyncio.ensure_future(conn.kick("Your username is now in use by another session"))

    async def publish(self, room_name: str, sender: Optional["ClientConnection"], message: Dict,
                      *, persist: bool, exclude_sender: bool) -> Optional[str]:
        """Send a message to a room on every replica. Chat messages are
        persisted and get an ID; presence notices are not. Raises
        ClusterUnavailable if the shared backend is down."""
        origin = getattr(sender, "key", None) if (exclude_sender and sender is not None) else None
        return await self.cluster.publish(room_name, message, persist=persist, origin=origin)

    async def deliver_local(self, room_name: str, msg_id: Optional[str], message: Dict,
                            origin: Optional[str]) -> int:
        """Fan a published message out to this replica's members of the
        room.

        Encode once. Then write the same bytes into every member's socket
        buffer synchronously: no coroutine or task per member, which is
        what made broadcast cost grow with room size. Only members whose
        buffer is past its high-water mark (slow readers) are awaited, all
        concurrently and each for at most write_timeout, so one stalled
        member never delays the others."""
        members = self._rooms.get(room_name)
        if not members:
            return 0
        out = dict(message)
        if msg_id is not None:
            out["id"] = msg_id
        frame = encode_frame(out)
        delivered = 0
        backed_up = []
        others = []
        for conn in list(members):
            if origin is not None and getattr(conn, "key", None) == origin:
                continue
            if getattr(conn, "closed", False):
                continue
            fast = getattr(conn, "deliver_nowait", None)
            if fast is None:
                others.append(conn)  # minimal connections (tests): the awaited path
                continue
            try:
                if fast(room_name, msg_id, frame):
                    delivered += 1
                else:
                    backed_up.append(conn)
            except Exception as e:
                logger.warning(f"Failed to send to {getattr(conn, 'username', '?')}: {e!r}")
        if backed_up or others:
            results = await asyncio.gather(
                *(c.wait_drained() for c in backed_up),
                *(self._deliver_one(c, room_name, msg_id, out, frame) for c in others),
                return_exceptions=True,
            )
            delivered += sum(1 for r in results[:len(backed_up)] if not isinstance(r, BaseException))
            delivered += sum(1 for r in results[len(backed_up):] if r is True)
        self._message_count += delivered
        return delivered

    async def _deliver_one(self, conn: "ClientConnection", room_name: str, msg_id: Optional[str],
                           message: Dict, frame: bytes) -> bool:
        if getattr(conn, "closed", False):
            return False
        try:
            if hasattr(conn, "deliver"):
                await conn.deliver(room_name, msg_id, frame)
            elif hasattr(conn, "send_bytes"):
                await conn.send_bytes(frame)
            else:
                await conn.send(message)
            return True
        except Exception as e:
            logger.warning(f"Failed to send to {getattr(conn, 'username', '?')}: {e!r}")
            return False

    def claim_username(self, username: str, connection: "ClientConnection") -> bool:
        """Reserve a username for this connection. False if another live
        connection holds it (names compare case-insensitively)."""
        key = username.casefold()
        holder = self._usernames.get(key)
        if holder is not None and holder is not connection:
            return False
        self._usernames[key] = connection
        return True

    def release_username(self, username: Optional[str], connection: "ClientConnection") -> None:
        """Free a username, only if this connection is the one holding it."""
        if username is None:
            return
        key = username.casefold()
        if self._usernames.get(key) is connection:
            del self._usernames[key]

    def is_member(self, room_name: str, connection: "ClientConnection") -> bool:
        return connection in self._rooms.get(room_name, ())

    def join(self, room_name: str, connection: "ClientConnection") -> int:
        """
        Subscribe a connection to a room.
        
        Args:
            room_name: The room identifier
            connection: The client connection to add
            
        Returns:
            Number of members in room after join
        """
        self._rooms[room_name].add(connection)
        self._user_rooms[connection].add(room_name)
        
        member_count = len(self._rooms[room_name])
        logger.info(
            f"User {connection.username} joined room '{room_name}' "
            f"(now {member_count} members)"
        )
        return member_count
    
    def leave(self, room_name: str, connection: "ClientConnection") -> int:
        """
        Unsubscribe a connection from a room.
        
        Args:
            room_name: The room identifier
            connection: The client connection to remove
            
        Returns:
            Number of members remaining in room
        """
        if room_name in self._rooms:
            self._rooms[room_name].discard(connection)
            
            # Cleanup empty rooms to prevent memory leaks
            if not self._rooms[room_name]:
                del self._rooms[room_name]
                logger.info(f"Room '{room_name}' deleted (no members)")
        
        if connection in self._user_rooms:
            self._user_rooms[connection].discard(room_name)
        
        remaining = len(self._rooms.get(room_name, set()))
        logger.info(
            f"User {connection.username} left room '{room_name}' "
            f"({remaining} members remaining)"
        )
        return remaining
    
    def leave_all(self, connection: "ClientConnection") -> None:
        """
        Remove connection from all rooms (cleanup on disconnect).
        
        Args:
            connection: The client connection to remove
        """
        if connection not in self._user_rooms:
            return
        
        # Copy set to avoid modification during iteration
        rooms_copy = set(self._user_rooms[connection])
        
        for room_name in rooms_copy:
            self.leave(room_name, connection)
        
        # Final cleanup
        if connection in self._user_rooms:
            del self._user_rooms[connection]
        
        logger.info(f"User {connection.username} removed from all rooms")
    
    async def broadcast(
        self,
        room_name: str,
        sender: "ClientConnection",
        message: Dict,
        exclude_sender: bool = True
    ) -> int:
        """
        Broadcast a message to all subscribers in a room.
        
        Args:
            room_name: Target room
            sender: The connection sending the message
            message: The message payload to broadcast
            exclude_sender: If True, don't send to sender (echo prevention)
            
        Returns:
            Number of recipients who received the message
        """
        if room_name not in self._rooms:
            logger.warning(f"Broadcast to non-existent room '{room_name}'")
            return 0
        
        recipients = self._rooms[room_name].copy()  # Snapshot to avoid race conditions
        
        if exclude_sender:
            recipients.discard(sender)
        
        if not recipients:
            return 0
        
        # Encode once; every recipient gets the same bytes. Then send to all
        # recipients concurrently. A healthy recipient's send completes
        # without waiting; a backed-up one waits at most write_timeout and is
        # then disconnected, so one slow client cannot stall the room.
        frame = encode_frame(message)
        send_tasks = [
            self._safe_send(conn, message, frame)
            for conn in recipients
        ]
        
        results = await asyncio.gather(*send_tasks, return_exceptions=True)
        
        # Count successful sends
        success_count = sum(1 for r in results if r is True)
        
        async with self._stats_lock:
            self._message_count += success_count
        
        logger.debug(
            f"Broadcast in '{room_name}': {success_count}/{len(recipients)} delivered"
        )
        
        return success_count
    
    async def _safe_send(
        self, 
        connection: "ClientConnection",
        message: Dict,
        frame: Optional[bytes] = None,
    ) -> bool:
        """
        Safely send message to a connection with error handling.
        
        Returns:
            True if successful, False otherwise
        """
        if getattr(connection, "closed", False):
            return False  # already leaving; not a delivery
        try:
            if frame is not None and hasattr(connection, "send_bytes"):
                await connection.send_bytes(frame)
            else:
                await connection.send(message)
            return True
        except Exception as e:
            logger.warning(f"Failed to send to {connection.username}: {e!r}")
            # Don't propagate - connection will be cleaned up by its own handler
            return False
    
    def get_room_members(self, room_name: str) -> Set["ClientConnection"]:
        """Get all members in a room."""
        return self._rooms.get(room_name, set()).copy()
    
    def get_user_rooms(self, connection: "ClientConnection") -> Set[str]:
        """Get all rooms a user is in."""
        return self._user_rooms.get(connection, set()).copy()
    
    def get_stats(self) -> Dict:
        """Statistics for this replica (rooms and members held here)."""
        return {
            "total_rooms": len(self._rooms),
            "total_connections": len(self._usernames),
            "total_messages": self._message_count,
            "rooms": {
                name: len(members)
                for name, members in self._rooms.items()
            },
            "cluster": self.cluster.stats(),
        }
    
    def __len__(self) -> int:
        """Return total number of active rooms."""
        return len(self._rooms)
