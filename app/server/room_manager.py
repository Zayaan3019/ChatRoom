"""
Pub/Sub Room Manager - Message Routing Engine
Manages chat rooms and broadcasts messages to subscribers.
"""
import asyncio
import logging
from typing import Dict, Set, Optional, TYPE_CHECKING
from collections import defaultdict

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
    
    def __init__(self) -> None:
        # room_name -> set of connections
        self._rooms: Dict[str, Set["ClientConnection"]] = defaultdict(set)
        
        # connection -> set of room names (for cleanup)
        self._user_rooms: Dict["ClientConnection", Set[str]] = defaultdict(set)
        
        self._stats_lock = asyncio.Lock()
        self._message_count: int = 0
        
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
        
        # Send to all recipients concurrently
        send_tasks = [
            self._safe_send(conn, message)
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
        message: Dict
    ) -> bool:
        """
        Safely send message to a connection with error handling.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            await connection.send(message)
            return True
        except Exception as e:
            logger.error(
                f"Failed to send to {connection.username}: {e}",
                exc_info=True
            )
            # Don't propagate - connection will be cleaned up by its own handler
            return False
    
    def get_room_members(self, room_name: str) -> Set["ClientConnection"]:
        """Get all members in a room."""
        return self._rooms.get(room_name, set()).copy()
    
    def get_user_rooms(self, connection: "ClientConnection") -> Set[str]:
        """Get all rooms a user is in."""
        return self._user_rooms.get(connection, set()).copy()
    
    def get_stats(self) -> Dict:
        """Get system statistics."""
        return {
            "total_rooms": len(self._rooms),
            "total_connections": len(self._user_rooms),
            "total_messages": self._message_count,
            "rooms": {
                name: len(members) 
                for name, members in self._rooms.items()
            }
        }
    
    def __len__(self) -> int:
        """Return total number of active rooms."""
        return len(self._rooms)
