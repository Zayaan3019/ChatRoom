"""
Enhanced Async CLI Client with Reconnection Logic
Production-grade client with auto-reconnect, message queuing, and error recovery.
"""
import asyncio
import logging
import sys
from typing import Optional, Deque
from asyncio import StreamReader, StreamWriter
from collections import deque

from ..core.protocol import read_frame, write_frame
from ..core.exceptions import DisconnectError, ProtocolError
from ..core.config import get_client_config, ClientConfig

logger = logging.getLogger(__name__)


class EnhancedChatClient:
    """
    Production-grade async chat client.
    
    Features:
        - Automatic reconnection with exponential backoff
        - Message queuing during disconnects
        - Heartbeat/keepalive
        - Graceful error handling
        - Connection state management
    """
    
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None) -> None:
        config = get_client_config()
        
        self.host = host or config.host
        self.port = port or config.port
        self.config = config
        
        self.reader: Optional[StreamReader] = None
        self.writer: Optional[StreamWriter] = None
        
        self.username: Optional[str] = None
        self.current_room: Optional[str] = None
        
        self._running = False
        self._connected = False
        self._reconnect_attempts = 0
        
        # Message queue for offline messages
        self._message_queue: Deque = deque(maxlen=config.message_queue_size)
        
        # Heartbeat
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._last_heartbeat: float = 0.0
    
    async def connect(self) -> bool:
        """
        Establish connection to server.
        
        Returns:
            True if connected successfully
        """
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.config.connect_timeout
            )
            
            logger.info(f"Connected to {self.host}:{self.port}")
            self._connected = True
            self._reconnect_attempts = 0
            return True
        
        except asyncio.TimeoutError:
            logger.error(f"Connection timeout to {self.host}:{self.port}")
            return False
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    async def reconnect(self) -> bool:
        """
        Attempt to reconnect with exponential backoff.
        
        Returns:
            True if reconnected successfully
        """
        if not self.config.auto_reconnect:
            return False
        
        while self._reconnect_attempts < self.config.max_reconnect_attempts:
            self._reconnect_attempts += 1
            delay = self.config.reconnect_delay * (self.config.reconnect_backoff ** (self._reconnect_attempts - 1))
            
            print(f"🔄 Reconnecting in {delay:.1f}s (attempt {self._reconnect_attempts}/{self.config.max_reconnect_attempts})...")
            await asyncio.sleep(delay)
            
            if await self.connect():
                print("✅ Reconnected successfully!")
                
                # Re-authenticate
                if self.username:
                    if await self.authenticate(self.username):
                        # Flush queued messages
                        await self._flush_message_queue()
                        return True
                    else:
                        print("❌ Re-authentication failed")
                        self._connected = False
                        return False
        
        print(f"❌ Max reconnection attempts ({self.config.max_reconnect_attempts}) exceeded")
        return False
    
    async def authenticate(self, username: str) -> bool:
        """
        Send AUTH message and wait for confirmation.
        
        Returns:
            True if authentication successful
        """
        if not self._connected:
            return False
        
        self.username = username
        
        try:
            await write_frame(self.writer, {
                "type": "AUTH",
                "username": username
            })
            
            # Wait for AUTH_OK
            response = await asyncio.wait_for(
                read_frame(self.reader),
                timeout=10.0
            )
            
            if response.get("type") == "AUTH_OK":
                print(f"✅ {response.get('message')}")
                
                # Start heartbeat
                if self._heartbeat_task:
                    self._heartbeat_task.cancel()
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                
                return True
            elif response.get("type") == "ERROR":
                print(f"❌ Authentication failed: {response.get('message')}")
                return False
            else:
                print(f"❌ Unexpected response: {response}")
                return False
        
        except asyncio.TimeoutError:
            print("❌ Authentication timeout")
            return False
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False
    
    async def run(self) -> None:
        """
        Main client loop with reconnection support.
        """
        self._running = True
        
        try:
            while self._running:
                if not self._connected:
                    # Try to reconnect
                    if not await self.reconnect():
                        print("❌ Connection lost. Exiting...")
                        break
                
                try:
                    # Run both loops concurrently
                    await asyncio.gather(
                        self._input_loop(),
                        self._receive_loop(),
                        return_exceptions=False
                    )
                
                except DisconnectError:
                    print("\n⚠️  Connection lost. Attempting to reconnect...")
                    self._connected = False
                    await self.disconnect(graceful=False)
                    
                except KeyboardInterrupt:
                    print("\n👋 Disconnecting...")
                    break
                
                except Exception as e:
                    logger.error(f"Client error: {e}", exc_info=True)
                    self._connected = False
        
        finally:
            await self.disconnect()
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat to keep connection alive."""
        import time
        
        while self._connected and self._running:
            try:
                await asyncio.sleep(30.0)  # Heartbeat every 30s
                
                if self._connected:
                    await write_frame(self.writer, {"type": "PING"})
                    self._last_heartbeat = time.time()
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Heartbeat error: {e}")
                break
    
    async def _flush_message_queue(self) -> None:
        """Send queued messages after reconnection."""
        if not self._message_queue:
            return
        
        print(f"📤 Sending {len(self._message_queue)} queued messages...")
        
        while self._message_queue:
            try:
                message = self._message_queue.popleft()
                await write_frame(self.writer, message)
                await asyncio.sleep(0.1)  # Rate limit
            except Exception as e:
                logger.error(f"Failed to send queued message: {e}")
                break
    
    async def _queue_message(self, message: dict) -> None:
        """Queue message for sending when reconnected."""
        if len(self._message_queue) >= self.config.message_queue_size:
            print("⚠️  Message queue full, dropping oldest message")
        
        self._message_queue.append(message)
        print(f"📝 Message queued (offline mode, {len(self._message_queue)} pending)")
    
    async def _input_loop(self) -> None:
        """
        Read user input and send to server (non-blocking).
        """
        print("\n" + "="*60)
        print("📱 Enhanced Chat Client - Commands:")
        print("  /join <room>   - Join a chat room")
        print("  /leave <room>  - Leave a room")
        print("  /rooms         - List your rooms")
        print("  /stats         - Server statistics")
        print("  /quit          - Disconnect")
        print("  <message>      - Send message to current room")
        print("="*60 + "\n")
        
        while self._running and self._connected:
            try:
                # Non-blocking input using asyncio
                line = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self._read_input
                )
                
                if not line:
                    continue
                
                await self._process_input(line)
                
            except Exception as e:
                if self._connected:
                    logger.error(f"Input error: {e}")
                break
    
    def _read_input(self) -> str:
        """Blocking input wrapper for executor."""
        try:
            prompt = f"[{self.current_room or 'lobby'}] " if self.current_room else "> "
            return input(prompt).strip()
        except EOFError:
            return "/quit"
    
    async def _process_input(self, line: str) -> None:
        """Parse and execute user command."""
        if not line:
            return
        
        # Command routing
        if line.startswith("/"):
            parts = line.split(maxsplit=1)
            command = parts[0].lower()
            arg = parts[1] if len(parts) > 1 else ""
            
            if command == "/join":
                await self._cmd_join(arg)
            elif command == "/leave":
                await self._cmd_leave(arg)
            elif command == "/rooms":
                await self._cmd_list_rooms()
            elif command == "/stats":
                await self._cmd_stats()
            elif command == "/quit":
                await self._cmd_quit()
            else:
                print(f"❌ Unknown command: {command}")
        else:
            # Regular message
            await self._send_message(line)
    
    async def _send_with_queue(self, message: dict) -> None:
        """Send message or queue if disconnected."""
        if self._connected:
            try:
                await write_frame(self.writer, message)
            except Exception as e:
                logger.error(f"Send failed: {e}")
                self._connected = False
                await self._queue_message(message)
        else:
            await self._queue_message(message)
    
    async def _cmd_join(self, room: str) -> None:
        """Join a room."""
        if not room:
            print("❌ Usage: /join <room_name>")
            return
        
        await self._send_with_queue({
            "type": "JOIN",
            "room": room
        })
        
        self.current_room = room
    
    async def _cmd_leave(self, room: str) -> None:
        """Leave a room."""
        if not room:
            room = self.current_room
        
        if not room:
            print("❌ Usage: /leave <room_name>")
            return
        
        await self._send_with_queue({
            "type": "LEAVE",
            "room": room
        })
        
        if room == self.current_room:
            self.current_room = None
    
    async def _cmd_list_rooms(self) -> None:
        """List rooms."""
        await self._send_with_queue({
            "type": "LIST_ROOMS"
        })
    
    async def _cmd_stats(self) -> None:
        """Get server statistics."""
        await self._send_with_queue({
            "type": "STATS"
        })
    
    async def _cmd_quit(self) -> None:
        """Disconnect from server."""
        await self._send_with_queue({
            "type": "QUIT"
        })
        self._running = False
    
    async def _send_message(self, text: str) -> None:
        """Send message to current room."""
        if not self.current_room:
            print("❌ You must join a room first (/join <room>)")
            return
        
        await self._send_with_queue({
            "type": "MSG",
            "room": self.current_room,
            "text": text
        })
    
    async def _receive_loop(self) -> None:
        """
        Continuously receive messages from server.
        """
        while self._running and self._connected:
            try:
                message = await asyncio.wait_for(
                    read_frame(self.reader),
                    timeout=self.config.read_timeout
                )
                self._display_message(message)
                
            except asyncio.TimeoutError:
                # This is normal - just a timeout
                continue
            except DisconnectError:
                raise  # Propagate to trigger reconnection
            except ProtocolError as e:
                print(f"\n❌ Protocol error: {e}")
                raise DisconnectError("Protocol error")
            except Exception as e:
                logger.error(f"Receive error: {e}", exc_info=True)
                raise DisconnectError(f"Receive error: {e}")
    
    def _display_message(self, message: dict) -> None:
        """Format and display server message."""
        msg_type = message.get("type")
        
        if msg_type == "PONG":
            # Heartbeat response
            return
        
        elif msg_type == "MSG":
            # Chat message
            room = message.get("room")
            username = message.get("username")
            text = message.get("text")
            
            if username == self.username:
                # Echo of our own message
                return
            
            print(f"[{room}] {username}: {text}")
            
        elif msg_type == "JOINED":
            room = message.get("room")
            members = message.get("members")
            print(f"✅ Joined room '{room}' ({members} members)")
            
        elif msg_type == "LEFT":
            room = message.get("room")
            print(f"👋 Left room '{room}'")
            
        elif msg_type == "USER_JOINED":
            username = message.get("username")
            room = message.get("room")
            print(f"📥 {username} joined {room}")
            
        elif msg_type == "USER_LEFT":
            username = message.get("username")
            room = message.get("room")
            print(f"📤 {username} left {room}")
            
        elif msg_type == "ROOM_LIST":
            rooms = message.get("rooms", [])
            if rooms:
                print(f"📋 Your rooms: {', '.join(rooms)}")
            else:
                print("📋 You are not in any rooms")
            
        elif msg_type == "STATS":
            data = message.get("data", {})
            print("\n📊 Server Statistics:")
            print(f"  Total Rooms: {data.get('total_rooms')}")
            print(f"  Total Connections: {data.get('total_connections')}")
            print(f"  Total Messages: {data.get('total_messages')}")
            
            rooms = data.get('rooms', {})
            if rooms:
                print("  Room Details:")
                for name, count in rooms.items():
                    print(f"    - {name}: {count} members")
            print()
            
        elif msg_type == "ERROR":
            error = message.get("message")
            print(f"❌ Error: {error}")
            
        elif msg_type == "BYE":
            print(f"👋 {message.get('message')}")
            self._running = False
            
        else:
            print(f"⚠️  Unknown message: {message}")
    
    async def disconnect(self, graceful: bool = True) -> None:
        """Close connection."""
        self._running = False
        self._connected = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        
        if graceful and self.writer and not self.writer.is_closing():
            try:
                await write_frame(self.writer, {"type": "QUIT"})
            except:
                pass
        
        if self.writer and not self.writer.is_closing():
            self.writer.close()
            await self.writer.wait_closed()
        
        logger.info("Disconnected")


async def run_client(host: str = "127.0.0.1", port: int = 8888) -> None:
    """
    Run the enhanced chat client with reconnection.
    """
    client = EnhancedChatClient(host, port)
    
    try:
        if not await client.connect():
            print("❌ Failed to connect to server")
            return
        
        # Get username
        username = input("Enter your username: ").strip()
        if not username:
            print("Username required")
            return
        
        # Authenticate
        if not await client.authenticate(username):
            return
        
        # Start main loop
        await client.run()
        
    except Exception as e:
        logger.error(f"Client failed: {e}", exc_info=True)
    finally:
        await client.disconnect()
