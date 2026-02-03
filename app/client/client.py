"""
Async CLI Chat Client
Demonstrates non-blocking concurrent read/write operations.
"""
import asyncio
import logging
import sys
from typing import Optional
from asyncio import StreamReader, StreamWriter

from ..core.protocol import read_frame, write_frame
from ..core.exceptions import DisconnectError, ProtocolError

logger = logging.getLogger(__name__)


class ChatClient:
    """
    Non-blocking CLI chat client.
    
    Architecture:
        Two concurrent tasks:
            1. input_loop: Read user input (non-blocking)
            2. receive_loop: Receive server messages
    """
    
    def __init__(self, host: str = "127.0.0.1", port: int = 8888) -> None:
        self.host = host
        self.port = port
        
        self.reader: Optional[StreamReader] = None
        self.writer: Optional[StreamWriter] = None
        
        self.username: Optional[str] = None
        self.current_room: Optional[str] = None
        
        self._running = False
    
    async def connect(self) -> None:
        """Establish connection to server."""
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.host, 
                self.port
            )
            logger.info(f"Connected to {self.host}:{self.port}")
            self._running = True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise
    
    async def authenticate(self, username: str) -> bool:
        """
        Send AUTH message and wait for confirmation.
        
        Returns:
            True if authentication successful
        """
        self.username = username
        
        await write_frame(self.writer, {
            "type": "AUTH",
            "username": username
        })
        
        # Wait for AUTH_OK
        response = await read_frame(self.reader)
        
        if response.get("type") == "AUTH_OK":
            print(f"✅ {response.get('message')}")
            return True
        elif response.get("type") == "ERROR":
            print(f"❌ Authentication failed: {response.get('message')}")
            return False
        else:
            print(f"❌ Unexpected response: {response}")
            return False
    
    async def run(self) -> None:
        """
        Main client loop - spawns input and receive tasks.
        """
        try:
            # Run both loops concurrently
            await asyncio.gather(
                self._input_loop(),
                self._receive_loop(),
                return_exceptions=False
            )
        except KeyboardInterrupt:
            print("\n👋 Disconnecting...")
        except Exception as e:
            logger.error(f"Client error: {e}", exc_info=True)
        finally:
            await self.disconnect()
    
    async def _input_loop(self) -> None:
        """
        Read user input and send to server (non-blocking).
        
        Commands:
            /join <room>   - Join a chat room
            /leave <room>  - Leave a room
            /rooms         - List your rooms
            /stats         - Server statistics
            /quit          - Disconnect
            <message>      - Send message to current room
        """
        print("\n" + "="*60)
        print("📱 Chat Client - Commands:")
        print("  /join <room>   - Join a chat room")
        print("  /leave <room>  - Leave a room")
        print("  /rooms         - List your rooms")
        print("  /stats         - Server statistics")
        print("  /quit          - Disconnect")
        print("  <message>      - Send message to current room")
        print("="*60 + "\n")
        
        while self._running:
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
    
    async def _cmd_join(self, room: str) -> None:
        """Join a room."""
        if not room:
            print("❌ Usage: /join <room_name>")
            return
        
        await write_frame(self.writer, {
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
        
        await write_frame(self.writer, {
            "type": "LEAVE",
            "room": room
        })
        
        if room == self.current_room:
            self.current_room = None
    
    async def _cmd_list_rooms(self) -> None:
        """List rooms."""
        await write_frame(self.writer, {
            "type": "LIST_ROOMS"
        })
    
    async def _cmd_stats(self) -> None:
        """Get server statistics."""
        await write_frame(self.writer, {
            "type": "STATS"
        })
    
    async def _cmd_quit(self) -> None:
        """Disconnect from server."""
        await write_frame(self.writer, {
            "type": "QUIT"
        })
        self._running = False
    
    async def _send_message(self, text: str) -> None:
        """Send message to current room."""
        if not self.current_room:
            print("❌ You must join a room first (/join <room>)")
            return
        
        await write_frame(self.writer, {
            "type": "MSG",
            "room": self.current_room,
            "text": text
        })
    
    async def _receive_loop(self) -> None:
        """
        Continuously receive messages from server.
        """
        while self._running:
            try:
                message = await read_frame(self.reader)
                self._display_message(message)
                
            except DisconnectError:
                print("\n❌ Connection lost")
                self._running = False
                break
            except ProtocolError as e:
                print(f"\n❌ Protocol error: {e}")
                self._running = False
                break
            except Exception as e:
                logger.error(f"Receive error: {e}", exc_info=True)
                self._running = False
                break
    
    def _display_message(self, message: dict) -> None:
        """Format and display server message."""
        msg_type = message.get("type")
        
        if msg_type == "MSG":
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
    
    async def disconnect(self) -> None:
        """Close connection."""
        self._running = False
        
        if self.writer and not self.writer.is_closing():
            self.writer.close()
            await self.writer.wait_closed()
        
        logger.info("Disconnected")


async def run_client(host: str = "127.0.0.1", port: int = 8888) -> None:
    """
    Run the chat client.
    
    Usage:
        asyncio.run(run_client())
    """
    client = ChatClient(host, port)
    
    try:
        await client.connect()
        
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
