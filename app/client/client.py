"""
Async CLI Chat Client
Demonstrates non-blocking concurrent read/write operations, over TLS when the
server uses it, with token or password login and room history.
"""
import asyncio
import logging
from typing import Optional
from asyncio import StreamReader, StreamWriter

from ..core.protocol import read_frame, write_frame
from ..core.exceptions import DisconnectError, ProtocolError
from ..core.config import ClientConfig, get_client_config
from .common import HELP, Credentials, RoomCursor, open_connection, render

logger = logging.getLogger(__name__)

JOIN_HISTORY = 50


class ChatClient:
    """
    Non-blocking CLI chat client.

    Architecture:
        Two concurrent tasks:
            1. input_loop: Read user input (non-blocking)
            2. receive_loop: Receive server messages
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 8888,
                 config: Optional[ClientConfig] = None) -> None:
        self.host = host
        self.port = port
        self.config = config or get_client_config()

        self.reader: Optional[StreamReader] = None
        self.writer: Optional[StreamWriter] = None

        self.username: Optional[str] = None
        self.current_room: Optional[str] = None
        self.cursor = RoomCursor()

        self._running = False

    async def connect(self) -> None:
        """Establish connection to server (TLS if configured)."""
        try:
            self.reader, self.writer = await open_connection(self.host, self.port, self.config)
            secure = " (TLS)" if self.config.tls else ""
            logger.info(f"Connected to {self.host}:{self.port}{secure}")
            self._running = True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    async def authenticate(self, credentials) -> bool:
        """
        Send AUTH and wait for confirmation. Accepts a Credentials object or,
        for open mode, a plain username.

        Returns:
            True if authentication successful
        """
        if isinstance(credentials, str):
            credentials = Credentials(username=credentials)
        await write_frame(self.writer, credentials.auth_frame())

        response = await read_frame(self.reader)

        if response.get("type") == "AUTH_OK":
            # The server decides the name (with a token it comes from the token).
            self.username = response.get("username") or credentials.username
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
        """Read user input and send to server (non-blocking)."""
        print(HELP)

        while self._running:
            try:
                line = await asyncio.get_running_loop().run_in_executor(None, self._read_input)
                if not line:
                    continue
                await self._process_input(line)
            except Exception as e:
                logger.error(f"Input error: {e}")
                break

    def _read_input(self) -> str:
        """Blocking input wrapper for executor."""
        try:
            prompt = f"[{self.current_room}] " if self.current_room else "> "
            return input(prompt).strip()
        except EOFError:
            return "/quit"

    async def _process_input(self, line: str) -> None:
        """Parse and execute user command."""
        if not line:
            return

        if line.startswith("/"):
            parts = line.split(maxsplit=1)
            command = parts[0].lower()
            arg = parts[1].strip() if len(parts) > 1 else ""

            if command == "/join":
                await self._cmd_join(arg)
            elif command == "/leave":
                await self._cmd_leave(arg)
            elif command == "/history":
                await self._cmd_history(arg)
            elif command == "/rooms":
                await write_frame(self.writer, {"type": "LIST_ROOMS"})
            elif command == "/stats":
                await write_frame(self.writer, {"type": "STATS"})
            elif command == "/quit":
                await self._cmd_quit()
            else:
                print(f"❌ Unknown command: {command}")
        else:
            await self._send_message(line)

    async def _cmd_join(self, room: str) -> None:
        """Join a room and fetch its recent history."""
        if not room:
            print("❌ Usage: /join <room_name>")
            return
        await write_frame(self.writer, {"type": "JOIN", "room": room, "history": JOIN_HISTORY})
        self.current_room = room

    async def _cmd_leave(self, room: str) -> None:
        """Leave a room."""
        room = room or self.current_room
        if not room:
            print("❌ Usage: /leave <room_name>")
            return
        await write_frame(self.writer, {"type": "LEAVE", "room": room})
        self.cursor.forget(room)
        if room == self.current_room:
            self.current_room = None

    async def _cmd_history(self, room: str) -> None:
        """Page back: messages older than the oldest one on screen."""
        room = room or self.current_room
        if not room:
            print("❌ Usage: /history <room_name>")
            return
        frame = {"type": "HISTORY", "room": room, "limit": JOIN_HISTORY}
        if room in self.cursor.oldest:
            frame["before"] = self.cursor.oldest[room]
        await write_frame(self.writer, frame)

    async def _cmd_quit(self) -> None:
        """Disconnect from server."""
        await write_frame(self.writer, {"type": "QUIT"})
        self._running = False

    async def _send_message(self, text: str) -> None:
        """Send message to current room."""
        if not self.current_room:
            print("❌ You must join a room first (/join <room>)")
            return
        await write_frame(self.writer, {"type": "MSG", "room": self.current_room, "text": text})

    async def _receive_loop(self) -> None:
        """Continuously receive messages from server."""
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
        text = render(message, self.username, self.cursor)
        if text is not None:
            print(text)
        if message.get("type") == "BYE":
            self._running = False

    async def disconnect(self) -> None:
        """Close connection."""
        self._running = False

        if self.writer and not self.writer.is_closing():
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass

        logger.info("Disconnected")


def prompt_credentials(config: ClientConfig, username: Optional[str] = None,
                       ask_password: bool = False) -> Optional[Credentials]:
    """Token from config/CLI if present; otherwise ask for a username (and a
    password, hidden, when the server uses password login)."""
    import getpass

    if config.token:
        return Credentials(username=username or "", token=config.token)
    username = username or input("Enter your username: ").strip()
    if not username:
        print("Username required")
        return None
    password = getpass.getpass("Password: ") if ask_password else None
    return Credentials(username=username, password=password)


async def run_client(host: str = "127.0.0.1", port: int = 8888, config: Optional[ClientConfig] = None,
                     username: Optional[str] = None, ask_password: bool = False) -> None:
    """
    Run the chat client.

    Usage:
        asyncio.run(run_client())
    """
    config = config or get_client_config()
    client = ChatClient(host, port, config)

    try:
        await client.connect()
        credentials = prompt_credentials(config, username, ask_password)
        if credentials is None or not await client.authenticate(credentials):
            return
        await client.run()
    except Exception as e:
        logger.error(f"Client failed: {e}", exc_info=True)
    finally:
        await client.disconnect()
