"""
Enhanced Async CLI Client with Reconnection Logic
Production-grade client with auto-reconnect, message queuing, and error recovery.
"""
import asyncio
import logging
import ssl
import time
from typing import Optional, Deque, Set, Union
from asyncio import StreamReader, StreamWriter
from collections import deque

from ..core.protocol import read_frame, write_frame
from ..core.exceptions import DisconnectError, ProtocolError
from ..core.config import get_client_config, ClientConfig
from .common import HELP, Credentials, RoomCursor, open_connection, render

logger = logging.getLogger(__name__)

JOIN_HISTORY = 50


class EnhancedChatClient:
    """
    Production-grade async chat client.

    Features:
        - Automatic reconnection with exponential backoff
        - Message queuing during disconnects
        - Heartbeat/keepalive
        - Graceful error handling
        - Connection state management
        - TLS with certificate verification; token or password login
        - After a reconnect, rooms are rejoined from the last message seen
          (JOIN with `since`), so messages sent meanwhile are not lost
    """

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 config: Optional[ClientConfig] = None) -> None:
        config = config or get_client_config()

        self.host = host or config.host
        self.port = port or config.port
        self.config = config

        self.reader: Optional[StreamReader] = None
        self.writer: Optional[StreamWriter] = None

        self.username: Optional[str] = None
        self.current_room: Optional[str] = None
        self._credentials: Optional[Credentials] = None
        self._rooms: Set[str] = set()     # rooms to rejoin after a reconnect
        self.cursor = RoomCursor()        # newest/oldest message id per room

        self._running = False
        self._connected = False
        self._reconnect_attempts = 0
        self._retry = config.auto_reconnect

        # Message queue for offline messages
        self._message_queue: Deque = deque(maxlen=config.message_queue_size)

        # Heartbeat
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._last_heartbeat: float = 0.0

    async def connect(self) -> bool:
        """
        Establish connection to server (TLS if configured).

        Returns:
            True if connected successfully
        """
        try:
            self.reader, self.writer = await open_connection(self.host, self.port, self.config)

            logger.info(f"Connected to {self.host}:{self.port}{' (TLS)' if self.config.tls else ''}")
            self._connected = True
            self._reconnect_attempts = 0
            return True

        except asyncio.TimeoutError:
            logger.error(f"Connection timeout to {self.host}:{self.port}")
            return False
        except ssl.SSLCertVerificationError as e:
            # Not a network blip: retrying cannot help, so stop.
            logger.error(f"Server certificate rejected: {e.verify_message}")
            self._retry = False
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
        if not self._retry:
            return False

        while self._retry and self._reconnect_attempts < self.config.max_reconnect_attempts:
            self._reconnect_attempts += 1
            delay = self.config.reconnect_delay * (self.config.reconnect_backoff ** (self._reconnect_attempts - 1))

            print(f"🔄 Reconnecting in {delay:.1f}s (attempt {self._reconnect_attempts}/{self.config.max_reconnect_attempts})...")
            await asyncio.sleep(delay)

            if await self.connect():
                print("✅ Reconnected successfully!")

                # Log in again, rejoin rooms from where we left off, then
                # send what was typed while offline.
                if self._credentials:
                    if await self.authenticate(self._credentials):
                        self._running = True
                        await self._rejoin_rooms()
                        await self._flush_message_queue()
                        return True
                    else:
                        print("❌ Re-authentication failed")
                        self._connected = False
                        return False

        print(f"❌ Max reconnection attempts ({self.config.max_reconnect_attempts}) exceeded")
        return False

    async def authenticate(self, credentials: Union[Credentials, str]) -> bool:
        """
        Send AUTH and wait for confirmation. Accepts Credentials or, for an
        open-mode server, a plain username.

        Returns:
            True if authentication successful
        """
        if not self._connected:
            return False
        if isinstance(credentials, str):
            credentials = Credentials(username=credentials)
        self._credentials = credentials

        try:
            await write_frame(self.writer, credentials.auth_frame())

            # Wait for AUTH_OK
            response = await asyncio.wait_for(
                read_frame(self.reader),
                timeout=10.0
            )

            if response.get("type") == "AUTH_OK":
                # The server decides the name (with a token, from the token).
                self.username = response.get("username") or credentials.username
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
        # One input task for the whole session: typing continues while
        # offline (messages are queued), and no stray input threads pile up
        # across reconnects.
        input_task = asyncio.create_task(self._input_loop())

        try:
            while self._running:
                if not self._connected:
                    # Try to reconnect
                    if not await self.reconnect():
                        print("❌ Connection lost. Exiting...")
                        break

                try:
                    await self._receive_loop()

                except DisconnectError:
                    print("\n⚠️  Connection lost. Attempting to reconnect...")
                    self._connected = False
                    await self._drop_connection()

                except KeyboardInterrupt:
                    print("\n👋 Disconnecting...")
                    break

                except Exception as e:
                    logger.error(f"Client error: {e}", exc_info=True)
                    self._connected = False
                    await self._drop_connection()

        finally:
            input_task.cancel()
            await self.disconnect()

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat to keep connection alive."""
        while self._connected:
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

    async def _rejoin_rooms(self) -> None:
        """After a reconnect: JOIN each room again, asking for everything
        after the newest message already seen. The server replays the gap,
        then switches to live messages, with no duplicates or holes."""
        for room in sorted(self._rooms):
            await write_frame(self.writer, self._join_frame(room))

    def _join_frame(self, room: str) -> dict:
        frame = {"type": "JOIN", "room": room}
        since = self.cursor.newest.get(room)
        if since:
            frame["since"] = since
        else:
            frame["history"] = JOIN_HISTORY
        return frame

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
        print(HELP)

        while self._running:
            try:
                # Non-blocking input using asyncio
                line = await asyncio.get_running_loop().run_in_executor(
                    None,
                    self._read_input
                )

                if not line:
                    continue

                await self._process_input(line)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Input error: {e}")

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

        # Command routing
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
                await self._send_with_queue({"type": "LIST_ROOMS"})
            elif command == "/stats":
                await self._send_with_queue({"type": "STATS"})
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
        """Join a room and fetch its recent history (or the messages missed
        since it was last open)."""
        if not room:
            print("❌ Usage: /join <room_name>")
            return

        await self._send_with_queue(self._join_frame(room))

        self._rooms.add(room)
        self.current_room = room

    async def _cmd_leave(self, room: str) -> None:
        """Leave a room."""
        room = room or self.current_room

        if not room:
            print("❌ Usage: /leave <room_name>")
            return

        await self._send_with_queue({
            "type": "LEAVE",
            "room": room
        })

        self._rooms.discard(room)
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
        await self._send_with_queue(frame)

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
        text = render(message, self.username, self.cursor)
        if text is not None:
            print(text)
        if message.get("type") == "BYE":
            self._running = False

    async def _drop_connection(self) -> None:
        """Close a broken socket without ending the session, so run() can
        reconnect."""
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self.writer and not self.writer.is_closing():
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass

    async def disconnect(self, graceful: bool = True) -> None:
        """Close connection."""
        self._running = False

        if graceful and self._connected and self.writer and not self.writer.is_closing():
            try:
                await write_frame(self.writer, {"type": "QUIT"})
            except Exception:
                pass
        self._connected = False
        await self._drop_connection()

        logger.info("Disconnected")


async def run_client(host: str = "127.0.0.1", port: int = 8888, config: Optional[ClientConfig] = None,
                     username: Optional[str] = None, ask_password: bool = False) -> None:
    """
    Run the enhanced chat client with reconnection.
    """
    from .client import prompt_credentials

    config = config or get_client_config()
    client = EnhancedChatClient(host, port, config)

    try:
        if not await client.connect():
            print("❌ Failed to connect to server")
            return

        credentials = prompt_credentials(config, username, ask_password)
        if credentials is None or not await client.authenticate(credentials):
            return

        # Start main loop
        await client.run()

    except Exception as e:
        logger.error(f"Client failed: {e}", exc_info=True)
    finally:
        await client.disconnect()
