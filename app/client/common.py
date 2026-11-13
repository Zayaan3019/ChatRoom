"""
Pieces both CLI clients share: opening a (TLS) connection, building the AUTH
frame for each login mode, and printing server frames.
"""
import asyncio
import ssl
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set, Tuple

from ..core.config import ClientConfig


@dataclass
class Credentials:
    """What the user proves their identity with. With a token the server
    takes the username from the token, so `username` may be empty."""
    username: str = ""
    token: Optional[str] = None
    password: Optional[str] = None

    def auth_frame(self) -> Dict[str, Any]:
        frame: Dict[str, Any] = {"type": "AUTH"}
        if self.token:
            frame["token"] = self.token
        if self.username:
            frame["username"] = self.username
        if self.password is not None:
            frame["password"] = self.password
        return frame


def make_ssl_context(config: ClientConfig) -> Optional[ssl.SSLContext]:
    """Verifying TLS context, or None for plain TCP. Certificate and hostname
    checks are always on; a private CA is trusted with `tls_ca`."""
    if not config.tls:
        return None
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=config.tls_ca)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context


async def open_connection(host: str, port: int, config: ClientConfig,
                          timeout: Optional[float] = None) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    context = make_ssl_context(config)
    kwargs: Dict[str, Any] = {}
    if context is not None:
        kwargs = {"ssl": context, "server_hostname": config.tls_server_name or host}
    return await asyncio.wait_for(asyncio.open_connection(host, port, **kwargs),
                                  timeout=timeout or config.connect_timeout)


@dataclass
class RoomCursor:
    """Per-room ids the client has seen: `newest` to resume after a
    reconnect, `oldest` to page further back with /history."""
    newest: Dict[str, str] = field(default_factory=dict)
    oldest: Dict[str, str] = field(default_factory=dict)
    seen: Dict[str, Set[str]] = field(default_factory=dict)
    page_start: Dict[str, str] = field(default_factory=dict)  # first id of the history page being received

    @staticmethod
    def _key(msg_id: str) -> Tuple[int, int]:
        ms, _, seq = msg_id.partition("-")
        return int(ms), int(seq or 0)

    def note(self, room: str, msg_id: str, history: bool = False) -> bool:
        """Record an id; False if it was already shown (at-least-once
        delivery means a resumed stream can repeat a message)."""
        if history:
            self.page_start.setdefault(room, msg_id)
        seen = self.seen.setdefault(room, set())
        if msg_id in seen:
            return False
        seen.add(msg_id)
        if len(seen) > 5000:
            seen.clear()
        key = self._key(msg_id)
        if room not in self.newest or key > self._key(self.newest[room]):
            self.newest[room] = msg_id
        if room not in self.oldest or key < self._key(self.oldest[room]):
            self.oldest[room] = msg_id
        return True

    def end_page(self, room: str, gap: bool) -> None:
        """A history page finished. After a gap, older messages must be
        paged from the start of this page, not from what was seen before
        the gap."""
        start = self.page_start.pop(room, None)
        if gap and start is not None:
            self.oldest[room] = start

    def forget(self, room: str) -> None:
        for table in (self.newest, self.oldest, self.seen, self.page_start):
            table.pop(room, None)


def render(message: Dict[str, Any], me: Optional[str], cursor: Optional[RoomCursor] = None) -> Optional[str]:
    """Text to print for one server frame, or None to print nothing."""
    kind = message.get("type")
    room = message.get("room")
    if kind == "MSG":
        msg_id = message.get("id")
        if cursor is not None and isinstance(msg_id, str) and isinstance(room, str):
            if not cursor.note(room, msg_id, bool(message.get("history"))):
                return None
        user, text = message.get("username"), message.get("text")
        if message.get("history"):
            stamp = str(message.get("timestamp", ""))[11:16]
            return f"  [{room} {stamp}] {user}: {text}"
        if user == me:
            return None  # our own live message: already on screen as typed
        return f"[{room}] {user}: {text}"
    if kind == "HISTORY_END":
        if cursor is not None and isinstance(room, str):
            cursor.end_page(room, bool(message.get("gap")))
        if message.get("error"):
            return f"⚠️  {message['error']}"
        count = message.get("count", 0)
        if message.get("gap"):
            return f"── many messages in '{room}' while you were away: latest {count} shown (/history for older) ──"
        more = " (/history for older)" if message.get("more") else ""
        return f"── {count} earlier message{'s' if count != 1 else ''} in '{room}'{more} ──" if count else None
    if kind == "RESYNC":
        return f"⚠️  Missed too many messages in '{room}' during a network blip; /history shows them"
    if kind == "JOINED":
        return f"✅ Joined room '{room}' ({message.get('members')} members)"
    if kind == "LEFT":
        return f"👋 Left room '{room}'"
    if kind == "USER_JOINED":
        return f"📥 {message.get('username')} joined {room}"
    if kind == "USER_LEFT":
        return f"📤 {message.get('username')} left {room}"
    if kind == "ROOM_LIST":
        rooms = message.get("rooms", [])
        return f"📋 Your rooms: {', '.join(rooms)}" if rooms else "📋 You are not in any rooms"
    if kind == "STATS":
        data = message.get("data", {})
        lines = ["", "📊 Server Statistics:",
                 f"  Total Rooms: {data.get('total_rooms')}",
                 f"  Total Connections: {data.get('total_connections')}",
                 f"  Total Messages: {data.get('total_messages')}"]
        for name, count in (data.get("rooms") or {}).items():
            lines.append(f"    - {name}: {count} members")
        return "\n".join(lines) + "\n"
    if kind == "ERROR":
        return f"❌ Error: {message.get('message')}"
    if kind == "BYE":
        return f"👋 {message.get('message')}"
    if kind in ("PONG", "AUTH_OK"):
        return None
    return f"⚠️  Unknown message: {message}"


HELP = """
============================================================
📱 Chat Client - Commands:
  /join <room>     - Join a room (shows the last 50 messages)
  /leave [room]    - Leave a room
  /history [room]  - Show older messages
  /rooms           - List your rooms
  /stats           - Server statistics
  /quit            - Disconnect
  <message>        - Send message to current room
============================================================
"""
