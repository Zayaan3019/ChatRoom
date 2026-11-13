"""Helpers shared by the security, history and cluster tests."""
import asyncio
import contextlib
from typing import Any, Dict, List, Optional

from app.core.config import ServerConfig
from app.core.metrics import MetricsCollector
from app.core.protocol import read_frame, write_frame
from app.server.enhanced_server import EnhancedChatServer

SECRET = "x" * 40  # 40-byte test signing secret


def make_config(**overrides) -> ServerConfig:
    base: Dict[str, Any] = dict(host="127.0.0.1", port=0, metrics_port=0, health_check_interval=0.05,
                                shutdown_timeout=3.0)
    base.update(overrides)
    return ServerConfig(**base)


async def start_server(cluster=None, **overrides) -> EnhancedChatServer:
    srv = EnhancedChatServer(config=make_config(**overrides), metrics=MetricsCollector(), cluster=cluster)
    srv._task = asyncio.create_task(srv.start())
    await asyncio.wait_for(srv.ready.wait(), 10)
    srv.test_port = srv.server.sockets[0].getsockname()[1]
    return srv


async def stop_server(srv: EnhancedChatServer) -> None:
    await srv.stop()
    try:
        await asyncio.wait_for(srv._task, 5)
    except asyncio.TimeoutError:
        srv._task.cancel()


@contextlib.asynccontextmanager
async def running(cluster=None, **overrides):
    srv = await start_server(cluster=cluster, **overrides)
    try:
        yield srv
    finally:
        await stop_server(srv)


class Client:
    """A raw protocol client for tests."""

    def __init__(self, reader, writer) -> None:
        self.reader, self.writer = reader, writer

    @classmethod
    async def open(cls, port: int, **kwargs) -> "Client":
        reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port, **kwargs), 5)
        return cls(reader, writer)

    async def send(self, frame: Dict[str, Any]) -> None:
        await write_frame(self.writer, frame)

    async def recv(self, timeout: float = 3.0) -> Dict[str, Any]:
        return await asyncio.wait_for(read_frame(self.reader), timeout)

    async def recv_until(self, kind: str, timeout: float = 3.0) -> List[Dict[str, Any]]:
        """Frames up to and including the first of type `kind`."""
        frames = []
        while True:
            frame = await self.recv(timeout)
            frames.append(frame)
            if frame.get("type") == kind:
                return frames

    async def expect_closed(self, timeout: float = 3.0) -> bool:
        """The server closed the connection (EOF, or a reset when it closed
        with our bytes still unread -- what Windows reports)."""
        try:
            data = await asyncio.wait_for(self.reader.read(), timeout)
        except (ConnectionResetError, ConnectionAbortedError):
            return True
        return data == b""

    async def login(self, **fields) -> Dict[str, Any]:
        await self.send({"type": "AUTH", **fields})
        return await self.recv()

    async def join(self, room: str, **extra) -> Dict[str, Any]:
        await self.send({"type": "JOIN", "room": room, **extra})
        return await self.recv()

    async def say(self, room: str, text: str) -> Dict[str, Any]:
        """Send and wait for our own copy (it carries the message id)."""
        await self.send({"type": "MSG", "room": room, "text": text})
        while True:
            frame = await self.recv()
            if frame.get("type") == "MSG" and frame.get("text") == text:
                return frame

    async def close(self) -> None:
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:
            pass


async def user(port: int, name: str, **kwargs) -> Client:
    client = await Client.open(port, **kwargs)
    reply = await client.login(username=name)
    assert reply["type"] == "AUTH_OK", reply
    return client


async def drain(client: Client, quiet: float = 0.2) -> List[Dict[str, Any]]:
    """Everything that arrives until the connection is quiet."""
    frames = []
    while True:
        try:
            frames.append(await client.recv(quiet))
        except asyncio.TimeoutError:
            return frames


def texts(frames: List[Dict[str, Any]]) -> List[Optional[str]]:
    return [f.get("text") for f in frames if f.get("type") == "MSG"]
