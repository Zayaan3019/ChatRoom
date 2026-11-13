"""
Main TCP Server - Accept Loop
Listens for incoming connections and spawns handler tasks.
"""
import asyncio
import logging
import signal
from typing import Optional, Set
from asyncio import StreamReader, StreamWriter

from .connection import ClientConnection
from .enhanced_server import listen_options
from .room_manager import RoomManager

logger = logging.getLogger(__name__)


class ChatServer:
    """
    Minimal async TCP chat server: the protocol, rooms and authentication,
    with no rate limiting and no metrics endpoint. It is the core the
    integration tests drive directly. Deployments (`python main.py`,
    `python -m app.main server`, the Docker image) run EnhancedChatServer.

    Architecture:
        - Single-threaded asyncio event loop
        - Each connection is a coroutine (lightweight)
        - Non-blocking I/O for all operations
    """
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8888) -> None:
        self.host = host
        self.port = port
        
        self.room_manager = RoomManager()
        self.server: Optional[asyncio.AbstractServer] = None
        
        # Track active connection tasks
        self._connection_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        
    async def start(self) -> None:
        """
        Start the server and begin accepting connections.
        
        This is the main entry point.
        """
        # Register signal handlers for graceful shutdown
        self._register_signals()
        
        # Start TCP server
        self.server = await asyncio.start_server(
            self._handle_client, self.host, self.port, **listen_options()
        )
        
        addrs = ", ".join(str(sock.getsockname()) for sock in self.server.sockets)
        logger.info(f"🚀 Chat Server listening on {addrs}")
        logger.info(f"Server ready to accept connections...")
        
        # Keep server running until shutdown signal
        async with self.server:
            try:
                await self._shutdown_event.wait()
            except asyncio.CancelledError:
                logger.info("Server task cancelled")
        
        logger.info("Server stopped")
    
    async def _handle_client(
        self, 
        reader: StreamReader, 
        writer: StreamWriter
    ) -> None:
        """
        Callback for each new connection.
        Creates a ClientConnection and spawns handler task.
        """
        addr = writer.get_extra_info("peername")
        
        # Create connection wrapper
        connection = ClientConnection(reader, writer, self.room_manager, addr)
        
        # Spawn handler task
        task = asyncio.create_task(connection.handle())
        self._connection_tasks.add(task)
        
        # Cleanup on completion
        task.add_done_callback(self._connection_tasks.discard)
        
        logger.debug(f"Active connections: {len(self._connection_tasks)}")
    
    def _register_signals(self) -> None:
        """Register signal handlers for graceful shutdown."""
        try:
            loop = asyncio.get_running_loop()
            
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self._shutdown(s))
                )
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            logger.warning("Signal handlers not supported on this platform")
    
    async def _shutdown(self, sig: signal.Signals) -> None:
        """
        Graceful shutdown sequence.
        
        Steps:
            1. Stop accepting new connections
            2. Wait for existing connections to finish (with timeout)
            3. Force close remaining connections
        """
        logger.info(f"Received signal {sig.name}, initiating shutdown...")
        
        # Stop accepting new connections
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        logger.info(f"Closing {len(self._connection_tasks)} active connections...")
        
        # Give connections 10 seconds to finish gracefully
        if self._connection_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._connection_tasks, return_exceptions=True),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for connections to close")
                
                # Force cancel remaining tasks
                for task in self._connection_tasks:
                    if not task.done():
                        task.cancel()
        
        # Print final statistics
        stats = self.room_manager.get_stats()
        logger.info(f"Final stats: {stats}")
        
        # Signal main loop to exit
        self._shutdown_event.set()
    
    async def stop(self) -> None:
        """Programmatic shutdown (for testing)."""
        await self._shutdown(signal.SIGTERM)
    
    def get_stats(self) -> dict:
        """Get server statistics."""
        return {
            "active_connections": len(self._connection_tasks),
            **self.room_manager.get_stats()
        }


async def run_server(host: str = "0.0.0.0", port: int = 8888) -> None:
    """
    Convenience function to run the server.
    
    Usage:
        asyncio.run(run_server())
    """
    server = ChatServer(host, port)
    await server.start()
