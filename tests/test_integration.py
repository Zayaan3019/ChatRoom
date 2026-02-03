"""
Integration Tests for Chat Server
Tests the full protocol stack: binary framing, connection handling, room management.
"""
import asyncio
import pytest
import json
import struct
from asyncio import StreamReader, StreamWriter

from app.core.protocol import read_frame, write_frame, HEADER_FORMAT
from app.core.exceptions import DisconnectError, FramingError, InvalidMessageError
from app.server.server import ChatServer
from app.server.room_manager import RoomManager


# Fixtures

@pytest.fixture
async def server():
    """Create and start a test server."""
    srv = ChatServer(host="127.0.0.1", port=0)  # Port 0 = random available port
    
    # Start server in background
    server_task = asyncio.create_task(srv.start())
    
    # Wait for server to be ready
    await asyncio.sleep(0.1)
    
    # Get actual port
    port = srv.server.sockets[0].getsockname()[1]
    srv.test_port = port
    
    yield srv
    
    # Cleanup
    await srv.stop()
    try:
        await asyncio.wait_for(server_task, timeout=2.0)
    except asyncio.TimeoutError:
        server_task.cancel()


@pytest.fixture
async def client_connection(server):
    """Create a client connection to the test server."""
    reader, writer = await asyncio.open_connection(
        "127.0.0.1",
        server.test_port
    )
    
    yield reader, writer
    
    # Cleanup
    writer.close()
    await writer.wait_closed()


# Protocol Tests

class TestProtocol:
    """Test binary protocol framing."""
    
    @pytest.mark.asyncio
    async def test_write_read_frame(self):
        """Test round-trip frame encoding/decoding."""
        # Create in-memory streams
        reader_stream = asyncio.StreamReader()
        writer_stream = asyncio.StreamWriter(
            transport=None,
            protocol=None,
            reader=None,
            loop=asyncio.get_event_loop()
        )
        
        # Mock the write/drain methods
        written_data = bytearray()
        
        def mock_write(data):
            written_data.extend(data)
        
        async def mock_drain():
            pass
        
        writer_stream.write = mock_write
        writer_stream.drain = mock_drain
        
        # Test data
        test_message = {
            "type": "MSG",
            "room": "general",
            "text": "Hello, World! 🌍"
        }
        
        # Write frame
        await write_frame(writer_stream, test_message)
        
        # Feed to reader
        reader_stream.feed_data(bytes(written_data))
        
        # Read frame
        decoded = await read_frame(reader_stream)
        
        # Verify
        assert decoded == test_message
    
    @pytest.mark.asyncio
    async def test_frame_size_validation(self):
        """Test that oversized frames are rejected."""
        reader_stream = asyncio.StreamReader()
        
        # Create a frame header claiming 11MB (exceeds 10MB limit)
        oversized_length = 11 * 1024 * 1024
        header = struct.pack(HEADER_FORMAT, oversized_length)
        
        reader_stream.feed_data(header)
        
        # Should raise FramingError
        with pytest.raises(FramingError, match="exceeds maximum"):
            await read_frame(reader_stream)
    
    @pytest.mark.asyncio
    async def test_incomplete_frame(self):
        """Test handling of incomplete frame (disconnect during read)."""
        reader_stream = asyncio.StreamReader()
        
        # Send header claiming 100 bytes
        header = struct.pack(HEADER_FORMAT, 100)
        reader_stream.feed_data(header)
        
        # Send only 50 bytes, then EOF
        reader_stream.feed_data(b"X" * 50)
        reader_stream.feed_eof()
        
        # Should raise DisconnectError
        with pytest.raises(DisconnectError, match="Connection closed"):
            await read_frame(reader_stream)
    
    @pytest.mark.asyncio
    async def test_invalid_json(self):
        """Test handling of malformed JSON in frame body."""
        reader_stream = asyncio.StreamReader()
        
        # Valid header but invalid JSON body
        body = b"{invalid json"
        header = struct.pack(HEADER_FORMAT, len(body))
        
        reader_stream.feed_data(header + body)
        
        with pytest.raises(InvalidMessageError, match="JSON parse failed"):
            await read_frame(reader_stream)


# Room Manager Tests

class TestRoomManager:
    """Test pub/sub room management."""
    
    def test_join_leave(self):
        """Test basic join/leave operations."""
        manager = RoomManager()
        
        # Mock connections
        class MockConn:
            def __init__(self, name):
                self.username = name
        
        alice = MockConn("alice")
        bob = MockConn("bob")
        
        # Join room
        count = manager.join("general", alice)
        assert count == 1
        
        count = manager.join("general", bob)
        assert count == 2
        
        # Leave room
        count = manager.leave("general", alice)
        assert count == 1
        
        # Check room still exists
        assert "general" in manager._rooms
        
        # Last person leaves
        count = manager.leave("general", bob)
        assert count == 0
        
        # Room should be deleted
        assert "general" not in manager._rooms
    
    @pytest.mark.asyncio
    async def test_broadcast(self):
        """Test message broadcasting."""
        manager = RoomManager()
        
        # Mock connection with send method
        class MockConn:
            def __init__(self, name):
                self.username = name
                self.received = []
            
            async def send(self, message):
                self.received.append(message)
        
        alice = MockConn("alice")
        bob = MockConn("bob")
        charlie = MockConn("charlie")
        
        # Join room
        manager.join("general", alice)
        manager.join("general", bob)
        manager.join("general", charlie)
        
        # Broadcast from alice
        message = {"type": "MSG", "text": "Hello"}
        count = await manager.broadcast("general", alice, message, exclude_sender=True)
        
        # Should reach bob and charlie (not alice)
        assert count == 2
        assert len(bob.received) == 1
        assert len(charlie.received) == 1
        assert len(alice.received) == 0
    
    def test_leave_all(self):
        """Test removing user from all rooms."""
        manager = RoomManager()
        
        class MockConn:
            def __init__(self, name):
                self.username = name
        
        alice = MockConn("alice")
        
        # Join multiple rooms
        manager.join("room1", alice)
        manager.join("room2", alice)
        manager.join("room3", alice)
        
        assert len(manager.get_user_rooms(alice)) == 3
        
        # Leave all
        manager.leave_all(alice)
        
        assert len(manager.get_user_rooms(alice)) == 0
        assert len(manager._rooms) == 0  # All empty rooms deleted


# Integration Tests

class TestIntegration:
    """End-to-end tests with real server."""
    
    @pytest.mark.asyncio
    async def test_authentication(self, client_connection):
        """Test client authentication flow."""
        reader, writer = client_connection
        
        # Send AUTH message
        await write_frame(writer, {
            "type": "AUTH",
            "username": "test_user"
        })
        
        # Receive AUTH_OK
        response = await asyncio.wait_for(read_frame(reader), timeout=1.0)
        
        assert response["type"] == "AUTH_OK"
        assert "Welcome" in response["message"]
    
    @pytest.mark.asyncio
    async def test_join_room(self, client_connection):
        """Test joining a room."""
        reader, writer = client_connection
        
        # Authenticate
        await write_frame(writer, {"type": "AUTH", "username": "alice"})
        await read_frame(reader)  # Consume AUTH_OK
        
        # Join room
        await write_frame(writer, {"type": "JOIN", "room": "test_room"})
        
        # Receive JOINED confirmation
        response = await asyncio.wait_for(read_frame(reader), timeout=1.0)
        
        assert response["type"] == "JOINED"
        assert response["room"] == "test_room"
        assert response["members"] == 1
    
    @pytest.mark.asyncio
    async def test_message_broadcast(self, server):
        """Test message broadcasting between two clients."""
        # Client 1 (Alice)
        reader1, writer1 = await asyncio.open_connection("127.0.0.1", server.test_port)
        await write_frame(writer1, {"type": "AUTH", "username": "alice"})
        await read_frame(reader1)  # AUTH_OK
        
        # Client 2 (Bob)
        reader2, writer2 = await asyncio.open_connection("127.0.0.1", server.test_port)
        await write_frame(writer2, {"type": "AUTH", "username": "bob"})
        await read_frame(reader2)  # AUTH_OK
        
        # Both join same room
        await write_frame(writer1, {"type": "JOIN", "room": "test"})
        await read_frame(reader1)  # JOINED
        
        await write_frame(writer2, {"type": "JOIN", "room": "test"})
        await read_frame(reader2)  # JOINED
        
        # Bob should receive USER_JOINED notification
        notification = await asyncio.wait_for(read_frame(reader2), timeout=1.0)
        assert notification["type"] == "USER_JOINED"
        assert notification["username"] == "alice"
        
        # Alice sends message
        await write_frame(writer1, {
            "type": "MSG",
            "room": "test",
            "text": "Hello Bob!"
        })
        
        # Alice receives echo
        echo = await asyncio.wait_for(read_frame(reader1), timeout=1.0)
        assert echo["type"] == "MSG"
        assert echo["text"] == "Hello Bob!"
        
        # Bob receives message
        msg = await asyncio.wait_for(read_frame(reader2), timeout=1.0)
        assert msg["type"] == "MSG"
        assert msg["username"] == "alice"
        assert msg["text"] == "Hello Bob!"
        
        # Cleanup
        writer1.close()
        writer2.close()
        await writer1.wait_closed()
        await writer2.wait_closed()
    
    @pytest.mark.asyncio
    async def test_graceful_disconnect(self, client_connection):
        """Test QUIT command."""
        reader, writer = client_connection
        
        # Authenticate
        await write_frame(writer, {"type": "AUTH", "username": "test"})
        await read_frame(reader)
        
        # Send QUIT
        await write_frame(writer, {"type": "QUIT"})
        
        # Receive BYE
        response = await asyncio.wait_for(read_frame(reader), timeout=1.0)
        assert response["type"] == "BYE"


# Stress Tests

class TestStress:
    """Load and stress tests."""
    
    @pytest.mark.asyncio
    async def test_concurrent_connections(self, server):
        """Test handling multiple concurrent connections."""
        num_clients = 50
        connections = []
        
        try:
            # Create 50 concurrent connections
            for i in range(num_clients):
                reader, writer = await asyncio.open_connection(
                    "127.0.0.1",
                    server.test_port
                )
                
                # Authenticate
                await write_frame(writer, {
                    "type": "AUTH",
                    "username": f"user_{i}"
                })
                await read_frame(reader)
                
                connections.append((reader, writer))
            
            # Verify all connected
            assert len(connections) == num_clients
            
            # Get stats
            stats = server.get_stats()
            assert stats["active_connections"] == num_clients
            
        finally:
            # Cleanup
            for reader, writer in connections:
                writer.close()
                await writer.wait_closed()
    
    @pytest.mark.asyncio
    async def test_message_throughput(self, server):
        """Test high-volume message throughput."""
        reader, writer = await asyncio.open_connection("127.0.0.1", server.test_port)
        
        try:
            # Authenticate
            await write_frame(writer, {"type": "AUTH", "username": "speedtest"})
            await read_frame(reader)
            
            # Join room
            await write_frame(writer, {"type": "JOIN", "room": "speed"})
            await read_frame(reader)
            
            # Send 100 messages rapidly
            num_messages = 100
            
            for i in range(num_messages):
                await write_frame(writer, {
                    "type": "MSG",
                    "room": "speed",
                    "text": f"Message {i}"
                })
            
            # Read all echoes
            for i in range(num_messages):
                response = await asyncio.wait_for(read_frame(reader), timeout=5.0)
                assert response["type"] == "MSG"
            
        finally:
            writer.close()
            await writer.wait_closed()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
