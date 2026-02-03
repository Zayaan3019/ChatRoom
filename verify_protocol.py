"""
Quick verification script to test the binary protocol.
Run this to verify the foundation is working correctly.
"""
import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from app.core.protocol import write_frame, read_frame
from app.core.exceptions import DisconnectError


async def test_protocol():
    """Test basic protocol operations."""
    print("🧪 Testing Binary Protocol Layer...")
    print("=" * 60)
    
    # Create in-memory stream pair
    reader = asyncio.StreamReader()
    
    # Mock writer
    buffer = bytearray()
    
    class MockWriter:
        def write(self, data):
            buffer.extend(data)
        
        async def drain(self):
            pass
        
        def is_closing(self):
            return False
    
    writer = MockWriter()
    
    # Test 1: Simple message
    print("\n✅ Test 1: Write and read simple message")
    test_msg = {"type": "AUTH", "username": "alice"}
    
    await write_frame(writer, test_msg)
    print(f"   Sent: {test_msg}")
    print(f"   Binary size: {len(buffer)} bytes")
    
    # Feed to reader
    reader.feed_data(bytes(buffer))
    decoded = await read_frame(reader)
    print(f"   Received: {decoded}")
    
    assert decoded == test_msg, "Message mismatch!"
    print("   ✅ PASSED")
    
    # Test 2: Complex message with Unicode
    print("\n✅ Test 2: Unicode support")
    buffer.clear()
    
    test_msg2 = {
        "type": "MSG",
        "room": "general",
        "text": "Hello 世界! 🌍 Привет",
        "timestamp": "2026-02-03T10:30:00"
    }
    
    await write_frame(writer, test_msg2)
    print(f"   Sent: {test_msg2}")
    
    reader = asyncio.StreamReader()
    reader.feed_data(bytes(buffer))
    decoded2 = await read_frame(reader)
    print(f"   Received: {decoded2}")
    
    assert decoded2 == test_msg2, "Unicode message mismatch!"
    print("   ✅ PASSED")
    
    # Test 3: Large message
    print("\n✅ Test 3: Large message (1MB)")
    buffer.clear()
    
    large_text = "X" * (1024 * 1024)  # 1MB of data
    test_msg3 = {"type": "MSG", "data": large_text}
    
    await write_frame(writer, test_msg3)
    print(f"   Sent: {len(large_text)} characters")
    print(f"   Binary size: {len(buffer)} bytes")
    
    reader = asyncio.StreamReader()
    reader.feed_data(bytes(buffer))
    decoded3 = await read_frame(reader)
    
    assert decoded3["data"] == large_text, "Large message mismatch!"
    print("   ✅ PASSED")
    
    # Test 4: Disconnect handling
    print("\n✅ Test 4: Disconnect detection")
    reader = asyncio.StreamReader()
    reader.feed_eof()
    
    try:
        await read_frame(reader)
        assert False, "Should have raised DisconnectError"
    except DisconnectError as e:
        print(f"   Correctly detected disconnect: {e}")
        print("   ✅ PASSED")
    
    print("\n" + "=" * 60)
    print("🎉 All protocol tests PASSED!")
    print("\n✅ Foundation is solid. Ready to build the rest!")


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║    High-Performance Async Chat Server - Verification    ║
    ║                Binary Protocol Test Suite                ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    asyncio.run(test_protocol())
