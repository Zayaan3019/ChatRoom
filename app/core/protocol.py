"""
Binary Protocol Implementation - Length-Prefixed Framing
This is the foundation layer that solves TCP stream fragmentation.

Protocol Format:
    [4 bytes: Length (Network Order)] [N bytes: JSON Body (UTF-8)]
    
Example:
    Message: {"type": "MSG", "data": "Hello"}
    Encoded: b'\x00\x00\x00\x1e{"type": "MSG", "data": "Hello"}'
"""
import struct
import json
import asyncio
from typing import Dict, Any, Optional
from asyncio import StreamReader, StreamWriter

from .exceptions import DisconnectError, FramingError, InvalidMessageError
from .config import get_server_config


# Constants
HEADER_SIZE: int = 4  # 4 bytes for unsigned integer
HEADER_FORMAT: str = "!I"  # Network byte order (big-endian), unsigned int

def get_max_frame_size() -> int:
    """Get max frame size from config."""
    try:
        return get_server_config().max_frame_size
    except:
        return 10 * 1024 * 1024  # Fallback


async def read_frame(reader: StreamReader) -> Dict[str, Any]:
    """
    Read a single frame from the TCP stream.
    
    Algorithm:
        1. Read exactly 4 bytes (header) containing payload length
        2. Unpack as unsigned integer in network byte order
        3. Read exactly N bytes (body)
        4. Decode UTF-8 and parse JSON
        
    Args:
        reader: Asyncio StreamReader connected to client
        
    Returns:
        Parsed JSON dictionary
        
    Raises:
        DisconnectError: Client disconnected (EOF)
        FramingError: Protocol violation (incomplete data, oversized frame)
        InvalidMessageError: JSON decoding failed
    """
    try:
        # Step 1: Read header (4 bytes)
        header_bytes = await reader.readexactly(HEADER_SIZE)
        
        # Step 2: Unpack length
        (body_length,) = struct.unpack(HEADER_FORMAT, header_bytes)
        
        # Step 3: Validate frame size
        if body_length == 0:
            raise FramingError("Received frame with zero-length body")
        
        max_size = get_max_frame_size()
        if body_length > max_size:
            raise FramingError(
                f"Frame size {body_length} exceeds maximum {max_size}"
            )
        
        # Step 4: Read body
        body_bytes = await reader.readexactly(body_length)
        
        # Step 5: Decode and parse JSON
        try:
            body_text = body_bytes.decode("utf-8")
            message = json.loads(body_text)
            return message
        except UnicodeDecodeError as e:
            raise InvalidMessageError(f"UTF-8 decode failed: {e}", e)
        except json.JSONDecodeError as e:
            raise InvalidMessageError(f"JSON parse failed: {e}", e)
            
    except asyncio.IncompleteReadError as e:
        # Client disconnected before sending complete frame
        raise DisconnectError(
            f"Connection closed during frame read (expected {e.expected}, got {len(e.partial)})",
            e
        )
    except ConnectionResetError as e:
        raise DisconnectError("Connection reset by peer", e)
    except Exception as e:
        # Catch-all for unexpected errors
        if isinstance(e, (DisconnectError, FramingError, InvalidMessageError)):
            raise
        raise FramingError(f"Unexpected error reading frame: {e}", e)


async def write_frame(writer: StreamWriter, data: Dict[str, Any]) -> None:
    """
    Write a single frame to the TCP stream.
    
    Algorithm:
        1. Serialize dictionary to JSON string
        2. Encode to UTF-8 bytes
        3. Calculate length N
        4. Pack N as 4-byte unsigned integer (network order)
        5. Write header + body
        6. Drain write buffer (await network I/O)
        
    Args:
        writer: Asyncio StreamWriter connected to client
        data: Dictionary to serialize and send
        
    Raises:
        InvalidMessageError: JSON serialization failed
        DisconnectError: Write failed (connection closed)
    """
    try:
        # Step 1-2: Serialize to JSON and encode
        body_text = json.dumps(data, separators=(",", ":"))  # Compact encoding
        body_bytes = body_text.encode("utf-8")
        
        # Step 3: Calculate length
        body_length = len(body_bytes)
        
        max_size = get_max_frame_size()
        if body_length > max_size:
            raise InvalidMessageError(
                f"Message size {body_length} exceeds maximum {max_size}"
            )
        
        # Step 4: Pack header
        header_bytes = struct.pack(HEADER_FORMAT, body_length)
        
        # Step 5: Write frame (header + body)
        writer.write(header_bytes + body_bytes)
        
        # Step 6: Drain buffer (critical for flow control)
        await writer.drain()
        
    except (TypeError, ValueError) as e:
        raise InvalidMessageError(f"JSON serialization failed: {e}", e)
    except ConnectionResetError as e:
        raise DisconnectError("Connection reset during write", e)
    except BrokenPipeError as e:
        raise DisconnectError("Broken pipe (client disconnected)", e)
    except Exception as e:
        if isinstance(e, (DisconnectError, InvalidMessageError)):
            raise
        raise DisconnectError(f"Unexpected error writing frame: {e}", e)


async def read_frame_with_timeout(
    reader: StreamReader, 
    timeout: float = 300.0
) -> Optional[Dict[str, Any]]:
    """
    Read frame with timeout to prevent indefinite hangs.
    
    Args:
        reader: StreamReader instance
        timeout: Seconds before timeout (default 5 minutes)
        
    Returns:
        Parsed message or None on timeout
    """
    try:
        return await asyncio.wait_for(read_frame(reader), timeout=timeout)
    except asyncio.TimeoutError:
        raise DisconnectError(f"Read timeout after {timeout}s")
