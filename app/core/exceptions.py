"""
Custom exceptions for the chat server protocol.
"""
from typing import Optional


class ProtocolError(Exception):
    """Base exception for protocol-related errors."""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None) -> None:
        super().__init__(message)
        self.original_error = original_error


class DisconnectError(ProtocolError):
    """Raised when a client disconnects unexpectedly."""
    pass


class FramingError(ProtocolError):
    """Raised when binary frame parsing fails."""
    pass


class InvalidMessageError(ProtocolError):
    """Raised when message payload is invalid."""
    pass
