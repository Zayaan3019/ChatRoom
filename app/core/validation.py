"""
Input Validation and Sanitization
Prevents injection attacks and ensures data integrity.
"""
import re
from typing import Optional, Tuple


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


class InputValidator:
    """
    Validates and sanitizes user input.
    
    Security features:
        - Length limits
        - Character whitelisting
        - Pattern matching
        - Content sanitization
    """
    
    # Allowed character patterns
    USERNAME_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+$')
    ROOM_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+$')
    
    # Blacklisted patterns (command injection, etc.)
    DANGEROUS_PATTERNS = [
        re.compile(r'<script', re.IGNORECASE),
        re.compile(r'javascript:', re.IGNORECASE),
        re.compile(r'on\w+\s*=', re.IGNORECASE),  # Event handlers
        re.compile(r'\x00'),  # Null bytes
    ]
    
    @staticmethod
    def validate_username(username: str, max_length: int = 50) -> Tuple[bool, Optional[str]]:
        """
        Validate username.
        
        Rules:
            - 1-50 characters
            - Alphanumeric, underscore, hyphen only
            - No leading/trailing whitespace
        
        Returns:
            (is_valid, error_message)
        """
        if not username:
            return False, "Username cannot be empty"
        
        if len(username) > max_length:
            return False, f"Username too long (max {max_length} characters)"
        
        if not InputValidator.USERNAME_PATTERN.match(username):
            return False, "Username can only contain letters, numbers, underscore, and hyphen"
        
        return True, None
    
    @staticmethod
    def validate_room_name(room_name: str, max_length: int = 100) -> Tuple[bool, Optional[str]]:
        """
        Validate room name.
        
        Rules:
            - 1-100 characters
            - Alphanumeric, underscore, hyphen only
        
        Returns:
            (is_valid, error_message)
        """
        if not room_name:
            return False, "Room name cannot be empty"
        
        if len(room_name) > max_length:
            return False, f"Room name too long (max {max_length} characters)"
        
        if not InputValidator.ROOM_NAME_PATTERN.match(room_name):
            return False, "Room name can only contain letters, numbers, underscore, and hyphen"
        
        return True, None
    
    @staticmethod
    def validate_message_text(text: str, max_length: int = 10000) -> Tuple[bool, Optional[str]]:
        """
        Validate message text.
        
        Rules:
            - Not empty
            - Max length limit
            - No dangerous patterns (XSS, injection)
        
        Returns:
            (is_valid, error_message)
        """
        if not text:
            return False, "Message cannot be empty"
        
        if len(text) > max_length:
            return False, f"Message too long (max {max_length} characters)"
        
        # Check for dangerous patterns
        for pattern in InputValidator.DANGEROUS_PATTERNS:
            if pattern.search(text):
                return False, "Message contains forbidden content"
        
        return True, None
    
    @staticmethod
    def sanitize_text(text: str) -> str:
        """
        Sanitize text for safe display.
        
        Removes:
            - Control characters (except newline, tab)
            - Null bytes
            - Leading/trailing whitespace
        """
        # Remove null bytes
        text = text.replace('\x00', '')
        
        # Remove other control characters except \n and \t
        text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\t')
        
        # Normalize whitespace
        text = ' '.join(text.split())
        
        return text.strip()
    
    @staticmethod
    def validate_message_type(msg_type: str) -> bool:
        """
        Validate message type.
        
        Only allow known message types to prevent injection.
        """
        valid_types = {
            "AUTH", "AUTH_OK",
            "JOIN", "JOINED",
            "MSG",
            "LEAVE", "LEFT",
            "LIST_ROOMS", "ROOM_LIST",
            "STATS",
            "QUIT", "BYE",
            "ERROR",
            "USER_JOINED", "USER_LEFT",
            "PING", "PONG"  # Heartbeat
        }
        return msg_type in valid_types
    
    @staticmethod
    def validate_command(command: dict, required_fields: list) -> Tuple[bool, Optional[str]]:
        """
        Validate command structure.
        
        Args:
            command: The command dictionary
            required_fields: List of required field names
        
        Returns:
            (is_valid, error_message)
        """
        if not isinstance(command, dict):
            return False, "Command must be a dictionary"
        
        if "type" not in command:
            return False, "Command missing 'type' field"
        
        if not InputValidator.validate_message_type(command["type"]):
            return False, f"Invalid message type: {command['type']}"
        
        for field in required_fields:
            if field not in command:
                return False, f"Command missing required field: '{field}'"
        
        return True, None
