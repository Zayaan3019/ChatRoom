"""
Input Validation and Sanitization
Prevents injection attacks and ensures data integrity.
"""
import html
import re
import unicodedata
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
    
    # Markup that can execute script when a client renders messages as HTML.
    # Matched against a normalised copy of the text (see _normalise_for_scan),
    # so encodings like "&#x3C;script" or "java\tscript:" cannot slip past,
    # while ordinary prose ("money = 5", "x < y") is left alone.
    DANGEROUS_PATTERNS = [
        # Tags that run or load active content.
        re.compile(
            r"<\s*/?\s*(script|iframe|frame|object|embed|applet|base|meta|link|style|svg|math|form)\b",
            re.IGNORECASE,
        ),
        # An event-handler attribute inside a tag: <img src=x onerror=...>.
        re.compile(r"""<[^>]*[\s/"'](on[a-z]+)\s*=""", re.IGNORECASE),
        # Script-capable URL schemes. Also matched with all whitespace removed,
        # so "java script:" is caught too.
        re.compile(r"(javascript|vbscript|livescript)\s*:", re.IGNORECASE),
        re.compile(r"data\s*:\s*text/html", re.IGNORECASE),
        # CSS expression() (legacy IE script execution).
        re.compile(r"expression\s*\(", re.IGNORECASE),
    ]
    _SCHEME_PATTERNS = DANGEROUS_PATTERNS[2:4]

    @staticmethod
    def _normalise_for_scan(text: str) -> str:
        """The text as a browser might interpret it, for pattern matching only.

        - decode HTML entities repeatedly ("&amp;lt;" -> "&lt;" -> "<")
        - Unicode NFKC, so a full-width "<" (U+FF1C) becomes "<"
        - drop control and zero-width characters browsers ignore inside URLs
        """
        previous = None
        for _ in range(3):
            if text == previous:
                break
            previous, text = text, html.unescape(text)
        text = unicodedata.normalize("NFKC", text)
        keep = {"\n", "\t", " "}
        return "".join(
            ch for ch in text if ch in keep or unicodedata.category(ch) not in {"Cc", "Cf"}
        )

    @classmethod
    def contains_dangerous_markup(cls, text: str) -> bool:
        """True if the text carries markup or a URL scheme that can run script."""
        scanned = cls._normalise_for_scan(text)
        if any(p.search(scanned) for p in cls.DANGEROUS_PATTERNS):
            return True
        squeezed = re.sub(r"\s+", "", scanned)
        return any(p.search(squeezed) for p in cls._SCHEME_PATTERNS)

    @staticmethod
    def escape_html(text: str) -> str:
        """Escape for display in an HTML client. The protocol carries raw text;
        any client that renders HTML must still escape on output."""
        return html.escape(text, quote=True)

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
        
        if '\x00' in text:
            return False, "Message contains forbidden content"
        if InputValidator.contains_dangerous_markup(text):
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
