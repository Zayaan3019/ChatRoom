"""
Rate Limiting - Token Bucket Algorithm
Prevents abuse and ensures fair resource usage.
"""
import asyncio
import time
from typing import Dict, Optional
from collections import defaultdict


class TokenBucket:
    """
    Token bucket rate limiter.
    
    Algorithm:
        - Tokens are added at a constant rate
        - Each action consumes tokens
        - If no tokens available, action is denied
    """
    
    def __init__(self, rate: float, burst: int) -> None:
        """
        Args:
            rate: Tokens per second
            burst: Maximum tokens (bucket capacity)
        """
        self.rate = rate
        self.burst = burst
        self.tokens = float(burst)
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()
    
    async def consume(self, tokens: int = 1) -> bool:
        """
        Attempt to consume tokens.
        
        Returns:
            True if tokens consumed, False if rate limited
        """
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            
            # Add tokens based on time elapsed
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            else:
                return False
    
    async def wait_for_token(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """
        Wait until tokens are available.
        
        Returns:
            True if acquired, False if timeout
        """
        start = time.monotonic()
        
        while True:
            if await self.consume(tokens):
                return True
            
            if timeout and (time.monotonic() - start) >= timeout:
                return False
            
            # Wait before retrying
            await asyncio.sleep(0.1)


class RateLimiter:
    """
    Per-user rate limiter using token buckets.
    
    Tracks multiple rate limits per user:
        - Messages per minute
        - Joins per minute
        - Commands per minute
    """
    
    def __init__(
        self,
        messages_per_minute: int = 60,
        joins_per_minute: int = 10,
        burst: int = 10
    ) -> None:
        self.messages_rate = messages_per_minute / 60.0  # Convert to per-second
        self.joins_rate = joins_per_minute / 60.0
        self.burst = burst
        
        # user_id -> action_type -> TokenBucket
        self._buckets: Dict[str, Dict[str, TokenBucket]] = defaultdict(dict)
        self._lock = asyncio.Lock()
    
    def _get_bucket(self, user_id: str, action: str) -> TokenBucket:
        """Get or create token bucket for user and action."""
        if action not in self._buckets[user_id]:
            if action == "message":
                rate = self.messages_rate
            elif action == "join":
                rate = self.joins_rate
            else:
                rate = 1.0  # Default rate
            
            self._buckets[user_id][action] = TokenBucket(rate, self.burst)
        
        return self._buckets[user_id][action]
    
    async def check_message(self, user_id: str) -> bool:
        """Check if user can send a message."""
        bucket = self._get_bucket(user_id, "message")
        return await bucket.consume(1)
    
    async def check_join(self, user_id: str) -> bool:
        """Check if user can join a room."""
        bucket = self._get_bucket(user_id, "join")
        return await bucket.consume(1)
    
    async def check_action(self, user_id: str, action: str) -> bool:
        """Check if user can perform generic action."""
        bucket = self._get_bucket(user_id, action)
        return await bucket.consume(1)
    
    async def cleanup_user(self, user_id: str) -> None:
        """Remove rate limit tracking for disconnected user."""
        async with self._lock:
            if user_id in self._buckets:
                del self._buckets[user_id]
    
    def get_stats(self) -> Dict:
        """Get rate limiter statistics."""
        return {
            "tracked_users": len(self._buckets),
            "messages_rate": self.messages_rate * 60,
            "joins_rate": self.joins_rate * 60,
            "burst": self.burst
        }


class ConnectionLimiter:
    """
    Limits concurrent connections globally and per-IP.
    """
    
    def __init__(
        self,
        max_total: int = 10000,
        max_per_ip: int = 100
    ) -> None:
        self.max_total = max_total
        self.max_per_ip = max_per_ip
        
        self.total_connections = 0
        self._ip_connections: Dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()
    
    async def acquire(self, ip_address: str) -> bool:
        """
        Attempt to acquire connection slot.
        
        Returns:
            True if allowed, False if limit exceeded
        """
        async with self._lock:
            # Check global limit
            if self.total_connections >= self.max_total:
                return False
            
            # Check per-IP limit
            if self._ip_connections[ip_address] >= self.max_per_ip:
                return False
            
            # Grant connection
            self.total_connections += 1
            self._ip_connections[ip_address] += 1
            return True
    
    async def release(self, ip_address: str) -> None:
        """Release connection slot."""
        async with self._lock:
            self.total_connections = max(0, self.total_connections - 1)
            self._ip_connections[ip_address] = max(0, self._ip_connections[ip_address] - 1)
            
            # Cleanup empty entries
            if self._ip_connections[ip_address] == 0:
                del self._ip_connections[ip_address]
    
    def get_stats(self) -> Dict:
        """Get connection statistics."""
        return {
            "total_connections": self.total_connections,
            "max_total": self.max_total,
            "max_per_ip": self.max_per_ip,
            "ip_breakdown": dict(self._ip_connections)
        }
