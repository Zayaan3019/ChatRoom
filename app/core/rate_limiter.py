"""
Rate Limiting - Token Bucket Algorithm
Prevents abuse and ensures fair resource usage.

Messages are limited per user AND per IP address. A message is allowed only
when both buckets have a token, and then both are charged together, so a
refusal from one never wastes the other's token. The per-IP bucket is what
stops a client from multiplying its allowance by opening many connections or
reconnecting under new usernames.

The server is a single asyncio event loop and none of the bucket operations
await, so each check is atomic without locks.
"""
import time
from typing import Callable, Dict, Optional, Tuple


class TokenBucket:
    """
    Token bucket.

    Tokens refill continuously at `rate` per second up to `burst`; each action
    takes one. A full bucket is indistinguishable from a new one, which is
    what lets idle buckets be dropped without changing behaviour.
    """

    def __init__(self, rate: float, burst: int, clock: Callable[[], float] = time.monotonic) -> None:
        if rate <= 0 or burst <= 0:
            raise ValueError("rate and burst must be positive")
        self.rate = rate
        self.burst = burst
        self._clock = clock
        self.tokens = float(burst)
        self.last_update = clock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self.last_update)
        self.tokens = min(float(self.burst), self.tokens + elapsed * self.rate)
        self.last_update = now

    def available(self) -> float:
        """Tokens available right now."""
        self._refill()
        return self.tokens

    def take(self, tokens: int = 1) -> None:
        """Remove tokens; call only after available() showed enough."""
        self.tokens -= tokens

    def is_full(self) -> bool:
        return self.available() >= self.burst

    def try_consume(self, tokens: int = 1) -> bool:
        if self.available() >= tokens:
            self.take(tokens)
            return True
        return False

    async def consume(self, tokens: int = 1) -> bool:
        """Async-compatible wrapper kept for callers that await it."""
        return self.try_consume(tokens)


def _consume_all(buckets: Tuple[TokenBucket, ...], tokens: int = 1) -> bool:
    """Charge every bucket, or none of them."""
    if all(b.available() >= tokens for b in buckets):
        for b in buckets:
            b.take(tokens)
        return True
    return False


class RateLimiter:
    """
    Token-bucket limits per user and per IP.

    Keys:
        ("user", name, action)  - each user's own allowance
        ("ip", address, action) - shared by every connection from one IP
    """

    PRUNE_EVERY = 1024  # checks between sweeps of idle (full) buckets

    def __init__(
        self,
        messages_per_minute: int = 60,
        joins_per_minute: int = 10,
        burst: int = 10,
        messages_per_minute_per_ip: Optional[int] = None,
        joins_per_minute_per_ip: Optional[int] = None,
        auth_attempts_per_minute_per_ip: int = 10,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.burst = burst
        self._clock = clock
        # Per-second rates for each (scope, action).
        self._rates: Dict[Tuple[str, str], float] = {
            ("user", "message"): messages_per_minute / 60.0,
            ("user", "join"): joins_per_minute / 60.0,
            ("ip", "message"): (messages_per_minute_per_ip or messages_per_minute * 5) / 60.0,
            ("ip", "join"): (joins_per_minute_per_ip or joins_per_minute * 5) / 60.0,
            # Login attempts: per IP only (the username is what is being proved).
            ("ip", "auth"): auth_attempts_per_minute_per_ip / 60.0,
        }
        self._auth_burst = max(1, auth_attempts_per_minute_per_ip)
        # The IP bucket's burst scales with its rate, so a handful of users
        # behind one NAT can each still send their own burst.
        self._ip_burst_factor = max(
            1.0, self._rates[("ip", "message")] / max(self._rates[("user", "message")], 1e-9)
        )
        self._buckets: Dict[Tuple[str, str, str], TokenBucket] = {}
        self._checks = 0

    # Kept for compatibility with code that reads these attributes.
    @property
    def messages_rate(self) -> float:
        return self._rates[("user", "message")]

    @property
    def joins_rate(self) -> float:
        return self._rates[("user", "join")]

    def _bucket(self, scope: str, key: str, action: str) -> TokenBucket:
        bucket_key = (scope, key, action)
        bucket = self._buckets.get(bucket_key)
        if bucket is None:
            rate = self._rates.get((scope, action), 1.0)
            if action == "auth":
                burst = self._auth_burst
            elif scope == "user":
                burst = self.burst
            else:
                burst = max(self.burst, round(self.burst * self._ip_burst_factor))
            bucket = TokenBucket(rate, burst, self._clock)
            self._buckets[bucket_key] = bucket
        return bucket

    def _check(self, action: str, user_id: str, ip_address: Optional[str]) -> bool:
        self._checks += 1
        if self._checks % self.PRUNE_EVERY == 0:
            self.prune()
        buckets = [self._bucket("user", user_id, action)]
        if ip_address:
            buckets.append(self._bucket("ip", ip_address, action))
        return _consume_all(tuple(buckets))

    async def check_message(self, user_id: str, ip_address: Optional[str] = None) -> bool:
        """True if this user (and its IP) may send a message now."""
        return self._check("message", user_id, ip_address)

    async def check_join(self, user_id: str, ip_address: Optional[str] = None) -> bool:
        """True if this user (and its IP) may join a room now."""
        return self._check("join", user_id, ip_address)

    async def check_auth(self, ip_address: Optional[str]) -> bool:
        """True if this IP may make another login attempt now. Every AUTH
        costs a token, so password guessing is capped per address."""
        self._checks += 1
        if not ip_address:
            return True
        return _consume_all((self._bucket("ip", ip_address, "auth"),))

    async def check_action(self, user_id: str, action: str, ip_address: Optional[str] = None) -> bool:
        return self._check(action, user_id, ip_address)

    def prune(self) -> int:
        """Drop buckets that have refilled completely. Lossless: a full bucket
        behaves exactly like a new one. Returns how many were dropped."""
        idle = [key for key, bucket in self._buckets.items() if bucket.is_full()]
        for key in idle:
            del self._buckets[key]
        return len(idle)

    async def cleanup_user(self, user_id: str) -> None:
        """Kept for compatibility. A user's bucket is deliberately NOT reset on
        disconnect -- reconnecting must not refill an allowance -- and is
        removed by prune() once it has refilled on its own."""
        return None

    def get_stats(self) -> Dict:
        users = {key[1] for key in self._buckets if key[0] == "user"}
        ips = {key[1] for key in self._buckets if key[0] == "ip"}
        return {
            "tracked_users": len(users),
            "tracked_ips": len(ips),
            "messages_rate": self._rates[("user", "message")] * 60,
            "messages_rate_per_ip": self._rates[("ip", "message")] * 60,
            "joins_rate": self._rates[("user", "join")] * 60,
            "burst": self.burst,
        }


class ConnectionLimiter:
    """
    Limits concurrent connections globally and per IP.
    """

    def __init__(self, max_total: int = 10000, max_per_ip: int = 100) -> None:
        self.max_total = max_total
        self.max_per_ip = max_per_ip
        self.total_connections = 0
        self._ip_connections: Dict[str, int] = {}

    async def acquire(self, ip_address: str) -> bool:
        """Take a connection slot; False if a limit is reached."""
        if self.total_connections >= self.max_total:
            return False
        if self._ip_connections.get(ip_address, 0) >= self.max_per_ip:
            return False
        self.total_connections += 1
        self._ip_connections[ip_address] = self._ip_connections.get(ip_address, 0) + 1
        return True

    async def release(self, ip_address: str) -> None:
        """Return a connection slot."""
        self.total_connections = max(0, self.total_connections - 1)
        remaining = self._ip_connections.get(ip_address, 0) - 1
        if remaining > 0:
            self._ip_connections[ip_address] = remaining
        else:
            self._ip_connections.pop(ip_address, None)

    def get_stats(self) -> Dict:
        return {
            "total_connections": self.total_connections,
            "max_total": self.max_total,
            "max_per_ip": self.max_per_ip,
            "ip_breakdown": dict(self._ip_connections),
        }
