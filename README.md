# Chatroom: an asyncio TCP chat server with its own protocol

A multi-room chat server in pure Python (standard library only, including its Redis client).
- Clients speak a **length-prefixed binary protocol** over TCP, with **TLS**.
- Rooms work as **publish/subscribe** topics that **span every server replica** through Redis. Every message is **stored** with a per-room sequence ID, so clients get history and can resume after a disconnect without gaps or duplicates.
- **Login** is a signed token (HS256 JWT) or a scrypt-hashed password.
- The production server adds **token-bucket rate limiting per user and per IP**, **regex-based input validation** against XSS payloads, connection limits, slow-client protection and a **Prometheus** endpoint.

## Measured performance

From `bench/RESULTS.md`: one process on one core, Python 3.12, Core Ultra 7 155H, median of 3 runs.

| | |
|---|---|
| Frames delivered per second, 50-member room | **67,695** |
| Messages per second, 1:1 | **7,504** |
| Latency, 1:1, light load | 0.46 ms p50 / 1.09 ms p99 |
| Idle connections held | **10,000** at 7.8 KiB each |

Later measurements, same machine:
- Fan-out with no task per member delivers 1.45-1.55x the frames per second of the version above, measured back to back.
- Two replicas sharing Redis deliver 85,594 frames/s to a room split across them.
- A message crossing replicas arrives in 0.79 ms p50.

The details, TLS costs included, are in [bench/RESULTS.md](bench/RESULTS.md).

## Quick start

```bash
python main.py                       # one server on :8888, metrics on :9090 (open login, no TLS)
python -m app.main client            # interactive client (another terminal)
```

Client commands:
- `/join <room>` (shows the last 50 messages);
- `/leave [room]`;
- `/history [room]` (older messages);
- `/rooms`, `/stats`, `/quit`.

Any other line is sent to your current room. `--reconnect` uses the client
that reconnects by itself and resumes each room where it left off.

The full stack runs in Docker: TLS at Nginx, two replicas sharing Redis,
Prometheus and Grafana.

```bash
docker compose up --build            # first start creates ./secrets (CA, certificate, token secret)
python -m app.core.auth issue-token --user alice --secret-file secrets/jwt.secret > alice.jwt
python -m app.main client --tls --ca secrets/ca.pem --token-file alice.jwt
# health: http://localhost/health   Prometheus: http://localhost:9090   Grafana: http://localhost:3000
```

Settings are `CHAT_*` environment variables; every one is listed with its default in [.env.example](.env.example).

## The protocol

```
+----------------------+-------------------------------+
| length: 4 bytes      | body: `length` bytes          |
| big-endian uint32    | UTF-8 JSON object             |
+----------------------+-------------------------------+
```

TCP is a byte stream: one `send()` can arrive as several reads, or several sends as one read. The 4-byte length says exactly where each message ends, and `readexactly()` waits for exactly that many bytes.

Frames larger than `CHAT_MAX_FRAME_SIZE` (128 KiB) are rejected before the body is read. A body that is not a JSON object is a protocol error.

| Client -> server | Server -> client |
|---|---|
| `AUTH {token}` or `AUTH {username, password}` (must be first; open mode: `{username}`) | `AUTH_OK {username}`, or `ERROR "Invalid credentials"` + close |
| `JOIN {room, history?: n, since?: id}` | `JOINED {room, members}`, then any requested history (`MSG ... "history": true`) and `HISTORY_END {count, more, gap?}`; others get `USER_JOINED` |
| `MSG {room, text}` | `MSG {id, room, username, text, timestamp}` to every member, sender included (the sender's copy is its acknowledgement) |
| `HISTORY {room, before?: id, limit?}` | an older page, then `HISTORY_END` |
| `LEAVE {room}` (members only) | `LEFT {room, remaining}`; others get `USER_LEFT` |
| `LIST_ROOMS`, `STATS`, `PING` | `ROOM_LIST`, `STATS`, `PONG` |
| `QUIT` | `BYE`, then the server closes the socket |
| (backplane outage longer than the repair window) | `RESYNC {room}`: page older messages with `HISTORY` |

Message IDs (`<ms>-<seq>`) increase strictly within a room. A client that
reconnects sends `JOIN {room, since: <last id seen>}` and receives exactly what
it missed. When a connection ends, the user leaves every room, the remaining
members get `USER_LEFT`, and the username is freed on every replica.

## Several servers, one chat

```
client ─TLS─> nginx (TLS ends; PROXY protocol) ─> replica 1 ─┐
                                               └─> replica 2 ─┴─> Redis
```

- **One order per room.** Each message is stored in the room's Redis Stream
  and published to its channel in one atomic Lua script, so every replica
  sees the same order.
- **No gaps after a lost subscriber connection.** A replica that loses its
  subscription repairs the gap from the stream when it reconnects.
- **Usernames.** Names are leases (`SET NX PX`), unique across replicas, and
  freed within 30 s if a replica crashes.
- **Presence.** Member counts include every replica.
- **History survives restarts.** Redis keeps an append-only file.
- **Redis outage.** Messages are refused with an error rather than lost
  silently, and `/health` turns 503 so the load balancer stops sending users
  to that replica.

Without `CHAT_REDIS_URL`, one process keeps the last 200 messages per room in memory.

## Protection

| Threat | Defence |
|---|---|
| Impersonation | `token` mode: HS256 JWT. The username is the token's subject. The algorithm is pinned (no `alg: none`), the signature is compared in constant time, and `exp`/`nbf`/`iss`/`aud` are checked. `password` mode: scrypt, hashed in a worker thread, with equal work for unknown users. One generic failure message. |
| Password guessing | 10 login attempts per minute per client IP |
| Eavesdropping | TLS 1.2+ (at the server, or at nginx). The server refuses to start in a credential mode without TLS unless told TLS ends in front of it. |
| Spoofed client IPs behind the balancer | The PROXY protocol header is accepted only from configured proxy addresses, and connections from anywhere else are refused |
| Message floods | Token buckets. Every message must pass the sender's bucket (60/min, burst 10) **and** its IP's bucket (300/min), both charged together. |
| Connection floods | Global cap (10,000) and per-IP cap (100) |
| XSS payloads | Regex rules for script-capable tags, in-tag event handlers and `javascript:`/`data:text/html` URLs. They run on a normalised copy (HTML entities decoded, Unicode NFKC, invisible characters dropped). Clients that render HTML must still escape on output. |
| Slow or stalled clients | A recipient whose socket buffer stays full for 10 s is disconnected, so it cannot hold up a room's broadcast |
| Oversized frames | 128 KiB limit, checked from the header before reading the body |

Secrets (`scripts/init_secrets.py`):
- a private CA and server certificate (EC P-256);
- the token secret and the Redis password.

They are read from files (`*_FILE` variables), never from the command line.

## Observability

`:9090/metrics` serves Prometheus text:
- connections: total, active, refused;
- messages: total, refused, rate;
- message-handling latency as a summary;
- active rooms;
- rate-limited requests;
- errors by type (protocol, timeout, disconnect, slow client, auth, backplane);
- history messages served;
- backplane state: up, reconnects, repaired messages, resyncs.

`/health` returns 503 while shutting down, when full, or when the backplane
is down. The compose stack provisions a Grafana dashboard.

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest                     # 103 tests; the 8 Redis tests skip without a Redis
docker run -d --name chat-test-redis -p 127.0.0.1:6390:6379 redis:7.4-alpine redis-server --requirepass testpw
CHAT_TEST_REDIS_URL=redis://:testpw@127.0.0.1:6390/0 python -m pytest   # all 111
```

The suite covers:
- framing, rooms, broadcast and the connection lifecycle;
- limits, validation and metrics;
- **login:** token forgery and expiry cases, scrypt, the user file, the
  per-IP login limit, and config that refuses unsafe settings;
- **TLS:** real handshakes, untrusted CAs, wrong host names and plain
  clients;
- **PROXY protocol:** v1 and v2 parsing, trusted and untrusted proxies;
- **history:** paging, resume, the history-to-live hand-over with no gaps or
  duplicates, and bounded memory;
- **the Redis client:** against a fake server;
- **two real replicas on Redis:** shared rooms, one global order, unique
  names, crash-safe leases, history across restarts, gap repair, `RESYNC`,
  and a Redis outage with recovery.

## Layout

```
main.py                         production server entry point
app/core/protocol.py            framing: encode_frame, read_frame, write_frame
app/core/auth.py                tokens (HS256), scrypt passwords, user file, CLI
app/core/redis_client.py        asyncio Redis client: pipelining, Lua scripts, self-healing pub/sub
app/core/rate_limiter.py        token buckets (per user, per IP, logins), connection limiter
app/core/validation.py          input rules and XSS patterns
app/core/metrics.py             counters, percentiles, Prometheus text
app/core/config.py              ServerConfig / ClientConfig from CHAT_* variables
app/server/cluster.py           backplane: LocalCluster (one process) / RedisCluster (replicas)
app/server/connection.py        one client: login, commands, history, send, cleanup
app/server/room_manager.py      local room members; encode-once, zero-task fan-out
app/server/proxy_protocol.py    PROXY protocol v1/v2 behind a load balancer
app/server/enhanced_server.py   TLS, limits, metrics HTTP, shutdown (what runs in production)
app/server/server.py            minimal server (no limits/metrics), used by tests
app/client/                     terminal clients (basic, and one that reconnects and resumes)
scripts/init_secrets.py         CA, certificate, token secret, Redis password
bench/                          load generator, suite script and measured results
```

Design, failure modes and scaling limits are in [ARCHITECTURE.md](ARCHITECTURE.md). Deployment notes are in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).
