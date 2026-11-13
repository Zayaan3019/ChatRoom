# Chatroom: an asyncio TCP chat server with its own protocol

A multi-room chat server in pure Python (standard library only).
- Clients speak a **length-prefixed binary protocol** over raw TCP.
- Rooms work as **publish/subscribe** topics.
- The production server adds **token-bucket rate limiting per user and per IP**, **regex-based input validation** against XSS payloads, connection limits, slow-client protection, and a **Prometheus** endpoint.

## Measured performance

From `bench/RESULTS.md`: one process on one core, Python 3.12, Core Ultra 7 155H, median of 3 runs.

| | |
|---|---|
| Frames delivered per second, 50-member room | **67,695** |
| Messages per second, 1:1 | **7,504** |
| Latency, 1:1, light load | 0.46 ms p50 / 1.09 ms p99 |
| Idle connections held | **10,000** at 7.8 KiB each |

## Quick start

```bash
python main.py                       # production server on :8888, metrics on :9090
python -m app.main client            # interactive client (another terminal)
```

Client commands: `/join <room>`, `/leave <room>`, `/rooms`, `/stats`, `/quit`. Any other line is sent to your current room.

Docker, with two servers behind Nginx plus Prometheus and Grafana:

```bash
docker compose up --build
# chat: localhost:8888   health: http://localhost/health
# Prometheus: http://localhost:9090   Grafana: http://localhost:3000 (dashboard "Chat Server")
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
| `AUTH {username}` (must be first; names are unique, case-insensitively) | `AUTH_OK` or `ERROR` + close |
| `JOIN {room}` | `JOINED {room, members}`; others get `USER_JOINED` |
| `MSG {room, text}` | `MSG {room, username, text, timestamp}` to every member, sender included |
| `LEAVE {room}` (members only) | `LEFT {room, remaining}`; others get `USER_LEFT` |
| `LIST_ROOMS`, `STATS`, `PING` | `ROOM_LIST`, `STATS`, `PONG` |
| `QUIT` | `BYE`, then the server closes the socket |

When a connection ends for any reason (QUIT, disconnect, error, slow client), the user leaves every room, the remaining members get `USER_LEFT`, and the username is freed.

## Protection

| Threat | Defence |
|---|---|
| Message floods | Token buckets. Every message must pass the sender's bucket (60/min, burst 10) **and** its IP's bucket (300/min), both charged together. New usernames or extra connections do not multiply the allowance. |
| Connection floods | Global cap (10,000) and per-IP cap (100) |
| XSS payloads | Regex rules for script-capable tags, in-tag event handlers and `javascript:`/`data:text/html` URLs. They run on a normalised copy (HTML entities decoded, Unicode NFKC, invisible characters dropped), so encoded forms are caught and ordinary text like `money = 5` passes. Clients that render HTML must still escape on output (`InputValidator.escape_html`). |
| Slow or stalled clients | A recipient whose socket buffer stays full for 10 s (`CHAT_WRITE_TIMEOUT`) is disconnected, so it cannot hold up a room's broadcast |
| Oversized frames | 128 KiB limit, checked from the header before reading the body |
| Idle connections | Closed after 300 s of silence (`PING` keeps a connection open) |

There is no password: AUTH claims a free username. Usernames are unique only within one server process.

## Observability

`:9090/metrics` serves Prometheus text:
- connections: total, active, refused;
- messages: total, refused, rate;
- message-handling latency as a summary (p50/p95/p99 plus `_sum`/`_count`);
- active rooms;
- rate-limited requests;
- errors by type (protocol, timeout, disconnect, slow client).

`/health` returns 503 while shutting down or when full. `/stats` returns JSON. The compose stack provisions a Grafana dashboard.

## Tests

```bash
pip install -r requirements-dev.txt
python -m pytest            # 48 tests, about 3 s
```

The suite covers:
- framing, rooms and broadcast;
- the connection lifecycle (QUIT, abrupt disconnects, failed and slow sends);
- username uniqueness;
- per-user and per-IP limits;
- XSS payloads and false positives;
- metrics and configuration;
- the production server's HTTP endpoints, connection caps and prompt shutdown.

## Layout

```
main.py                     production server entry point
app/core/protocol.py        framing: encode_frame, read_frame, write_frame
app/core/rate_limiter.py    token buckets (per user, per IP), connection limiter
app/core/validation.py      input rules and XSS patterns
app/core/metrics.py         counters, percentiles, Prometheus text
app/core/config.py          ServerConfig / ClientConfig from CHAT_* variables
app/server/connection.py    one client: auth, commands, send, cleanup
app/server/room_manager.py  rooms, usernames, gather-based broadcast
app/server/enhanced_server.py  limits, metrics HTTP, shutdown (what runs in production)
app/server/server.py        minimal server (no limits/metrics), used by tests
app/client/                 terminal clients (basic, and one with reconnect + heartbeat)
bench/                      load generator and measured results
```

Design, trade-offs and scaling limits are in [ARCHITECTURE.md](ARCHITECTURE.md). Deployment notes are in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).
