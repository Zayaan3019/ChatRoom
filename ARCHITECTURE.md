# Architecture

## Layers

```
 client ──TCP──> [ nginx stream LB ] ──> chat server process (one per core)
                                          │
   framing      app/core/protocol.py      │  4-byte length + JSON object
   connection   app/server/connection.py  │  one per client: auth, commands, send, cleanup
   rooms        app/server/room_manager.py│  room -> set of connections; username registry
   policy       rate_limiter / validation │  token buckets, input rules
   telemetry    app/core/metrics.py       │  counters + latency window -> :9090/metrics
```

Each server process is one asyncio event loop. Every client is one coroutine
(`ClientConnection.handle`) that reads a frame, processes it and waits for the
next. Waiting on a socket costs no thread, so thousands of idle clients cost
only memory. The measurement is 7.8 KiB per connection (`bench/RESULTS.md`).

## Framing

TCP delivers a byte stream, not messages. Every message is
`[uint32 big-endian length][UTF-8 JSON object]`. The reader:
1. does `readexactly(4)` for the length;
2. rejects the frame if the length is 0 or larger than `max_frame_size`, before allocating anything;
3. does `readexactly(length)`, then parses.

A peer that closes cleanly between frames counts as a normal disconnect. A
peer that closes in the middle of a frame counts as a disconnect error.

`encode_frame()` builds the wire bytes once. A broadcast reuses those bytes
for every member.

## Rooms (pub/sub)

`RoomManager` keeps three dicts:
- `room -> set(connection)`;
- `connection -> set(room)`;
- `casefolded username -> connection`.

Join and leave are set operations, O(1) on average. An empty room is deleted
at once, so the room count and gauge go down as well as up.

### Broadcast

```
frame = encode_frame(message)                     # once
await gather(*(send_bytes(c, frame) for c in members))
```

`send_bytes` writes into the socket's buffer and normally returns without
waiting. It waits only when that buffer is past its high-water mark, which
means this client is not reading fast enough. It then waits at most
`write_timeout` (10 s), and on timeout the client is disconnected and counted
as a slow client.

So one stalled member costs the broadcast at most one timeout, once, and the
other members are not delayed by it. Total work per message is O(members): one
encode plus one buffer append per member.

## Connection lifecycle

```
accept ─> AUTH (30 s deadline, unique username) ─> command loop ─> close()
                                                      │
                 QUIT / EOF / idle 300 s / protocol error / failed or slow send
```

Two flags keep cleanup correct:
- `_closing` means "stop reading and sending". QUIT, a failed send or `close()` sets it.
- `_cleaned_up` makes `close()` run its work exactly once, however the connection ended.

`close()` then:
1. leaves every room;
2. frees the username;
3. tells each remaining room `USER_LEFT`;
4. closes the socket, aborting it if the peer will not finish the close within 2 s.

A failed send also detaches the connection at once: it leaves every room and
the transport is aborted. Later broadcasts stop targeting it, and its read
loop wakes up to run `close()`.

## Abuse controls (enhanced server)

| Control | Where | Rule |
|---|---|---|
| Message rate | `RateLimiter` | Token buckets keyed by ("user", name) and ("ip", address). A message needs a token from **both**, taken atomically: a refusal by one bucket charges neither. The IP bucket allows 300/min (burst scales by the same ratio), so a few users behind one NAT are fine, but one machine cannot multiply its allowance with new names. Idle full buckets are pruned, so memory follows active clients. |
| Join rate | `RateLimiter` | Its own per-user bucket |
| Connections | `ConnectionLimiter` | 10,000 total, 100 per IP, checked at accept |
| Input | `InputValidator` | Usernames `[A-Za-z0-9_-]`, 1-50 chars; room names the same, 1-100 chars; messages up to 10,000 chars, no NUL bytes; at most 50 rooms per user and 1,000 members per room. XSS regexes run on a normalised copy (HTML entities decoded 3×, NFKC, control/format characters removed). |
| Frame size | protocol | 128 KiB |

## Observability

- **Counters and gauges.** The hot path only increments counters and appends
  the handling time to a fixed-size deque (the last 10,000 samples).
- **Scrape-time work.** Percentiles (nearest rank) and the message rate are
  computed only when `/metrics` is scraped, never per message.
- **HTTP endpoints.** A tiny HTTP responder on the metrics port serves
  `/metrics`, `/health` (computed live: 503 when shutting down or at the
  connection cap) and `/stats`.
- **Compose stack.** It provisions a Prometheus data source and an
  eight-panel Grafana dashboard.

## Shutdown

On SIGINT or SIGTERM the server:
1. stops accepting connections;
2. sends `BYE` to every client and detaches them;
3. waits for the handler tasks (bounded);
4. closes the metrics listener.

`tests/test_hardening.py` checks that this finishes promptly with clients
still connected.

## Scaling and its limits

- **One core per process.** To use more cores, run more processes or
  containers behind the load balancer. The compose file runs two behind
  nginx `least_conn`.
- **Rooms do not span processes.** Two users on different replicas, in the
  same room name, do not see each other's messages. Usernames are also
  unique only per process.
  - The fix is a backplane: publish each room message to Redis pub/sub or
    NATS, and have every process deliver it to its local members.
  - The alternative is to route by room, which conflicts with
    one-connection-many-rooms.
  - Neither is implemented.
- **No persistence.** Messages are delivered live and never stored. A client
  that is offline misses them.
- **No authentication or TLS.** AUTH claims a free name. In production, put
  TLS on the load balancer and verify a token in AUTH.

## The two servers

- `app/server/enhanced_server.py` is the deployed server. It adds connection
  limits, rate limiting, metrics HTTP, health and shutdown.
  - `main.py` and `python -m app.main server` both start it.
- `app/server/server.py` is the minimal core: framing, rooms and lifecycle,
  with no policy. The integration tests use it to test those parts alone.
