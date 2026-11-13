# Architecture

## Layers

```
 client ──TLS──> [ nginx stream: TLS ends, PROXY protocol ] ──> chat replica 1 ─┐
                                                           └──> chat replica 2 ─┤
                                                                                 ▼
                                                 Redis: one stream + one pub/sub channel per room,
                                                        username leases, presence sets

   framing      app/core/protocol.py        4-byte length + JSON object
   connection   app/server/connection.py    one per client: login, commands, history, send, cleanup
   rooms        app/server/room_manager.py  local members of each room; encode-once fan-out
   backplane    app/server/cluster.py       LocalCluster (one process) or RedisCluster (many)
   redis        app/core/redis_client.py    asyncio RESP2 client: pipelining, Lua, self-healing pub/sub
   login        app/core/auth.py            HS256 tokens, scrypt passwords
   edge         app/server/proxy_protocol.py  real client address behind a TCP load balancer
   policy       rate_limiter / validation   token buckets, input rules
   telemetry    app/core/metrics.py         counters + latency window -> :9090/metrics
```

Each server process is one asyncio event loop. Every client is one coroutine
(`ClientConnection.handle`) that reads a frame, processes it and waits for the
next. Waiting on a socket costs no thread, so thousands of idle clients cost
only memory. The measurement is 7.8 KiB per connection (`bench/RESULTS.md`).

Everything is standard library, including the Redis client. The server image
installs no packages.

## Framing

TCP delivers a byte stream, not messages. Every message is
`[uint32 big-endian length][UTF-8 JSON object]`. The reader:
1. does `readexactly(4)` for the length;
2. rejects the frame if the length is 0 or larger than `max_frame_size`, before allocating anything;
3. does `readexactly(length)`, then parses.

A peer that closes cleanly between frames counts as a normal disconnect. A
peer that closes in the middle of a frame counts as a disconnect error.

`encode_frame()` builds the wire bytes once. A broadcast reuses those bytes
for every member on the replica.

## Rooms across replicas (the backplane)

A room is not owned by any server. Every replica that has at least one
member of a room subscribes to that room's Redis channel. Publishing goes
through Redis, even when every member is on the same replica, so there is
one path and one order.

### Publishing: one atomic step

A Lua script runs inside Redis, so nothing can interleave with it:

```
id = XADD chat:room:<room>:log MAXLEN ~ 10000 * e <envelope>   -- store
PEXPIRE chat:room:<room>:log <30 days>                         -- idle rooms expire
PUBLISH chat:room:<room> "<id>|<envelope>"                     -- live delivery
```

- **One order.** Storage and the live channel happen in the same atomic step.
  So the stream's order is the order every replica receives messages in.
  Stream IDs (`<ms>-<seq>`) increase strictly, which makes them per-room
  sequence numbers.
- **Lost on neither path.** A message is stored and published, or neither:
  there is no half state. The sender gets its own copy, with the ID, as its
  acknowledgement.
- **Same envelope everywhere.** The envelope is `{"m": message, "o": origin}`.
  `origin` is the sending connection's ID, which lets the sender's replica
  apply `exclude_sender` for presence notices.
- **Join/leave notices are live-only.** `USER_JOINED` / `USER_LEFT` use the
  same script with storage switched off, so history holds only messages.

### Receiving: per-room state machine

Each subscribed room on a replica has a small state machine
(`_RoomState`): `init -> live`, and `live -> repair -> live`.

- **Deduplication.** Each room remembers the newest ID it delivered. A
  message with an ID at or below that is a duplicate and is dropped.
- **Gap repair.** Pub/sub is fire-and-forget: anything published while the
  subscriber connection is down is gone from the channel. When the
  connection drops, every room switches to `repair`, and new messages are
  buffered. Once all channels are resubscribed, the replica reads the stream
  from the last delivered ID (`XRANGE (last +`). It delivers that gap, then
  the buffer, deduplicating as it goes.
- **Long outages.** If the gap holds more than `repair_limit` (1,000)
  messages, the replica sends clients a `RESYNC` instead of a flood. Clients
  page older messages with `HISTORY`.
- **Per-room queues.** Delivery to local members goes through one queue and
  one worker per room. So a slow fan-out in one room never holds up the
  subscriber connection or other rooms, and order within a room is kept.

The tests drop the subscriber connection on purpose
(`tests/test_cluster_redis.py`). They check that the missed messages arrive
once each, in order, and that a long gap produces `RESYNC`.

### Delivery guarantee

- **Per room.** At-least-once from Redis to each replica, exactly-once to
  clients (deduplication by ID), in one global order per room.
- **To the sender.** Its own copy confirms the message is stored.
- **On failure.** If Redis cannot be reached, the server refuses the message
  ("Message not sent: service temporarily unavailable") rather than
  pretending. The client can resend it.

### Fan-out on a replica

```
frame = encode_frame(message_with_id)            # once per message
for member in local members:                     # no task, no await
    member.deliver_nowait(frame)                 # append to its socket buffer
await gather(wait_drained(m) for m in backed_up) # only members past the high-water mark
```

Writing to a socket buffer never blocks, so the common case does no `await`
at all. Only a member whose buffer is past its high-water mark (it is not
reading) is awaited, for at most `write_timeout` (10 s). Those waits run
concurrently. A member that times out is disconnected as a slow client, so
one stalled member costs a broadcast at most one timeout and never delays
the others.

Before this, `gather()` created one Task per member per message. That Task
was the main per-member cost: removing it delivers 1.45-1.55x the frames per
second in a 50-member room (`bench/RESULTS.md`).

### Usernames across replicas: leases

A username is a Redis key `chat:user:<casefolded name>`.
- **Claim.** `SET key owner NX PX 30000`: the first claimant wins, on every
  replica at once.
- **Renew.** A heartbeat renews every lease the replica holds every 10 s. It
  sends all renewals in one pipeline and uses a Lua compare-and-renew, so it
  never extends someone else's lease.
- **Crash.** If a replica crashes, its names free themselves within 30 s.
- **Release.** Release is compare-and-delete: a replica that wakes up late
  never deletes a lease another replica now holds.
- **Lost lease.** If a lease is lost anyway (for example, Redis restarted
  without its data), the heartbeat tries to take it back. If someone else has
  it, the local session is ended. Two live sessions never share a name.

### Presence across replicas

- **Storage.** Each room has a sorted set of `username -> expiry`. Joining
  adds the user, and the heartbeat refreshes entries.
- **Counting.** A small Lua script deletes expired entries, then counts. So
  `JOINED {members}` counts members on every replica, and a crashed
  replica's users fall out within one lease period.

## History

- **Single process (`LocalCluster`).**
  - Each room has an in-memory log, with IDs in the same `<ms>-<seq>` form.
  - It is bounded per room (200 messages) and in total bytes (64 MiB).
  - When over the byte cap, the least recently active rooms lose their
    oldest messages first.
  - It does not survive a restart.
- **Cluster (`RedisCluster`).**
  - Each room has a Redis Stream, trimmed to about 10,000 entries.
  - An idle room's log expires after 30 days.
  - With Redis's append-only file (`appendfsync everysec`), history survives
    restarts. At most about 1 s of messages is lost if the machine (not just
    the process) crashes.

Clients ask for history; nothing is pushed unasked.

| Request | Server sends |
|---|---|
| `JOIN {room, history: n}` | the newest *n* (≤ 100), oldest first, each marked `"history": true` with its `id`, then `HISTORY_END {count, more}` |
| `JOIN {room, since: id}` | everything after `id` (≤ 500): the resume after a reconnect |
| `HISTORY {room, before: id, limit}` | the page before `id`, for scrolling back |

### The hand-over from history to live messages

A client that joins while others are talking must see every message exactly
once, in order. A message sent while the history is being read lands both in
the stored log and in the live feed. So the JOIN runs in this order:

1. Subscribe first. From then on, nothing published to the room can be
   missed on this replica.
2. Hold the room's live messages for this connection in a buffer.
3. Send `JOINED`, then the history.
4. Release the buffer, dropping every message whose ID is at or below the
   newest one sent as history.

Delivery during the release queues behind the buffer, so nothing overtakes it.

**Long absences.** If a `since` resume finds more messages than it may
replay, the server sends the newest page and marks it `gap: true`. Replaying
the oldest page would leave a hole between that page and the live messages.
The skipped messages can still be reached with `HISTORY`.

## Login

`CHAT_AUTH_MODE` selects one of three modes:

| Mode | AUTH carries | Check |
|---|---|---|
| `open` (development) | `username` | any free name |
| `token` | `token` | HS256 JWT from a login service that shares the secret; the username is the token's `sub` |
| `password` | `username`, `password` | scrypt hash in a user file (`python -m app.core.auth add-user`) |

**Token checks**, in this order:
1. Length, then structure.
2. Algorithm: only HS256. `none` and every other value are rejected, which
   closes the algorithm-confusion attack.
3. Signature, compared in constant time.
4. `exp`, `nbf` and `iat`, with 30 s of clock leeway.
5. `iss` and `aud`.
6. `sub` must be a valid username.

**Passwords.**
- scrypt (N=2^14, r=8, p=1, about 16 MiB and 50 ms per hash), with a
  per-user salt.
- Hashing runs in a worker thread, so it never blocks the event loop.
- An unknown user is checked against a dummy hash, so timing does not reveal
  which names exist.

**Failures.**
- Every failure gets the same reply, "Invalid credentials", and the
  connection is closed.
- Logins are rate-limited per client IP (10 a minute).
- `chat_errors_total{type="auth"}` counts the failures.

## Transport security

**At the server (single node).**
- Settings: `CHAT_TLS_CERT` / `CHAT_TLS_KEY`.
- TLS 1.2 minimum, with the standard library's modern defaults.
- A 10 s handshake deadline.
- The server refuses to start in `token` or `password` mode without TLS,
  unless `CHAT_ALLOW_PLAINTEXT_AUTH` says TLS ends in front of it.

**At the load balancer (the compose stack).**
- nginx ends TLS and forwards plain TCP over the private network. It prefixes
  each connection with a **PROXY protocol** header naming the real client.
- Without that header, every per-IP control (the login limit, connection
  caps, message limits) would see only nginx's address, and all users would
  share one allowance.
- The server reads the header (v1 text or v2 binary) only from
  `CHAT_TRUSTED_PROXIES`. A connection from anywhere else, or without the
  header, is refused. Otherwise any client could claim any address.
- The header is read byte by byte, up to the spec's 107-byte limit, so it
  cannot be used to make the server buffer.

**Setup.** `scripts/init_secrets.py` creates:
- a private CA (EC P-256);
- a server certificate whose names include every service name;
- the token secret and the Redis password.

It never overwrites existing files. In compose, only the runtime files reach
the containers; the CA's private key stays in `./secrets` on the host.

## Connection lifecycle

```
accept ─> [PROXY header] ─> AUTH (30 s deadline, verify, local name, cluster lease)
       ─> command loop ─> close()
              │
              QUIT / EOF / idle 300 s / protocol error / failed or slow send / lease lost
```

Two flags keep cleanup correct:
- `_closing` means "stop reading and sending".
- `_cleaned_up` makes `close()` run exactly once.

`close()` then:
1. leaves every room;
2. removes presence;
3. announces `USER_LEFT`;
4. drops its room subscriptions;
5. releases the username lease;
6. closes the socket.

Each step tolerates Redis being down.

## Abuse controls

| Control | Where | Rule |
|---|---|---|
| Message rate | `RateLimiter` | Token buckets keyed by ("user", name) and ("ip", address). A message needs a token from **both**, taken atomically: a refusal by one bucket charges neither. |
| Login rate | `RateLimiter` | 10 attempts a minute per client IP (the real one, behind the proxy) |
| Join rate | `RateLimiter` | Its own per-user bucket |
| Connections | `ConnectionLimiter` | 10,000 total, 100 per IP, checked at accept |
| Input | `InputValidator` | Usernames `[A-Za-z0-9_-]`, 1-50 chars; room names the same, 1-100 chars; messages up to 10,000 chars, no NUL bytes; at most 50 rooms per user and 1,000 members per room. XSS regexes run on a normalised copy. |
| Frame size | protocol | 128 KiB |

## Observability and health

- **Counters on the hot path.** The hot path only increments counters and
  records the handling time in a fixed-size window. Percentiles are computed
  when `/metrics` is scraped.
- **Backplane metrics:**
  - `chat_backplane_up`;
  - `chat_backplane_reconnects_total`;
  - `chat_backplane_repaired_messages_total`;
  - `chat_backplane_resyncs_total`;
  - `chat_errors_total{type="backplane"}`;
  - `chat_history_messages_sent_total`.
- **Health.** `/health` returns 503 when shutting down, when at the
  connection cap, or when the backplane is down. So a load balancer stops
  sending new users to a replica that cannot serve them.

## Failure modes

| Failure | What happens |
|---|---|
| A replica crashes | Its clients reconnect through nginx to the other replica and resume each room with `since` (the resilient client does this itself). Their names free within one lease period. |
| The subscriber connection drops | Gap repair from the stream; clients see nothing unusual. |
| Redis unreachable | Messages are refused with an error; `/health` 503; reconnects with backoff; repair on return. |
| Redis restarts | Leases are re-taken by the heartbeat. History survives with AOF. Scripts reload on `NOSCRIPT`. |
| A stalled client | Disconnected after `write_timeout` (10 s), without delaying the room. |

## Scaling and its limits

- **One core per process.** To use more cores, run more replicas. Rooms,
  names and history are shared, so any client can use any replica. No sticky
  sessions are needed.
- **One Redis.** Every message passes through one Redis instance (around
  100k simple operations a second on one core). For more scale or
  availability:
  - Redis Sentinel or a managed replica pair, for failover;
  - shard rooms over several Redis instances by hashing the room name.

  Neither is implemented: the client takes one URL.
- **Limits are per replica.** Rate limits and connection caps are counted
  per replica. With N replicas a client could get up to N times the
  allowance by spreading connections. Sharing them in Redis would close
  that, at one Redis round trip per message.
- **Fan-out per message is O(replicas + members).** Every replica with a
  member receives every message of that room. Very large rooms spread across
  many replicas would want room-aware routing.
- **Tokens are not revocable** before they expire. Keep them short-lived (the
  CLI defaults to 1 h) and have the login service refuse to renew.
- **Encryption is transport-level.** TLS protects the wire; the server reads
  messages. End-to-end encryption would need client-side keys.

## The two servers

- `app/server/enhanced_server.py` is the deployed server. It adds TLS, login,
  the backplane, connection limits, rate limiting, metrics HTTP, health and
  shutdown.
  - `main.py` and `python -m app.main server` both start it.
- `app/server/server.py` is the minimal core: framing, rooms and lifecycle,
  with no policy. The integration tests use it to test those parts alone.
