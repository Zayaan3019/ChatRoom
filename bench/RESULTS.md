# Benchmark results

Every number here comes from `bench/bench_server.py` and is reproducible with
the commands below. Each is the **median of 3 runs**.

**Setup:**
- Server and load generator run in one container: `python:3.12-slim`, Python 3.12.16, Linux 6.6 (WSL2), Intel Core Ultra 7 155H.
- One server process. Python runs the event loop on a single core.
- Rate limiting is off and logging is at WARNING, so the run measures capacity rather than the limiter.

```bash
CHAT_ENABLE_RATE_LIMITING=false CHAT_LOG_LEVEL=WARNING \
CHAT_MAX_CONNECTIONS=20000 CHAT_MAX_CONNECTIONS_PER_IP=20000 python main.py &
PID=$!
python bench/bench_server.py fanout --clients 50 --senders 5 --messages 2000 --server-pid $PID
python bench/bench_server.py fanout --clients 2 --senders 1 --messages 20000 --window 64 --server-pid $PID
python bench/bench_server.py fanout --clients 2 --senders 1 --messages 2000 --window 1 --server-pid $PID
python bench/bench_server.py fanout --clients 50 --senders 1 --messages 500 --window 1 --server-pid $PID
python bench/bench_server.py connections --count 10000 --server-pid $PID
```

## Throughput (server saturated: 100% of one core)

| Scenario | Messages/s | Frames delivered/s |
|---|---:|---:|
| 50 members in one room, 5 senders | 1,354 | **67,695** |
| 1:1 (2 members), pipelined | **7,504** | 15,008 |

A message to a 50-member room is 50 frames, so the per-message cost grows with
room size. The server's real unit of work is the delivered frame.

## Latency (light load, one message in flight)

| Scenario | p50 | p99 |
|---|---:|---:|
| 1:1, send to receipt | 0.46 ms | 1.09 ms |
| 50-member room, send to each receipt | 2.72 ms | 5.36 ms |

Under saturation, latency becomes queueing time: about 110 ms (p50) in the
50-member run above, because the load generator keeps 160 messages in flight.

## Connections

| Metric | Value |
|---|---:|
| Idle authenticated connections held | **10,000** (all accepted) |
| Server memory per connection | 7.8 KiB (RSS 27 MiB -> 101 MiB) |
| Message round trip on a fresh client while 10,000 are held | 0.59 ms p50, 0.88 ms p99 |

## What changed the numbers

Broadcasts used to JSON-encode the frame once per recipient and wrap every
write in its own timeout task. Now a broadcast encodes the frame once and
writes the same bytes to every member. The timed wait happens only when a
member's socket buffer is past its high-water mark (a slow client). In the
50-member scenario this took deliveries from 34,515/s to 67,695/s, and 1:1
went from 6,194 to 7,504 msg/s.

## Cluster mode, TLS and a narrower gather in fan-out (2026-11-13)

These come from `bench/run_suite.sh`, which runs every scenario below in one
`python:3.12-slim` container, with Redis 7.4 in a second container on the
same Docker network. Same machine as above; median of 3 runs each.

```bash
docker network create chatbench
docker run -d --name chatbench-redis --network chatbench redis:7.4-alpine \
    redis-server --appendonly yes --appendfsync everysec
docker run --rm --network chatbench -v "$PWD":/src -w /src python:3.12-slim \
    sh bench/run_suite.sh redis://chatbench-redis:6379/0
```

### Fan-out: `asyncio.gather` only where waiting is needed

A broadcast now writes the encoded frame into each member's socket buffer
synchronously. It awaits only members whose buffer is past its high-water
mark: slow readers, still capped at `write_timeout` each and awaited
concurrently with `asyncio.gather`. Before, the `gather` wrapped every member's send in its own Task.

That Task was the main per-member cost. Previous commit vs this one, back to
back in the same container (two rounds, median of 3 each):

| Scenario | Speed-up |
|---|---:|
| 50-member room, frames delivered/s | **1.45x - 1.55x** |
| 1:1 pipelined, messages/s | 1.22x - 1.48x |
| 1:1 latency | unchanged (p50 0.39 - 0.51 ms both) |

The same build now also stores every message (in memory, for history) and
gives it an ID, so the speed-up is net of that work.

### Results for this build

| Scenario | Messages/s | Frames delivered/s | Latency p50 / p99 |
|---|---:|---:|---:|
| One server, 50 members, 5 senders | 1,847 | **92,338** | (saturated) |
| One server, 1:1 pipelined | **10,922** | 21,843 | (saturated) |
| One server, 1:1, one in flight | | | **0.48 / 1.14 ms** |
| **Two replicas + Redis**, 50 members split over both, 5 senders | 1,712 | **85,594** | (saturated) |
| Two replicas + Redis, 1:1 across replicas, pipelined | 1,687 | 3,375 | 33 / 93 ms |
| Two replicas + Redis, 1:1 across replicas, one in flight | | | **0.79 / 2.97 ms** |
| **TLS** + token login, one server, 50 members | 1,067 | 53,358 | (saturated) |
| TLS, 1:1 pipelined | 5,491 | 10,982 | (saturated) |
| TLS, 1:1, one in flight | | | 0.47 / 1.20 ms |

**Two replicas.**
- A room split over two replicas delivers about the same frames per second
  as one server holding the whole room. Each message also takes a Redis round
  trip and a pub/sub hop, while the fan-out work is shared by two processes.
- The cross-replica hop adds about 0.3 ms at p50.
- One sending connection is limited by the Redis round trip (about 1.7k
  msg/s). The server finishes one message, store and publish included,
  before it reads that connection's next one. That is what keeps each
  sender's messages in order and lets every message be acknowledged with its
  ID. Total throughput grows with the number of senders, not with one
  sender's speed.

**TLS in the server process.**
- Large-room throughput is about 40% lower. Each member's copy is
  encrypted separately.
- Latency at light load is unchanged.
- Memory per connection is high:

| | Plain TCP | TLS |
|---|---:|---:|
| Server memory per idle connection | 7.8 KiB | **288 KiB** (2,000 connections: RSS 56 -> 620 MiB) |
| Full handshakes per second (client on the same CPU) | | 393 |

asyncio's TLS layer keeps a 256 KiB read buffer per connection. That is why
the compose stack ends TLS at nginx: a server then holds plain connections
at 7.8 KiB each. In-process TLS is for a single node with a modest number of
clients.

## Limits of these numbers

- **One process per core.** Python runs one event loop per process.
  Scaling means more replicas behind the load balancer, and rooms now span
  them through Redis. The replicas and Redis above share one machine with
  the load generator.
- **Same-machine load generator.** It shares the CPU, so these are lower
  bounds for the server. The server CPU reading (about 100%) shows the
  server was the bottleneck in the throughput runs.
- **Not measured:** WAN latency, Redis on its own host, nginx's TLS
  throughput, or more than 10,000 connections.
