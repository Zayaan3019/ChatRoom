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

## Limits of these numbers

- **Single process.** Python runs one event loop per process, so scaling
  means more processes behind the load balancer. Rooms do not span processes
  (there is no backplane); see ARCHITECTURE.md.
- **Same-machine load generator.** It shares the CPU, so these are lower
  bounds for the server. The server CPU reading (100%) shows the server was
  the bottleneck in the throughput runs.
- **Not measured:** WAN latency, TLS (not implemented), or more than 10,000
  connections.
