"""
Load generator for the chat server. Measures what the README reports.

    # terminal 1: the server, limits off so the benchmark measures capacity
    CHAT_ENABLE_RATE_LIMITING=false CHAT_LOG_LEVEL=WARNING python main.py
    # terminal 2
    python bench/bench_server.py fanout --clients 50 --senders 5 --messages 2000
    python bench/bench_server.py connections --count 10000 --server-pid <pid>

fanout       N clients join one room; S of them send M messages each, keeping
             up to W unacknowledged (a sender's own copy is its ack). Reports
             messages broadcast per second, frames delivered per second
             (= messages x members), end-to-end latency (send -> each
             receiver) and, with --server-pid on Linux, the server's CPU.
connections  Opens and authenticates COUNT connections, holds them, then
             times a message round trip on a fresh connection. Reports the
             server's RSS per connection (Linux, --server-pid).

The load generator runs in one asyncio process; when it, not the server, is
the bottleneck the server CPU reading shows it (well under 100%).

Cluster and security options (fanout):
    --targets a:8888,b:8888   spread clients round-robin over several servers
                              (replicas sharing Redis), so most deliveries
                              cross the backplane
    --tls --ca secrets/ca.pem connect with TLS, verifying the server
    --token-secret-file F     log in with tokens signed by F (token mode)
"""
import argparse
import asyncio
import json
import os
import platform
import ssl
import statistics
import struct
import sys
import time

try:
    import resource  # Linux/macOS: raise the open-file limit
except ImportError:  # Windows
    resource = None

HEADER = struct.Struct("!I")


def frame(obj) -> bytes:
    body = json.dumps(obj, separators=(",", ":")).encode()
    return HEADER.pack(len(body)) + body


async def read(reader):
    (n,) = HEADER.unpack(await reader.readexactly(4))
    return json.loads(await reader.readexactly(n))


TLS = None          # ssl.SSLContext when --tls
SERVER_NAME = None
TOKEN_SECRET = None  # sign a token per client when set
NAME_PREFIX = ""     # differs per repeated run, so names never collide


def auth_frame(name):
    if TOKEN_SECRET:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from app.core.auth import issue_token
        return {"type": "AUTH", "token": issue_token(name, TOKEN_SECRET)}
    return {"type": "AUTH", "username": name}


async def open_client(host, port, name, room=None):
    if TLS is not None:
        reader, writer = await asyncio.open_connection(host, port, ssl=TLS, server_hostname=SERVER_NAME or host)
    else:
        reader, writer = await asyncio.open_connection(host, port)
    writer.write(frame(auth_frame(NAME_PREFIX + name)))
    await writer.drain()
    reply = await read(reader)
    if reply.get("type") != "AUTH_OK":
        raise RuntimeError(f"{name}: {reply}")
    if room:
        writer.write(frame({"type": "JOIN", "room": room}))
        await writer.drain()
        while (await read(reader)).get("type") != "JOINED":
            pass
    return reader, writer


def cpu_seconds(pid):
    """utime + stime of a process, from /proc (Linux)."""
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")


def rss_kib(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return 0


def pct(values, q):
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, int(round(q * (len(ordered) - 1)))))]


async def fanout(args):
    room = "bench"
    targets = [(t.rsplit(":", 1)[0], int(t.rsplit(":", 1)[1])) for t in args.targets.split(",")] \
        if args.targets else [(args.host, args.port)]
    clients = []
    for i in range(args.clients):
        host, port = targets[i % len(targets)]  # senders 0,1,.. land on different servers
        clients.append(await open_client(host, port, f"b{i}", room))
    # drain USER_JOINED notices
    await asyncio.sleep(0.5)
    for reader, _ in clients:
        while True:
            try:
                await asyncio.wait_for(read(reader), 0.01)
            except asyncio.TimeoutError:
                break

    total_msgs = args.senders * args.messages
    expected = total_msgs * args.clients
    latencies = []
    received = 0
    done = asyncio.Event()
    acks = [asyncio.Semaphore(args.window) for _ in range(args.senders)]

    async def receive(idx, reader):
        nonlocal received
        while received < expected:
            msg = await read(reader)
            if msg.get("type") != "MSG":
                continue
            sender, sent_ns = msg["text"].split(":")[1:3]
            latencies.append((time.perf_counter_ns() - int(sent_ns)) / 1e6)
            received += 1
            if idx == int(sender):
                acks[idx].release()
            if received >= expected:
                done.set()

    async def send(idx, writer):
        for seq in range(args.messages):
            await acks[idx].acquire()
            writer.write(frame({"type": "MSG", "room": room,
                                "text": f"m:{idx}:{time.perf_counter_ns()}:{seq}"}))
            await writer.drain()

    cpu0 = cpu_seconds(args.server_pid) if args.server_pid else None
    t0 = time.perf_counter()
    receivers = [asyncio.create_task(receive(i, r)) for i, (r, _) in enumerate(clients)]
    await asyncio.gather(*(send(i, clients[i][1]) for i in range(args.senders)))
    await asyncio.wait_for(done.wait(), 300)
    elapsed = time.perf_counter() - t0
    cpu1 = cpu_seconds(args.server_pid) if args.server_pid else None
    for t in receivers:
        t.cancel()

    result = {
        "scenario": "fanout",
        "clients_in_room": args.clients,
        "senders": args.senders,
        "messages": total_msgs,
        "seconds": round(elapsed, 2),
        "messages_per_second": round(total_msgs / elapsed),
        "deliveries_per_second": round(received / elapsed),
        "latency_ms_p50": round(pct(latencies, 0.50), 2),
        "latency_ms_p99": round(pct(latencies, 0.99), 2),
        "servers": len(targets),
        "tls": TLS is not None,
    }
    if cpu0 is not None:
        result["server_cpu_percent_of_one_core"] = round(100 * (cpu1 - cpu0) / elapsed)
    for _, w in clients:
        w.close()
    return result


async def connections(args):
    rss0 = rss_kib(args.server_pid) if args.server_pid else None
    sem = asyncio.Semaphore(200)
    held = []

    async def one(i):
        async with sem:
            held.append(await open_client(args.host, args.port, f"c{i}"))

    t0 = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(args.count)))
    setup = time.perf_counter() - t0
    await asyncio.sleep(2)

    # With everything held open, time a message round trip on a fresh client.
    r, w = await open_client(args.host, args.port, "probe", "probe-room")
    rtts = []
    for i in range(200):
        start = time.perf_counter()
        w.write(frame({"type": "MSG", "room": "probe-room", "text": f"p{i}"}))
        await w.drain()
        while (await read(r)).get("type") != "MSG":
            pass
        rtts.append((time.perf_counter() - start) * 1000)

    result = {
        "scenario": "connections",
        "connections_held": len(held),
        "setup_seconds": round(setup, 2),
        "connects_per_second": round(len(held) / setup),
        "probe_rtt_ms_p50": round(statistics.median(rtts), 2),
        "probe_rtt_ms_p99": round(pct(rtts, 0.99), 2),
    }
    if rss0 is not None:
        rss1 = rss_kib(args.server_pid)
        result["server_rss_mib_before"] = round(rss0 / 1024, 1)
        result["server_rss_mib_after"] = round(rss1 / 1024, 1)
        result["server_kib_per_connection"] = round((rss1 - rss0) / max(1, len(held)), 1)
    for _, writer in held:
        writer.close()
    w.close()
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("scenario", choices=["fanout", "connections"])
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8888)
    p.add_argument("--server-pid", type=int, default=None)
    p.add_argument("--clients", type=int, default=50)
    p.add_argument("--senders", type=int, default=5)
    p.add_argument("--messages", type=int, default=2000)
    p.add_argument("--window", type=int, default=32)
    p.add_argument("--count", type=int, default=10000)
    p.add_argument("--targets", help="host:port,host:port (fanout across replicas)")
    p.add_argument("--tls", action="store_true")
    p.add_argument("--ca", help="CA certificate for --tls")
    p.add_argument("--server-name", help="certificate name to verify (default: host)")
    p.add_argument("--token-secret-file", help="sign login tokens with this secret")
    p.add_argument("--repeat", type=int, default=1, help="run N times and report the median run")
    args = p.parse_args()

    global TLS, SERVER_NAME, TOKEN_SECRET
    if args.tls:
        TLS = ssl.create_default_context(cafile=args.ca)
        SERVER_NAME = args.server_name
    if args.token_secret_file:
        with open(args.token_secret_file) as f:
            TOKEN_SECRET = f.read().strip()
    if resource is not None:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    global NAME_PREFIX
    runs = []
    for r in range(max(1, args.repeat)):
        NAME_PREFIX = f"r{r}" if args.repeat > 1 else ""
        runs.append(asyncio.run(fanout(args) if args.scenario == "fanout" else connections(args)))
    key = "messages_per_second" if args.scenario == "fanout" else "connects_per_second"
    if args.window == 1 and args.scenario == "fanout":
        key = "latency_ms_p50"  # latency runs: the median latency run
    ordered = sorted(runs, key=lambda r: r[key])
    result = dict(ordered[len(ordered) // 2])
    if len(runs) > 1:
        result["runs"] = len(runs)
        result[f"all_{key}"] = [r[key] for r in runs]
    result["python"] = platform.python_version()
    result["machine"] = platform.processor() or platform.machine()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
