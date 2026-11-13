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
"""
import argparse
import asyncio
import json
import os
import platform
import resource
import statistics
import struct
import time

HEADER = struct.Struct("!I")


def frame(obj) -> bytes:
    body = json.dumps(obj, separators=(",", ":")).encode()
    return HEADER.pack(len(body)) + body


async def read(reader):
    (n,) = HEADER.unpack(await reader.readexactly(4))
    return json.loads(await reader.readexactly(n))


async def open_client(host, port, name, room=None):
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(frame({"type": "AUTH", "username": name}))
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
    clients = [await open_client(args.host, args.port, f"b{i}", room) for i in range(args.clients)]
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
    args = p.parse_args()

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    result = asyncio.run(fanout(args) if args.scenario == "fanout" else connections(args))
    result["python"] = platform.python_version()
    result["machine"] = platform.processor() or platform.machine()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
