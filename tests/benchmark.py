"""
Performance Benchmark Suite
Tests server under 1,000+ concurrent connections with realistic load patterns.
"""
import asyncio
import logging
import statistics
import time
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from asyncio import StreamReader, StreamWriter

from app.core.protocol import read_frame, write_frame
from app.core.exceptions import DisconnectError

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class BenchmarkMetrics:
    """Performance metrics from benchmark run."""
    
    # Connection metrics
    total_clients: int = 0
    successful_connections: int = 0
    failed_connections: int = 0
    connection_times: List[float] = field(default_factory=list)
    
    # Message metrics
    total_messages_sent: int = 0
    total_messages_received: int = 0
    message_latencies: List[float] = field(default_factory=list)
    
    # Throughput
    messages_per_second: float = 0.0
    duration_seconds: float = 0.0
    
    # Errors
    errors: List[str] = field(default_factory=list)
    
    def calculate_stats(self) -> Dict:
        """Calculate statistical summary."""
        conn_times = self.connection_times
        latencies = self.message_latencies
        
        return {
            "connections": {
                "total": self.total_clients,
                "successful": self.successful_connections,
                "failed": self.failed_connections,
                "success_rate": f"{(self.successful_connections / max(self.total_clients, 1)) * 100:.1f}%",
                "avg_connect_time_ms": statistics.mean(conn_times) * 1000 if conn_times else 0,
                "median_connect_time_ms": statistics.median(conn_times) * 1000 if conn_times else 0,
            },
            "messages": {
                "sent": self.total_messages_sent,
                "received": self.total_messages_received,
                "delivery_rate": f"{(self.total_messages_received / max(self.total_messages_sent, 1)) * 100:.1f}%",
            },
            "latency_ms": {
                "min": min(latencies) * 1000 if latencies else 0,
                "max": max(latencies) * 1000 if latencies else 0,
                "mean": statistics.mean(latencies) * 1000 if latencies else 0,
                "median": statistics.median(latencies) * 1000 if latencies else 0,
                "p95": statistics.quantiles(latencies, n=20)[18] * 1000 if len(latencies) > 20 else 0,
                "p99": statistics.quantiles(latencies, n=100)[98] * 1000 if len(latencies) > 100 else 0,
            },
            "throughput": {
                "messages_per_second": self.messages_per_second,
                "duration_seconds": self.duration_seconds,
            },
            "errors": {
                "count": len(self.errors),
                "sample": self.errors[:10],
            }
        }
    
    def print_report(self) -> None:
        """Print formatted benchmark report."""
        stats = self.calculate_stats()
        
        print("\n" + "="*70)
        print(" BENCHMARK RESULTS ".center(70, "="))
        print("="*70)
        
        print("\n📊 CONNECTION METRICS")
        print(f"  Total Clients:        {stats['connections']['total']}")
        print(f"  Successful:           {stats['connections']['successful']}")
        print(f"  Failed:               {stats['connections']['failed']}")
        print(f"  Success Rate:         {stats['connections']['success_rate']}")
        print(f"  Avg Connect Time:     {stats['connections']['avg_connect_time_ms']:.2f} ms")
        print(f"  Median Connect Time:  {stats['connections']['median_connect_time_ms']:.2f} ms")
        
        print("\n📨 MESSAGE METRICS")
        print(f"  Messages Sent:        {stats['messages']['sent']}")
        print(f"  Messages Received:    {stats['messages']['received']}")
        print(f"  Delivery Rate:        {stats['messages']['delivery_rate']}")
        
        print("\n⚡ LATENCY (ms)")
        print(f"  Min:                  {stats['latency_ms']['min']:.2f}")
        print(f"  Max:                  {stats['latency_ms']['max']:.2f}")
        print(f"  Mean:                 {stats['latency_ms']['mean']:.2f}")
        print(f"  Median:               {stats['latency_ms']['median']:.2f}")
        print(f"  P95:                  {stats['latency_ms']['p95']:.2f}")
        print(f"  P99:                  {stats['latency_ms']['p99']:.2f}")
        
        print("\n🚀 THROUGHPUT")
        print(f"  Messages/Second:      {stats['throughput']['messages_per_second']:.2f}")
        print(f"  Duration:             {stats['throughput']['duration_seconds']:.2f}s")
        
        if stats['errors']['count'] > 0:
            print(f"\n❌ ERRORS ({stats['errors']['count']} total)")
            for error in stats['errors']['sample']:
                print(f"  - {error}")
        
        print("\n" + "="*70)


class BenchmarkClient:
    """Single benchmark client instance."""
    
    def __init__(
        self,
        client_id: int,
        host: str,
        port: int,
        metrics: BenchmarkMetrics
    ) -> None:
        self.client_id = client_id
        self.host = host
        self.port = port
        self.metrics = metrics
        
        self.username = f"user_{client_id}"
        self.room = f"room_{client_id % 10}"  # Distribute across 10 rooms
        
        self.reader: Optional[StreamReader] = None
        self.writer: Optional[StreamWriter] = None
        self._connected = False
    
    async def connect(self) -> bool:
        """Connect to server."""
        start_time = time.time()
        
        try:
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=10.0
            )
            
            connect_time = time.time() - start_time
            self.metrics.connection_times.append(connect_time)
            
            self._connected = True
            self.metrics.successful_connections += 1
            return True
            
        except Exception as e:
            self.metrics.failed_connections += 1
            self.metrics.errors.append(f"Client {self.client_id} connect failed: {e}")
            return False
    
    async def authenticate(self) -> bool:
        """Authenticate with server."""
        try:
            await write_frame(self.writer, {
                "type": "AUTH",
                "username": self.username
            })
            
            response = await asyncio.wait_for(
                read_frame(self.reader),
                timeout=5.0
            )
            
            return response.get("type") == "AUTH_OK"
            
        except Exception as e:
            self.metrics.errors.append(f"Client {self.client_id} auth failed: {e}")
            return False
    
    async def join_room(self) -> bool:
        """Join a room."""
        try:
            await write_frame(self.writer, {
                "type": "JOIN",
                "room": self.room
            })
            
            response = await asyncio.wait_for(
                read_frame(self.reader),
                timeout=5.0
            )
            
            return response.get("type") == "JOINED"
            
        except Exception as e:
            self.metrics.errors.append(f"Client {self.client_id} join failed: {e}")
            return False
    
    async def send_message(self, text: str) -> float:
        """Send message and return latency."""
        start_time = time.time()
        
        try:
            await write_frame(self.writer, {
                "type": "MSG",
                "room": self.room,
                "text": text
            })
            
            self.metrics.total_messages_sent += 1
            
            # Wait for echo (message delivered to room)
            while True:
                response = await asyncio.wait_for(
                    read_frame(self.reader),
                    timeout=5.0
                )
                
                if response.get("type") == "MSG":
                    latency = time.time() - start_time
                    self.metrics.message_latencies.append(latency)
                    self.metrics.total_messages_received += 1
                    return latency
                
                # Skip other message types (USER_JOINED, etc)
                
        except Exception as e:
            self.metrics.errors.append(f"Client {self.client_id} send failed: {e}")
            return -1.0
    
    async def disconnect(self) -> None:
        """Disconnect from server."""
        if self.writer and not self.writer.is_closing():
            try:
                await write_frame(self.writer, {"type": "QUIT"})
            except:
                pass
            
            self.writer.close()
            await self.writer.wait_closed()


class BenchmarkRunner:
    """Orchestrates benchmark execution."""
    
    def __init__(self, host: str = "127.0.0.1", port: int = 8888) -> None:
        self.host = host
        self.port = port
        self.metrics = BenchmarkMetrics()
    
    async def run_spike_test(self, num_clients: int = 1000) -> None:
        """
        Spike Test: Rapidly create many connections.
        Tests server's ability to handle connection bursts.
        """
        print(f"\n🔥 SPIKE TEST: {num_clients} rapid connections")
        print("-" * 70)
        
        self.metrics = BenchmarkMetrics()
        self.metrics.total_clients = num_clients
        
        start_time = time.time()
        
        # Create all clients rapidly
        tasks = []
        for i in range(num_clients):
            client = BenchmarkClient(i, self.host, self.port, self.metrics)
            tasks.append(self._run_client_spike(client))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        self.metrics.duration_seconds = time.time() - start_time
        self.metrics.print_report()
    
    async def _run_client_spike(self, client: BenchmarkClient) -> None:
        """Single client workflow for spike test."""
        try:
            if not await client.connect():
                return
            
            if not await client.authenticate():
                return
            
            # Just hold connection for a bit
            await asyncio.sleep(5.0)
            
        finally:
            await client.disconnect()
    
    async def run_throughput_test(
        self,
        num_clients: int = 100,
        messages_per_client: int = 50
    ) -> None:
        """
        Throughput Test: Sustained message load.
        Tests server's message processing capacity.
        """
        print(f"\n🚀 THROUGHPUT TEST: {num_clients} clients × {messages_per_client} messages")
        print("-" * 70)
        
        self.metrics = BenchmarkMetrics()
        self.metrics.total_clients = num_clients
        
        start_time = time.time()
        
        # Create clients gradually (ramp up)
        clients = []
        for i in range(num_clients):
            client = BenchmarkClient(i, self.host, self.port, self.metrics)
            clients.append(client)
            
            # Connect
            if not await client.connect():
                continue
            
            # Authenticate
            if not await client.authenticate():
                continue
            
            # Join room
            if not await client.join_room():
                continue
            
            # Small delay to avoid overwhelming server
            if i % 10 == 0:
                await asyncio.sleep(0.1)
        
        print(f"✅ {len(clients)} clients connected and joined rooms")
        
        # Send messages from all clients
        print(f"📨 Sending {num_clients * messages_per_client} messages...")
        
        tasks = []
        for client in clients:
            if client._connected:
                tasks.append(self._send_messages(client, messages_per_client))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Cleanup
        print("🧹 Cleaning up...")
        for client in clients:
            await client.disconnect()
        
        self.metrics.duration_seconds = time.time() - start_time
        self.metrics.messages_per_second = (
            self.metrics.total_messages_sent / self.metrics.duration_seconds
        )
        
        self.metrics.print_report()
    
    async def _send_messages(self, client: BenchmarkClient, count: int) -> None:
        """Send multiple messages from a client."""
        for i in range(count):
            await client.send_message(f"Message {i} from {client.username}")
            await asyncio.sleep(0.05)  # 20 msg/sec per client
    
    async def run_endurance_test(
        self,
        num_clients: int = 500,
        duration_minutes: int = 5
    ) -> None:
        """
        Endurance Test: Long-running load.
        Tests server stability over time.
        """
        print(f"\n⏱️  ENDURANCE TEST: {num_clients} clients for {duration_minutes} minutes")
        print("-" * 70)
        
        self.metrics = BenchmarkMetrics()
        self.metrics.total_clients = num_clients
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        # Create clients
        clients = []
        for i in range(num_clients):
            client = BenchmarkClient(i, self.host, self.port, self.metrics)
            
            if await client.connect():
                if await client.authenticate():
                    if await client.join_room():
                        clients.append(client)
            
            if i % 50 == 0:
                await asyncio.sleep(0.5)
        
        print(f"✅ {len(clients)} clients ready")
        
        # Send messages continuously
        print(f"📨 Sending messages for {duration_minutes} minutes...")
        
        tasks = []
        for client in clients:
            tasks.append(self._send_messages_endurance(client, end_time))
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Cleanup
        for client in clients:
            await client.disconnect()
        
        self.metrics.duration_seconds = time.time() - start_time
        self.metrics.messages_per_second = (
            self.metrics.total_messages_sent / self.metrics.duration_seconds
        )
        
        self.metrics.print_report()
    
    async def _send_messages_endurance(
        self,
        client: BenchmarkClient,
        end_time: float
    ) -> None:
        """Send messages until end time."""
        msg_count = 0
        
        while time.time() < end_time:
            await client.send_message(f"Endurance message {msg_count}")
            msg_count += 1
            await asyncio.sleep(1.0)  # 1 msg/sec per client


async def main():
    """Run benchmark suite."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Chat Server Benchmark")
    parser.add_argument("--host", default="127.0.0.1", help="Server host")
    parser.add_argument("--port", type=int, default=8888, help="Server port")
    parser.add_argument("--test", choices=["spike", "throughput", "endurance", "all"], default="all")
    
    args = parser.parse_args()
    
    runner = BenchmarkRunner(args.host, args.port)
    
    print("="*70)
    print(" CHAT SERVER BENCHMARK SUITE ".center(70, "="))
    print("="*70)
    
    if args.test in ["spike", "all"]:
        await runner.run_spike_test(num_clients=1000)
        await asyncio.sleep(5)
    
    if args.test in ["throughput", "all"]:
        await runner.run_throughput_test(num_clients=100, messages_per_client=50)
        await asyncio.sleep(5)
    
    if args.test in ["endurance", "all"]:
        await runner.run_endurance_test(num_clients=500, duration_minutes=2)
    
    print("\n✅ Benchmark complete!")


if __name__ == "__main__":
    asyncio.run(main())
