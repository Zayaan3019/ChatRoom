# 🚀 Quick Start Guide

## Step 1: Verify Installation

The protocol verification test confirms the binary framing layer works correctly:

```powershell
python verify_protocol.py
```

Expected output:
```
🎉 All protocol tests PASSED!
✅ Foundation is solid. Ready to build the rest!
```

## Step 2: Start the Server

Open a terminal and run:

```powershell
python -m app.main server
```

You should see:
```
2026-02-03 10:30:00 [INFO] __main__: Starting server mode on 0.0.0.0:8888
2026-02-03 10:30:00 [INFO] server: 🚀 Chat Server listening on ('0.0.0.0', 8888)
2026-02-03 10:30:00 [INFO] server: Server ready to accept connections...
```

## Step 3: Connect Client #1 (Alice)

Open a **new terminal** and run:

```powershell
python -m app.main client
```

When prompted:
```
Enter your username: alice
```

You'll see:
```
✅ Welcome, alice!

====================================================================
📱 Chat Client - Commands:
  /join <room>   - Join a chat room
  /leave <room>  - Leave a room
  /rooms         - List your rooms
  /stats         - Server statistics
  /quit          - Disconnect
====================================================================

> 
```

Join a room:
```
> /join general
✅ Joined room 'general' (1 members)
```

## Step 4: Connect Client #2 (Bob)

Open **another terminal** and run:

```powershell
python -m app.main client
```

Enter username:
```
Enter your username: bob
```

Join the same room:
```
> /join general
✅ Joined room 'general' (2 members)
📥 alice joined general
```

## Step 5: Chat!

**Alice's terminal:**
```
[general] Hello Bob!
```

**Bob's terminal:**
```
[general] alice: Hello Bob!
[general] Hi Alice! How are you?
```

**Alice's terminal:**
```
[general] bob: Hi Alice! How are you?
```

## Advanced Usage

### Multiple Rooms

```
> /join lobby
✅ Joined room 'lobby' (1 members)

> /join tech-talk
✅ Joined room 'tech-talk' (3 members)

> /rooms
📋 Your rooms: lobby, tech-talk, general
```

### Get Statistics

```
> /stats

📊 Server Statistics:
  Total Rooms: 3
  Total Connections: 5
  Total Messages: 142
  Room Details:
    - general: 2 members
    - lobby: 1 members
    - tech-talk: 3 members
```

### Leave a Room

```
> /leave lobby
👋 Left room 'lobby'
```

### Disconnect

```
> /quit
👋 Goodbye!
```

## Testing

Run the full test suite:

```powershell
# Install pytest first
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run with output
pytest tests/ -v -s
```

## Docker Deployment

Build and run in container:

```powershell
# Build image
docker build -t chat-server:latest .

# Run server
docker run -d --name chat-server -p 8888:8888 chat-server:latest

# View logs
docker logs -f chat-server

# Connect client (from host machine)
python -m app.main client

# Stop server
docker stop chat-server
docker rm chat-server
```

## Troubleshooting

### Port Already in Use

```powershell
# Use a different port
python -m app.main server --port 9000
python -m app.main client --port 9000
```

### Connection Refused

Make sure the server is running:
```powershell
# Check if port 8888 is listening
netstat -an | findstr "8888"
```

### Module Not Found

Run from project root:
```powershell
cd "C:\Users\Mohamed Zayaan\Downloads\ChatRoom"
python -m app.main server
```

## Performance Testing

Simulate 100 concurrent users:

```python
# Create test_load.py
import asyncio
from app.client.client import ChatClient

async def spawn_client(i):
    client = ChatClient()
    await client.connect()
    await client.authenticate(f"user_{i}")
    await client.run()

async def main():
    tasks = [spawn_client(i) for i in range(100)]
    await asyncio.gather(*tasks)

asyncio.run(main())
```

## Next Steps

1. **Add TLS/SSL**: Wrap sockets with `ssl.create_default_context()`
2. **Add Authentication**: Implement JWT tokens or OAuth2
3. **Add Persistence**: Store messages in Redis/PostgreSQL
4. **Add Rate Limiting**: Prevent spam with token bucket algorithm
5. **Add Monitoring**: Export metrics to Prometheus

## Architecture Highlights

### Binary Protocol Benefits

- **4-byte header** = Constant-time parsing
- **No line scanning** = Better than `\n` delimited protocols
- **Predictable buffer sizes** = No surprise memory spikes
- **1MB messages** = Tested and working

### Asyncio Advantages

- **Single-threaded** = No GIL contention
- **Non-blocking I/O** = 1000+ connections on 1 thread
- **Native coroutines** = Clean code with `async/await`
- **StreamReader/Writer** = High-level abstractions over raw sockets

### Pub/Sub Design

- **O(1) join/leave** = Dict + Set operations
- **O(N) broadcast** = Iterate subscribers
- **Memory-safe** = Empty rooms auto-deleted
- **No database** = Pure in-memory (ephemeral)

## Key Files Explained

| File | Purpose | Lines | Complexity |
|------|---------|-------|------------|
| `app/core/protocol.py` | Binary framing | 150 | ⭐⭐⭐⭐⭐ CRITICAL |
| `app/server/room_manager.py` | Pub/Sub logic | 180 | ⭐⭐⭐⭐ |
| `app/server/connection.py` | Client handler | 300 | ⭐⭐⭐⭐ |
| `app/server/server.py` | Accept loop | 150 | ⭐⭐⭐ |
| `app/client/client.py` | CLI client | 350 | ⭐⭐⭐ |

## Common Patterns

### Graceful Shutdown
```python
# Server handles SIGTERM/SIGINT
loop.add_signal_handler(signal.SIGTERM, shutdown_callback)
```

### Error Recovery
```python
try:
    await read_frame(reader)
except DisconnectError:
    # Clean up connection
    await connection.close()
```

### Concurrent Tasks
```python
# Run input and receive loops simultaneously
await asyncio.gather(
    input_loop(),
    receive_loop()
)
```

## Production Checklist

- [ ] Add TLS certificates
- [ ] Implement rate limiting
- [ ] Add health check endpoint
- [ ] Set up log aggregation (ELK stack)
- [ ] Configure reverse proxy (nginx)
- [ ] Set up monitoring (Prometheus + Grafana)
- [ ] Implement message persistence
- [ ] Add load balancer
- [ ] Set up CI/CD pipeline
- [ ] Write operational runbook

---

**Need Help?** Open an issue on GitHub or contact the development team.

**Found a Bug?** Please report it with:
- Python version
- OS (Windows/Linux/Mac)
- Steps to reproduce
- Error logs
