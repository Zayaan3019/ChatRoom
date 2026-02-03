# 🚀 Quick Start Guide

## 5-Minute Setup

### Option 1: Docker (Recommended for Production)

```bash
# 1. Clone and navigate
cd ChatRoom

# 2. Start the entire stack
docker-compose up -d

# 3. Check everything is running
docker-compose ps

# 4. Test connection
python -m app.client.enhanced_client
```

**That's it!** You now have:
- ✅ 2 chat server instances (load balanced)
- ✅ Nginx load balancer
- ✅ Prometheus metrics collection
- ✅ Grafana dashboards

**Access:**
- Chat: `localhost:8888`
- Metrics: `http://localhost:9090/metrics`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)

---

### Option 2: Local Development

```bash
# 1. Setup environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 2. Start server (Terminal 1)
python main.py

# 3. Start client (Terminal 2)
python -m app.client.enhanced_client
```

---

## First Connection

```
$ python -m app.client.enhanced_client

Enter your username: alice
✅ Authentication successful

> /join general
✅ Joined room 'general' (0 members)

[general] > Hello World!

[general] > /stats
📊 Server Statistics:
  Total Rooms: 1
  Total Connections: 1
  Total Messages: 1

[general] > /quit
👋 Goodbye!
```

---

## Commands Quick Reference

| Command | Description | Example |
|---------|-------------|---------|
| `/join <room>` | Join a chat room | `/join tech-talk` |
| `/leave <room>` | Leave a room | `/leave tech-talk` |
| `/rooms` | List your rooms | `/rooms` |
| `/stats` | Server statistics | `/stats` |
| `/quit` | Disconnect | `/quit` |
| `<text>` | Send message | `Hello everyone!` |

---

## View Metrics

```bash
# Health check
curl http://localhost:9090/health

# Prometheus metrics
curl http://localhost:9090/metrics

# Server statistics
curl http://localhost:9090/stats
```

---

## Run Benchmarks

```bash
# All tests (spike + throughput + endurance)
python -m tests.benchmark --test all

# Just spike test (1000 connections)
python -m tests.benchmark --test spike

# Just throughput test
python -m tests.benchmark --test throughput
```

Expected output:
```
📊 CONNECTION METRICS
  Total Clients:        1000
  Successful:           998
  Success Rate:         99.8%

⚡ LATENCY (ms)
  P95:                  45.8
  P99:                  89.3

🚀 THROUGHPUT
  Messages/Second:      12,450
```

---

## Configuration

Copy `.env.example` to `.env` and edit:

```bash
cp .env.example .env
nano .env  # or your favorite editor
```

Key settings:
```bash
# Connection limits
CHAT_MAX_CONNECTIONS=10000
CHAT_MAX_CONNECTIONS_PER_IP=50

# Rate limiting
CHAT_MAX_MESSAGES_PER_MINUTE=60
CHAT_ENABLE_RATE_LIMITING=true

# Logging
CHAT_LOG_LEVEL=INFO
```

---

## Troubleshooting

### Server won't start
```bash
# Check if port is in use
netstat -ano | findstr :8888  # Windows
lsof -i :8888  # Linux/Mac

# View logs
docker-compose logs -f chat-server-1
```

### Client can't connect
```bash
# Test server is listening
telnet localhost 8888

# Check Docker containers
docker-compose ps

# Restart stack
docker-compose restart
```

### High latency
```bash
# Check metrics
curl http://localhost:9090/metrics | grep latency

# Check CPU usage
docker stats

# Review configuration
grep CHAT_MAX .env
```

---

## Next Steps

1. **Read Full Documentation**
   - [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) - What was built
   - [docs/PRODUCTION_DEPLOYMENT.md](docs/PRODUCTION_DEPLOYMENT.md) - Production setup

2. **Run Performance Tests**
   ```bash
   python -m tests.benchmark --test all
   ```

3. **Setup Monitoring**
   - Open Grafana: `http://localhost:3000`
   - Login: admin/admin
   - Add Prometheus datasource
   - Import dashboard

4. **Scale Up**
   ```bash
   # Add more server instances
   docker-compose up -d --scale chat-server=5
   ```

---

## Production Checklist

- [ ] Configure `.env` with production settings
- [ ] Enable rate limiting (`CHAT_ENABLE_RATE_LIMITING=true`)
- [ ] Set appropriate connection limits
- [ ] Configure TLS/SSL (via Nginx)
- [ ] Setup log aggregation (ELK/Splunk)
- [ ] Configure monitoring alerts (Prometheus Alertmanager)
- [ ] Run benchmark tests
- [ ] Setup automated backups of configuration
- [ ] Document incident response procedures

---

## Getting Help

- Check [README.md](README.md) for features
- Review [docs/PRODUCTION_DEPLOYMENT.md](docs/PRODUCTION_DEPLOYMENT.md)
- View logs: `docker-compose logs -f`
- Check metrics: `http://localhost:9090/metrics`
- Run tests: `pytest tests/`

---

**You're all set! Start chatting! 🎉**
