# Production Deployment Guide

## 🚀 Overview

This guide covers deploying the production-grade chat server with monitoring, load balancing, and high availability.

---

## 📋 Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Prometheus (for metrics)
- Grafana (for dashboards)
- Nginx (for load balancing)

---

## 🔧 Configuration

### Environment Variables

Create a `.env` file:

```bash
# Server Configuration
CHAT_SERVER_HOST=0.0.0.0
CHAT_SERVER_PORT=8888
CHAT_METRICS_PORT=9090

# Rate Limiting
CHAT_MAX_CONNECTIONS=10000
CHAT_MAX_MESSAGES_PER_MINUTE=60
CHAT_MAX_MESSAGES_PER_HOUR=1800
CHAT_MAX_CONNECTIONS_PER_IP=50
CHAT_ENABLE_RATE_LIMITING=true

# Resource Limits
CHAT_MAX_FRAME_SIZE=10485760  # 10 MB
CHAT_MAX_USERNAME_LENGTH=32
CHAT_MAX_ROOM_NAME_LENGTH=64
CHAT_MAX_MESSAGE_LENGTH=4096
CHAT_MAX_ROOMS_PER_USER=20
CHAT_MAX_ROOM_SIZE=1000

# Timeouts
CHAT_AUTH_TIMEOUT=30.0
CHAT_READ_TIMEOUT=300.0
CHAT_WRITE_TIMEOUT=10.0
CHAT_SHUTDOWN_TIMEOUT=10.0

# Logging
CHAT_LOG_LEVEL=INFO
```

### Load Configuration

```python
from app.core.config import get_server_config

config = get_server_config()
```

---

## 🐳 Docker Deployment

### Single Instance

```bash
# Build image
docker build -t chat-server:latest .

# Run server
docker run -d \
  --name chat-server \
  -p 8888:8888 \
  -p 9090:9090 \
  --env-file .env \
  --restart unless-stopped \
  chat-server:latest
```

### Docker Compose (Multi-Instance)

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  chat-server-1:
    build: .
    ports:
      - "8888:8888"
      - "9091:9090"
    environment:
      - CHAT_SERVER_HOST=0.0.0.0
      - CHAT_SERVER_PORT=8888
      - CHAT_METRICS_PORT=9090
    env_file:
      - .env
    restart: unless-stopped
    networks:
      - chat-network

  chat-server-2:
    build: .
    ports:
      - "8889:8888"
      - "9092:9090"
    environment:
      - CHAT_SERVER_HOST=0.0.0.0
      - CHAT_SERVER_PORT=8888
      - CHAT_METRICS_PORT=9090
    env_file:
      - .env
    restart: unless-stopped
    networks:
      - chat-network

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - chat-server-1
      - chat-server-2
    restart: unless-stopped
    networks:
      - chat-network

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
    restart: unless-stopped
    networks:
      - chat-network

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana-dashboards:/etc/grafana/provisioning/dashboards:ro
    depends_on:
      - prometheus
    restart: unless-stopped
    networks:
      - chat-network

networks:
  chat-network:
    driver: bridge

volumes:
  prometheus-data:
  grafana-data:
```

Start stack:

```bash
docker-compose up -d
```

---

## ⚖️ Load Balancing with Nginx

Create `nginx.conf`:

```nginx
events {
    worker_connections 4096;
}

stream {
    upstream chat_backend {
        least_conn;  # Load balance by least connections
        server chat-server-1:8888 max_fails=3 fail_timeout=30s;
        server chat-server-2:8888 max_fails=3 fail_timeout=30s;
    }

    server {
        listen 8888;
        proxy_pass chat_backend;
        proxy_connect_timeout 10s;
        proxy_timeout 300s;
    }
}

http {
    upstream metrics_backend {
        server chat-server-1:9090;
        server chat-server-2:9090;
    }

    server {
        listen 80;
        
        location /metrics {
            proxy_pass http://metrics_backend/metrics;
        }
        
        location /health {
            proxy_pass http://metrics_backend/health;
        }
        
        location /stats {
            proxy_pass http://metrics_backend/stats;
        }
    }
}
```

---

## 📊 Monitoring with Prometheus

Create `prometheus.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'chat-server'
    static_configs:
      - targets:
          - 'chat-server-1:9090'
          - 'chat-server-2:9090'
    metrics_path: '/metrics'
```

Access Prometheus at `http://localhost:9090`

### Key Metrics to Monitor

```promql
# Connection metrics
chat_connections_total
chat_connections_active

# Message throughput
rate(chat_messages_total[1m])

# Latency percentiles
chat_message_latency_p50
chat_message_latency_p95
chat_message_latency_p99

# Error rate
rate(chat_errors_total[1m])

# Rate limiting
chat_rate_limit_hits_total
```

---

## 📈 Grafana Dashboard

Create dashboard with panels:

1. **Connections Over Time**
   - Query: `chat_connections_active`
   - Type: Time series

2. **Messages/Second**
   - Query: `rate(chat_messages_total[1m])`
   - Type: Graph

3. **Latency Percentiles**
   - Queries:
     - `chat_message_latency_p50`
     - `chat_message_latency_p95`
     - `chat_message_latency_p99`
   - Type: Graph

4. **Error Rate**
   - Query: `rate(chat_errors_total[1m])`
   - Type: Graph

5. **Top Rooms by Activity**
   - Query: `topk(10, chat_room_messages_total)`
   - Type: Table

Access Grafana at `http://localhost:3000` (default: admin/admin)

---

## 🔥 Performance Testing

### Run Benchmark Suite

```bash
# All tests
python -m tests.benchmark --host localhost --port 8888 --test all

# Spike test (1000 rapid connections)
python -m tests.benchmark --test spike

# Throughput test (100 clients × 50 messages)
python -m tests.benchmark --test throughput

# Endurance test (500 clients for 5 minutes)
python -m tests.benchmark --test endurance
```

### Expected Performance

| Metric | Target | Notes |
|--------|--------|-------|
| Concurrent Connections | 1,000+ | With rate limiting |
| Messages/Second | 10,000+ | Per server instance |
| P95 Latency | < 50ms | Under normal load |
| P99 Latency | < 100ms | Under normal load |
| Connection Success Rate | > 99% | During spike tests |
| Message Delivery Rate | > 99.9% | In steady state |

---

## 🔒 Security Hardening

### Firewall Rules

```bash
# Allow chat port
sudo ufw allow 8888/tcp

# Allow metrics port (internal only)
sudo ufw deny 9090/tcp
sudo ufw allow from 10.0.0.0/8 to any port 9090

# Enable firewall
sudo ufw enable
```

### TLS/SSL Termination

Use Nginx for TLS:

```nginx
server {
    listen 443 ssl;
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    
    location / {
        proxy_pass http://chat_backend;
    }
}
```

### Rate Limiting Configuration

Adjust in `.env`:

```bash
# Aggressive rate limiting
CHAT_MAX_MESSAGES_PER_MINUTE=30
CHAT_MAX_CONNECTIONS_PER_IP=10

# Relaxed rate limiting
CHAT_MAX_MESSAGES_PER_MINUTE=120
CHAT_MAX_CONNECTIONS_PER_IP=100
```

---

## 🚨 Health Checks

### Liveness Probe

```bash
curl http://localhost:9090/health

# Response
{
  "status": "healthy",
  "uptime_seconds": 3600.5,
  "version": "1.0.0"
}
```

### Readiness Probe

```bash
curl http://localhost:9090/stats

# Response
{
  "total_connections": 150,
  "total_rooms": 25,
  "total_messages": 50000,
  "rooms": {
    "general": 50,
    "tech": 30
  }
}
```

### Kubernetes Health Checks

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 9090
  initialDelaySeconds: 30
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /stats
    port: 9090
  initialDelaySeconds: 5
  periodSeconds: 5
```

---

## 📝 Logging

### Structured Logging

Logs are written to:
- `chat_server.log` (file)
- stdout (console)

Format:
```
2024-01-15 10:30:45 - app.server - INFO - Client user_123 connected from 192.168.1.100
2024-01-15 10:30:46 - app.server - WARNING - Rate limit exceeded for user_123
```

### Log Aggregation

Use ELK stack or similar:

```bash
# Install filebeat
sudo apt-get install filebeat

# Configure filebeat.yml
filebeat.inputs:
- type: log
  paths:
    - /var/log/chat_server.log
  
output.elasticsearch:
  hosts: ["localhost:9200"]
```

---

## 🔄 Scaling Strategies

### Vertical Scaling

Increase resources:
- CPU: 4-8 cores per instance
- RAM: 4-8 GB per instance
- Network: 1 Gbps+ bandwidth

### Horizontal Scaling

Add more instances:

```bash
# Scale to 5 instances
docker-compose up -d --scale chat-server=5
```

### Session Affinity

Not required - stateless per-connection design allows any instance to handle any client.

---

## 🛠️ Troubleshooting

### High Latency

1. Check metrics: `chat_message_latency_p99`
2. Review CPU usage: `docker stats`
3. Check rate limiting: `chat_rate_limit_hits_total`

### Connection Failures

1. Check connection limit: `chat_connections_active`
2. Review logs: `tail -f chat_server.log`
3. Test connectivity: `telnet localhost 8888`

### Memory Leaks

1. Monitor memory: `docker stats`
2. Check room sizes: `curl http://localhost:9090/stats`
3. Review message queues

---

## 📦 Backup & Recovery

### State Management

Server is stateless - no backup required. However:

1. **Configuration**: Backup `.env` file
2. **Logs**: Archive `chat_server.log` periodically
3. **Metrics**: Prometheus retains 15 days by default

### Disaster Recovery

1. Restore configuration
2. Deploy new instances
3. Clients auto-reconnect

---

## 🎯 Best Practices

1. **Always use rate limiting** in production
2. **Monitor P99 latency** - it reveals tail issues
3. **Set connection limits** per IP to prevent abuse
4. **Enable TLS** for public deployments
5. **Use multiple instances** behind load balancer
6. **Alert on error rates** > 1%
7. **Regular load testing** to validate capacity
8. **Log aggregation** for debugging

---

## 📞 Support

For issues:
1. Check logs: `docker logs chat-server`
2. Review metrics: `http://localhost:9090/metrics`
3. Run health check: `curl http://localhost:9090/health`
4. Test with benchmark: `python -m tests.benchmark --test spike`
