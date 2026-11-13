# Deployment

## Single process

```bash
python main.py                     # Python 3.10+, no third-party packages
CHAT_PORT=9000 CHAT_LOG_FILE=chat.log python main.py
```

Every setting is a `CHAT_*` environment variable. [.env.example](../.env.example)
lists them all with their defaults.

A malformed value stops startup with an error that names the variable. The
server does not silently fall back to a default.

## Docker Compose stack

```bash
cp .env.example .env               # optional; edit values
docker compose up --build -d
docker compose ps                  # all services should be "healthy"
```

| Service | Port on the host | Purpose |
|---|---|---|
| nginx | 8888 (TCP), 80 (HTTP) | `least_conn` TCP load balancer for chat. On port 80 it proxies `/health`, `/metrics` and `/stats`. |
| chat-server-1, -2 | 9091, 9092 | Each server's own metrics and health, for debugging. The chat port is reached only through nginx. |
| prometheus | 9090 | Scrapes both servers every 10 s |
| grafana | `$GRAFANA_PORT` (3000) | The "Chat Server" dashboard is provisioned automatically |

Health and start order:
- **Server health checks.** Each server container is checked with the real
  `/health` endpoint, using Python's `urllib` because the slim image has no
  curl.
- **Start order.** nginx starts only after both servers report healthy.

Compose passes the most-used settings to the servers:
- host, ports, connection caps;
- rate limits;
- log level.

To change any other setting, add it to the `environment:` block of the
`x-chat-server` anchor.

### Quick check after deploying

```bash
curl -s localhost/health            # {"status": "healthy"}
curl -s localhost:9091/metrics | grep chat_connections_active
python -m app.main client           # connects to localhost:8888 through nginx
```

## Operations

- **Shutdown.** On SIGTERM (as sent by `docker stop`) the server stops
  accepting, sends `BYE` to every client, closes them and exits.
  `CHAT_SHUTDOWN_TIMEOUT` bounds the wait.
- **Logs.** Logs go to stdout (`docker compose logs chat-server-1`).
  `CHAT_LOG_FILE` also writes them to a file.
- **Image.** The container runs as a non-root user.
- **Alert ideas.** Useful alerts, using the exported metrics:
  - `chat_connections_active` near `CHAT_MAX_CONNECTIONS`;
  - a rising `chat_errors_total{type="slow_client"}`;
  - a rising `chat_rate_limited_requests_total`;
  - `chat_message_latency_ms{quantile="0.99"}` above your target.

## Before exposing it to the internet

These are not implemented. Each is a known gap.

1. **TLS.** Terminate it at the load balancer: nginx `stream` with
   `ssl_certificate`, or a cloud load balancer.
2. **Real authentication.** AUTH currently claims any free username. Verify a
   signed token (for example a JWT from your login service) inside AUTH.
3. **Cross-server rooms.** With more than one replica, users on different
   replicas do not share rooms. Add a pub/sub backplane (Redis or NATS) before
   running several replicas for one community. Otherwise run a single replica.
4. **Metrics access.** `/metrics` and `/stats` are unauthenticated. Keep the
   metrics port on a private network.
5. **Persistence.** There is none. Messages are live-only.
