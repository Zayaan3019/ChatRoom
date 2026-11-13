# Deployment

## Single process

```bash
python main.py                     # Python 3.10+, no third-party packages
CHAT_PORT=9000 CHAT_LOG_FILE=chat.log python main.py
```

Every setting is a `CHAT_*` environment variable. [.env.example](../.env.example)
lists them all with their defaults.

A malformed or unsafe value stops startup with an error that names the
variable. The server does not silently fall back to a default. For example,
it refuses token login without TLS, or a token secret shorter than 32 bytes.

### Secure single node

```bash
python scripts/init_secrets.py secrets --hosts chat.example.com,localhost
CHAT_TLS_CERT=secrets/server.pem CHAT_TLS_KEY=secrets/server.key \
CHAT_AUTH_MODE=token CHAT_JWT_SECRET_FILE=secrets/jwt.secret python main.py

# a login token for alice (normally issued by your login service)
python -m app.core.auth issue-token --user alice --ttl 3600 --secret-file secrets/jwt.secret > alice.jwt
python -m app.main client --tls --ca secrets/ca.pem --server-name chat.example.com --token-file alice.jwt
```

For password login instead, use `CHAT_AUTH_MODE=password` with
`CHAT_USERS_FILE=users.json`, and add users with
`python -m app.core.auth add-user alice --users-file users.json`. The server
re-reads the file when it changes, so no restart is needed.

## Docker Compose stack

```bash
cp .env.example .env               # optional; edit values
docker compose up --build -d
docker compose ps                  # all services should be "healthy"; init "exited (0)"
```

| Service | Port on the host | Purpose |
|---|---|---|
| init | none | One-shot. It creates `./secrets` on the first start (CA, server certificate, token secret, Redis password) and copies the runtime files, never the CA key, to a volume. Existing files are kept. |
| redis | none (private network) | Rooms, usernames, presence and history. Password-protected, append-only file (`everysec`), `noeviction`. |
| chat-server-1, -2 | 9091, 9092 | Replicas. Each server's own metrics and health, for debugging. The chat port is reached only through nginx. |
| nginx | 8888 (TLS), 80 (HTTP) | Ends TLS. A `least_conn` TCP load balancer that forwards the client address with the PROXY protocol. On port 80 it proxies `/health`, `/metrics` and `/stats`. |
| prometheus | 9090 | Scrapes both servers every 10 s |
| grafana | `$GRAFANA_PORT` (3000) | The "Chat Server" dashboard is provisioned automatically |

Start order: init, then Redis (healthy), then both servers (healthy), then
nginx. The servers trust PROXY headers only from the address the name
`nginx` resolves to (`CHAT_TRUSTED_PROXIES=nginx`, re-resolved if nginx
restarts with a new address); a connection from anything else is refused.

### Quick check after deploying

```bash
curl -s localhost/health            # {"status": "healthy"}
python -m app.core.auth issue-token --user alice --secret-file secrets/jwt.secret > alice.jwt
python -m app.main client --tls --ca secrets/ca.pem --token-file alice.jwt
```

Users connected through nginx may land on either replica. They share rooms,
history and the username namespace. Restarting Redis (`docker compose restart
redis`) keeps the history.

### Your own domain

```bash
CHAT_TLS_HOSTS=chat.example.com,localhost docker compose run --rm init \
    python scripts/init_secrets.py /secrets --export /runtime --owner 10001:10001 --force
```

`--force` regenerates every secret. Existing tokens stop working, and clients
need the new `ca.pem`.

With a public certificate (for example from Let's Encrypt), give nginx that
certificate and key instead. Clients then need no `--ca`.

## Operations

- **Shutdown.** On SIGTERM (as sent by `docker stop`) the server:
  1. stops accepting connections;
  2. sends `BYE` to every client;
  3. releases its username leases;
  4. exits.

  `CHAT_SHUTDOWN_TIMEOUT` bounds the wait.
- **Rolling restart.** Restart one replica at a time. Clients of the
  restarting replica reconnect through nginx to the other, and the resilient
  client (`--reconnect`) resumes each room where it left off.
- **Logs.** Logs go to stdout (`docker compose logs chat-server-1`).
- **Image.** The container runs as a non-root user (UID 10001) and installs
  no packages.
- **Redis backups.** `docker compose exec redis redis-cli -a ... BGREWRITEAOF`,
  then copy the `redis-data` volume.
- **Alert ideas:**
  - `chat_backplane_up == 0`;
  - a rising `chat_backplane_resyncs_total` (long subscriber outages);
  - `chat_errors_total{type="auth"}` spikes (password guessing);
  - `chat_connections_active` near `CHAT_MAX_CONNECTIONS`;
  - a rising `chat_errors_total{type="slow_client"}`;
  - `chat_message_latency_ms{quantile="0.99"}` above your target.

## Known limits

1. **Redis is a single point of failure.** While it is down, messages are
   refused with an error, not lost silently, and `/health` turns 503. Add
   Sentinel or a managed Redis with a replica for failover.
2. **Rate limits are per replica.** N replicas allow up to N times the
   per-user and per-IP rates.
3. **Metrics access.** `/metrics` and `/stats` are unauthenticated. Keep the
   metrics ports on a private network.
4. **Inside the stack.** The nginx-to-server hop is plain TCP on the
   private Docker network. For TLS on that hop too, run the servers with
   their own certificates and use TLS passthrough at the balancer. The
   server then sees the balancer's address, because it does not read a PROXY
   header ahead of its own TLS handshake (the two settings are mutually
   exclusive).
