# Porter DB Stress

Time-boxed, dashboard-driven stress generator for **Postgres** (RDS / Aurora) and **Redis** (ElastiCache). Built to validate CloudWatch / Prometheus metrics — connections, CPU, memory, disk — with *real* DB load rather than synthetic numbers.

## Why this exists

When you're working on database metrics dashboards (RDS DatabaseConnections, Aurora CPUUtilization, ElastiCache EngineCPUUtilization, etc.) you need a way to *move the needle on demand*. This app gives you sliders and a stop button, runs real workloads, and tears down everything when you're done.

Designed to run cheap and stop hard:

- **Hard duration caps** (default 600s) — every job auto-terminates even if you close the tab.
- **Bounded resource caps** — max workers, max connections, max concurrent jobs (all env-configurable).
- **Big red "Stop ALL" button** in the UI.
- **Isolated objects** — every Postgres table/row and every Redis key this app writes uses an unmistakable `stresstest_` prefix. Redis keys also carry a 10-minute TTL so leaks self-clear.
- **Auto-cleanup on stop**: Postgres tables get `DROP TABLE` on the way out; Redis keys expire.

## What workloads it ships with

### Postgres
| Workload | What it does | Metric it moves |
|---|---|---|
| `connections` | Opens N idle sessions and holds them | `DatabaseConnections` |
| `cpu` | `generate_series` + md5 sort across N workers | `CPUUtilization` |
| `memory` | Wide hash aggregates that spill `work_mem` | `FreeableMemory`, `SwapUsage` |
| `disk` | INSERT/DELETE batches on isolated unlogged tables | `WriteIOPS`, `WriteThroughput` |
| `oltp` | pgbench-style 75/25 read/write on `stresstest_accounts` | mixed baseline |

### Redis
| Workload | What it does | Metric it moves |
|---|---|---|
| `connections` | Opens N persistent connections, pings to hold | `CurrConnections` |
| `cpu` | Pipelined GET/SET across many workers | `EngineCPUUtilization` |
| `memory` | Writes large TTLed values | `BytesUsedForCache` |
| `disk` | Periodic `BGSAVE` + writes (best-effort — ElastiCache may reject) | replication-snapshot IO |

## Run it

### Local with Docker Compose (with throwaway Postgres + Redis)

```bash
docker compose up --build
# open http://localhost:8000
```

### Against your real RDS / Aurora / ElastiCache

```bash
cp .env.example .env
# edit .env — fill in POSTGRES_URL and REDIS_URL
docker compose run --rm --service-ports --no-deps app
```

Or just build and run the image directly:

```bash
docker build -t porter-db-stress .
docker run --rm -p 8000:8000 \
  -e POSTGRES_URL="postgresql://user:pass@host:5432/db?sslmode=require" \
  -e REDIS_URL="rediss://:pass@host:6379/0" \
  porter-db-stress
```

## Running on Porter

The app reads credentials from environment variables only — no config files baked into the image.

1. Build and push the image (or point Porter at this repo).
2. Add a Porter app and link your Aurora / ElastiCache datastores.
3. Inject the credentials as env vars. Either:
   - **Bundle form**: `POSTGRES_URL`, `REDIS_URL` (one full URL each).
   - **Discrete form**: `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE`, `PG_SSLMODE` and `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_DB`, `REDIS_TLS`. The app composes the URL itself.
4. Tweak safety caps if needed: `MAX_JOB_DURATION_SEC`, `MAX_WORKERS_PER_JOB`, `MAX_CONNECTIONS_PER_JOB`, `MAX_CONCURRENT_JOBS`, `ALLOW_DISK_WORKLOADS`.
5. Expose port 8000 and put it behind your usual ingress / auth.

> **Cost note:** the workloads only burn the *target* DB while a job is running. The app itself is a small Python container. You can scale it to zero between sessions.

## API surface

| Method | Path | Notes |
|---|---|---|
| `GET` | `/` | Dashboard |
| `GET` | `/healthz` | Liveness |
| `GET` | `/api/config` | Effective safety caps + target wiring |
| `GET` | `/api/targets/check` | Live connectivity check to both DBs |
| `GET` | `/api/jobs` | List jobs |
| `POST` | `/api/jobs` | Start a job (see body below) |
| `POST` | `/api/jobs/{id}/stop` | Stop one job |
| `POST` | `/api/jobs/stop-all` | Emergency stop |
| `POST` | `/api/cleanup/redis` | SCAN+UNLINK any leftover `stresstest_*` Redis keys |
| `WS` | `/ws` | Live tick stream |

`POST /api/jobs` body:

```json
{
  "target": "postgres",          // or "redis"
  "workload": "cpu",             // connections | cpu | memory | disk | oltp
  "duration_seconds": 60,
  "workers": 8,                  // omit for `connections`
  "connections": 50,              // only for `connections`
  "intensity": 200000,            // postgres/cpu
  "rows": 500000,                 // postgres/memory
  "rows_per_batch": 50000,        // postgres/disk
  "table_rows": 100000,           // postgres/oltp
  "value_bytes": 4096,            // redis/memory
  "keys_per_batch": 500,          // redis/memory
  "pipeline_depth": 200,          // redis/cpu
  "interval_seconds": 15          // redis/disk
}
```
