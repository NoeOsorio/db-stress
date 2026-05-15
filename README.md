# Porter DB Stress

<p>
  <a href="https://www.python.org/downloads/release/python-3120/"><img alt="Python" src="https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white"></a>
  <a href="https://fastapi.tiangolo.com/"><img alt="FastAPI" src="https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white"></a>
  <a href="https://docs.docker.com/compose/"><img alt="Docker" src="https://img.shields.io/badge/Docker-compose-2496ED?logo=docker&logoColor=white"></a>
  <a href="https://www.postgresql.org/"><img alt="Postgres" src="https://img.shields.io/badge/Postgres-13%2B-336791?logo=postgresql&logoColor=white"></a>
  <a href="https://redis.io/"><img alt="Redis" src="https://img.shields.io/badge/Redis-6%2B-DC382D?logo=redis&logoColor=white"></a>
  <a href="https://porter.run/"><img alt="Porter" src="https://img.shields.io/badge/Porter-ready-7B61FF"></a>
  <img alt="License" src="https://img.shields.io/badge/license-MIT-green">
</p>

> Time-boxed, dashboard-driven stress generator for **Postgres** (RDS / Aurora) and **Redis** (ElastiCache). Built to validate CloudWatch / Prometheus dashboards — connections, CPU, memory, disk — with *real* DB load rather than synthetic numbers.

<p>
  <img alt="Postgres" src="https://img.shields.io/badge/workloads-connections%20%E2%80%A2%20cpu%20%E2%80%A2%20memory%20%E2%80%A2%20disk%20%E2%80%A2%20oltp-336791">
  <img alt="Redis" src="https://img.shields.io/badge/redis-connections%20%E2%80%A2%20cpu%20%E2%80%A2%20memory%20%E2%80%A2%20disk-DC382D">
  <img alt="Safety" src="https://img.shields.io/badge/safety-hard%20duration%20cap%20%E2%80%A2%20auto%20cleanup-success">
</p>

---

## Table of contents

- [Why this exists](#why-this-exists)
- [Safety model](#safety-model)
- [Workloads](#workloads)
- [Quickstart](#quickstart)
  - [1 · Local Docker Compose](#1--local-docker-compose-throwaway-postgres--redis)
  - [2 · Against real RDS / ElastiCache](#2--against-real-rds--elasticache)
  - [3 · Deploy on Porter](#3--deploy-on-porter)
- [Environment variables](#environment-variables)
- [API reference](#api-reference)
- [Project layout](#project-layout)
- [Development](#development)

---

## Why this exists

When you're building dashboards for `RDS DatabaseConnections`, `Aurora CPUUtilization`, `ElastiCache EngineCPUUtilization`, etc., you need a way to **move the needle on demand**. This app gives you sliders, a stop button, and runs *real* workloads — then tears everything down when you're done.

## Safety model

Designed to run cheap and stop hard:

- **Hard duration caps** (default 600 s) — every job auto-terminates even if you close the tab.
- **Bounded resource caps** — max workers, max connections, max concurrent jobs (all env-configurable).
- **Big red “Stop ALL” button** in the dashboard.
- **Isolated objects** — every Postgres table/row and every Redis key this app writes carries an unmistakable `stresstest_` prefix. Redis keys also have a 10-minute TTL, so leaks self-clear.
- **Auto-cleanup on stop** — Postgres tables get `DROP TABLE` on the way out; Redis keys expire.

## Workloads

### Postgres

| Workload      | What it does                                              | Metric it moves                |
| ------------- | --------------------------------------------------------- | ------------------------------ |
| `connections` | Opens N idle sessions and holds them                      | `DatabaseConnections`          |
| `cpu`         | `generate_series` + `md5` sort across N workers           | `CPUUtilization`               |
| `memory`      | Wide hash aggregates that spill `work_mem`                | `FreeableMemory`, `SwapUsage`  |
| `disk`        | INSERT / DELETE batches on isolated unlogged tables       | `WriteIOPS`, `WriteThroughput` |
| `oltp`        | pgbench-style 75/25 read/write on `stresstest_accounts`   | mixed baseline                 |

### Redis

| Workload      | What it does                                                                  | Metric it moves          |
| ------------- | ----------------------------------------------------------------------------- | ------------------------ |
| `connections` | Opens N persistent connections, pings to hold                                 | `CurrConnections`        |
| `cpu`         | Pipelined GET / SET across many workers                                       | `EngineCPUUtilization`   |
| `memory`      | Writes large TTL'd values                                                     | `BytesUsedForCache`      |
| `disk`        | Periodic `BGSAVE` + writes (best-effort — ElastiCache may reject the command) | replication-snapshot I/O |

---

## Quickstart

### 1 · Local Docker Compose (throwaway Postgres + Redis)

```bash
git clone git@github.com:NoeOsorio/db-stress.git
cd db-stress
docker compose up --build
```

Open <http://localhost:8000>. Postgres and Redis containers come up alongside the app, ready to be stressed.

### 2 · Against real RDS / ElastiCache

```bash
cp .env.example .env
# edit .env — fill in DB_URL and REDIS_URL (or the discrete DB_HOST/... vars)
docker compose run --rm --service-ports --no-deps app
```

Or build and run the image directly with env vars:

```bash
docker build -t porter-db-stress .
docker run --rm -p 8000:8000 \
  -e DB_URL="postgresql://user:pass@host:5432/db?sslmode=require" \
  -e REDIS_URL="rediss://:pass@host:6379/0" \
  porter-db-stress
```

### 3 · Deploy on Porter

The app reads credentials from environment variables only — no config files baked into the image.

1. **Build & push** the image, or point Porter at this repo.
2. **Link your datastores** (Aurora / RDS / ElastiCache) to the Porter app. Porter injects credentials as env vars in two forms automatically:
   - **Bundle form** — `DB_URL` for Postgres, `REDIS_URL` for Redis.
   - **Discrete form** — `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_NAME` for Postgres and `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASS` for Redis.

   This app **understands both** — no remapping needed.
3. **Tune safety caps** if needed (see below).
4. **Expose port 8000** behind your usual ingress / auth layer.

> **Cost note:** the workloads only burn the *target* DB while a job is running. The app itself is a small Python container; scale it to zero between sessions.

---

## Environment variables

Two equivalent forms are accepted for each target. The app prefers the URL form when both are set.

### Postgres

| Name          | Aliases also accepted | Required               | Default | Notes                              |
| ------------- | --------------------- | ---------------------- | ------- | ---------------------------------- |
| `DB_URL`      | `POSTGRES_URL`        | One of (URL / bundle)  | —       | Full DSN, e.g. `postgresql://…`    |
| `DB_HOST`     | `PG_HOST`             | If `DB_URL` is empty   | —       |                                    |
| `DB_PORT`     | `PG_PORT`             |                        | `5432`  |                                    |
| `DB_USER`     | `PG_USER`             | If `DB_URL` is empty   | —       |                                    |
| `DB_PASS`     | `DB_PASSWORD`, `PG_PASSWORD` |                | —       | URL-encoded automatically          |
| `DB_NAME`     | `PG_DATABASE`         | If `DB_URL` is empty   | —       |                                    |
| `DB_SSLMODE`  | `PG_SSLMODE`          |                        | —       | Set to `require` for RDS / Aurora  |

### Redis

| Name           | Aliases also accepted | Required              | Default | Notes                       |
| -------------- | --------------------- | --------------------- | ------- | --------------------------- |
| `REDIS_URL`    | —                     | One of (URL / bundle) | —       | `redis://` or `rediss://`   |
| `REDIS_HOST`   | —                     | If `REDIS_URL` empty  | —       |                             |
| `REDIS_PORT`   | —                     |                       | `6379`  |                             |
| `REDIS_PASS`   | `REDIS_PASSWORD`      |                       | —       | URL-encoded automatically   |
| `REDIS_DB`     | —                     |                       | `0`     |                             |
| `REDIS_TLS`    | —                     |                       | `false` | `true` → `rediss://`        |

### Safety caps

| Name                      | Default | Purpose                                                  |
| ------------------------- | ------- | -------------------------------------------------------- |
| `MAX_JOB_DURATION_SEC`    | `600`   | Hard auto-stop for every job.                            |
| `MAX_WORKERS_PER_JOB`     | `50`    | Per-job worker ceiling.                                  |
| `MAX_CONNECTIONS_PER_JOB` | `200`   | Per-job connection ceiling (for `connections` workload). |
| `MAX_CONCURRENT_JOBS`     | `5`     | Across all targets.                                      |
| `ALLOW_DISK_WORKLOADS`    | `true`  | Set `false` to disable the destructive disk tests.       |

---

## API reference

| Method | Path                       | Notes                                                  |
| ------ | -------------------------- | ------------------------------------------------------ |
| `GET`  | `/`                        | Dashboard (static HTML)                                |
| `GET`  | `/healthz`                 | Liveness                                               |
| `GET`  | `/api/config`              | Effective safety caps + target wiring                  |
| `GET`  | `/api/targets/check`       | Live connectivity check against both DBs               |
| `GET`  | `/api/jobs`                | List jobs                                              |
| `POST` | `/api/jobs`                | Start a job (body below)                               |
| `POST` | `/api/jobs/{id}/stop`      | Stop one running job                                   |
| `POST` | `/api/jobs/stop-all`       | Emergency stop                                         |
| `DELETE` | `/api/jobs/{id}`         | Remove a finished job from the list                    |
| `POST` | `/api/jobs/clear-finished` | Remove every non-running job from the list            |
| `POST` | `/api/cleanup/redis`       | `SCAN` + `UNLINK` any leftover `stresstest_*` keys     |
| `WS`   | `/ws`                      | Live tick stream                                       |

### `POST /api/jobs` body

```json
{
  "target": "postgres",          // or "redis"
  "workload": "cpu",             // connections | cpu | memory | disk | oltp
  "duration_seconds": 60,
  "workers": 8,                  // omit for "connections"
  "connections": 50,             // only for "connections"
  "intensity": 200000,           // postgres/cpu
  "rows": 500000,                // postgres/memory
  "rows_per_batch": 50000,       // postgres/disk
  "table_rows": 100000,          // postgres/oltp
  "value_bytes": 4096,           // redis/memory
  "keys_per_batch": 500,         // redis/memory
  "pipeline_depth": 200,         // redis/cpu
  "interval_seconds": 15         // redis/disk
}
```

---

## Project layout

```
app/
├── config.py           # Pydantic settings — accepts DB_*/REDIS_* and legacy names
├── main.py             # FastAPI routes + WebSocket
├── manager.py          # Job lifecycle, caps, cleanup
├── stress/
│   ├── postgres.py     # All Postgres workloads
│   └── redis_stress.py # All Redis workloads
└── static/index.html   # Dashboard
```

---

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

PRs welcome. Keep workloads idempotent on cleanup and respect the `OBJECT_PREFIX` namespace.
