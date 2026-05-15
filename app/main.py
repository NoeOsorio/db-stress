"""FastAPI entrypoint.

Endpoints:
  GET  /healthz                — liveness
  GET  /api/config             — what targets are wired + safety caps
  GET  /api/targets/check      — actually try connecting to both DBs
  GET  /api/jobs               — list jobs
  POST /api/jobs               — start a job
  POST /api/jobs/{id}/stop     — stop a job
  POST /api/jobs/stop-all      — emergency stop
  POST /api/cleanup/redis      — sweep leftover stress keys
  WS   /ws                     — push live job ticks
  GET  /                       — dashboard
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Optional

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import settings
from .manager import JobManager

log = logging.getLogger("stress")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

app = FastAPI(title="Porter DB Stress", version="0.1.0")
manager = JobManager()

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class StartJobIn(BaseModel):
    target: str = Field(..., pattern="^(postgres|redis)$")
    workload: str = Field(..., pattern="^(connections|cpu|memory|disk|oltp)$")
    duration_seconds: float = Field(60, ge=1)
    workers: Optional[int] = Field(None, ge=1)
    connections: Optional[int] = Field(None, ge=1)
    # workload-specific knobs
    intensity: Optional[int] = None
    rows: Optional[int] = None
    rows_per_batch: Optional[int] = None
    table_rows: Optional[int] = None
    value_bytes: Optional[int] = None
    keys_per_batch: Optional[int] = None
    pipeline_depth: Optional[int] = None
    interval_seconds: Optional[float] = None

    def params(self) -> dict[str, Any]:
        return {k: v for k, v in self.model_dump().items()
                if v is not None and k not in ("target", "workload")}


# ---------------- dashboard ----------------

@app.get("/")
async def dashboard() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True}


# ---------------- config / target check ----------------

@app.get("/api/config")
async def get_config() -> dict:
    return {
        "postgres_configured": settings.postgres_dsn() is not None,
        "redis_configured": settings.redis_url() is not None,
        "caps": {
            "max_job_duration_sec": settings.MAX_JOB_DURATION_SEC,
            "max_workers_per_job": settings.MAX_WORKERS_PER_JOB,
            "max_connections_per_job": settings.MAX_CONNECTIONS_PER_JOB,
            "max_concurrent_jobs": settings.MAX_CONCURRENT_JOBS,
            "allow_disk_workloads": settings.ALLOW_DISK_WORKLOADS,
        },
    }


@app.get("/api/targets/check")
async def check_targets() -> dict:
    out: dict[str, Any] = {}

    dsn = settings.postgres_dsn()
    if dsn:
        try:
            conn = await asyncpg.connect(dsn=dsn, timeout=5)
            version = await conn.fetchval("SHOW server_version")
            await conn.close()
            out["postgres"] = {"ok": True, "version": version}
        except Exception as e:
            out["postgres"] = {"ok": False, "error": str(e)[:300]}
    else:
        out["postgres"] = {"ok": False, "error": "not configured"}

    url = settings.redis_url()
    if url:
        try:
            r = redis.from_url(url)
            info = await r.info("server")
            await r.aclose()
            out["redis"] = {"ok": True, "version": info.get("redis_version")}
        except Exception as e:
            out["redis"] = {"ok": False, "error": str(e)[:300]}
    else:
        out["redis"] = {"ok": False, "error": "not configured"}

    return out


# ---------------- jobs ----------------

@app.get("/api/jobs")
async def list_jobs() -> dict:
    return {"jobs": manager.list_jobs()}


@app.post("/api/jobs")
async def start_job(body: StartJobIn) -> dict:
    # Sanity: connection-based workloads need `connections`, others need `workers`.
    p = body.params()
    if body.workload == "connections" and "connections" not in p:
        raise HTTPException(400, "`connections` is required for the connections workload")
    if body.workload in {"cpu", "memory", "disk", "oltp"} and body.target == "postgres" and "workers" not in p:
        raise HTTPException(400, "`workers` is required for this workload")
    if body.workload in {"cpu", "memory"} and body.target == "redis" and "workers" not in p:
        raise HTTPException(400, "`workers` is required for this workload")

    try:
        job = await manager.start_job(body.target, body.workload, p)
    except RuntimeError as e:
        raise HTTPException(400, str(e))
    return job.to_dict()


@app.post("/api/jobs/{jid}/stop")
async def stop_job(jid: str) -> dict:
    job = await manager.stop_job(jid)
    if not job:
        raise HTTPException(404, "no such job")
    return job.to_dict()


@app.post("/api/jobs/stop-all")
async def stop_all() -> dict:
    n = await manager.stop_all()
    return {"stopped": n}


@app.delete("/api/jobs/{jid}")
async def delete_job(jid: str) -> dict:
    try:
        job = manager.delete_job(jid)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    if not job:
        raise HTTPException(404, "no such job")
    return {"removed": jid}


@app.post("/api/jobs/clear-finished")
async def clear_finished() -> dict:
    return {"removed": manager.clear_finished()}


@app.post("/api/cleanup/redis")
async def cleanup_redis() -> dict:
    n = await manager.cleanup_redis_keys()
    return {"deleted": n}


# ---------------- WebSocket ----------------

@app.websocket("/ws")
async def ws(socket: WebSocket) -> None:
    await socket.accept()
    q = manager.subscribe()
    # initial snapshot
    try:
        await socket.send_json({"type": "snapshot", "jobs": manager.list_jobs()})
        while True:
            payload = await q.get()
            await socket.send_json(payload)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.warning("ws error: %s", e)
    finally:
        manager.unsubscribe(q)
