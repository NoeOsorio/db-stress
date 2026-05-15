"""Job manager: lifecycle, safety caps, metrics aggregation."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from .config import settings
from .stress import postgres as pg
from .stress import redis_stress as rs


@dataclass
class JobMetrics:
    started_at: float
    ops: int = 0
    errors: int = 0
    latency_sum_ms: float = 0.0
    last_event: dict = field(default_factory=dict)
    samples: deque = field(default_factory=lambda: deque(maxlen=120))

    def record(self, event: dict) -> None:
        self.last_event = event
        if "error" in event:
            self.errors += 1
        if "ops" in event:
            self.ops += int(event["ops"])
        elif "latency_ms" in event:
            self.ops += 1
        if "latency_ms" in event:
            self.latency_sum_ms += float(event["latency_ms"])

    def snapshot(self) -> dict:
        elapsed = max(time.monotonic() - self.started_at, 0.001)
        avg_lat = (self.latency_sum_ms / self.ops) if self.ops else 0.0
        return {
            "elapsed_sec": round(elapsed, 1),
            "ops": self.ops,
            "ops_per_sec": round(self.ops / elapsed, 1),
            "errors": self.errors,
            "avg_latency_ms": round(avg_lat, 2),
            "last_event": self.last_event,
        }


@dataclass
class Job:
    id: str
    target: str  # "postgres" or "redis"
    workload: str
    params: dict
    status: str = "running"  # running | done | error | stopped
    error: Optional[str] = None
    metrics: JobMetrics = field(default_factory=lambda: JobMetrics(started_at=time.monotonic()))
    task: Optional[asyncio.Task] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "target": self.target,
            "workload": self.workload,
            "params": self.params,
            "status": self.status,
            "error": self.error,
            "metrics": self.metrics.snapshot(),
        }


class JobManager:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = asyncio.Lock()
        self._subscribers: set[asyncio.Queue] = set()

    # ---------- subscriptions (for WebSocket fan-out) ----------

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    def _broadcast(self, payload: dict) -> None:
        dead: list[asyncio.Queue] = []
        for q in self._subscribers:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                dead.append(q)
        for q in dead:
            self._subscribers.discard(q)

    # ---------- job lifecycle ----------

    def list_jobs(self) -> list[dict]:
        return [j.to_dict() for j in self._jobs.values()]

    def get_job(self, jid: str) -> Optional[Job]:
        return self._jobs.get(jid)

    async def _running_count(self) -> int:
        return sum(1 for j in self._jobs.values() if j.status == "running")

    def _enforce_caps(self, target: str, workload: str, params: dict) -> dict:
        """Clamp user-supplied params to the configured hard caps."""
        capped = dict(params)
        # Duration
        capped["duration_seconds"] = min(
            float(capped.get("duration_seconds", 60)),
            float(settings.MAX_JOB_DURATION_SEC),
        )
        # Workers / connections
        if "workers" in capped:
            capped["workers"] = max(1, min(int(capped["workers"]), settings.MAX_WORKERS_PER_JOB))
        if "connections" in capped:
            capped["connections"] = max(1, min(int(capped["connections"]), settings.MAX_CONNECTIONS_PER_JOB))
        return capped

    async def start_job(self, target: str, workload: str, params: dict) -> Job:
        if await self._running_count() >= settings.MAX_CONCURRENT_JOBS:
            raise RuntimeError(
                f"Concurrent job limit reached ({settings.MAX_CONCURRENT_JOBS}). "
                f"Stop a running job first."
            )

        params = self._enforce_caps(target, workload, params)
        jid = uuid.uuid4().hex[:10]
        job = Job(id=jid, target=target, workload=workload, params=params)

        coro = self._dispatch(job)
        job.task = asyncio.create_task(self._supervise(job, coro))
        self._jobs[jid] = job
        self._broadcast({"type": "job_started", "job": job.to_dict()})
        return job

    async def stop_job(self, jid: str) -> Optional[Job]:
        job = self._jobs.get(jid)
        if not job or job.status != "running":
            return job
        if job.task:
            job.task.cancel()
        return job

    async def stop_all(self) -> int:
        count = 0
        for j in list(self._jobs.values()):
            if j.status == "running" and j.task:
                j.task.cancel()
                count += 1
        return count

    def delete_job(self, jid: str) -> Optional[Job]:
        """Remove a finished job from the registry. Returns None if missing,
        raises if the job is still running (caller should stop it first)."""
        job = self._jobs.get(jid)
        if not job:
            return None
        if job.status == "running":
            raise RuntimeError("Cannot remove a running job — stop it first.")
        del self._jobs[jid]
        self._broadcast({"type": "job_removed", "id": jid})
        return job

    def clear_finished(self) -> int:
        """Remove every non-running job. Returns the number removed."""
        removed = 0
        for jid in [j.id for j in self._jobs.values() if j.status != "running"]:
            del self._jobs[jid]
            self._broadcast({"type": "job_removed", "id": jid})
            removed += 1
        return removed

    # ---------- dispatch ----------

    def _make_reporter(self, job: Job) -> Callable[[dict], None]:
        def _report(event: dict) -> None:
            job.metrics.record(event)
            # Broadcast a compact tick to subscribers — full snapshot every event
            # is fine because subscribers can drop with QueueFull.
            self._broadcast({
                "type": "job_tick",
                "id": job.id,
                "snapshot": job.metrics.snapshot(),
                "status": job.status,
            })
        return _report

    def _dispatch(self, job: Job) -> Awaitable:
        report = self._make_reporter(job)
        t = job.target
        w = job.workload
        p = job.params

        if t == "postgres":
            dsn = settings.postgres_dsn()
            if not dsn:
                raise RuntimeError("No Postgres DSN configured")
            if w == "connections":
                return pg.connections_storm(
                    dsn=dsn,
                    connections=int(p["connections"]),
                    hold_seconds=float(p["duration_seconds"]),
                    report=report,
                )
            if w == "cpu":
                return pg.cpu_burn(
                    dsn=dsn,
                    workers=int(p["workers"]),
                    duration_seconds=float(p["duration_seconds"]),
                    report=report,
                    intensity=int(p.get("intensity", 200_000)),
                )
            if w == "memory":
                return pg.memory_pressure(
                    dsn=dsn,
                    workers=int(p["workers"]),
                    duration_seconds=float(p["duration_seconds"]),
                    report=report,
                    rows=int(p.get("rows", 500_000)),
                )
            if w == "disk":
                if not settings.ALLOW_DISK_WORKLOADS:
                    raise RuntimeError("Disk workloads disabled by config")
                return pg.disk_churn(
                    dsn=dsn,
                    workers=int(p["workers"]),
                    duration_seconds=float(p["duration_seconds"]),
                    report=report,
                    object_prefix=settings.OBJECT_PREFIX,
                    rows_per_batch=int(p.get("rows_per_batch", 50_000)),
                )
            if w == "oltp":
                return pg.mixed_oltp(
                    dsn=dsn,
                    workers=int(p["workers"]),
                    duration_seconds=float(p["duration_seconds"]),
                    report=report,
                    object_prefix=settings.OBJECT_PREFIX,
                    table_rows=int(p.get("table_rows", 100_000)),
                )

        if t == "redis":
            url = settings.redis_url()
            if not url:
                raise RuntimeError("No Redis URL configured")
            if w == "connections":
                return rs.connections_storm(
                    url=url,
                    connections=int(p["connections"]),
                    hold_seconds=float(p["duration_seconds"]),
                    report=report,
                )
            if w == "cpu":
                return rs.cpu_burn(
                    url=url,
                    workers=int(p["workers"]),
                    duration_seconds=float(p["duration_seconds"]),
                    report=report,
                    pipeline_depth=int(p.get("pipeline_depth", 200)),
                )
            if w == "memory":
                return rs.memory_fill(
                    url=url,
                    workers=int(p["workers"]),
                    duration_seconds=float(p["duration_seconds"]),
                    report=report,
                    object_prefix=settings.OBJECT_PREFIX,
                    value_bytes=int(p.get("value_bytes", 4096)),
                    keys_per_batch=int(p.get("keys_per_batch", 500)),
                )
            if w == "disk":
                if not settings.ALLOW_DISK_WORKLOADS:
                    raise RuntimeError("Disk workloads disabled by config")
                return rs.disk_churn(
                    url=url,
                    duration_seconds=float(p["duration_seconds"]),
                    report=report,
                    object_prefix=settings.OBJECT_PREFIX,
                    interval_seconds=float(p.get("interval_seconds", 15.0)),
                )

        raise RuntimeError(f"Unknown workload: {t}/{w}")

    async def _supervise(self, job: Job, coro: Awaitable) -> None:
        try:
            await coro
            job.status = "done"
        except asyncio.CancelledError:
            job.status = "stopped"
        except Exception as e:
            job.status = "error"
            job.error = str(e)[:500]
        finally:
            self._broadcast({"type": "job_finished", "job": job.to_dict()})

    # ---------- best-effort cleanup ----------

    async def cleanup_redis_keys(self) -> int:
        url = settings.redis_url()
        if not url:
            return 0
        counted = {"n": 0}

        def _r(e: dict) -> None:
            if "cleanup_deleted" in e:
                counted["n"] = int(e["cleanup_deleted"])

        await rs.cleanup_keys(url, settings.OBJECT_PREFIX, _r)
        return counted["n"]
