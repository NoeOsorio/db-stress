"""Redis stress workloads.

All keys this app writes are prefixed with `OBJECT_PREFIX` and given a
short TTL, so even an ungraceful shutdown won't leave the server with
ballooning memory.
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from typing import Callable

import redis.asyncio as redis

ReportFn = Callable[[dict], None]

_CLEANUP_TTL_SEC = 600  # all stress keys expire within 10 min even on crash


def _client(url: str) -> redis.Redis:
    return redis.from_url(url, decode_responses=False)


async def connections_storm(
    url: str,
    connections: int,
    hold_seconds: float,
    report: ReportFn,
) -> None:
    clients: list[redis.Redis] = []
    try:
        for i in range(connections):
            try:
                c = _client(url)
                # Force the actual TCP connection by issuing a ping.
                await c.ping()
                clients.append(c)
                report({"opened": len(clients)})
            except Exception as e:
                report({"error": str(e)[:200], "opened": len(clients)})
                await asyncio.sleep(0.1)
        end = time.monotonic() + hold_seconds
        while time.monotonic() < end:
            for c in clients:
                try:
                    await c.ping()
                except Exception:
                    pass
            report({"opened": len(clients), "held_for": int(hold_seconds - (end - time.monotonic()))})
            await asyncio.sleep(2)
    finally:
        for c in clients:
            try:
                await c.aclose()
            except Exception:
                pass


async def cpu_burn(
    url: str,
    workers: int,
    duration_seconds: float,
    report: ReportFn,
    pipeline_depth: int = 200,
) -> None:
    """High op/sec to push Engine CPU on ElastiCache.

    Pipelined GET/SET on small keys — that's what hurts redis-server,
    which is single-threaded for command execution.
    """
    async def _worker(worker_id: int) -> None:
        client = _client(url)
        try:
            while True:
                t0 = time.perf_counter()
                try:
                    async with client.pipeline(transaction=False) as pipe:
                        for _ in range(pipeline_depth):
                            k = f"stress:cpu:{random.randint(0, 1000)}"
                            pipe.set(k, b"x", ex=_CLEANUP_TTL_SEC)
                            pipe.get(k)
                        await pipe.execute()
                    ms = (time.perf_counter() - t0) * 1000
                    report({
                        "worker": worker_id,
                        "ops": pipeline_depth * 2,
                        "latency_ms": ms,
                    })
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    report({"worker": worker_id, "error": str(e)[:200]})
                    await asyncio.sleep(0.2)
        finally:
            await client.aclose()

    tasks = [asyncio.create_task(_worker(i)) for i in range(workers)]
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=duration_seconds)
    except asyncio.TimeoutError:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def memory_fill(
    url: str,
    workers: int,
    duration_seconds: float,
    report: ReportFn,
    object_prefix: str,
    value_bytes: int = 4096,
    keys_per_batch: int = 500,
) -> None:
    """Push used_memory + BytesUsedForCache by writing big values.

    All keys get TTL so memory drains automatically after the test ends.
    """
    payload = os.urandom(value_bytes)

    async def _worker(worker_id: int) -> None:
        client = _client(url)
        i = 0
        try:
            while True:
                t0 = time.perf_counter()
                try:
                    async with client.pipeline(transaction=False) as pipe:
                        for j in range(keys_per_batch):
                            k = f"{object_prefix}mem:w{worker_id}:{i}:{j}"
                            pipe.set(k, payload, ex=_CLEANUP_TTL_SEC)
                        await pipe.execute()
                    i += 1
                    ms = (time.perf_counter() - t0) * 1000
                    report({
                        "worker": worker_id,
                        "bytes_written": keys_per_batch * value_bytes,
                        "latency_ms": ms,
                    })
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    report({"worker": worker_id, "error": str(e)[:200]})
                    await asyncio.sleep(0.2)
        finally:
            await client.aclose()

    tasks = [asyncio.create_task(_worker(i)) for i in range(workers)]
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=duration_seconds)
    except asyncio.TimeoutError:
        pass
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def disk_churn(
    url: str,
    duration_seconds: float,
    report: ReportFn,
    object_prefix: str,
    interval_seconds: float = 15.0,
) -> None:
    """Trigger BGSAVE periodically to push disk IO on ElastiCache.

    Useful for replication-snapshot metrics. Only runs if BGSAVE is
    permitted — managed ElastiCache may reject it; in that case the
    workload writes large values to drive AOF/RDB churn instead.
    """
    client = _client(url)
    end = time.monotonic() + duration_seconds
    payload = os.urandom(64 * 1024)  # 64 KB
    counter = 0
    try:
        while time.monotonic() < end:
            counter += 1
            # Write some big values first — guarantees fork has work.
            async with client.pipeline(transaction=False) as pipe:
                for j in range(100):
                    k = f"{object_prefix}disk:{counter}:{j}"
                    pipe.set(k, payload, ex=_CLEANUP_TTL_SEC)
                await pipe.execute()
            try:
                await client.bgsave()
                report({"bgsave": counter, "ok": 1})
            except Exception as e:
                # ElastiCache often forbids BGSAVE — that's fine.
                report({"bgsave": counter, "bgsave_error": str(e)[:200]})
            await asyncio.sleep(interval_seconds)
    finally:
        await client.aclose()


async def cleanup_keys(url: str, object_prefix: str, report: ReportFn) -> None:
    """Best-effort sweep of any leftover stress keys (SCAN + UNLINK)."""
    client = _client(url)
    deleted = 0
    try:
        async for key in client.scan_iter(match=f"{object_prefix}*", count=1000):
            try:
                await client.unlink(key)
                deleted += 1
            except Exception:
                pass
        report({"cleanup_deleted": deleted})
    finally:
        await client.aclose()
