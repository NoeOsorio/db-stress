"""Postgres stress workloads.

All workloads:
  * cooperate with asyncio.CancelledError (so /stop is instant)
  * report per-iteration metrics back via the `report` callback
  * confine writes to objects prefixed by `OBJECT_PREFIX` for safe cleanup
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Awaitable, Callable

import asyncpg

ReportFn = Callable[[dict], None]


async def _open_pool(dsn: str, size: int) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=dsn,
        min_size=min(size, 5),
        max_size=size,
        command_timeout=30,
    )


async def connections_storm(
    dsn: str,
    connections: int,
    hold_seconds: float,
    report: ReportFn,
) -> None:
    """Open `connections` idle sessions and hold them.

    Used to push the connection-count metric on RDS/Aurora and exercise
    pgbouncer / max_connections limits.
    """
    conns: list[asyncpg.Connection] = []
    try:
        for i in range(connections):
            try:
                c = await asyncpg.connect(dsn=dsn, timeout=10)
                conns.append(c)
                report({"opened": len(conns), "held": len(conns)})
            except Exception as e:  # one failed conn shouldn't sink the job
                report({"error": str(e)[:200], "opened": len(conns), "held": len(conns)})
                await asyncio.sleep(0.1)
        # Keep them alive, periodically ping. Each ping is reported as an op
        # with its latency, so the dashboard's ops/sec actually moves and the
        # user can see the workload is alive (not just "DatabaseConnections =
        # N on the cloud-side dashboard").
        end = time.monotonic() + hold_seconds
        while time.monotonic() < end:
            for c in conns:
                t0 = time.perf_counter()
                try:
                    await c.execute("SELECT 1")
                    ms = (time.perf_counter() - t0) * 1000
                    report({"latency_ms": ms, "ok": 1, "held": len(conns)})
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    report({"error": str(e)[:200], "held": len(conns)})
            await asyncio.sleep(2)
    finally:
        for c in conns:
            try:
                await c.close()
            except Exception:
                pass


async def _worker_loop(
    pool: asyncpg.Pool,
    query: str,
    report: ReportFn,
    worker_id: int,
) -> None:
    while True:
        t0 = time.perf_counter()
        try:
            async with pool.acquire() as conn:
                await conn.execute(query)
            ms = (time.perf_counter() - t0) * 1000
            report({"worker": worker_id, "latency_ms": ms, "ok": 1})
        except asyncio.CancelledError:
            raise
        except Exception as e:
            report({"worker": worker_id, "error": str(e)[:200], "ok": 0})
            await asyncio.sleep(0.5)


async def cpu_burn(
    dsn: str,
    workers: int,
    duration_seconds: float,
    report: ReportFn,
    intensity: int = 200_000,
) -> None:
    """CPU-heavy queries.

    `generate_series` + md5 sort burns CPU on the server without writing
    anything to disk and without growing memory beyond work_mem.
    """
    pool = await _open_pool(dsn, workers)
    query = (
        f"SELECT count(*) FROM "
        f"(SELECT md5(g::text) m FROM generate_series(1, {intensity}) g "
        f"ORDER BY m) s"
    )
    try:
        tasks = [
            asyncio.create_task(_worker_loop(pool, query, report, i))
            for i in range(workers)
        ]
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=duration_seconds)
        except asyncio.TimeoutError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await pool.close()


async def memory_pressure(
    dsn: str,
    workers: int,
    duration_seconds: float,
    report: ReportFn,
    rows: int = 500_000,
) -> None:
    """Force large in-memory hash aggregates / sorts.

    Uses a large `generate_series` with a hash aggregate over a wide row.
    Spills to temp files past work_mem — useful for exercising the
    FreeableMemory / SwapUsage metrics.
    """
    pool = await _open_pool(dsn, workers)
    query = (
        f"SELECT bucket, count(*), sum(length(payload)) FROM "
        f"(SELECT (g % 10000) bucket, repeat(md5(g::text), 8) payload "
        f"FROM generate_series(1, {rows}) g) s "
        f"GROUP BY bucket"
    )
    try:
        tasks = [
            asyncio.create_task(_worker_loop(pool, query, report, i))
            for i in range(workers)
        ]
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=duration_seconds)
        except asyncio.TimeoutError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await pool.close()


async def disk_churn(
    dsn: str,
    workers: int,
    duration_seconds: float,
    report: ReportFn,
    object_prefix: str,
    rows_per_batch: int = 50_000,
) -> None:
    """Write-heavy workload that produces WAL + disk IO.

    Creates an isolated table per worker (`<prefix>w<n>`), runs
    INSERT/DELETE batches against it, and drops the table at the end.
    This exercises WriteIOPS, WriteThroughput, and WAL generation.
    """
    pool = await _open_pool(dsn, workers)
    table_names = [f"{object_prefix}w{i}" for i in range(workers)]

    async def _setup(conn: asyncpg.Connection, table: str) -> None:
        await conn.execute(
            f'CREATE UNLOGGED TABLE IF NOT EXISTS "{table}" ('
            f'  id bigserial primary key,'
            f'  payload text not null,'
            f'  created_at timestamptz default now()'
            f')'
        )

    async def _teardown(conn: asyncpg.Connection, table: str) -> None:
        await conn.execute(f'DROP TABLE IF EXISTS "{table}"')

    try:
        # setup
        async with pool.acquire() as conn:
            for t in table_names:
                await _setup(conn, t)

        async def _worker(worker_id: int) -> None:
            table = table_names[worker_id]
            while True:
                t0 = time.perf_counter()
                try:
                    async with pool.acquire() as conn:
                        await conn.execute(
                            f'INSERT INTO "{table}" (payload) '
                            f'SELECT repeat(md5(g::text), 8) '
                            f'FROM generate_series(1, {rows_per_batch}) g'
                        )
                        # Keep table bounded — delete oldest rows
                        await conn.execute(
                            f'DELETE FROM "{table}" WHERE id IN ('
                            f'  SELECT id FROM "{table}" ORDER BY id ASC '
                            f'  LIMIT {rows_per_batch})'
                        )
                    ms = (time.perf_counter() - t0) * 1000
                    report({"worker": worker_id, "latency_ms": ms, "rows": rows_per_batch})
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    report({"worker": worker_id, "error": str(e)[:200]})
                    await asyncio.sleep(0.5)

        tasks = [asyncio.create_task(_worker(i)) for i in range(workers)]
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=duration_seconds)
        except asyncio.TimeoutError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        # cleanup
        try:
            async with pool.acquire() as conn:
                for t in table_names:
                    await _teardown(conn, t)
        except Exception as e:
            report({"cleanup_error": str(e)[:200]})
        await pool.close()


async def mixed_oltp(
    dsn: str,
    workers: int,
    duration_seconds: float,
    report: ReportFn,
    object_prefix: str,
    table_rows: int = 100_000,
) -> None:
    """pgbench-style mixed read/write workload.

    Creates a shared accounts table, hammers it with point-lookups and
    UPDATE-by-pk in a 75/25 ratio. Mirrors what a real OLTP app puts on
    Aurora.
    """
    pool = await _open_pool(dsn, workers)
    table = f"{object_prefix}accounts"

    try:
        async with pool.acquire() as conn:
            await conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{table}" ('
                f'  id bigint primary key,'
                f'  balance bigint not null default 0,'
                f'  filler text not null'
                f')'
            )
            count = await conn.fetchval(f'SELECT count(*) FROM "{table}"')
            if count < table_rows:
                await conn.execute(
                    f'INSERT INTO "{table}" (id, balance, filler) '
                    f'SELECT g, 1000, repeat(md5(g::text), 4) '
                    f'FROM generate_series({count + 1}, {table_rows}) g '
                    f'ON CONFLICT (id) DO NOTHING'
                )

        async def _worker(worker_id: int) -> None:
            while True:
                t0 = time.perf_counter()
                pk = random.randint(1, table_rows)
                op = "read" if random.random() < 0.75 else "write"
                try:
                    async with pool.acquire() as conn:
                        if op == "read":
                            await conn.fetchrow(
                                f'SELECT id, balance FROM "{table}" WHERE id = $1', pk
                            )
                        else:
                            await conn.execute(
                                f'UPDATE "{table}" SET balance = balance + 1 '
                                f'WHERE id = $1', pk
                            )
                    ms = (time.perf_counter() - t0) * 1000
                    report({"worker": worker_id, "op": op, "latency_ms": ms})
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    report({"worker": worker_id, "error": str(e)[:200]})
                    await asyncio.sleep(0.2)

        tasks = [asyncio.create_task(_worker(i)) for i in range(workers)]
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=duration_seconds)
        except asyncio.TimeoutError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        try:
            async with pool.acquire() as conn:
                await conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        except Exception as e:
            report({"cleanup_error": str(e)[:200]})
        await pool.close()
