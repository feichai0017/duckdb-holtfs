#!/usr/bin/env python3
"""Measure HoltFS index freshness and maintenance paths.

This benchmark isolates the user-facing index lifecycle:

- cheap manifest status checks
- prefix refresh for a known unchanged partition
- prefix refresh for newly added partitions
- full validate scans
- full rebuild through holtfs_refresh without a prefix

It does not read Parquet payloads.
"""

from __future__ import annotations

import argparse
import os
import shutil
import statistics
import sys
import time
from pathlib import Path

from metadata_discovery import DuckDBSession, count_from_rows, create_dataset, median, sql_string


def partition_prefix(partition: int) -> str:
    date = f"date=2026-05-{(partition // 24) % 28 + 1:02d}"
    hour = f"hour={partition % 24:02d}"
    return os.path.join(date, hour)


def delta_prefix(run: int) -> str:
    return os.path.join("date=2026-12-31", f"hour={run % 24:02d}")


def create_delta_partition(source: Path, prefix: str, files: int, run: int) -> None:
    root = source / Path(prefix)
    root.mkdir(parents=True, exist_ok=True)
    for i in range(files):
        path = root / f"part-delta-{run:04d}-{i:08d}.parquet"
        path.write_bytes(f"holtfs-delta-{run}-{i}\n".encode())


def scalar_int(rows: list[str]) -> int:
    return count_from_rows(rows)


def time_query(session: DuckDBSession, sql: str, runs: int) -> tuple[int, list[float]]:
    times: list[float] = []
    value = -1
    for _ in range(runs):
        elapsed, rows = session.query(sql)
        value = scalar_int(rows)
        times.append(elapsed)
    return value, times


def time_mutating_count(session: DuckDBSession, sql: str, runs: int) -> tuple[int, list[float]]:
    times: list[float] = []
    count = -1
    for _ in range(runs):
        elapsed, rows = session.query(sql)
        count = scalar_int(rows)
        times.append(elapsed)
    return count, times


def print_timing(name: str, result: str, times: list[float], full_rebuild_median: float | None = None) -> None:
    med_ms = median(times) * 1000
    min_ms = min(times) * 1000
    max_ms = max(times) * 1000
    speedup = ""
    if full_rebuild_median is not None:
        speedup = f"{full_rebuild_median / median(times):.2f}x"
    print(f"| {name} | {result} | {med_ms:.2f} | {min_ms:.2f} | {max_ms:.2f} | {speedup} |")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, default=Path("./build/release/duckdb"))
    parser.add_argument(
        "--extension",
        type=Path,
        default=Path("./build/release/extension/holtfs/holtfs.duckdb_extension"),
    )
    parser.add_argument("--workdir", type=Path, default=Path("/tmp/holtfs-maintenance-bench"))
    parser.add_argument("--files", type=int, default=20000)
    parser.add_argument("--partitions", type=int, default=240)
    parser.add_argument("--delta-files", type=int, default=100)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--recreate", action="store_true")
    args = parser.parse_args()

    source = args.workdir / "source" / "bucket" / "table"
    index_path = args.workdir / "index.holt"
    create_dataset(source, args.files, args.partitions, args.recreate)
    if index_path.exists():
        shutil.rmtree(index_path)

    source_sql = sql_string(str(source))
    index_sql = sql_string(str(index_path))
    unchanged_prefix = partition_prefix(max(args.partitions // 2, 0))
    unchanged_prefix_sql = sql_string(unchanged_prefix)

    with DuckDBSession(args.duckdb, args.extension) as session:
        build_elapsed, rows = session.query(
            "SELECT indexed_files FROM holtfs_index("
            f"{source_sql}, mode := 'persistent', index_path := {index_sql})"
        )
        initial_files = scalar_int(rows)

        status_sql = (
            "SELECT CAST(is_stale AS INTEGER) FROM holtfs_status("
            f"{index_sql}, mode := 'persistent', max_age_seconds := 3600)"
        )
        status_value, status_times = time_query(session, status_sql, args.runs)
        if status_value != 0:
            raise RuntimeError("fresh index unexpectedly reported stale")

        unchanged_sql = (
            "SELECT refreshed_files FROM holtfs_refresh("
            f"{source_sql}, {index_sql}, mode := 'persistent', prefix := {unchanged_prefix_sql})"
        )
        unchanged_count, unchanged_times = time_mutating_count(session, unchanged_sql, args.runs)

        validate_sql = (
            "SELECT CAST(is_current AS INTEGER) FROM holtfs_validate("
            f"{source_sql}, {index_sql}, mode := 'persistent')"
        )
        validate_value, validate_times = time_query(session, validate_sql, args.runs)
        if validate_value != 1:
            raise RuntimeError("index validation failed before mutation")

        full_refresh_sql = (
            "SELECT indexed_files FROM holtfs_refresh("
            f"{source_sql}, {index_sql}, mode := 'persistent')"
        )
        full_count, full_refresh_times = time_mutating_count(session, full_refresh_sql, args.runs)

        delta_counts: list[int] = []
        delta_times: list[float] = []
        for run in range(args.runs):
            prefix = delta_prefix(run)
            create_delta_partition(source, prefix, args.delta_files, run)
            elapsed, rows = session.query(
                "SELECT refreshed_files FROM holtfs_refresh("
                f"{source_sql}, {index_sql}, mode := 'persistent', prefix := {sql_string(prefix)})"
            )
            delta_counts.append(scalar_int(rows))
            delta_times.append(elapsed)

        final_validate_elapsed, rows = session.query(
            "SELECT CAST(is_current AS INTEGER) FROM holtfs_validate("
            f"{source_sql}, {index_sql}, mode := 'persistent')"
        )
        if scalar_int(rows) != 1:
            raise RuntimeError("index validation failed after delta refresh")

        _, rows = session.query(
            "SELECT indexed_files FROM holtfs_status("
            f"{index_sql}, mode := 'persistent', max_age_seconds := 3600)"
        )
        final_indexed_files = scalar_int(rows)

    expected_delta = args.delta_files
    if any(count != expected_delta for count in delta_counts):
        raise RuntimeError(f"delta refresh count mismatch: {delta_counts}, expected {expected_delta}")

    full_median = statistics.median(full_refresh_times)
    print("# HoltFS Index Maintenance Benchmark")
    print()
    print(f"- initial files: {initial_files}")
    print(f"- final indexed files: {final_indexed_files}")
    print(f"- partitions: {args.partitions}")
    print(f"- delta files per run: {args.delta_files}")
    print(f"- source: `{source}`")
    print(f"- duckdb: `{args.duckdb}`")
    print(f"- extension: `{args.extension}`")
    print(f"- initial persistent build: {build_elapsed * 1000:.2f} ms")
    print(f"- final validate after deltas: {final_validate_elapsed * 1000:.2f} ms")
    print(f"- runs: {args.runs}")
    print()
    print("| Path | Result | Median ms | Min ms | Max ms | vs full refresh |")
    print("|---|---:|---:|---:|---:|---:|")
    print_timing("holtfs_status manifest read", f"stale={status_value}", status_times, full_median)
    print_timing("prefix refresh unchanged partition", str(unchanged_count), unchanged_times, full_median)
    print_timing("full validate scan", f"current={validate_value}", validate_times, full_median)
    print_timing("full refresh/rebuild", str(full_count), full_refresh_times)
    print_timing("prefix refresh new partition", str(expected_delta), delta_times, full_median)
    return 0


if __name__ == "__main__":
    sys.exit(main())
