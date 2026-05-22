#!/usr/bin/env python3
"""Compare native DuckDB glob discovery with Holt-backed namespace scans.

The benchmark isolates the file-discovery/planning path. It does not
read Parquet payloads and should not be used as an end-to-end scan
benchmark.
"""

from __future__ import annotations

import argparse
import os
import shutil
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class DuckDBSession:
    def __init__(self, duckdb: Path, extension: Path):
        self.duckdb = duckdb
        self.extension = extension
        self.proc: subprocess.Popen[str] | None = None

    def __enter__(self) -> "DuckDBSession":
        self.proc = subprocess.Popen(
            [str(self.duckdb), "-unsigned", "-csv"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.query(f"SELECT 1")
        self.exec(f"LOAD {sql_string(str(self.extension))}")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.proc and self.proc.stdin:
            try:
                self.proc.stdin.write(".quit\n")
                self.proc.stdin.flush()
            except BrokenPipeError:
                pass
        if self.proc:
            self.proc.wait(timeout=10)

    def exec(self, sql: str) -> None:
        if not self.proc or not self.proc.stdin or not self.proc.stdout:
            raise RuntimeError("DuckDB session is not open")
        token = "__holtfs_bench_done_" + uuid.uuid4().hex + "__"
        self.proc.stdin.write(sql.rstrip(";") + ";\n")
        self.proc.stdin.write(f"SELECT {sql_string(token)};\n")
        self.proc.stdin.flush()
        while True:
            line = self.proc.stdout.readline()
            if line == "":
                stderr = ""
                if self.proc.stderr:
                    stderr = self.proc.stderr.read()
                raise RuntimeError(f"DuckDB exited while running statement. stderr:\n{stderr}")
            if token in line:
                break

    def query(self, sql: str) -> tuple[float, list[str]]:
        if not self.proc or not self.proc.stdin or not self.proc.stdout:
            raise RuntimeError("DuckDB session is not open")
        token = "__holtfs_bench_done_" + uuid.uuid4().hex + "__"
        statement = sql.rstrip(";")
        self.proc.stdin.write(f"COPY ({statement}) TO STDOUT (FORMAT CSV, HEADER false);\n")
        self.proc.stdin.write(f"SELECT {sql_string(token)};\n")
        self.proc.stdin.flush()

        rows: list[str] = []
        start = time.perf_counter()
        while True:
            line = self.proc.stdout.readline()
            if line == "":
                stderr = ""
                if self.proc.stderr:
                    stderr = self.proc.stderr.read()
                raise RuntimeError(f"DuckDB exited while running query. stderr:\n{stderr}")
            line = line.rstrip("\n")
            if token in line:
                break
            if line:
                rows.append(line)
        elapsed = time.perf_counter() - start
        return elapsed, rows


def count_parquet_files(root: Path) -> int:
    return sum(1 for path in root.rglob("*.parquet") if path.is_file())


def create_dataset(root: Path, files: int, partitions: int, recreate: bool) -> None:
    if recreate and root.exists():
        shutil.rmtree(root)
    if root.exists() and count_parquet_files(root) == files:
        return
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)

    partitions = max(partitions, 1)
    for i in range(files):
        part = i % partitions
        date = f"date=2026-05-{(part // 24) % 28 + 1:02d}"
        hour = f"hour={part % 24:02d}"
        path = root / date / hour / f"part-{i:08d}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"holtfs\n")


def median(values: list[float]) -> float:
    return statistics.median(values)


def count_from_rows(rows: list[str]) -> int:
    if not rows:
        raise RuntimeError("query returned no rows")
    return int(rows[-1].split(",", 1)[0])


def time_count(session: DuckDBSession, sql: str, runs: int) -> tuple[int, list[float]]:
    times: list[float] = []
    count = -1
    for _ in range(runs):
        elapsed, rows = session.query(sql)
        count = count_from_rows(rows)
        times.append(elapsed)
    return count, times


def print_result(name: str, count: int, times: list[float], native_median: float | None = None) -> None:
    med_ms = median(times) * 1000
    min_ms = min(times) * 1000
    max_ms = max(times) * 1000
    speedup = ""
    if native_median is not None:
        speedup = f"{native_median / median(times):.2f}x"
    print(f"| {name} | {count} | {med_ms:.2f} | {min_ms:.2f} | {max_ms:.2f} | {speedup} |")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", type=Path, default=Path("./build/release/duckdb"))
    parser.add_argument(
        "--extension",
        type=Path,
        default=Path("./build/release/extension/holtfs/holtfs.duckdb_extension"),
    )
    parser.add_argument("--workdir", type=Path, default=Path("/tmp/holtfs-metadata-bench"))
    parser.add_argument("--files", type=int, default=50000)
    parser.add_argument("--partitions", type=int, default=240)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--recreate", action="store_true")
    args = parser.parse_args()

    source = args.workdir / "source" / "bucket" / "table"
    index_path = args.workdir / "index.holt"
    memory_name = "bench_memory"

    create_dataset(source, args.files, args.partitions, args.recreate)
    if index_path.exists():
        shutil.rmtree(index_path)

    source_sql = sql_string(str(source))
    index_sql = sql_string(str(index_path))
    memory_sql = sql_string(memory_name)
    prefix_sql = sql_string(str(source) + os.sep)
    glob_sql = sql_string(str(source) + os.sep + "**" + os.sep + "*.parquet")

    with DuckDBSession(args.duckdb, args.extension) as session:
        build_persistent, rows = session.query(
            "SELECT indexed_files FROM holtfs_index("
            f"{source_sql}, mode := 'persistent', index_path := {index_sql})"
        )
        persistent_files = count_from_rows(rows)

        build_memory, rows = session.query(
            "SELECT indexed_files FROM holtfs_index("
            f"{source_sql}, mode := 'memory', name := {memory_sql})"
        )
        memory_files = count_from_rows(rows)

        native_sql = f"SELECT count(*) FROM glob({glob_sql})"
        persistent_sql = (
            "SELECT count(*) FROM holt_files("
            f"{index_sql}, mode := 'persistent', prefix := {prefix_sql}) WHERE entry_type = 'key'"
        )
        memory_sql_query = (
            "SELECT count(*) FROM holt_files("
            f"{memory_sql}, mode := 'memory', prefix := {prefix_sql}) WHERE entry_type = 'key'"
        )

        # Warm all discovery paths before timing the steady state.
        session.query(native_sql)
        session.query(persistent_sql)
        session.query(memory_sql_query)

        native_count, native_times = time_count(session, native_sql, args.runs)
        persistent_count, persistent_times = time_count(session, persistent_sql, args.runs)
        memory_count, memory_times = time_count(session, memory_sql_query, args.runs)

    if native_count != persistent_count or native_count != memory_count:
        raise RuntimeError(
            f"count mismatch: native={native_count}, persistent={persistent_count}, memory={memory_count}"
        )
    if persistent_files != native_count or memory_files != native_count:
        raise RuntimeError(
            f"index build mismatch: persistent={persistent_files}, memory={memory_files}, native={native_count}"
        )

    native_median = median(native_times)
    print("# HoltFS Metadata Discovery Benchmark")
    print()
    print(f"- files: {native_count}")
    print(f"- partitions: {args.partitions}")
    print(f"- source: `{source}`")
    print(f"- duckdb: `{args.duckdb}`")
    print(f"- extension: `{args.extension}`")
    print(f"- persistent build: {build_persistent * 1000:.2f} ms")
    print(f"- memory build: {build_memory * 1000:.2f} ms")
    print(f"- runs: {args.runs} warm-cache iterations")
    print()
    print("| Path | Count | Median ms | Min ms | Max ms | vs native |")
    print("|---|---:|---:|---:|---:|---:|")
    print_result("DuckDB native glob", native_count, native_times)
    print_result("Holt persistent scan", persistent_count, persistent_times, native_median)
    print_result("Holt memory scan", memory_count, memory_times, native_median)
    return 0


if __name__ == "__main__":
    sys.exit(main())
