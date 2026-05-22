# HoltFS Benchmarks

These benchmarks isolate DuckDB's file-discovery/planning path:

- native baseline: DuckDB `glob('/path/**.parquet')`
- Holt persistent index: `holt_files(index_path, mode := 'persistent')`
- Holt memory index: `holt_files(name, mode := 'memory')`

They intentionally do **not** read Parquet payloads. The claim is about
namespace discovery (`list/stat/glob`), not scan/decode throughput.

## Run

Build the extension first:

```sh
make GEN=ninja release
```

Then run:

```sh
python3 benchmark/metadata_discovery.py \
  --duckdb ./build/release/duckdb \
  --extension ./build/release/extension/holtfs/holtfs.duckdb_extension \
  --files 100000 \
  --partitions 1000 \
  --runs 7 \
  --recreate
```

To measure index maintenance costs:

```sh
python3 benchmark/index_maintenance.py \
  --duckdb ./build/release/duckdb \
  --extension ./build/release/extension/holtfs/holtfs.duckdb_extension \
  --files 100000 \
  --partitions 1000 \
  --delta-files 100 \
  --runs 7 \
  --recreate
```

The script creates a local lakehouse-shaped tree:

```text
source/bucket/table/date=.../hour=.../part-XXXXXXXX.parquet
```

It reports persistent index build time, memory index build time, and
warm-cache discovery latency for native glob vs Holt scans.

See [`RESULTS.md`](RESULTS.md) for a local reference run.

`metadata_discovery.py` measures repeated file discovery after an index
exists. `index_maintenance.py` measures the operational paths around that
index: cheap status checks, known-prefix refresh, full validate, and full
refresh/rebuild.

`holtfs_status` is a cheap manifest/TTL check. It does not walk the source
namespace. Use `holtfs_validate` when the benchmark or application needs an
exact stale-index audit.

`holtfs_validate` is intentionally not part of the latency comparison.
Validation walks the source namespace and is the correctness check for a
snapshot index, not the accelerated discovery path.

## Interpreting Results

Use this benchmark to answer one narrow question:

> If DuckDB already has a metadata index, how much faster is file
> discovery than repeatedly walking the filesystem namespace?

Do not use it to claim faster Parquet reads. For S3/object-store
claims, rerun against a real bucket or a benchmark harness that models
`ListObjectsV2` pagination and network latency.
