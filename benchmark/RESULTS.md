# HoltFS Metadata Discovery Results

This is a local reference run, not a publication-grade result. It
measures warm-cache file discovery on the current macOS development
machine and isolates namespace planning only.

Environment:

- date: May 22, 2026
- machine: Apple M3 Pro, 12 CPU cores, 36 GiB memory
- OS: macOS 26.3, Darwin 25.3.0, arm64
- DuckDB: release build from this repository
- HoltFS: v0.1.0

Command:

```sh
python3 benchmark/metadata_discovery.py \
  --duckdb ./build/release/duckdb \
  --extension ./build/release/extension/holtfs/holtfs.duckdb_extension \
  --workdir /tmp/holtfs-metadata-bench-100k \
  --files 100000 \
  --partitions 1000 \
  --runs 7 \
  --recreate
```

Dataset:

- 100,000 local `.parquet` placeholder files
- 1,000 lakehouse-shaped partitions
- shape: `source/bucket/table/date=.../hour=.../part-XXXXXXXX.parquet`
- all timings are warm-cache iterations after index build

Index build:

| Mode | Build Time |
|---|---:|
| Persistent Holt index | 2895.41 ms |
| Memory Holt index | 4449.09 ms |

Discovery:

| Path | Count | Median ms | Min ms | Max ms | vs Native |
|---|---:|---:|---:|---:|---:|
| DuckDB native `glob()` | 100,000 | 806.68 | 796.67 | 913.21 | 1.00x |
| Holt persistent scan | 100,000 | 48.79 | 48.09 | 50.98 | 16.53x |
| Holt memory scan | 100,000 | 32.91 | 32.32 | 33.45 | 24.51x |

Interpretation:

- This supports the intended claim: HoltFS accelerates repeated file
  discovery after a metadata index exists.
- It does not claim faster Parquet scan/decode.
- S3/object-store claims need a separate run that includes
  `ListObjectsV2` pagination, network latency, and stale-index
  validation policy.

## Index Maintenance Reference

Command:

```sh
python3 benchmark/index_maintenance.py \
  --duckdb ./build/release/duckdb \
  --extension ./build/release/extension/holtfs/holtfs.duckdb_extension \
  --workdir /tmp/holtfs-maintenance-bench-100k \
  --files 100000 \
  --partitions 1000 \
  --delta-files 100 \
  --runs 7 \
  --recreate
```

Dataset:

- 100,000 initial local placeholder files
- 100,700 final indexed files after seven delta-prefix refreshes
- 1,000 lakehouse-shaped partitions
- all timings are local warm-cache iterations

Index build:

| Mode | Build Time |
|---|---:|
| Initial persistent Holt index | 2860.94 ms |
| Final full validate after deltas | 2709.52 ms |

Maintenance:

| Path | Result | Median ms | Min ms | Max ms | vs Full Refresh |
|---|---:|---:|---:|---:|---:|
| `holtfs_status` manifest read | stale=0 | 0.66 | 0.63 | 0.94 | 4319.07x |
| Prefix refresh, unchanged partition | 100 | 52.94 | 51.13 | 54.26 | 54.25x |
| Full validate scan | current=1 | 2654.06 | 2646.86 | 2676.11 | 1.08x |
| Full refresh/rebuild | 100,000 | 2871.83 | 2852.22 | 2896.86 | 1.00x |
| Prefix refresh, new partition | 100 | 53.33 | 49.72 | 55.78 | 53.85x |

Interpretation:

- `holtfs_status` is the cheap fast path for "can I use this snapshot?"
  when the application relies on a TTL policy.
- Prefix refresh is the intended maintenance path when an application
  already knows which partition changed.
- Full validate remains an exact audit path and is expected to cost about
  the same order as walking the source namespace.
