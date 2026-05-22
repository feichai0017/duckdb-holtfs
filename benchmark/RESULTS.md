# HoltFS Metadata Discovery Results

This is a local reference run, not a publication-grade result. It
measures warm-cache file discovery on the current macOS development
machine and isolates namespace planning only.

Command:

```sh
python3 benchmark/metadata_discovery.py \
  --duckdb ./build/release/duckdb \
  --extension ./build/release/extension/holtfs/holtfs.duckdb_extension \
  --workdir /tmp/holtfs-metadata-bench-20k \
  --files 20000 \
  --partitions 240 \
  --runs 5 \
  --recreate
```

Dataset:

- 20,000 local `.parquet` placeholder files
- 240 lakehouse-shaped partitions
- shape: `source/bucket/table/date=.../hour=.../part-XXXXXXXX.parquet`
- all timings are warm-cache iterations

Index build:

| Mode | Build Time |
|---|---:|
| Persistent Holt index | 572.57 ms |
| Memory Holt index | 840.83 ms |

Discovery:

| Path | Count | Median ms | Min ms | Max ms | vs Native |
|---|---:|---:|---:|---:|---:|
| DuckDB native glob | 20,000 | 165.53 | 163.73 | 171.70 | 1.00x |
| Holt persistent scan | 20,000 | 9.47 | 9.21 | 12.98 | 17.48x |
| Holt memory scan | 20,000 | 6.09 | 5.92 | 6.80 | 27.17x |

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
  --workdir /tmp/holtfs-maintenance-bench-20k \
  --files 20000 \
  --partitions 240 \
  --delta-files 100 \
  --runs 5 \
  --recreate
```

Dataset:

- 20,000 initial local placeholder files
- 20,500 final indexed files after five delta-prefix refreshes
- 240 lakehouse-shaped partitions
- all timings are local warm-cache iterations

Index build:

| Mode | Build Time |
|---|---:|
| Initial persistent Holt index | 603.70 ms |
| Final full validate after deltas | 633.94 ms |

Maintenance:

| Path | Result | Median ms | Min ms | Max ms | vs Full Refresh |
|---|---:|---:|---:|---:|---:|
| `holtfs_status` manifest read | stale=0 | 0.69 | 0.64 | 0.91 | 874.36x |
| Prefix refresh, unchanged partition | 83 | 19.14 | 18.04 | 21.09 | 31.31x |
| Full validate scan | current=1 | 525.47 | 524.22 | 615.08 | 1.14x |
| Full refresh/rebuild | 20,000 | 599.45 | 521.68 | 606.93 | 1.00x |
| Prefix refresh, new partition | 100 | 19.10 | 18.01 | 21.71 | 31.39x |

Interpretation:

- `holtfs_status` is the cheap fast path for "can I use this snapshot?"
  when the application relies on a TTL policy.
- Prefix refresh is the intended maintenance path when an application
  already knows which partition changed.
- Full validate remains an exact audit path and is expected to cost about
  the same order as walking the source namespace.
