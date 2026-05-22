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
