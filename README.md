# duckdb-holtfs

`duckdb-holtfs` is a DuckDB extension for querying file/object
metadata indexes stored in [Holt](https://github.com/feichai0017/holt).
It is based on DuckDB's official
[`extension-template`](https://github.com/duckdb/extension-template).

The current extension exposes file/glob indexing plus Holt-backed
namespace listing. Indexes can be either persistent or in-memory:

```sql
LOAD holtfs;

SELECT *
FROM holtfs_index('/lake/table',
                  mode := 'persistent',
                  index_path := '/var/cache/duckdb/table.holt');

SELECT *
FROM holtfs_index('s3://bucket/table/**/*.parquet',
                  mode := 'persistent',
                  index_path := '/var/cache/duckdb/s3-table.holt');

SELECT *
FROM holtfs_status('/var/cache/duckdb/table.holt',
                   mode := 'persistent',
                   max_age_seconds := 3600);

SELECT *
FROM holtfs_refresh('/lake/table',
                    '/var/cache/duckdb/table.holt',
                    mode := 'persistent',
                    prefix := 'date=2026-05-22');

SELECT *
FROM holt_files(
  '/var/cache/duckdb/table.holt',
  mode := 'persistent',
  prefix := 's3://bucket/table/date=2026-05-22/',
  delimiter := '/',
  max_files := 1000
);

SELECT *
FROM holtfs_index('/lake/table',
                  mode := 'memory',
                  name := 'table_cache');

SELECT *
FROM holt_files('table_cache',
                mode := 'memory',
                prefix := '/lake/table/',
                delimiter := '/');

SELECT *
FROM holt_parquet_scan('table_cache',
                       mode := 'memory',
                       prefix := '/lake/table/date=2026-05-22/');

SELECT *
FROM holtfs_validate('/lake/table',
                     'table_cache',
                     mode := 'memory');
```

`holt_files` returns:

```text
entry_type VARCHAR  -- key or common_prefix
path       VARCHAR  -- object/file path
value      BLOB     -- empty unless include_value := true
version    UBIGINT  -- Holt record version for key entries
```

## Install

After the extension is accepted into DuckDB Community Extensions:

```sql
INSTALL holtfs FROM community;
LOAD holtfs;
```

Until then, build from source and load the unsigned local extension:

```sh
./build/release/duckdb -unsigned \
  -c "LOAD './build/release/extension/holtfs/holtfs.duckdb_extension'; SELECT holtfs_version();"
```

## Build

Clone with submodules:

```sh
git clone --recurse-submodules https://github.com/feichai0017/duckdb-holtfs.git
cd duckdb-holtfs
make
```

DuckDB's extension Makefile does not handle spaces in the checkout
path reliably, so build from a path without spaces.

The build uses the `third_party/holt` submodule and runs:

```sh
cargo build -p holt-ffi --release --locked
```

The resulting `libholt_ffi.a` is statically linked into the DuckDB
extension. To use a different Holt checkout:

```sh
make GEN=ninja EXT_FLAGS="-DHOLT_ROOT=/path/to/holt"
```

## Usage

Build or replace a persistent local metadata index:

```sql
SELECT *
FROM holtfs_index('/data/lake/table',
                  mode := 'persistent',
                  index_path := '/var/cache/duckdb/table.holt');
```

Build an in-memory index for the current DuckDB process:

```sql
SELECT *
FROM holtfs_index('/data/lake/table',
                  mode := 'memory',
                  name := 'table_cache');
```

The index stores regular files as `path -> metadata` records where the
metadata payload currently contains
`size=<bytes>;kind=file;mtime_us=<epoch-micros>`.

Read a Holt index:

```sql
SELECT entry_type, path, version
FROM holt_files('/var/cache/duckdb/table.holt',
                mode := 'persistent',
                prefix := 's3://bucket/table/',
                delimiter := '/');
```

Set `include_value := true` when the metadata payload is needed:

```sql
SELECT path, value, version
FROM holt_files('/var/cache/duckdb/table.holt',
                mode := 'persistent',
                prefix := 's3://bucket/table/',
                include_value := true);
```

Read an in-memory index by name:

```sql
SELECT entry_type, path
FROM holt_files('table_cache',
                mode := 'memory',
                prefix := '/data/lake/table/');
```

Scan Parquet files through DuckDB's native Parquet reader, using Holt
only for file discovery:

```sql
SELECT *
FROM holt_parquet_scan('/var/cache/duckdb/table.holt',
                       mode := 'persistent',
                       prefix := '/data/lake/table/date=2026-05-22/',
                       hive_partitioning := true);
```

`holt_parquet_scan` rewrites to `read_parquet([...])` during binding.
DuckDB still owns the Parquet reader, projection/filter pushdown, and
payload I/O. HoltFS only supplies the concrete file list from the
persistent or memory namespace index. It also forwards common Parquet
options such as `filename`, `hive_partitioning`, and `union_by_name`.

Check index freshness without walking the source namespace:

```sql
SELECT indexed_files,
       indexed_bytes,
       age_seconds,
       is_stale
FROM holtfs_status('/var/cache/duckdb/table.holt',
                   mode := 'persistent',
                   max_age_seconds := 3600);
```

`holtfs_status` reads Holt's internal manifest and optionally applies a
TTL-style age policy. It is cheap, but it does not discover external file
changes by itself.

Refresh a known changed partition without rebuilding the whole table
index:

```sql
SELECT refreshed_files,
       removed_keys,
       indexed_files
FROM holtfs_refresh('/data/lake/table',
                    '/var/cache/duckdb/table.holt',
                    mode := 'persistent',
                    prefix := 'date=2026-05-22');
```

The `prefix` argument may be a path under `source_path` or a relative
subtree such as a Hive partition. If `prefix` is omitted, `holtfs_refresh`
performs a full replace, matching `holtfs_index`.

`holtfs_rebuild` is the explicit full-rebuild form:

```sql
SELECT indexed_files
FROM holtfs_rebuild('/data/lake/table',
                    '/var/cache/duckdb/table.holt',
                    mode := 'persistent');
```

Validate whether a snapshot index still matches the current filesystem
metadata:

```sql
SELECT source_files,
       indexed_files,
       changed_files,
       missing_files,
       deleted_files,
       is_current
FROM holtfs_validate('/data/lake/table',
                     '/var/cache/duckdb/table.holt',
                     mode := 'persistent');
```

`holtfs_validate` compares the source tree against indexed
`size=<bytes>;kind=file;mtime_us=<epoch-micros>` records. `missing_files`
means the source has files absent from Holt, `deleted_files` means Holt
still has keys whose files disappeared, and `changed_files` means the
indexed size or mtime no longer matches.

Source paths can be regular files, local directories, or explicit DuckDB
glob patterns:

```sql
SELECT *
FROM holtfs_index('/data/lake/table/**/*.parquet',
                  mode := 'persistent',
                  index_path := '/var/cache/duckdb/table.holt');
```

For object stores, load `httpfs` or `cache_httpfs` first, then index the
same `s3://` paths that DuckDB can glob:

```sql
LOAD httpfs;

SELECT *
FROM holtfs_index('s3://bucket/table/**/*.parquet',
                  mode := 'persistent',
                  index_path := '/var/cache/duckdb/table.holt');
```

When a non-local source path is not a file or directory, HoltFS treats it
as a Parquet dataset root and tries `**/*.parquet`. Prefix refresh is not
available for glob source paths; use `holtfs_rebuild` or rebuild a
narrower partition glob instead.

Full rebuilds are exact. A memory index name is atomically replaced after
the fresh tree is built. A persistent index is first built in a temporary
sibling path and only then published over the old path, so index build
failures leave the previous index intact. Prefix refresh is intended for
the common lakehouse pattern where the writer knows which partition just
changed.

## Scope

This repository is intentionally narrow:

- `holtfs_index(source_path, mode := ..., index_path := ... | name := ...)`
  indexes regular files from a file, directory, dataset root, or glob into Holt.
- `holtfs_status(index_ref, mode := ..., max_age_seconds := ...)` reads
  the stored manifest without walking the source namespace.
- `holtfs_refresh(source_path, index_ref, mode := ..., prefix := ...)`
  refreshes a known changed subtree, or performs a full replace when
  `prefix` is omitted.
- `holtfs_rebuild(source_path, index_ref, mode := ...)` explicitly rebuilds
  an index from the current source snapshot.
- `holt_files(index_ref, mode := ...)` lists an existing Holt metadata
  index through Holt's C ABI.
- `holt_parquet_scan(index_ref, mode := ..., prefix := ...)` delegates
  indexed Parquet file lists to DuckDB's native `read_parquet`.
- `holtfs_validate(source_path, index_ref, mode := ...)` checks whether a
  snapshot index is current against local file size and mtime.

HoltFS complements data-cache extensions such as `cache_httpfs`: those
extensions cache bytes fetched by the filesystem layer, while HoltFS
persists a path/object metadata index so repeated planning, listing, and
glob-style discovery can skip namespace walks.

## Benchmark

The benchmark under [`benchmark/`](benchmark/) compares DuckDB native
`glob()` discovery with Holt persistent and memory scans. It measures
metadata discovery only; it does not read Parquet payloads.

Local reference run on May 22, 2026:

- machine: Apple M3 Pro, 12 CPU cores, 36 GiB memory, macOS 26.3
- DuckDB: release build from this repository
- workload: 100,000 local `.parquet` placeholder files across 1,000
  lakehouse-shaped partitions
- timing: 7 warm-cache iterations after index build
- scope: namespace discovery and index maintenance only

Discovery:

| Path | Count | Median ms | Min ms | Max ms | vs Native |
|---|---:|---:|---:|---:|---:|
| DuckDB native `glob()` | 100,000 | 806.68 | 796.67 | 913.21 | 1.00x |
| Holt persistent scan | 100,000 | 48.79 | 48.09 | 50.98 | 16.53x |
| Holt memory scan | 100,000 | 32.91 | 32.32 | 33.45 | 24.51x |

Index maintenance:

| Path | Result | Median ms | Min ms | Max ms | vs Full Refresh |
|---|---:|---:|---:|---:|---:|
| `holtfs_status` manifest read | stale=0 | 0.66 | 0.63 | 0.94 | 4319.07x |
| Prefix refresh, unchanged partition | 100 | 52.94 | 51.13 | 54.26 | 54.25x |
| Full validate scan | current=1 | 2654.06 | 2646.86 | 2676.11 | 1.08x |
| Full refresh/rebuild | 100,000 | 2871.83 | 2852.22 | 2896.86 | 1.00x |
| Prefix refresh, new partition | 100 | 53.33 | 49.72 | 55.78 | 53.85x |

These numbers support the intended claim: after a Holt metadata index
exists, repeated listing and glob-style planning can avoid walking the
filesystem namespace. They do not claim faster Parquet decoding or
object-store network I/O. For S3 claims, rerun the benchmark against a
real bucket because `ListObjectsV2` pagination, network latency, and
freshness policy dominate the result.
