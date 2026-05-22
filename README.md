# duckdb-holtfs

`duckdb-holtfs` is a DuckDB extension for querying file/object
metadata indexes stored in [Holt](https://github.com/feichai0017/holt).
It is based on DuckDB's official
[`extension-template`](https://github.com/duckdb/extension-template).

The current extension exposes local file indexing plus Holt-backed
namespace listing. Indexes can be either persistent or in-memory:

```sql
LOAD holtfs;

SELECT *
FROM holtfs_index('/lake/table',
                  mode := 'persistent',
                  index_path := '/var/cache/duckdb/table.holt',
                  refresh := 'replace');

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
                  name := 'table_cache',
                  refresh := 'replace');

SELECT *
FROM holt_files('table_cache',
                mode := 'memory',
                prefix := '/lake/table/',
                delimiter := '/');
```

`holt_files` returns:

```text
entry_type VARCHAR  -- key or common_prefix
path       VARCHAR  -- object/file path
value      BLOB     -- empty unless include_value := true
version    UBIGINT  -- Holt record version for key entries
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

## Run

```sh
./build/release/duckdb -unsigned \
  -c "LOAD './build/release/extension/holtfs/holtfs.duckdb_extension'; SELECT holtfs_version();"
```

Build or refresh a persistent local metadata index:

```sql
SELECT *
FROM holtfs_index('/data/lake/table',
                  mode := 'persistent',
                  index_path := '/var/cache/duckdb/table.holt',
                  refresh := 'replace');
```

Build an in-memory index for the current DuckDB process:

```sql
SELECT *
FROM holtfs_index('/data/lake/table',
                  mode := 'memory',
                  name := 'table_cache',
                  refresh := 'replace');
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

`refresh := 'replace'` is exact: an existing persistent index directory
is removed before rebuilding, and an existing memory index name is
atomically replaced. This avoids stale keys when files disappear.

## Scope

This repository is intentionally narrow:

- `holtfs_index(source_path, mode := ..., index_path := ... | name := ...)`
  indexes regular files from a local path into Holt.
- `holt_files(index_ref, mode := ...)` lists an existing Holt metadata
  index through Holt's C ABI.
- Direct Parquet delegation is the next step; it is not exposed as a
  placeholder SQL function.

## Benchmark

The benchmark under [`benchmark/`](benchmark/) compares DuckDB native
`glob()` discovery with Holt persistent and memory scans. It measures
metadata discovery only; it does not read Parquet payloads.
