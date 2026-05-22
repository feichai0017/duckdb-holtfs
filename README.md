# duckdb-holtfs

`duckdb-holtfs` is a DuckDB extension for querying file/object
metadata indexes stored in [Holt](https://github.com/feichai0017/holt).
It is based on DuckDB's official
[`extension-template`](https://github.com/duckdb/extension-template).

The current extension exposes Holt-backed namespace listing:

```sql
LOAD holtfs;

SELECT *
FROM holt_files(
  '/var/cache/duckdb/table.holt',
  prefix := 's3://bucket/table/date=2026-05-22/',
  delimiter := '/',
  max_files := 1000
);
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

For a Holt index that already exists:

```sql
SELECT entry_type, path, version
FROM holt_files('/var/cache/duckdb/table.holt',
                prefix := 's3://bucket/table/',
                delimiter := '/');
```

Set `include_value := true` when the metadata payload is needed:

```sql
SELECT path, value, version
FROM holt_files('/var/cache/duckdb/table.holt',
                prefix := 's3://bucket/table/',
                include_value := true);
```

## Scope

This repository is intentionally narrow:

- `holtfs_version()` is a smoke-test scalar function.
- `holt_files(...)` reads an existing Holt metadata index through
  Holt's C ABI.
- Index construction and direct Parquet delegation are the next steps;
  they are not exposed as placeholder SQL functions yet.
