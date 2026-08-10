# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(holtfs
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    EXTENSION_VERSION v0.2.0
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
