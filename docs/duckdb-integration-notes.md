# DuckDB Integration Notes (dbt-fusion)

This document summarizes the current state of DuckDB support in dbt-fusion, how to run it locally, and the recommended next steps to move from macro-level execution to full project runs.

## Status Overview

What’s implemented and working end-to-end:

- DuckDB typed adapter (`DuckdbAdapter`) with core execution surface:
  - `execute`/`add_query`, `get_relation`, `get_columns_in_relation`, `get_column_schema_from_query`, `quote`, and a basic Arrow→DuckDB type mapper.
  - Connection management via the ADBC-based `SqlEngine` (ADBC driver loaded dynamically).
- Dynamic ADBC driver loading for DuckDB (no CDN dependency):
  - `DUCKDB_DRIVER_PATH` (absolute path) or `DUCKDB_DRIVER_NAME` (library name) supported.
  - Fallback still tries `adbc_driver_duckdb` via OS loader and a sibling `lib/` directory.
- Auth: DuckDB defaults to in-memory (we intentionally don’t set a database-level URI to avoid driver incompatibility with `uri` as a database option).
- Macro sync + dispatch:
  - `scripts/sync_duckdb_macros.sh` copies macros from a local dbt-duckdb repo into embedded assets.
  - dbt-fusion includes `dbt-duckdb` macros and falls back to `dbt-postgres` for gaps.
- Tests (env-gated; require `DUCKDB_DRIVER_PATH` or `DUCKDB_DRIVER_NAME`):
  - `duckdb_smoke`: `select 1` via the adapter.
  - `duckdb_roundtrip`: create table + `get_relation` + `get_columns_in_relation` round-trip.
  - `duckdb_jinja`: minimal Jinja env + `adapter.execute(...)` call.
  - Macro-driven tests:
    - `duckdb_macro_datediff`: loads `duckdb__datediff` and calls it via `adapter.dispatch`.
    - `duckdb_macro_create_table`: renders `duckdb__create_table_as(...)`, executes the emitted SQL via the adapter, and validates results.

## How to Run Locally

1) Build & env

```
cargo build -p dbt-sa-cli -p dbt-fusion-adapter -p dbt-loader

# macOS (Homebrew)
export DUCKDB_DRIVER_PATH=/opt/homebrew/lib/libduckdb.dylib
# Linux (if libduckdb.so on loader path)
# export DUCKDB_DRIVER_NAME=duckdb
```

2) Sync macros (safe):

```
scripts/sync_duckdb_macros.sh /path/to/dbt-duckdb/dbt/include/duckdb/macros      # dry run (preview)
scripts/sync_duckdb_macros.sh /path/to/dbt-duckdb/dbt/include/duckdb/macros --force
cargo build -p dbt-loader   # embed synced assets
```

3) Run tests (env-gated):

```
cargo nextest run -p dbt-fusion-adapter --test duckdb_smoke
cargo nextest run -p dbt-fusion-adapter --test duckdb_roundtrip
cargo nextest run -p dbt-fusion-adapter --test duckdb_jinja
cargo nextest run -p dbt-fusion-adapter --test duckdb_macro_datediff
cargo nextest run -p dbt-fusion-adapter --test duckdb_macro_create_table
```

## Minimal Project Skeleton (dbt-sa-cli)

- `dbt_project.yml` (name/version; defaults are fine for a smoke run)
- `~/.dbt/profiles.yml`:

```yaml
duckdb_demo:
  target: dev
  outputs:
    dev:
      type: duckdb
      database: ":memory:"   # file-backed planned (see roadmap)
```

- `models/my_model.sql`:

```sql
select 42 as answer
```

- Commands:

```
target/debug/dbt-sa-cli deps  --project-dir . --target-path ./target
target/debug/dbt-sa-cli parse --project-dir . --target-path ./target
target/debug/dbt-sa-cli list  --project-dir . --target-path ./target
```

Note: A full “run” orchestration is WIP; today, you can execute SQL and macros via `adapter.execute`/`adapter.dispatch` through Jinja + BridgeAdapter (validated by the tests above).

## Design Notes / Decisions

- Relation rendering (three-part FQNs): DuckDB expects `catalog.schema.identifier` when the catalog exists. In in-memory sessions the catalog is `memory`, not `main`. We now detect the catalog from `information_schema.schemata` and render a proper three-part name (e.g., `memory.main.table`). This avoids binder errors like “Catalog `main` does not exist.” In multi-file (ATTACH) setups, the catalog is the attached alias (e.g., `mydb.main.table`).
- Identifier rules: treat DuckDB’s quoting/identifier rules similarly to Postgres (double-quoted, alphanumeric + `_` unquoted). Can be revisited if deviations are needed.
- Macro resolution: dispatch maps from `dbt` namespace to `dbt_duckdb` via a simple namespace map for tests. The full environment builder will populate more complete registries for projects.

## Roadmap / Next Steps

1) File-backed DuckDB auth

- Goal: open a DuckDB file DB (persistent state) instead of default in-memory.
- Approach:
  - Parse `profiles.yml` (e.g., `database`) for a file path.
  - Set the path via connection-level options when building the ADBC connection for DuckDB (rather than database-level URI which some builds reject).
  - Fallback: connect in-memory and run `ATTACH '/path/to/file.duckdb' AS <alias>` if required.
  - When a catalog is attached (in-memory alias or file alias), relation rendering will use three-part names with the detected/attached catalog.
- Deliverable: add a test that creates a table in a file-backed DB and verifies persistence across distinct connections.

2) Full materialization smoke test

- Register the minimal macro set needed for the `table` materialization from `dbt-duckdb/macros/materializations/table.sql` plus its dependencies.
- Render the materialization path using a small Jinja runner (or test) and execute emitted SQL via the adapter.
- Wire any missing adapter methods (e.g., rename/drop relation, apply_grants/persist_docs scaffolding) as needed.

3) Faster schema inference

- Use DuckDB prepared-statement metadata in `get_column_schema_from_query` instead of `LIMIT 0` for performance and fidelity. Keep `LIMIT 0` as a fallback.

4) Orchestration improvements (towards `run`)

- Extend `dbt-sa-cli` to a compile+run path that uses the materialization macros for each node and executes via the BridgeAdapter.
- Ensure `statement()` macro semantics and “transaction chunks” are aligned with dbt expectations.

5) Macro/namespace plumbing

- Expand the environment builder tests to register a larger set of dbt-duckdb macros and exercise dispatch with non-trivial dependencies.
- Confirm internal package order: `dbt_duckdb` → (parent chain if any) → `dbt`.

6) Polish & UX

- Improve adapter logging for rendered SQL (scrub secrets, make multi-statement logging readable).
- Expand error surface mapping from ADBC to `AdapterError` for clearer diagnostics.

## Open Questions

- Path option key for DuckDB ADBC:
  - Some builds don’t accept `OptionDatabase::Uri`; does the connection layer prefer `OptionConnection::Uri` or a different key (e.g., `path`)? We’ll detect and adapt.
- Transactions & `adapter.commit()` semantics:
  - Materializations assume well-defined begin/commit chunks; confirm behavior across Jinja statement blocks and ADBC statements.

## Quick Reference (env)

```
# macOS
export DUCKDB_DRIVER_PATH=/opt/homebrew/lib/libduckdb.dylib

# Linux (if loader can find lib)
export DUCKDB_DRIVER_NAME=duckdb
```

## Files Touched

- Adapter: `crates/dbt-fusion-adapter/src/duckdb/adapter.rs`
- Loader tweaks: `crates/dbt-loader/src/load_packages.rs`, macro assets under `crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/`
- Driver overrides: `crates/dbt-xdbc/src/driver.rs`
- Auth: `crates/dbt-auth/src/duckdb/mod.rs`
- Tests:
  - `duckdb_smoke.rs`, `duckdb_roundtrip.rs`, `duckdb_jinja.rs`
  - Macro-driven: `duckdb_macro_datediff.rs`, `duckdb_macro_create_table.rs`
- Script: `scripts/sync_duckdb_macros.sh`
