#!/usr/bin/env bash
set -euo pipefail

# Sync dbt-duckdb macros into dbt-fusion's embedded assets.
#
# Usage:
#   scripts/sync_duckdb_macros.sh /absolute/path/to/dbt-duckdb/dbt/include/duckdb/macros [--force]
#
# Notes:
# - By default, does a dry run (preview). Pass --force to actually copy.
# - Never uses --delete unless --force is given and the source path is verified.

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 /abs/path/to/dbt-duckdb/dbt/include/duckdb/macros [--force]" >&2
  exit 2
fi

SRC="$1"
FORCE="${2:-}" # optional

if [[ ! -d "$SRC" ]]; then
  echo "Source directory not found: $SRC" >&2
  exit 2
fi

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
DST="$REPO_ROOT/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros"

echo "Source:      $SRC"
echo "Destination: $DST"

mkdir -p "$DST"

if [[ "$FORCE" != "--force" ]]; then
  echo ""
  echo "Dry run (no changes). Preview below:"
  rsync -av --dry-run "$SRC"/ "$DST"/
  echo ""
  echo "To apply, rerun with --force"
  exit 0
fi

echo "Copying macros..."
rsync -av "$SRC"/ "$DST"/
echo "Done. Rebuild to embed the copied macros:"
echo "  cargo build -p dbt-loader"

