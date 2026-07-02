#!/usr/bin/env bash
set -euo pipefail
: "${DATABASE_URL:?DATABASE_URL não configurada}"
cd "$(dirname "$0")/.."
for f in migrations/*.sql; do
  [ -e "$f" ] || continue
  echo "Aplicando $f"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"
done
