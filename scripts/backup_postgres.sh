#!/usr/bin/env bash
set -euo pipefail
: "${DATABASE_URL:?DATABASE_URL não configurada}"
BACKUP_DIR=${BACKUP_DIR:-backups}
mkdir -p "$BACKUP_DIR"
file="$BACKUP_DIR/painel_leads_$(date +%F_%H%M).sql"
pg_dump "$DATABASE_URL" > "$file"
gzip "$file"
find "$BACKUP_DIR" -type f -name 'painel_leads_*.sql.gz' -mtime +14 -print -delete
