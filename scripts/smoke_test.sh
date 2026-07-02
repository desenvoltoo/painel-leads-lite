#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python -m compileall .
python - <<'PY'
from app import app
print("Flask app import OK:", app.name)
PY
python - <<'PY'
from services.database import healthcheck
print("Database module import OK")
PY
