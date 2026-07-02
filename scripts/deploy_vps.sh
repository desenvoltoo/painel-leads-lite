#!/usr/bin/env bash
set -euo pipefail
APP_DIR=${APP_DIR:-/var/www/painel-leads-lite}
DOMAIN=${DOMAIN:-SEU_DOMINIO.com.br}
cd "$APP_DIR"
git pull --ff-only
. .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
set -a; [ -f .env ] && . ./.env; set +a
./scripts/run_migrations.sh
./scripts/smoke_test.sh
sudo systemctl restart painel-leads-lite
sudo systemctl status painel-leads-lite --no-pager
curl -f "https://${DOMAIN}/api/health"
