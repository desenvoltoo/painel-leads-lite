#!/usr/bin/env bash
set -euo pipefail
APP_DIR=${APP_DIR:-/var/www/painel-leads-lite}
sudo apt update && sudo apt -y upgrade
sudo apt install -y python3 python3-venv python3-pip nginx postgresql-client certbot python3-certbot-nginx git curl
sudo mkdir -p "$APP_DIR"
sudo chown -R "${USER}:${USER}" "$APP_DIR"
cd "$APP_DIR"
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
[ -f .env ] || cp .env.example .env
sudo cp deploy/systemd/painel-leads-lite.service /etc/systemd/system/painel-leads-lite.service
sudo cp deploy/nginx/painel-leads-lite.conf /etc/nginx/sites-available/painel-leads-lite.conf
sudo ln -sf /etc/nginx/sites-available/painel-leads-lite.conf /etc/nginx/sites-enabled/painel-leads-lite.conf
sudo nginx -t
sudo systemctl daemon-reload
sudo systemctl enable painel-leads-lite
sudo systemctl restart nginx
printf '\nEdite %s/.env, ajuste domínio no Nginx, rode scripts/run_migrations.sh e emita SSL com certbot.\n' "$APP_DIR"
