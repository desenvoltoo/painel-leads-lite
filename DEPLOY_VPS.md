# Deploy na Hostinger VPS

## 1. Acessar VPS
```bash
ssh usuario@IP_DA_VPS
```
Crie usuário não-root, use chave SSH e desabilite senha para root quando possível.

## 2. Apontar domínio
No painel DNS da Hostinger, crie/ajuste registros `A` para `SEU_DOMINIO.com.br` e `www` apontando para o IP público da VPS. Aguarde propagação.

## 3. Instalar dependências e clonar
```bash
sudo apt update
sudo apt install -y git
sudo mkdir -p /var/www
cd /var/www
git clone URL_DO_REPOSITORIO painel-leads-lite
cd painel-leads-lite
./scripts/setup_vps.sh
```

## 4. Configurar `.env`
```bash
sudo nano /var/www/painel-leads-lite/.env
sudo chmod 640 /var/www/painel-leads-lite/.env
```
Configure `DATABASE_URL`, `FLASK_SECRET_KEY`, `DB_SCHEMA=public`, `LEADS_VIEW=vw_leads_painel_lite`.

## 5. Rodar migrations
```bash
set -a; . /var/www/painel-leads-lite/.env; set +a
/var/www/painel-leads-lite/scripts/run_migrations.sh
```

## 6. systemd
O modelo está em `deploy/systemd/painel-leads-lite.service`. Se a VPS usar outro usuário, altere `User=` e `Group=`.
```bash
sudo systemctl daemon-reload
sudo systemctl enable painel-leads-lite
sudo systemctl restart painel-leads-lite
sudo journalctl -u painel-leads-lite -f
```

## 7. Nginx
Edite `deploy/nginx/painel-leads-lite.conf` trocando `SEU_DOMINIO.com.br`, instale em `/etc/nginx/sites-available` e valide:
```bash
sudo nginx -t
sudo systemctl reload nginx
```

## 8. SSL Certbot
```bash
sudo certbot --nginx -d SEU_DOMINIO.com.br -d www.SEU_DOMINIO.com.br
sudo certbot renew --dry-run
```

## 9. Segurança VPS
```bash
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw enable
```
Mantenha pacotes atualizados, proteja `.env`, configure backups e rotação de logs.

## 10. Atualização
```bash
cd /var/www/painel-leads-lite
DOMAIN=SEU_DOMINIO.com.br ./scripts/deploy_vps.sh
```

## 11. Validação
```bash
curl -f https://SEU_DOMINIO.com.br/api/health
curl -f https://SEU_DOMINIO.com.br/api/leads
curl -f https://SEU_DOMINIO.com.br/api/gestao/resumo
```

## 12. Validação completa de dependências e testes

No ambiente do Codex, a instalação via `pip` falhou com `403 Forbidden` por bloqueio de rede. Na VPS Hostinger, depois de configurar internet/DNS, valide explicitamente:

```bash
cd /var/www/painel-leads-lite
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
python -m compileall .
pytest -q
./scripts/smoke_test.sh
```

Não considere o deploy validado até `pytest -q` e os endpoints principais responderem no ambiente da VPS.
