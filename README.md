# Painel de Leads Lite

Aplicação Flask para consulta e gestão operacional de leads. O deploy oficial atual é em VPS Hostinger com PostgreSQL/Supabase, Gunicorn, Nginx e SSL Let's Encrypt. O deploy antigo em Google Cloud Run/BigQuery está obsoleto e não deve ser usado como destino final.

## Rotas principais
- `/`: painel de leads.
- `/gestao`: gestão operacional.
- APIs: `/api/health`, `/api/leads`, `/api/leads/options`, `/api/leads/export`, `/api/gestao/resumo`, `/api/gestao/funil`, `/api/gestao/evolucao`, `/api/gestao/rankings`, `/api/gestao/produtividade`, `/api/gestao/fila`, `/api/gestao/qualidade`, `/api/gestao/importacoes`, `/api/gestao/rejeicoes`.

## Banco de dados
- PostgreSQL local na VPS ou Supabase via `DATABASE_URL`.
- Schema padrão: `public`.
- Fonte configurável: `LEADS_VIEW=vw_leads_painel_lite`.
- A view precisa expor os campos de leads usados pelo painel.

## Rodando localmente
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edite DATABASE_URL
./scripts/run_migrations.sh
flask --app app:app run
```

## Variáveis
Veja `.env.example`. Obrigatórias em produção: `DATABASE_URL`, `FLASK_SECRET_KEY`, `DB_SCHEMA`, `LEADS_VIEW`, `APP_ENV`.

## Testes
```bash
python -m compileall .
pytest -q
```

## Estrutura
- `app.py`: rotas Flask e autenticação.
- `services/database.py`: SQLAlchemy, healthcheck, helpers SQL e queries de leads.
- `services/gestao.py`: consultas PostgreSQL dos módulos de gestão.
- `services/gestao_operacional.py`: compatibilidade operacional.
- `migrations/`: SQL idempotente.
- `deploy/nginx/` e `deploy/systemd/`: exemplos para VPS.
- `scripts/`: instalação, deploy, migrations e backup.

## Segurança
Não commite `.env` nem segredos. Dados pessoais em logs/exportações de qualidade e rejeição devem ser mascarados.

## Validação de dependências e testes

No ambiente do Codex usado nesta revisão, `python -m pip install -r requirements.txt` falhou por bloqueio de rede do índice de pacotes (`403 Forbidden`). Isso é uma limitação do ambiente de execução, não uma validação bem-sucedida da instalação. Em uma VPS Hostinger ou máquina local com internet liberada, valide assim:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
python -m compileall .
pytest -q
```

Para uma checagem rápida sem depender de `pytest`, após instalar as dependências rode:

```bash
./scripts/smoke_test.sh
```
