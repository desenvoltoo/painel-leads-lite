#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${APP_DIR:-/etc/easypanel/projects/painel-leads/painel-leads/code}"
DB_CONTAINER="${DB_CONTAINER:-supabase_supabase-db-1}"
DB_USER="${DB_USER:-supabase_admin}"
DB_NAME="${DB_NAME:-postgres}"
MIGRATION="${APP_DIR}/migrations/023_telefone2_importacao_por_telefone.sql"
QUEUE_SCRIPT="${QUEUE_SCRIPT:-/root/processar-fila-leads.sh}"
QUEUE_LOG="${QUEUE_LOG:-/root/processar-fila-leads.nohup.log}"

cd "$APP_DIR"

if [[ ! -f "$MIGRATION" ]]; then
  echo "Migration nao encontrada: $MIGRATION" >&2
  exit 1
fi

echo "Parando o processador sequencial..."
pkill -f processar-fila-leads.sh || true
pkill -f 'psql.*sp_importar_somente_leads_novos' || true
pkill -f 'psql.*sp_processar_stg' || true
sleep 3

echo "Encerrando apenas sessoes de importacao ainda ativas..."
docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL'
SELECT
    pid,
    pg_terminate_backend(pid) AS encerrada,
    left(query, 180) AS consulta
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND (
       query ILIKE '%sp_importar_somente_leads_novos%'
    OR query ILIKE '%sp_processar_stg%'
  )
  AND query NOT ILIKE '%pg_stat_activity%';
SQL

rm -f /var/lock/processar-fila-leads.lock

BACKUP_DIR="/root/backups/telefone2-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BACKUP_DIR"

echo "Salvando definicoes atuais em $BACKUP_DIR..."
for item in \
  'modelo_estrela sp_importar_somente_leads_novos' \
  'modelo_estrela sp_processar_stg_leads_site' \
  'unifecaf sp_processar_upload'; do
  read -r schema rotina <<<"$item"
  docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -Atc "
SELECT pg_get_functiondef(p.oid)
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = '${schema}'
  AND p.proname = '${rotina}';
" > "${BACKUP_DIR}/${schema}_${rotina}.sql"
done

echo "Aplicando migration..."
docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 < "$MIGRATION"

echo "Validando colunas e funcoes..."
docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL'
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema IN ('modelo_estrela', 'unifecaf')
  AND table_name = 'dim_pessoa'
  AND column_name = 'telefone2'
ORDER BY table_schema;

SELECT
    n.nspname AS schema,
    p.proname AS rotina,
    position('telefone2' IN pg_get_functiondef(p.oid)) > 0 AS usa_telefone2
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE (n.nspname, p.proname) IN (
    ('modelo_estrela', 'sp_importar_somente_leads_novos'),
    ('modelo_estrela', 'sp_processar_stg_leads_site'),
    ('unifecaf', 'sp_processar_upload')
)
ORDER BY n.nspname, p.proname;
SQL

echo "Recolocando cargas interrompidas na fila..."
docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL'
UPDATE modelo_estrela.op_importacao_progresso
SET status = 'AGUARDANDO',
    etapa = 'FILA_PROCESSAMENTO',
    progresso = 20,
    mensagem = 'Carga recolocada apos migration telefone2.',
    erro = NULL,
    atualizado_em = now(),
    finalizado_em = NULL
WHERE status = 'PROCESSANDO';

UPDATE unifecaf.op_importacao_progresso
SET status = 'AGUARDANDO',
    etapa = 'FILA_PROCESSAMENTO',
    progresso = 20,
    mensagem = 'Carga recolocada apos migration telefone2.',
    erro = NULL,
    atualizado_em = now(),
    finalizado_em = NULL
WHERE status = 'PROCESSANDO';
SQL

echo "Iniciando um unico processador..."
nohup "$QUEUE_SCRIPT" >"$QUEUE_LOG" 2>&1 &
echo "PID: $!"
sleep 5
pgrep -af processar-fila-leads.sh

echo "Concluido. Acompanhe com: tail -f $QUEUE_LOG"
