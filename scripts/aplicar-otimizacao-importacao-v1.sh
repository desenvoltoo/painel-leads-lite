#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${APP_DIR:-/etc/easypanel/projects/painel-leads/painel-leads/code}"
DB_CONTAINER="${DB_CONTAINER:-supabase_supabase-db-1}"
DB_USER="${DB_USER:-supabase_admin}"
DB_NAME="${DB_NAME:-postgres}"
QUEUE_SCRIPT="${QUEUE_SCRIPT:-/root/processar-fila-leads.sh}"
QUEUE_LOG="${QUEUE_LOG:-/root/processar-fila-leads.nohup.log}"
MIGRATION="${APP_DIR}/migrations/027_otimizar_importacao_modelo_estrela.sql"
BACKUP_DIR="/root/backups/importacao-perf-$(date +%Y%m%d-%H%M%S)"

cd "$APP_DIR"

if [[ ! -f "$MIGRATION" ]]; then
  echo "Migration nao encontrada: $MIGRATION" >&2
  exit 1
fi

mkdir -p "$BACKUP_DIR"

echo "[1/10] Parando a fila..."
pkill -f processar-fila-leads.sh || true
sleep 2
rm -f /var/lock/processar-fila-leads.lock

echo "[2/10] Encerrando apenas sessoes de importacao..."
docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL'
SELECT
    pid,
    pg_terminate_backend(pid) AS encerrada,
    now() - query_start AS tempo,
    left(query, 220) AS consulta
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND (
       query ILIKE '%sp_importar_somente_leads_novos%'
    OR query ILIKE '%sp_processar_stg_leads_site%'
  )
  AND query NOT ILIKE '%pg_stat_activity%';
SQL
sleep 3

echo "[3/10] Salvando funcoes atuais em $BACKUP_DIR..."
for rotina in sp_processar_stg_leads_site sp_importar_somente_leads_novos; do
  docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -Atc "
SELECT pg_get_functiondef(
  ('modelo_estrela.${rotina}(text)')::regprocedure
);
" > "${BACKUP_DIR}/${rotina}.sql"
done

echo "[4/10] Validando a chave unica operacional..."
docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL'
DO $validate$
BEGIN
  IF to_regclass('modelo_estrela.ux_leads_painel_sk_pessoa_dim') IS NULL THEN
    RAISE EXCEPTION 'Indice ux_leads_painel_sk_pessoa_dim ausente';
  END IF;
END;
$validate$;
SQL

echo "[5/10] Aplicando a migration de funcoes..."
docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 < "$MIGRATION"

echo "[6/10] Removendo somente indices comprovadamente duplicados..."
for indice in \
  idx_me_dim_pessoa_celular_digits \
  idx_modelo_dim_pessoa_celular_normalizado \
  ix_dim_pessoa_celular_normalizado \
  idx_me_dim_pessoa_cpf_digits \
  idx_modelo_dim_pessoa_cpf_normalizado \
  ix_dim_pessoa_cpf_normalizado; do
  docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
    -c "DROP INDEX CONCURRENTLY IF EXISTS modelo_estrela.${indice};"
done

echo "[7/10] Ajustando parametros do PostgreSQL..."
docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL'
ALTER SYSTEM SET shared_buffers = '2GB';
ALTER SYSTEM SET effective_cache_size = '16GB';
ALTER SYSTEM SET random_page_cost = '1.25';
ALTER SYSTEM SET effective_io_concurrency = '200';
ALTER SYSTEM SET jit = 'off';
ALTER SYSTEM SET wal_compression = 'on';
ALTER SYSTEM SET checkpoint_completion_target = '0.9';
SELECT pg_reload_conf();
SQL

echo "[8/10] Reiniciando somente o banco para aplicar shared_buffers..."
docker restart "$DB_CONTAINER" >/dev/null

for tentativa in $(seq 1 60); do
  if docker exec "$DB_CONTAINER" pg_isready -U "$DB_USER" -d "$DB_NAME" >/dev/null 2>&1; then
    break
  fi
  if [[ "$tentativa" -eq 60 ]]; then
    echo "Banco nao ficou pronto dentro do tempo esperado" >&2
    exit 1
  fi
  sleep 2
done

echo "[9/10] Executando VACUUM ANALYZE..."
docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL'
VACUUM (ANALYZE) modelo_estrela.stg_leads_site;
VACUUM (ANALYZE) modelo_estrela.dim_pessoa;
VACUUM (ANALYZE) modelo_estrela.f_lead;
VACUUM (ANALYZE) modelo_estrela.leads_painel_lite;
VACUUM (ANALYZE) modelo_estrela.logs_importacoes;
VACUUM (ANALYZE) modelo_estrela.logs_rejeicoes_import;
SQL

echo "[10/10] Recuperando cargas interrompidas e iniciando uma fila..."
docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL'
UPDATE modelo_estrela.op_importacao_progresso p
SET
    status = 'AGUARDANDO',
    etapa = 'FILA_PROCESSAMENTO',
    progresso = 20,
    mensagem = 'Carga recuperada apos otimizacao do processamento.',
    erro = NULL,
    atualizado_em = now(),
    finalizado_em = NULL
WHERE p.status = 'PROCESSANDO'
  AND EXISTS (
      SELECT 1
      FROM modelo_estrela.stg_leads_site s
      WHERE s.upload_id = p.upload_id
  );
SQL

rm -f /var/lock/processar-fila-leads.lock
nohup "$QUEUE_SCRIPT" >"$QUEUE_LOG" 2>&1 &
echo "Processador iniciado no PID $!"
sleep 5

echo "================ VALIDACAO ================"
docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 <<'SQL'
SELECT name, setting, unit, pending_restart
FROM pg_settings
WHERE name IN (
  'shared_buffers', 'effective_cache_size', 'random_page_cost',
  'effective_io_concurrency', 'jit', 'wal_compression',
  'checkpoint_completion_target'
)
ORDER BY name;

SELECT
    position(
        'Blindagem estrutural da tabela operacional'
        IN pg_get_functiondef(
            'modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure
        )
    ) = 0 AS deduplicacao_global_removida,
    position(
        'WITH telefones_upload AS'
        IN pg_get_functiondef(
            'modelo_estrela.sp_importar_somente_leads_novos(text)'::regprocedure
        )
    ) > 0 AS triagem_restrita_ao_upload;

SELECT schemaname, tablename, indexname
FROM pg_indexes
WHERE schemaname = 'modelo_estrela'
  AND tablename = 'dim_pessoa'
  AND indexname IN (
    'idx_me_dim_pessoa_celular_digits',
    'idx_modelo_dim_pessoa_celular_normalizado',
    'ix_dim_pessoa_celular_normalizado',
    'idx_me_dim_pessoa_cpf_digits',
    'idx_modelo_dim_pessoa_cpf_normalizado',
    'ix_dim_pessoa_cpf_normalizado'
  );
SQL

pgrep -af processar-fila-leads.sh || true
echo "Concluido. Acompanhe com: tail -f $QUEUE_LOG"
