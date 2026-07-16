-- Gestão completa do pipeline de importação
-- Adaptada ao schema real do PostgreSQL/Supabase.

BEGIN;

ALTER TABLE modelo_estrela.logs_importacoes
  ADD COLUMN IF NOT EXISTS correlation_id text;

CREATE INDEX IF NOT EXISTS idx_logs_importacoes_criado_em
  ON modelo_estrela.logs_importacoes (criado_em DESC);

CREATE INDEX IF NOT EXISTS idx_logs_rejeicoes_upload_id
  ON modelo_estrela.logs_rejeicoes_import (upload_id);

CREATE INDEX IF NOT EXISTS idx_logs_rejeicoes_criado_em
  ON modelo_estrela.logs_rejeicoes_import (criado_em DESC);

CREATE INDEX IF NOT EXISTS idx_stg_leads_site_upload_id
  ON modelo_estrela.stg_leads_site (upload_id);

CREATE OR REPLACE VIEW modelo_estrela.vw_historico_importacoes AS
SELECT
  i.*,
  COALESCE(r.total_rejeicoes_log, 0)::bigint AS total_rejeicoes_log,
  COALESCE(r.motivos_rejeicao, '') AS motivos_rejeicao,
  COALESCE(s.linhas_pendentes_staging, 0)::bigint AS linhas_pendentes_staging,
  CASE
    WHEN COALESCE(s.linhas_pendentes_staging, 0) > 0
      AND upper(COALESCE(i.status, '')) IN ('CONCLUIDO', 'CONCLUIDO_COM_REJEICOES')
      THEN 'INCONSISTENTE'
    WHEN upper(COALESCE(i.status, '')) = 'ERRO' THEN 'ERRO'
    WHEN COALESCE(s.linhas_pendentes_staging, 0) > 0 THEN 'PENDENTE_STAGING'
    WHEN upper(COALESCE(i.status, '')) IN ('CONCLUIDO', 'CONCLUIDO_COM_REJEICOES') THEN 'CONSOLIDADO'
    ELSE upper(COALESCE(i.status, 'PENDENTE'))
  END AS status_pipeline,
  CASE
    WHEN COALESCE(s.linhas_pendentes_staging, 0) > 0 THEN true
    ELSE false
  END AS requer_atencao,
  CASE
    WHEN COALESCE(s.linhas_pendentes_staging, 0) > 0
      THEN 'Linhas ainda estão na staging e não chegaram à base real.'
    WHEN upper(COALESCE(i.status, '')) = 'ERRO'
      THEN COALESCE(NULLIF(i.mensagem, ''), 'Falha na importação.')
    WHEN COALESCE(r.total_rejeicoes_log, 0) > 0
      THEN 'Importação concluída com rejeições.'
    ELSE 'Importação consolidada com sucesso.'
  END AS diagnostico_pipeline
FROM modelo_estrela.logs_importacoes i
LEFT JOIN (
  SELECT
    upload_id,
    COUNT(*) AS total_rejeicoes_log,
    string_agg(
      DISTINCT COALESCE(motivo, 'Sem motivo'),
      ' | '
      ORDER BY COALESCE(motivo, 'Sem motivo')
    ) AS motivos_rejeicao
  FROM modelo_estrela.logs_rejeicoes_import
  WHERE upload_id IS NOT NULL
  GROUP BY upload_id
) r ON r.upload_id = i.upload_id
LEFT JOIN (
  SELECT upload_id, COUNT(*) AS linhas_pendentes_staging
  FROM modelo_estrela.stg_leads_site
  WHERE upload_id IS NOT NULL
  GROUP BY upload_id
) s ON s.upload_id = i.upload_id;

CREATE OR REPLACE VIEW modelo_estrela.vw_gestao_importacoes_resumo AS
SELECT
  COUNT(*)::bigint AS total_importacoes,
  COUNT(*) FILTER (WHERE status_pipeline = 'CONSOLIDADO')::bigint AS consolidadas,
  COUNT(*) FILTER (WHERE status_pipeline = 'INCONSISTENTE')::bigint AS inconsistentes,
  COUNT(*) FILTER (WHERE status_pipeline = 'PENDENTE_STAGING')::bigint AS pendentes_staging,
  COUNT(*) FILTER (WHERE status_pipeline = 'ERRO')::bigint AS com_erro,
  COALESCE(SUM(linhas_recebidas), 0)::bigint AS linhas_recebidas,
  COALESCE(SUM(linhas_validas), 0)::bigint AS linhas_validas,
  COALESCE(SUM(linhas_rejeitadas), 0)::bigint AS linhas_rejeitadas,
  COALESCE(SUM(linhas_pendentes_staging), 0)::bigint AS linhas_pendentes_staging,
  MAX(criado_em) AS ultima_importacao
FROM modelo_estrela.vw_historico_importacoes;

CREATE OR REPLACE VIEW modelo_estrela.vw_gestao_rejeicoes_import AS
SELECT
  row_number() OVER (
    ORDER BY COALESCE(r.criado_em, r.ts) DESC NULLS LAST,
             r.upload_id,
             r.linha
  ) AS rejeicao_id,
  r.upload_id,
  i.nome_arquivo,
  i.usuario,
  r.linha,
  r.motivo,
  r.campo,
  r.valor_mascarado,
  COALESCE(r.criado_em, r.ts) AS data_rejeicao,
  r.criado_em,
  r.ts,
  i.correlation_id,
  CASE WHEN r.upload_id IS NULL THEN true ELSE false END AS rejeicao_legada_sem_vinculo,
  CASE
    WHEN r.upload_id IS NULL THEN 'Rejeição antiga sem upload_id; não é possível associar ao arquivo original.'
    WHEN i.upload_id IS NULL THEN 'upload_id não encontrado em logs_importacoes.'
    ELSE 'Rejeição vinculada à importação.'
  END AS diagnostico_vinculo
FROM modelo_estrela.logs_rejeicoes_import r
LEFT JOIN modelo_estrela.logs_importacoes i
  ON i.upload_id = r.upload_id;

COMMIT;
