-- Gestão completa do pipeline de importação
-- Execute no PostgreSQL/Supabase após o deploy da aplicação.

BEGIN;

ALTER TABLE modelo_estrela.logs_importacoes
  ADD COLUMN IF NOT EXISTS correlation_id text,
  ADD COLUMN IF NOT EXISTS detalhes_json jsonb DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_logs_importacoes_criado_em
  ON modelo_estrela.logs_importacoes (criado_em DESC);

CREATE INDEX IF NOT EXISTS idx_logs_rejeicoes_upload_id
  ON modelo_estrela.logs_rejeicoes_import (upload_id);

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
  END AS status_pipeline
FROM modelo_estrela.logs_importacoes i
LEFT JOIN (
  SELECT
    upload_id,
    COUNT(*) AS total_rejeicoes_log,
    string_agg(DISTINCT COALESCE(motivo, 'Sem motivo'), ' | ' ORDER BY COALESCE(motivo, 'Sem motivo')) AS motivos_rejeicao
  FROM modelo_estrela.logs_rejeicoes_import
  GROUP BY upload_id
) r ON r.upload_id = i.upload_id
LEFT JOIN (
  SELECT upload_id, COUNT(*) AS linhas_pendentes_staging
  FROM modelo_estrela.stg_leads_site
  GROUP BY upload_id
) s ON s.upload_id = i.upload_id;

CREATE OR REPLACE VIEW modelo_estrela.vw_gestao_rejeicoes_import AS
SELECT
  r.id,
  r.upload_id,
  i.nome_arquivo,
  i.usuario,
  r.linha,
  r.motivo,
  r.campo,
  r.valor_mascarado,
  r.criado_em,
  i.correlation_id
FROM modelo_estrela.logs_rejeicoes_import r
LEFT JOIN modelo_estrela.logs_importacoes i ON i.upload_id = r.upload_id;

COMMIT;
