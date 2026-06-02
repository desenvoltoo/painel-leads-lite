-- Views de referência do módulo Gestão Operacional.
-- A prioridade máxima não usa data_disparo: lead nunca trabalhado é definido apenas por status IS NULL.
-- Não altera vw_leads_painel_lite.

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_leads_priorizados` AS
SELECT
  v.*,
  v.status IS NULL AS nunca_trabalhado,
  CASE
    WHEN UPPER(TRIM(CAST(v.status AS STRING))) = 'MAT'
      OR COALESCE(SAFE_CAST(v.flag_matriculado AS BOOL), FALSE)
      THEN 'BAIXA'
    WHEN v.status IS NULL THEN 'CRÍTICA'
    ELSE 'MÉDIA'
  END AS nivel_prioridade,
  CASE
    WHEN UPPER(TRIM(CAST(v.status AS STRING))) = 'MAT'
      OR COALESCE(SAFE_CAST(v.flag_matriculado AS BOOL), FALSE)
      THEN 10
    WHEN v.status IS NULL THEN 100
    ELSE 50
  END AS score_prioridade,
  CASE
    WHEN UPPER(TRIM(CAST(v.status AS STRING))) = 'MAT'
      OR COALESCE(SAFE_CAST(v.flag_matriculado AS BOOL), FALSE)
      THEN 'MATRICULADO'
    WHEN v.status IS NULL THEN 'NUNCA_TRABALHADO'
    ELSE 'EM_CARTEIRA'
  END AS etapa_operacional
FROM `painel-universidade.modelo_estrela.vw_leads_painel_lite` v;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_operacao_rpa_prioridade_corrigida` AS
SELECT
  COUNT(*) AS total_leads,
  COUNTIF(status IS NULL) AS nunca_trabalhados,
  COUNTIF(status IS NULL) AS leads_criticos,
  COUNTIF(status IS NOT NULL AND NOT (UPPER(TRIM(CAST(status AS STRING))) = 'MAT' OR COALESCE(SAFE_CAST(flag_matriculado AS BOOL), FALSE))) AS leads_em_carteira,
  COUNTIF(UPPER(TRIM(CAST(status AS STRING))) = 'MAT' OR COALESCE(SAFE_CAST(flag_matriculado AS BOOL), FALSE)) AS matriculados,
  SAFE_DIVIDE(COUNTIF(UPPER(TRIM(CAST(status AS STRING))) = 'MAT' OR COALESCE(SAFE_CAST(flag_matriculado AS BOOL), FALSE)), COUNT(*)) * 100 AS taxa_geral_conversao
FROM `painel-universidade.modelo_estrela.vw_leads_painel_lite`;
