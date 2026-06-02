-- Views de referência do módulo Gestão Operacional.
-- A prioridade máxima não usa data_disparo: lead nunca trabalhado é definido apenas por status vazio.

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_leads_priorizados` AS
SELECT
  v.*,
  (v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '') AS nunca_trabalhado,
  CASE
    WHEN UPPER(TRIM(COALESCE(CAST(v.matriculado AS STRING), ''))) IN ('SIM', 'S', 'TRUE', '1', 'MATRICULADO', 'MAT')
      OR REGEXP_CONTAINS(UPPER(TRIM(COALESCE(CAST(v.status_inscricao AS STRING), ''))), r'^(MAT|MATRICULADO)')
      OR REGEXP_CONTAINS(UPPER(TRIM(COALESCE(CAST(v.status AS STRING), ''))), r'^(MAT|MATRICULADO)')
      THEN 'BAIXA'
    WHEN v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '' THEN 'CRÍTICA'
    ELSE 'MÉDIA'
  END AS nivel_prioridade,
  CASE
    WHEN UPPER(TRIM(COALESCE(CAST(v.matriculado AS STRING), ''))) IN ('SIM', 'S', 'TRUE', '1', 'MATRICULADO', 'MAT')
      OR REGEXP_CONTAINS(UPPER(TRIM(COALESCE(CAST(v.status_inscricao AS STRING), ''))), r'^(MAT|MATRICULADO)')
      OR REGEXP_CONTAINS(UPPER(TRIM(COALESCE(CAST(v.status AS STRING), ''))), r'^(MAT|MATRICULADO)')
      THEN 10
    WHEN v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '' THEN 100
    ELSE 50
  END AS score_prioridade
FROM `painel-universidade.modelo_estrela.vw_leads_painel_lite` v;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_operacao_rpa_prioridade_corrigida` AS
SELECT
  COUNT(*) AS total_leads,
  COUNTIF(status IS NULL OR TRIM(CAST(status AS STRING)) = '') AS nunca_trabalhados,
  COUNTIF(status IS NULL OR TRIM(CAST(status AS STRING)) = '') AS leads_criticos
FROM `painel-universidade.modelo_estrela.vw_leads_painel_lite`;
