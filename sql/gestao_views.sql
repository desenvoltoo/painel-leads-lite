-- View de referência do módulo Gestão Operacional.
-- Usa somente objetos oficiais do dataset modelo_estrela e não cria arquitetura paralela.
-- Não altera vw_leads_painel_lite.

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_leads_priorizados` AS
SELECT
  v.*,
  (
    (v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '')
    AND v.data_disparo IS NULL
    AND v.data_ultima_acao IS NULL
  ) AS nunca_trabalhado,
  CASE
    WHEN UPPER(TRIM(CAST(v.status AS STRING))) IN ('MAT', 'MATRICULADO')
      OR COALESCE(SAFE_CAST(v.flag_matriculado AS BOOL), FALSE)
      THEN 99
    WHEN (v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '')
      AND v.data_disparo IS NULL
      AND v.data_ultima_acao IS NULL
      THEN 1
    WHEN (v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '')
      AND (v.data_disparo IS NOT NULL OR v.data_ultima_acao IS NOT NULL)
      THEN 2
    WHEN UPPER(TRIM(CAST(v.status AS STRING))) = 'EC'
      THEN 3
    ELSE 4
  END AS grupo_prioridade,
  CASE
    WHEN UPPER(TRIM(CAST(v.status AS STRING))) IN ('MAT', 'MATRICULADO')
      OR COALESCE(SAFE_CAST(v.flag_matriculado AS BOOL), FALSE)
      THEN 'BAIXA'
    WHEN (v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '')
      AND v.data_disparo IS NULL
      AND v.data_ultima_acao IS NULL
      THEN 'CRÍTICA'
    WHEN (v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '')
      THEN 'ALTA'
    WHEN UPPER(TRIM(CAST(v.status AS STRING))) = 'EC'
      THEN 'MÉDIA'
    ELSE 'MÉDIA'
  END AS nivel_prioridade,
  CASE
    WHEN UPPER(TRIM(CAST(v.status AS STRING))) IN ('MAT', 'MATRICULADO')
      OR COALESCE(SAFE_CAST(v.flag_matriculado AS BOOL), FALSE)
      THEN 0
    WHEN (v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '')
      AND v.data_disparo IS NULL
      AND v.data_ultima_acao IS NULL
      THEN 100
    WHEN (v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '')
      THEN 85
    WHEN UPPER(TRIM(CAST(v.status AS STRING))) = 'EC'
      THEN 70
    ELSE 40
  END AS score_prioridade,
  CASE
    WHEN UPPER(TRIM(CAST(v.status AS STRING))) IN ('MAT', 'MATRICULADO')
      OR COALESCE(SAFE_CAST(v.flag_matriculado AS BOOL), FALSE)
      THEN 'MATRICULADO'
    WHEN (v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '')
      AND v.data_disparo IS NULL
      AND v.data_ultima_acao IS NULL
      THEN 'NUNCA_TRABALHADO'
    WHEN (v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '')
      THEN 'SEM_STATUS_TRABALHADO'
    WHEN UPPER(TRIM(CAST(v.status AS STRING))) = 'EC'
      THEN 'EC'
    ELSE 'ELEGIVEL'
  END AS etapa_operacional
FROM `painel-universidade.modelo_estrela.vw_leads_painel_lite` v;
