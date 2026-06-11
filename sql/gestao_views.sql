-- View de referência do módulo Gestão Operacional.
-- Usa somente objetos oficiais do dataset modelo_estrela e não cria arquitetura paralela.
-- Não altera vw_leads_painel_lite.

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_leads_priorizados` AS
WITH base AS (
  SELECT
    v.*,
    NULLIF(REGEXP_REPLACE(COALESCE(CAST(v.celular AS STRING), ''), r'[^0-9]', ''), '') AS celular_limpo,
    UPPER(TRIM(COALESCE(CAST(v.status AS STRING), ''))) AS status_normalizado,
    UPPER(TRIM(COALESCE(CAST(v.status_inscricao AS STRING), ''))) AS status_inscricao_normalizado,
    UPPER(TRIM(COALESCE(CAST(v.tipo_negocio AS STRING), ''))) AS tipo_negocio_normalizado,
    COALESCE(
      SAFE_CAST(v.data_inscricao AS TIMESTAMP),
      TIMESTAMP(SAFE_CAST(v.data_inscricao AS DATE)),
      TIMESTAMP(SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(TRIM(CAST(v.data_inscricao AS STRING)), 1, 10))),
      TIMESTAMP(SAFE.PARSE_DATE('%d/%m/%Y', SUBSTR(TRIM(CAST(v.data_inscricao AS STRING)), 1, 10))),
      SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', SUBSTR(TRIM(CAST(v.data_inscricao AS STRING)), 1, 19)),
      SAFE.PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S', SUBSTR(TRIM(CAST(v.data_inscricao AS STRING)), 1, 19)),
      IF(
        SAFE_CAST(TRIM(CAST(v.data_inscricao AS STRING)) AS INT64) IS NULL,
        NULL,
        TIMESTAMP(DATE_ADD(DATE '1899-12-30', INTERVAL SAFE_CAST(TRIM(CAST(v.data_inscricao AS STRING)) AS INT64) DAY))
      )
    ) AS data_inscricao_normalizada
  FROM `painel-universidade.modelo_estrela.vw_leads_painel_lite` v
), regras AS (
  SELECT
    base.*,
    (
      status_normalizado IN ('MAT', 'MATRICULADO')
      OR status_inscricao_normalizado IN ('MAT', 'MATRICULADO')
      OR COALESCE(SAFE_CAST(flag_matriculado AS BOOL), FALSE)
      OR UPPER(TRIM(COALESCE(CAST(matriculado AS STRING), ''))) IN ('SIM', 'S', 'TRUE', '1', 'MATRICULADO', 'MAT')
    ) AS regra_matriculado,
    (
      NULLIF(TRIM(CAST(status AS STRING)), '') IS NULL
      OR status_normalizado IN ('NULL', 'N/A', 'NA', 'SEM STATUS', 'SEM INFORMACAO', 'SEM INFORMAÇÃO', '-')
    ) AS regra_sem_status,
    (
      status_normalizado = 'EC'
      OR status_inscricao_normalizado = 'EC'
      OR tipo_negocio_normalizado = 'EC'
    ) AS regra_ec,
    (
      status_normalizado IN ('CANCELADO', 'CANCELADA', 'CANC', 'DESCARTADO', 'DESCARTADA', 'ENCERRADO', 'ENCERRADA', 'PERDIDO', 'PERDIDA')
      OR status_inscricao_normalizado IN ('CANCELADO', 'CANCELADA', 'CANC', 'DESCARTADO', 'DESCARTADA', 'ENCERRADO', 'ENCERRADA', 'PERDIDO', 'PERDIDA')
      OR tipo_negocio_normalizado IN ('CANCELADO', 'CANCELADA', 'CANC', 'DESCARTADO', 'DESCARTADA', 'ENCERRADO', 'ENCERRADA', 'PERDIDO', 'PERDIDA')
    ) AS regra_excluido
  FROM base
)
SELECT
  regras.*,
  CASE
    WHEN regra_matriculado THEN 99
    WHEN regra_sem_status THEN 1
    WHEN regra_ec THEN 2
    WHEN NOT regra_excluido THEN 3
    ELSE 99
  END AS grupo_prioridade,
  CASE
    WHEN regra_matriculado THEN 'FORA DA FILA'
    WHEN regra_sem_status THEN 'ALTA'
    WHEN regra_ec THEN 'MÉDIA'
    WHEN NOT regra_excluido THEN 'NORMAL'
    ELSE 'FORA DA FILA'
  END AS nivel_prioridade,
  CASE
    WHEN regra_matriculado THEN 0
    WHEN regra_sem_status THEN 100
    WHEN regra_ec THEN 70
    WHEN NOT regra_excluido THEN 40
    ELSE 0
  END AS score_prioridade,
  CASE
    WHEN regra_matriculado THEN 'MATRICULADO'
    WHEN regra_sem_status THEN 'SEM_STATUS_RECENTE'
    WHEN regra_ec THEN 'EC'
    WHEN NOT regra_excluido THEN 'ELEGIVEL'
    ELSE 'NAO_ELEGIVEL'
  END AS etapa_operacional
FROM regras;
