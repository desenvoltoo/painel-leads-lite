-- Migração idempotente para monitoramento seguro de importações.
-- Não execute DROP/DELETE/TRUNCATE em produção.

CREATE TABLE IF NOT EXISTS `painel-universidade.modelo_estrela.logs_importacoes` (
  importacao_id STRING,
  nome_arquivo STRING,
  dt_upload TIMESTAMP,
  usuario STRING,
  total_recebido INT64,
  total_valido INT64,
  total_rejeitado INT64,
  total_inserido INT64,
  total_atualizado INT64,
  total_ignorado_antigo INT64,
  total_sem_celular INT64,
  status STRING,
  duracao_segundos FLOAT64,
  mensagem_erro_resumida STRING,
  job_id_bigquery STRING,
  criado_em TIMESTAMP
)
PARTITION BY DATE(dt_upload)
CLUSTER BY status, usuario;

ALTER TABLE `painel-universidade.modelo_estrela.logs_importacoes`
ADD COLUMN IF NOT EXISTS importacao_id STRING;
ALTER TABLE `painel-universidade.modelo_estrela.logs_importacoes`
ADD COLUMN IF NOT EXISTS total_ignorado_antigo INT64;
ALTER TABLE `painel-universidade.modelo_estrela.logs_importacoes`
ADD COLUMN IF NOT EXISTS job_id_bigquery STRING;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_logs_rejeicoes_import_mascarados` AS
SELECT
  dt_rejeicao,
  nome_arquivo,
  linha,
  motivo,
  IFNULL(CONCAT('***.***.***-', RIGHT(REGEXP_REPLACE(CAST(cpf AS STRING), r'\D', ''), 2)), '***') AS cpf_mascarado,
  IFNULL(CONCAT('***', RIGHT(REGEXP_REPLACE(CAST(celular AS STRING), r'\D', ''), 4)), '') AS celular_mascarado,
  CASE
    WHEN email IS NULL OR STRPOS(CAST(email AS STRING), '@') = 0 THEN ''
    ELSE CONCAT(SUBSTR(CAST(email AS STRING), 1, 1), '***', SUBSTR(CAST(email AS STRING), STRPOS(CAST(email AS STRING), '@')))
  END AS email_mascarado
FROM `painel-universidade.modelo_estrela.logs_rejeicoes_import`;
