-- Migração idempotente para histórico de importações do módulo /gestao.
-- Não execute DROP, TRUNCATE ou DELETE em produção.

CREATE TABLE IF NOT EXISTS
  `painel-universidade.modelo_estrela.logs_importacoes`
(
  id_importacao STRING,
  nome_arquivo STRING,
  usuario STRING,
  dt_upload TIMESTAMP,
  dt_inicio TIMESTAMP,
  dt_fim TIMESTAMP,
  total_recebido INT64,
  total_valido INT64,
  total_rejeitado INT64,
  total_inserido INT64,
  total_atualizado INT64,
  total_ignorado_antigo INT64,
  total_sem_celular INT64,
  status STRING,
  etapa STRING,
  mensagem_erro STRING,
  job_id STRING,
  duracao_segundos FLOAT64
)
PARTITION BY DATE(dt_upload)
CLUSTER BY status, usuario;

ALTER TABLE `painel-universidade.modelo_estrela.logs_importacoes`
ADD COLUMN IF NOT EXISTS id_importacao STRING;
ALTER TABLE `painel-universidade.modelo_estrela.logs_importacoes`
ADD COLUMN IF NOT EXISTS dt_inicio TIMESTAMP;
ALTER TABLE `painel-universidade.modelo_estrela.logs_importacoes`
ADD COLUMN IF NOT EXISTS dt_fim TIMESTAMP;
ALTER TABLE `painel-universidade.modelo_estrela.logs_importacoes`
ADD COLUMN IF NOT EXISTS etapa STRING;
ALTER TABLE `painel-universidade.modelo_estrela.logs_importacoes`
ADD COLUMN IF NOT EXISTS mensagem_erro STRING;
ALTER TABLE `painel-universidade.modelo_estrela.logs_importacoes`
ADD COLUMN IF NOT EXISTS job_id STRING;
ALTER TABLE `painel-universidade.modelo_estrela.logs_importacoes`
ADD COLUMN IF NOT EXISTS total_ignorado_antigo INT64;

ALTER TABLE `painel-universidade.modelo_estrela.logs_rejeicoes_import`
ADD COLUMN IF NOT EXISTS id_importacao STRING;
