-- Migração idempotente para histórico oficial de importações do /gestao.
-- Não remove dados/tabelas e não cria fontes alternativas.
CREATE TABLE IF NOT EXISTS `painel-universidade.modelo_estrela.logs_importacoes` (
  upload_id STRING NOT NULL,
  id_importacao STRING,
  nome_arquivo STRING,
  tipo_arquivo STRING,
  tamanho_arquivo_bytes INT64,
  usuario STRING,
  status STRING NOT NULL,
  etapa STRING,
  mensagem STRING,
  total_linhas INT64,
  linhas_recebidas INT64,
  linhas_validas INT64,
  linhas_inseridas INT64,
  linhas_atualizadas INT64,
  linhas_ignoradas INT64,
  linhas_rejeitadas INT64,
  duplicados_arquivo INT64,
  duplicados_banco INT64,
  erros INT64,
  detalhes_json STRING,
  correlation_id STRING,
  criado_em TIMESTAMP,
  iniciado_em TIMESTAMP,
  atualizado_em TIMESTAMP,
  finalizado_em TIMESTAMP,
  duracao_ms INT64
)
PARTITION BY DATE(criado_em)
CLUSTER BY status, etapa, upload_id;
