CREATE TABLE IF NOT EXISTS `painel-universidade.modelo_estrela.op_lotes_disparo` (
  lote_id STRING NOT NULL,
  nome_lote STRING,
  campanha STRING,
  tipo_disparo STRING,
  consultor_disparo STRING,
  quantidade_leads INT64,
  status_lote STRING,
  total_retorno INT64,
  total_positivo INT64,
  total_negativo INT64,
  total_matriculas INT64,
  taxa_retorno FLOAT64,
  taxa_matricula FLOAT64,
  criado_por STRING,
  created_at TIMESTAMP,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `painel-universidade.modelo_estrela.op_lote_leads` (
  lote_id STRING NOT NULL,
  sk_pessoa INT64,
  cpf STRING,
  nome STRING,
  celular STRING,
  email STRING,
  curso STRING,
  modalidade STRING,
  turno STRING,
  polo STRING,
  origem STRING,
  tipo_negocio STRING,
  campanha STRING,
  canal STRING,
  acao_comercial STRING,
  tipo_disparo STRING,
  consultor_disparo STRING,
  status_atendimento STRING,
  retorno BOOL,
  positivo BOOL,
  negativo BOOL,
  matriculado BOOL,
  observacao STRING,
  data_inscricao DATE,
  data_matricula DATE,
  data_disparo TIMESTAMP,
  score_prioridade INT64,
  nivel_prioridade STRING,
  etapa_operacional STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `painel-universidade.modelo_estrela.op_lead_eventos` (
  evento_id STRING NOT NULL,
  lote_id STRING,
  sk_pessoa INT64,
  cpf STRING,
  tipo_evento STRING,
  status_anterior STRING,
  status_novo STRING,
  descricao STRING,
  usuario STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `painel-universidade.modelo_estrela.op_bigquery_sync` (
  sync_id STRING NOT NULL,
  lote_id STRING,
  status_sync STRING,
  tentativas INT64,
  linhas_processadas INT64,
  erro STRING,
  created_at TIMESTAMP,
  synced_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `painel-universidade.modelo_estrela.op_regras_distribuicao` (
  regra_id STRING NOT NULL,
  nome_regra STRING,
  tipo_disparo STRING,
  consultor_disparo STRING,
  campanha STRING,
  curso STRING,
  polo STRING,
  origem STRING,
  nivel_prioridade STRING,
  quantidade_por_lote INT64,
  limite_lotes_ativos INT64,
  ativo BOOL,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `painel-universidade.modelo_estrela.op_config_operacional` (
  chave STRING NOT NULL,
  valor STRING,
  descricao STRING,
  updated_at TIMESTAMP
);
