CREATE TABLE IF NOT EXISTS leads_painel_lite (
  sk_pessoa text PRIMARY KEY,
  cpf text, celular text, nome text, email text, curso text, modalidade text,
  turno text, polo text, origem text, tipo_negocio text, consultor_comercial text,
  consultor_disparo text, campanha text, canal text, acao_comercial text,
  tipo_disparo text, peca_disparo text, texto_disparo text, qtd_acionamentos integer DEFAULT 0,
  status text, status_inscricao text, observacao text, flag_matriculado boolean DEFAULT false,
  data_inscricao timestamp, data_matricula timestamp, data_atualizacao timestamp,
  data_ultima_acao timestamp, data_disparo timestamp, dt_upload timestamp
);

CREATE TABLE IF NOT EXISTS logs_importacoes (
  upload_id text PRIMARY KEY, id_importacao text, nome_arquivo text, tipo_arquivo text,
  tamanho_arquivo_bytes bigint, usuario text, status text, etapa text, mensagem text,
  total_linhas integer, linhas_recebidas integer, linhas_validas integer,
  linhas_inseridas integer, linhas_atualizadas integer, linhas_ignoradas integer,
  linhas_rejeitadas integer, duplicados_arquivo integer, duplicados_banco integer,
  erros integer, criado_em timestamp DEFAULT now(), iniciado_em timestamp,
  atualizado_em timestamp DEFAULT now(), finalizado_em timestamp, duracao_ms bigint
);

CREATE TABLE IF NOT EXISTS logs_rejeicoes_import (
  id bigserial PRIMARY KEY, upload_id text, linha integer, motivo text, campo text,
  valor_mascarado text, criado_em timestamp DEFAULT now()
);

CREATE OR REPLACE VIEW vw_leads_painel_lite AS
SELECT sk_pessoa, cpf, celular, nome, email, curso, modalidade, turno, polo, origem,
       tipo_negocio, consultor_comercial, consultor_disparo, campanha, canal,
       acao_comercial, tipo_disparo, peca_disparo, texto_disparo, qtd_acionamentos,
       status, status_inscricao, observacao, flag_matriculado, data_inscricao,
       data_matricula, data_atualizacao, data_ultima_acao, data_disparo
FROM leads_painel_lite;

CREATE OR REPLACE VIEW vw_historico_importacoes AS SELECT * FROM logs_importacoes;
CREATE INDEX IF NOT EXISTS idx_leads_data_inscricao ON leads_painel_lite (data_inscricao);
CREATE INDEX IF NOT EXISTS idx_leads_data_atualizacao ON leads_painel_lite (data_atualizacao);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads_painel_lite (status);
CREATE INDEX IF NOT EXISTS idx_leads_cpf ON leads_painel_lite (cpf);
CREATE INDEX IF NOT EXISTS idx_leads_celular ON leads_painel_lite (celular);
