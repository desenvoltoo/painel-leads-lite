CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_leads_disponiveis_novos` AS
SELECT l.*
FROM `painel-universidade.modelo_estrela.vw_leads_priorizados` l
LEFT JOIN `painel-universidade.modelo_estrela.op_lote_leads` op
  ON l.sk_pessoa = op.sk_pessoa
 AND op.status_atendimento IN ('PENDENTE','EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU')
WHERE op.sk_pessoa IS NULL
  AND COALESCE(l.flag_matriculado, FALSE) = FALSE
  AND COALESCE(l.nunca_disparado, FALSE) = TRUE
  AND (l.consultor_disparo IS NULL OR TRIM(l.consultor_disparo) = '')
  AND (l.tipo_disparo IS NULL OR TRIM(l.tipo_disparo) = '')
  AND l.data_disparo IS NULL;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_leads_redisparo` AS
SELECT l.*
FROM `painel-universidade.modelo_estrela.vw_leads_priorizados` l
LEFT JOIN `painel-universidade.modelo_estrela.op_lote_leads` op
  ON l.sk_pessoa = op.sk_pessoa
 AND op.status_atendimento IN ('PENDENTE','EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU','CONCLUIDO','MAT','CANCELADO')
WHERE op.sk_pessoa IS NULL
  AND COALESCE(l.flag_matriculado, FALSE) = FALSE
  AND (COALESCE(l.nunca_disparado, FALSE) = FALSE OR TRIM(COALESCE(l.consultor_disparo, '')) != '' OR l.data_disparo IS NOT NULL);

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_esteira` AS
SELECT status_atendimento, COUNT(*) AS total
FROM `painel-universidade.modelo_estrela.op_lote_leads`
GROUP BY status_atendimento;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_dashboard` AS
SELECT
  COUNT(*) AS total_lotes,
  COUNTIF(status_lote='ABERTO') AS lotes_abertos,
  COUNTIF(status_lote='EM_ANDAMENTO') AS lotes_em_andamento,
  COUNTIF(status_lote='CONCLUIDO') AS lotes_concluidos,
  SUM(quantidade_leads) AS leads_liberados,
  SUM(total_retorno) AS retornos,
  SUM(total_positivo) AS positivos,
  SUM(total_negativo) AS negativos,
  SUM(total_matriculas) AS matriculas,
  AVG(taxa_retorno) AS taxa_retorno,
  AVG(taxa_matricula) AS taxa_matricula
FROM `painel-universidade.modelo_estrela.op_lotes_disparo`;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_fila_prioridade` AS
SELECT *
FROM `painel-universidade.modelo_estrela.vw_op_leads_disponiveis_novos`
ORDER BY data_inscricao DESC, score_prioridade DESC, dias_sem_acao DESC, sk_pessoa DESC;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_lotes_resumo` AS
SELECT * FROM `painel-universidade.modelo_estrela.op_lotes_disparo`;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_performance_consultor` AS
SELECT consultor_disparo, COUNT(*) AS total_leads, COUNTIF(retorno) AS retornos, COUNTIF(positivo) AS positivos, COUNTIF(negativo) AS negativos, COUNTIF(matriculado) AS matriculas,
  SAFE_DIVIDE(COUNTIF(retorno), COUNT(*))*100 AS taxa_retorno,
  SAFE_DIVIDE(COUNTIF(matriculado), COUNT(*))*100 AS taxa_matricula
FROM `painel-universidade.modelo_estrela.op_lote_leads`
GROUP BY consultor_disparo;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_performance_campanha` AS
SELECT campanha, COUNT(*) AS total_leads, COUNTIF(retorno) AS retornos, COUNTIF(positivo) AS positivos, COUNTIF(negativo) AS negativos, COUNTIF(matriculado) AS matriculas,
  SAFE_DIVIDE(COUNTIF(retorno), COUNT(*))*100 AS taxa_retorno,
  SAFE_DIVIDE(COUNTIF(matriculado), COUNT(*))*100 AS taxa_matricula
FROM `painel-universidade.modelo_estrela.op_lote_leads`
GROUP BY campanha;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_funil_disparos` AS
SELECT status_atendimento AS etapa, COUNT(*) AS total
FROM `painel-universidade.modelo_estrela.op_lote_leads`
GROUP BY status_atendimento;
