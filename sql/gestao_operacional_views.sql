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
WHERE COALESCE(l.flag_matriculado, FALSE) = FALSE
  AND COALESCE(l.nunca_disparado, FALSE) = FALSE;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_dashboard` AS
SELECT
  (SELECT COUNT(*) FROM `painel-universidade.modelo_estrela.vw_op_leads_disponiveis_novos`) AS leads_disponiveis,
  COUNT(*) AS total_lotes,
  COUNTIF(status_lote='ABERTO') AS lotes_abertos,
  COUNTIF(status_lote='EM_ANDAMENTO') AS lotes_em_andamento,
  COUNTIF(status_lote='CONCLUIDO') AS lotes_concluidos,
  COALESCE(SUM(quantidade_leads),0) AS leads_liberados,
  COALESCE(SUM(total_retorno),0) AS retornos,
  COALESCE(SUM(total_positivo),0) AS positivos,
  COALESCE(SUM(total_negativo),0) AS negativos,
  COALESCE(SUM(total_matriculas),0) AS matriculas,
  COALESCE(AVG(taxa_retorno),0) AS taxa_retorno,
  COALESCE(AVG(taxa_matricula),0) AS taxa_matricula
FROM `painel-universidade.modelo_estrela.op_lotes_disparo`;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_esteira` AS
SELECT status_atendimento, COUNT(*) AS total
FROM `painel-universidade.modelo_estrela.op_lote_leads`
GROUP BY status_atendimento;

CREATE OR REPLACE VIEW `painel-universidade.modelo_estrela.vw_op_fila_prioridade` AS
SELECT *
FROM `painel-universidade.modelo_estrela.vw_op_leads_disponiveis_novos`
ORDER BY data_inscricao DESC, score_prioridade DESC, COALESCE(nunca_disparado,FALSE) DESC, COALESCE(dias_sem_acao,0) DESC, sk_pessoa DESC;

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
SELECT 'PENDENTE' etapa, COUNTIF(status_atendimento='PENDENTE') total FROM `painel-universidade.modelo_estrela.op_lote_leads`
UNION ALL SELECT 'EM_ATENDIMENTO', COUNTIF(status_atendimento IN ('EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU')) FROM `painel-universidade.modelo_estrela.op_lote_leads`
UNION ALL SELECT 'FINALIZADO', COUNTIF(status_atendimento IN ('CONCLUIDO','MAT','CANCELADO')) FROM `painel-universidade.modelo_estrela.op_lote_leads`
UNION ALL SELECT 'MATRICULADO', COUNTIF(matriculado OR status_atendimento='MAT') FROM `painel-universidade.modelo_estrela.op_lote_leads`;
