BEGIN;

CREATE OR REPLACE VIEW unifecaf.vw_leads_painel_lite AS
SELECT
    p.sk_pessoa,
    p.cpf,
    p.celular,
    p.nome,
    p.email,
    c.curso,
    c.modalidade,
    NULL::text AS turno,
    u.unidade AS polo,
    o.origem,
    tn.tipo_negocio,
    cc.nome AS consultor_comercial,
    cd.nome AS consultor_disparo,
    cp.campanha,
    NULL::text AS canal,
    NULL::text AS acao_comercial,
    d.tipo_disparo,
    d.peca_disparo,
    d.texto_disparo,
    f.qtd_acionamentos,
    s.status,
    NULL::text AS status_inscricao,
    s.observacao,
    s.matriculado AS flag_matriculado,
    f.data_inscricao,
    f.data_matricula,
    f.atualizado_em AS data_atualizacao,
    f.data_ultima_interacao AS data_ultima_acao,
    f.data_disparo
FROM unifecaf.f_leads f
JOIN unifecaf.dim_pessoa p
  ON p.sk_pessoa = f.sk_pessoa
LEFT JOIN unifecaf.dim_curso c
  ON c.sk_curso = f.sk_curso
LEFT JOIN unifecaf.dim_unidade u
  ON u.sk_unidade = f.sk_unidade
LEFT JOIN unifecaf.dim_origem o
  ON o.sk_origem = f.sk_origem
LEFT JOIN unifecaf.dim_tipo_negocio tn
  ON tn.sk_tipo_negocio = f.sk_tipo_negocio
LEFT JOIN unifecaf.dim_status s
  ON s.sk_status = f.sk_status
LEFT JOIN unifecaf.dim_campanha cp
  ON cp.sk_campanha = f.sk_campanha
LEFT JOIN unifecaf.dim_disparo d
  ON d.sk_disparo = f.sk_disparo
LEFT JOIN unifecaf.dim_consultor cc
  ON cc.sk_consultor = f.sk_consultor_comercial
LEFT JOIN unifecaf.dim_consultor cd
  ON cd.sk_consultor = f.sk_consultor_disparo;

GRANT SELECT ON unifecaf.vw_leads_painel_lite
TO postgres, anon, authenticated, service_role;

COMMIT;
