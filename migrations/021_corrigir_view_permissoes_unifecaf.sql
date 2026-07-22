-- Execute no SQL Editor do Supabase com um usuário administrador.
-- Sem BEGIN/COMMIT propositalmente: um erro posterior não desfaz as etapas concluídas.

CREATE SCHEMA IF NOT EXISTS unifecaf;

DROP VIEW IF EXISTS unifecaf.vw_leads_painel_lite;

CREATE VIEW unifecaf.vw_leads_painel_lite AS
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

COMMENT ON VIEW unifecaf.vw_leads_painel_lite IS
'View compatível com o Painel de Leads Lite para a base UniFECAF.';

DO $grant$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_paineis') THEN
        GRANT USAGE ON SCHEMA unifecaf TO app_paineis;
        GRANT SELECT ON unifecaf.vw_leads_painel_lite TO app_paineis;
    ELSE
        RAISE EXCEPTION 'O papel app_paineis não existe neste banco.';
    END IF;
END
$grant$;

DO $grant$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'postgres') THEN
        GRANT USAGE ON SCHEMA unifecaf TO postgres;
        GRANT SELECT ON unifecaf.vw_leads_painel_lite TO postgres;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
        GRANT USAGE ON SCHEMA unifecaf TO anon;
        GRANT SELECT ON unifecaf.vw_leads_painel_lite TO anon;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        GRANT USAGE ON SCHEMA unifecaf TO authenticated;
        GRANT SELECT ON unifecaf.vw_leads_painel_lite TO authenticated;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
        GRANT USAGE ON SCHEMA unifecaf TO service_role;
        GRANT SELECT ON unifecaf.vw_leads_painel_lite TO service_role;
    END IF;
END
$grant$;

-- Índices de apoio. Rodam somente se as tabelas existirem.
CREATE INDEX IF NOT EXISTS idx_unifecaf_dim_pessoa_celular
ON unifecaf.dim_pessoa (
    regexp_replace(COALESCE(celular::text, ''), '[^0-9]', '', 'g')
);

CREATE INDEX IF NOT EXISTS idx_unifecaf_dim_pessoa_cpf
ON unifecaf.dim_pessoa (
    regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g')
);

CREATE INDEX IF NOT EXISTS idx_unifecaf_f_leads_pessoa
ON unifecaf.f_leads (sk_pessoa);

CREATE INDEX IF NOT EXISTS idx_unifecaf_f_leads_data_inscricao
ON unifecaf.f_leads (data_inscricao DESC);

CREATE INDEX IF NOT EXISTS idx_unifecaf_stg_leads_upload
ON unifecaf.stg_leads (upload_id);

-- Validações finais.
SELECT
    to_regclass('unifecaf.vw_leads_painel_lite') AS view_unifecaf,
    has_schema_privilege('app_paineis', 'unifecaf', 'USAGE') AS schema_usage_app,
    has_table_privilege('app_paineis', 'unifecaf.vw_leads_painel_lite', 'SELECT') AS view_select_app;

SELECT COUNT(*) AS total_leads_unifecaf
FROM unifecaf.vw_leads_painel_lite;
