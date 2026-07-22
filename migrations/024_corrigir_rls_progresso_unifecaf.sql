-- Corrige o bloqueio de RLS no acompanhamento de importacoes da UniFECAF.
-- O backend usa a role app_paineis para inserir e atualizar o progresso.

ALTER TABLE unifecaf.op_importacao_progresso DISABLE ROW LEVEL SECURITY;

GRANT USAGE ON SCHEMA unifecaf TO app_paineis;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE unifecaf.op_importacao_progresso TO app_paineis;

-- logs_importacoes tambem participa do inicio da importacao UniFECAF.
ALTER TABLE unifecaf.logs_importacoes DISABLE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE unifecaf.logs_importacoes TO app_paineis;

-- A staging precisa aceitar o COPY executado pela aplicacao.
ALTER TABLE unifecaf.stg_leads DISABLE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE unifecaf.stg_leads TO app_paineis;

-- Sequencias usadas pelas tabelas com identity.
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA unifecaf TO app_paineis;

-- Confere o estado final das tabelas envolvidas no upload.
SELECT
    n.nspname AS schema,
    c.relname AS tabela,
    c.relrowsecurity AS rls_ativo,
    has_table_privilege('app_paineis', format('%I.%I', n.nspname, c.relname), 'SELECT') AS pode_select,
    has_table_privilege('app_paineis', format('%I.%I', n.nspname, c.relname), 'INSERT') AS pode_insert,
    has_table_privilege('app_paineis', format('%I.%I', n.nspname, c.relname), 'UPDATE') AS pode_update
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'unifecaf'
  AND c.relname IN ('op_importacao_progresso', 'logs_importacoes', 'stg_leads')
ORDER BY c.relname;
