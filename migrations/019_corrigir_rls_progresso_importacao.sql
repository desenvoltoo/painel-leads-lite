BEGIN;

-- A tabela e interna ao backend do painel. O acesso e feito por conexao direta
-- PostgreSQL, nao pela API publica do Supabase.
ALTER TABLE modelo_estrela.op_importacao_progresso DISABLE ROW LEVEL SECURITY;

GRANT USAGE ON SCHEMA modelo_estrela TO postgres, anon, authenticated, service_role;
GRANT SELECT, INSERT, UPDATE, DELETE
ON TABLE modelo_estrela.op_importacao_progresso
TO postgres, anon, authenticated, service_role;

GRANT EXECUTE
ON FUNCTION modelo_estrela.fn_atualizar_progresso_importacao(
    text, text, text, numeric, integer, integer, integer, integer, text, text
)
TO postgres, anon, authenticated, service_role;

COMMIT;
