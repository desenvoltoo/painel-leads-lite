-- Otimizacao de upload e processamento para Anhanguera + UniFECAF
-- Execute em janela de menor movimento. ALTER TABLE ... SET UNLOGGED exige lock exclusivo curto.

-- ============================================================
-- 1) STAGINGS COMO UNLOGGED
-- ============================================================
-- As stagings podem ser reconstruidas a partir do arquivo original.
-- Em caso de crash abrupto do PostgreSQL, linhas pendentes da staging podem ser perdidas.

ALTER TABLE modelo_estrela.stg_leads_site SET UNLOGGED;
ALTER TABLE unifecaf.stg_leads SET UNLOGGED;

-- ============================================================
-- 2) INDICES DE ACESSO POR UPLOAD
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_me_stg_leads_site_upload_id
ON modelo_estrela.stg_leads_site (upload_id);

CREATE INDEX IF NOT EXISTS idx_unifecaf_stg_leads_upload_id
ON unifecaf.stg_leads (upload_id);

CREATE INDEX IF NOT EXISTS idx_unifecaf_stg_leads_upload_processado
ON unifecaf.stg_leads (upload_id, processado);

CREATE INDEX IF NOT EXISTS idx_me_importacao_progresso_status_atualizado
ON modelo_estrela.op_importacao_progresso (status, atualizado_em DESC);

CREATE INDEX IF NOT EXISTS idx_unifecaf_importacao_progresso_status_atualizado
ON unifecaf.op_importacao_progresso (status, atualizado_em DESC);

-- ============================================================
-- 3) INDICES DE CPF/CELULAR NORMALIZADOS
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_me_dim_pessoa_celular_digits
ON modelo_estrela.dim_pessoa (
  regexp_replace(COALESCE(celular::text, ''), '[^0-9]', '', 'g')
);

CREATE INDEX IF NOT EXISTS idx_me_dim_pessoa_cpf_digits
ON modelo_estrela.dim_pessoa (
  regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g')
);

CREATE INDEX IF NOT EXISTS idx_unifecaf_dim_pessoa_celular_digits
ON unifecaf.dim_pessoa (
  regexp_replace(COALESCE(celular::text, ''), '[^0-9]', '', 'g')
);

CREATE INDEX IF NOT EXISTS idx_unifecaf_dim_pessoa_cpf_digits
ON unifecaf.dim_pessoa (
  regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g')
);

-- ============================================================
-- 4) INDICES DE RESOLUCAO DAS DIMENSOES UNIFECAF
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_unifecaf_origem_norm
ON unifecaf.dim_origem (lower(btrim(origem)));

CREATE INDEX IF NOT EXISTS idx_unifecaf_unidade_norm
ON unifecaf.dim_unidade (lower(btrim(unidade)));

CREATE INDEX IF NOT EXISTS idx_unifecaf_tipo_negocio_norm
ON unifecaf.dim_tipo_negocio (lower(btrim(tipo_negocio)));

CREATE INDEX IF NOT EXISTS idx_unifecaf_campanha_norm
ON unifecaf.dim_campanha (lower(btrim(campanha)));

CREATE INDEX IF NOT EXISTS idx_unifecaf_consultor_tipo_nome_norm
ON unifecaf.dim_consultor (tipo, lower(btrim(nome)));

CREATE INDEX IF NOT EXISTS idx_unifecaf_curso_modalidade_norm
ON unifecaf.dim_curso (
  lower(btrim(curso)),
  lower(btrim(COALESCE(modalidade, '')))
);

CREATE INDEX IF NOT EXISTS idx_unifecaf_status_norm
ON unifecaf.dim_status (
  lower(btrim(status)),
  matriculado
);

CREATE INDEX IF NOT EXISTS idx_unifecaf_f_leads_pessoa
ON unifecaf.f_leads (sk_pessoa);

CREATE INDEX IF NOT EXISTS idx_unifecaf_f_leads_data_inscricao
ON unifecaf.f_leads (data_inscricao DESC);

-- ============================================================
-- 5) AUTOVACUUM MAIS AGRESSIVO NAS STAGINGS
-- ============================================================

ALTER TABLE modelo_estrela.stg_leads_site SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 500,
  autovacuum_analyze_threshold = 250
);

ALTER TABLE unifecaf.stg_leads SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 500,
  autovacuum_analyze_threshold = 250
);

-- ============================================================
-- 6) FUNCOES DE LIMPEZA DAS STAGINGS
-- ============================================================

CREATE OR REPLACE FUNCTION modelo_estrela.fn_limpar_staging_importacoes(
  p_dias integer DEFAULT 7
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = modelo_estrela, public
AS $$
DECLARE
  v_excluidas bigint := 0;
BEGIN
  DELETE FROM modelo_estrela.stg_leads_site s
  USING modelo_estrela.op_importacao_progresso p
  WHERE p.upload_id = s.upload_id
    AND p.status IN ('CONCLUIDO', 'ERRO')
    AND p.atualizado_em < now() - make_interval(days => GREATEST(p_dias, 1));

  GET DIAGNOSTICS v_excluidas = ROW_COUNT;
  RETURN v_excluidas;
END;
$$;

CREATE OR REPLACE FUNCTION unifecaf.fn_limpar_staging_importacoes(
  p_dias integer DEFAULT 7
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = unifecaf, public
AS $$
DECLARE
  v_excluidas bigint := 0;
BEGIN
  DELETE FROM unifecaf.stg_leads
  WHERE processado = true
    AND processado_em < now() - make_interval(days => GREATEST(p_dias, 1));

  GET DIAGNOSTICS v_excluidas = ROW_COUNT;
  RETURN v_excluidas;
END;
$$;

GRANT EXECUTE ON FUNCTION modelo_estrela.fn_limpar_staging_importacoes(integer)
TO app_paineis;

GRANT EXECUTE ON FUNCTION unifecaf.fn_limpar_staging_importacoes(integer)
TO app_paineis;

-- ============================================================
-- 7) ESTATISTICAS ATUALIZADAS
-- ============================================================

ANALYZE modelo_estrela.stg_leads_site;
ANALYZE modelo_estrela.dim_pessoa;
ANALYZE modelo_estrela.op_importacao_progresso;

ANALYZE unifecaf.stg_leads;
ANALYZE unifecaf.dim_pessoa;
ANALYZE unifecaf.dim_origem;
ANALYZE unifecaf.dim_unidade;
ANALYZE unifecaf.dim_tipo_negocio;
ANALYZE unifecaf.dim_curso;
ANALYZE unifecaf.dim_campanha;
ANALYZE unifecaf.dim_consultor;
ANALYZE unifecaf.dim_status;
ANALYZE unifecaf.f_leads;
ANALYZE unifecaf.op_importacao_progresso;

-- ============================================================
-- 8) VALIDACAO
-- ============================================================

SELECT
  n.nspname AS schema,
  c.relname AS tabela,
  c.relpersistence AS persistencia
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE (n.nspname, c.relname) IN (
  ('modelo_estrela', 'stg_leads_site'),
  ('unifecaf', 'stg_leads')
)
ORDER BY 1, 2;

-- relpersistence esperado: 'u' para UNLOGGED.

SELECT
  schemaname,
  tablename,
  indexname
FROM pg_indexes
WHERE schemaname IN ('modelo_estrela', 'unifecaf')
  AND (
    tablename IN ('stg_leads_site', 'stg_leads', 'dim_pessoa', 'op_importacao_progresso')
    OR indexname LIKE 'idx_unifecaf_%_norm'
  )
ORDER BY schemaname, tablename, indexname;
