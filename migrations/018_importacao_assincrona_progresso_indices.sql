BEGIN;

CREATE TABLE IF NOT EXISTS modelo_estrela.op_importacao_progresso (
    upload_id text PRIMARY KEY,
    modo text NOT NULL CHECK (modo IN ('SOMENTE_NOVOS', 'ATUALIZAR_EXISTENTES')),
    rotina text NOT NULL,
    arquivo text,
    status text NOT NULL DEFAULT 'AGUARDANDO'
        CHECK (status IN ('AGUARDANDO','STAGING','PROCESSANDO','CONCLUIDO','ERRO')),
    etapa text NOT NULL DEFAULT 'AGUARDANDO',
    linhas_total integer NOT NULL DEFAULT 0,
    linhas_processadas integer NOT NULL DEFAULT 0,
    linhas_inseridas integer NOT NULL DEFAULT 0,
    linhas_ignoradas integer NOT NULL DEFAULT 0,
    linhas_rejeitadas integer NOT NULL DEFAULT 0,
    duplicados_arquivo integer NOT NULL DEFAULT 0,
    existentes_por_celular integer NOT NULL DEFAULT 0,
    existentes_por_cpf integer NOT NULL DEFAULT 0,
    progresso numeric(5,2) NOT NULL DEFAULT 0 CHECK (progresso BETWEEN 0 AND 100),
    mensagem text,
    erro text,
    criado_em timestamptz NOT NULL DEFAULT now(),
    iniciado_em timestamptz,
    atualizado_em timestamptz NOT NULL DEFAULT now(),
    finalizado_em timestamptz
);

CREATE INDEX IF NOT EXISTS idx_importacao_progresso_status
    ON modelo_estrela.op_importacao_progresso (status, atualizado_em DESC);

CREATE INDEX IF NOT EXISTS idx_stg_leads_site_upload_id
    ON modelo_estrela.stg_leads_site (upload_id);

CREATE INDEX IF NOT EXISTS idx_dim_pessoa_celular_normalizado
    ON modelo_estrela.dim_pessoa (
        regexp_replace(COALESCE(celular::text, ''), '[^0-9]', '', 'g')
    );

CREATE INDEX IF NOT EXISTS idx_dim_pessoa_cpf_normalizado
    ON modelo_estrela.dim_pessoa (
        regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g')
    );

CREATE OR REPLACE FUNCTION modelo_estrela.fn_atualizar_progresso_importacao(
    p_upload_id text,
    p_status text,
    p_etapa text,
    p_progresso numeric,
    p_linhas_processadas integer DEFAULT NULL,
    p_linhas_inseridas integer DEFAULT NULL,
    p_linhas_ignoradas integer DEFAULT NULL,
    p_linhas_rejeitadas integer DEFAULT NULL,
    p_mensagem text DEFAULT NULL,
    p_erro text DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = modelo_estrela, public
AS $fn$
BEGIN
    UPDATE modelo_estrela.op_importacao_progresso
       SET status = COALESCE(p_status, status),
           etapa = COALESCE(p_etapa, etapa),
           progresso = LEAST(100, GREATEST(0, COALESCE(p_progresso, progresso))),
           linhas_processadas = COALESCE(p_linhas_processadas, linhas_processadas),
           linhas_inseridas = COALESCE(p_linhas_inseridas, linhas_inseridas),
           linhas_ignoradas = COALESCE(p_linhas_ignoradas, linhas_ignoradas),
           linhas_rejeitadas = COALESCE(p_linhas_rejeitadas, linhas_rejeitadas),
           mensagem = COALESCE(p_mensagem, mensagem),
           erro = COALESCE(p_erro, erro),
           iniciado_em = CASE WHEN p_status = 'PROCESSANDO' THEN COALESCE(iniciado_em, now()) ELSE iniciado_em END,
           finalizado_em = CASE WHEN p_status IN ('CONCLUIDO','ERRO') THEN now() ELSE finalizado_em END,
           atualizado_em = now()
     WHERE upload_id = p_upload_id;
END;
$fn$;

COMMIT;
