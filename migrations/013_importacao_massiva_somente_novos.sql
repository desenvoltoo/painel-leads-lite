-- Importação massiva temporária: insere somente leads que ainda não existem.
--
-- Regras:
--   1. Não atualiza nenhum lead já existente.
--   2. Considera existente quando encontrar o mesmo CPF OU o mesmo celular.
--   3. Deduplica o próprio arquivo, mantendo apenas a primeira linha de cada pessoa.
--   4. Linhas sem CPF e sem celular são ignoradas.
--   5. Usa a procedure oficial já existente para inserir os novos registros.
--
-- Uso:
--   SELECT *
--   FROM modelo_estrela.sp_importar_somente_leads_novos('UPLOAD_ID');

CREATE OR REPLACE FUNCTION modelo_estrela.sp_importar_somente_leads_novos(
    p_upload_id text
)
RETURNS TABLE (
    upload_id text,
    linhas_recebidas bigint,
    linhas_novas bigint,
    linhas_existentes_ignoradas bigint,
    duplicados_no_arquivo bigint,
    linhas_sem_identificador bigint,
    linhas_processadas bigint,
    linhas_rejeitadas bigint,
    status text,
    mensagem text
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_total bigint := 0;
    v_novas bigint := 0;
    v_existentes bigint := 0;
    v_duplicados bigint := 0;
    v_sem_identificador bigint := 0;
    v_processadas bigint := 0;
    v_rejeitadas bigint := 0;
    v_status text := 'CONCLUIDO';
    v_mensagem text;
BEGIN
    IF NULLIF(TRIM(p_upload_id), '') IS NULL THEN
        RAISE EXCEPTION 'upload_id é obrigatório';
    END IF;

    -- Evita duas importações concorrentes do mesmo upload.
    PERFORM pg_advisory_xact_lock(hashtext('IMPORTAR_SOMENTE_NOVOS:' || p_upload_id));

    SELECT COUNT(*)
      INTO v_total
      FROM modelo_estrela.stg_leads_site s
     WHERE s.upload_id = p_upload_id;

    IF v_total = 0 THEN
        RAISE EXCEPTION 'Nenhuma linha encontrada na staging para o upload_id %', p_upload_id;
    END IF;

    DROP TABLE IF EXISTS pg_temp.tmp_massivo_classificacao;

    CREATE TEMP TABLE tmp_massivo_classificacao
    ON COMMIT DROP
    AS
    WITH normalizada AS (
        SELECT
            s.id,
            s.linha_arquivo,
            NULLIF(modelo_estrela.fn_somente_numeros(s.cpf), '') AS cpf_limpo,
            NULLIF(modelo_estrela.fn_somente_numeros(s.celular), '') AS celular_limpo
        FROM modelo_estrela.stg_leads_site s
        WHERE s.upload_id = p_upload_id
    ),
    existente AS (
        SELECT
            n.*,
            EXISTS (
                SELECT 1
                FROM modelo_estrela.dim_pessoa p
                WHERE n.cpf_limpo IS NOT NULL
                  AND modelo_estrela.fn_somente_numeros(p.cpf) = n.cpf_limpo
            )
            OR EXISTS (
                SELECT 1
                FROM modelo_estrela.dim_pessoa p
                WHERE n.celular_limpo IS NOT NULL
                  AND modelo_estrela.fn_somente_numeros(p.celular) = n.celular_limpo
            )
            OR EXISTS (
                SELECT 1
                FROM modelo_estrela.leads_painel_lite l
                WHERE n.cpf_limpo IS NOT NULL
                  AND modelo_estrela.fn_somente_numeros(l.cpf) = n.cpf_limpo
            )
            OR EXISTS (
                SELECT 1
                FROM modelo_estrela.leads_painel_lite l
                WHERE n.celular_limpo IS NOT NULL
                  AND modelo_estrela.fn_somente_numeros(l.celular) = n.celular_limpo
            ) AS ja_existe
        FROM normalizada n
    ),
    ranqueada AS (
        SELECT
            e.*,
            CASE
                WHEN e.cpf_limpo IS NOT NULL THEN 'CPF:' || e.cpf_limpo
                WHEN e.celular_limpo IS NOT NULL THEN 'CEL:' || e.celular_limpo
                ELSE 'SEM_ID:' || e.id::text
            END AS chave_arquivo,
            ROW_NUMBER() OVER (
                PARTITION BY
                    CASE
                        WHEN e.cpf_limpo IS NOT NULL THEN 'CPF:' || e.cpf_limpo
                        WHEN e.celular_limpo IS NOT NULL THEN 'CEL:' || e.celular_limpo
                        ELSE 'SEM_ID:' || e.id::text
                    END
                ORDER BY e.linha_arquivo NULLS LAST, e.id
            ) AS rn
        FROM existente e
    )
    SELECT
        r.id,
        r.linha_arquivo,
        r.cpf_limpo,
        r.celular_limpo,
        CASE
            WHEN r.cpf_limpo IS NULL AND r.celular_limpo IS NULL THEN 'SEM_IDENTIFICADOR'
            WHEN r.ja_existe THEN 'EXISTENTE'
            WHEN r.rn > 1 THEN 'DUPLICADO_ARQUIVO'
            ELSE 'NOVO'
        END AS classificacao
    FROM ranqueada r;

    SELECT COUNT(*) FILTER (WHERE classificacao = 'NOVO'),
           COUNT(*) FILTER (WHERE classificacao = 'EXISTENTE'),
           COUNT(*) FILTER (WHERE classificacao = 'DUPLICADO_ARQUIVO'),
           COUNT(*) FILTER (WHERE classificacao = 'SEM_IDENTIFICADOR')
      INTO v_novas, v_existentes, v_duplicados, v_sem_identificador
      FROM tmp_massivo_classificacao;

    -- Tudo que não for NOVO é marcado como já processado para que a procedure
    -- oficial ignore essas linhas. Nenhum dado existente é atualizado.
    UPDATE modelo_estrela.stg_leads_site s
       SET processado = TRUE,
           erro = CASE c.classificacao
               WHEN 'EXISTENTE' THEN 'IGNORADO_JA_EXISTE'
               WHEN 'DUPLICADO_ARQUIVO' THEN 'IGNORADO_DUPLICADO_ARQUIVO'
               WHEN 'SEM_IDENTIFICADOR' THEN 'IGNORADO_SEM_IDENTIFICADOR'
               ELSE s.erro
           END
      FROM tmp_massivo_classificacao c
     WHERE s.id = c.id
       AND c.classificacao <> 'NOVO';

    -- Garante que apenas as linhas classificadas como novas estejam liberadas.
    UPDATE modelo_estrela.stg_leads_site s
       SET processado = FALSE,
           erro = NULL
      FROM tmp_massivo_classificacao c
     WHERE s.id = c.id
       AND c.classificacao = 'NOVO';

    IF v_novas > 0 THEN
        PERFORM *
        FROM modelo_estrela.sp_processar_stg_leads_site(p_upload_id);
    END IF;

    SELECT
        COALESCE(li.linhas_atualizadas, 0) + COALESCE(li.linhas_inseridas, 0),
        COALESCE(li.linhas_rejeitadas, 0),
        COALESCE(li.status, CASE WHEN v_novas > 0 THEN 'CONCLUIDO' ELSE 'CONCLUIDO_SEM_NOVOS' END)
      INTO v_processadas, v_rejeitadas, v_status
      FROM modelo_estrela.logs_importacoes li
     WHERE li.upload_id = p_upload_id
     ORDER BY li.criado_em DESC
     LIMIT 1;

    v_processadas := COALESCE(v_processadas, 0);
    v_rejeitadas := COALESCE(v_rejeitadas, 0);
    v_status := COALESCE(v_status, CASE WHEN v_novas > 0 THEN 'CONCLUIDO' ELSE 'CONCLUIDO_SEM_NOVOS' END);

    v_mensagem := format(
        '%s linha(s) recebida(s); %s nova(s) enviada(s) para inclusão; %s existente(s) ignorada(s); %s duplicada(s) no arquivo; %s sem identificador; %s rejeitada(s) pela procedure oficial.',
        v_total,
        v_novas,
        v_existentes,
        v_duplicados,
        v_sem_identificador,
        v_rejeitadas
    );

    -- Mantém o log claro para auditoria da carga massiva.
    UPDATE modelo_estrela.logs_importacoes
       SET mensagem = v_mensagem,
           linhas_recebidas = v_total,
           linhas_ignoradas = v_existentes + v_duplicados + v_sem_identificador,
           duplicados_arquivo = v_duplicados,
           duplicados_banco = v_existentes,
           atualizado_em = now()
     WHERE upload_id = p_upload_id;

    RETURN QUERY
    SELECT
        p_upload_id,
        v_total,
        v_novas,
        v_existentes,
        v_duplicados,
        v_sem_identificador,
        v_processadas,
        v_rejeitadas,
        v_status,
        v_mensagem;
END;
$$;

COMMENT ON FUNCTION modelo_estrela.sp_importar_somente_leads_novos(text)
IS 'Procedure temporária para carga massiva: insere somente leads inexistentes e nunca atualiza registros já presentes.';
