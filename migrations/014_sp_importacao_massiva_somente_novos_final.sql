-- ============================================================================
-- CARGA MASSIVA TEMPORÁRIA — SOMENTE LEADS NOVOS
--
-- Fonte de comparação: modelo_estrela.vw_leads_painel_lite
-- Ordem de identificação:
--   1. celular normalizado;
--   2. CPF normalizado, somente quando o celular não existe na base.
--
-- Regras:
--   - lead encontrado por celular: IGNORAR;
--   - celular não encontrado, mas CPF encontrado: IGNORAR;
--   - não atualiza nenhum lead existente;
--   - deduplica o próprio arquivo;
--   - rejeita linha sem celular e sem CPF;
--   - limite de 10.000 linhas por upload;
--   - somente as linhas classificadas como NOVO seguem para a procedure oficial.
--
-- Uso:
--   SELECT *
--   FROM modelo_estrela.sp_importar_somente_leads_novos('UPLOAD_ID');
-- ============================================================================

CREATE OR REPLACE FUNCTION modelo_estrela.sp_importar_somente_leads_novos(
    p_upload_id text
)
RETURNS TABLE (
    upload_id text,
    linhas_recebidas bigint,
    linhas_novas bigint,
    existentes_por_celular bigint,
    existentes_por_cpf bigint,
    duplicados_no_arquivo bigint,
    linhas_sem_identificador bigint,
    linhas_enviadas_procedure bigint,
    linhas_inseridas bigint,
    linhas_rejeitadas bigint,
    status text,
    mensagem text
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_total bigint := 0;
    v_novos bigint := 0;
    v_existentes_celular bigint := 0;
    v_existentes_cpf bigint := 0;
    v_duplicados bigint := 0;
    v_sem_identificador bigint := 0;
    v_inseridas bigint := 0;
    v_rejeitadas bigint := 0;
    v_status text := 'CONCLUIDO';
    v_mensagem text := '';
BEGIN
    IF NULLIF(BTRIM(p_upload_id), '') IS NULL THEN
        RAISE EXCEPTION 'upload_id é obrigatório';
    END IF;

    -- Impede duas execuções simultâneas para o mesmo upload.
    PERFORM pg_advisory_xact_lock(
        hashtext('SP_IMPORTAR_SOMENTE_NOVOS:' || p_upload_id)
    );

    SELECT COUNT(*)
      INTO v_total
      FROM modelo_estrela.stg_leads_site s
     WHERE s.upload_id = p_upload_id;

    IF v_total = 0 THEN
        RAISE EXCEPTION
            'Nenhuma linha encontrada em modelo_estrela.stg_leads_site para o upload_id %',
            p_upload_id;
    END IF;

    IF v_total > 10000 THEN
        RAISE EXCEPTION
            'O limite desta carga massiva é de 10.000 linhas por arquivo. O upload % possui % linhas. Divida o arquivo em lotes menores.',
            p_upload_id,
            v_total;
    END IF;

    DROP TABLE IF EXISTS pg_temp.tmp_massivo_somente_novos;

    CREATE TEMP TABLE tmp_massivo_somente_novos
    ON COMMIT DROP
    AS
    WITH staging_normalizada AS (
        SELECT
            s.ctid AS row_ctid,
            s.linha_arquivo,

            -- Celular brasileiro canônico:
            -- remove caracteres e também o DDI 55 quando vier com 12/13 dígitos.
            CASE
                WHEN LENGTH(regexp_replace(COALESCE(s.celular::text, ''), '[^0-9]', '', 'g')) IN (12, 13)
                 AND LEFT(regexp_replace(COALESCE(s.celular::text, ''), '[^0-9]', '', 'g'), 2) = '55'
                    THEN SUBSTRING(
                        regexp_replace(COALESCE(s.celular::text, ''), '[^0-9]', '', 'g')
                        FROM 3
                    )
                ELSE NULLIF(
                    regexp_replace(COALESCE(s.celular::text, ''), '[^0-9]', '', 'g'),
                    ''
                )
            END AS celular_limpo,

            NULLIF(
                regexp_replace(COALESCE(s.cpf::text, ''), '[^0-9]', '', 'g'),
                ''
            ) AS cpf_limpo
        FROM modelo_estrela.stg_leads_site s
        WHERE s.upload_id = p_upload_id
    ),
    view_normalizada AS (
        SELECT
            v.sk_pessoa,
            CASE
                WHEN LENGTH(regexp_replace(COALESCE(v.celular::text, ''), '[^0-9]', '', 'g')) IN (12, 13)
                 AND LEFT(regexp_replace(COALESCE(v.celular::text, ''), '[^0-9]', '', 'g'), 2) = '55'
                    THEN SUBSTRING(
                        regexp_replace(COALESCE(v.celular::text, ''), '[^0-9]', '', 'g')
                        FROM 3
                    )
                ELSE NULLIF(
                    regexp_replace(COALESCE(v.celular::text, ''), '[^0-9]', '', 'g'),
                    ''
                )
            END AS celular_limpo,
            NULLIF(
                regexp_replace(COALESCE(v.cpf::text, ''), '[^0-9]', '', 'g'),
                ''
            ) AS cpf_limpo
        FROM modelo_estrela.vw_leads_painel_lite v
    ),
    verificada AS (
        SELECT
            s.*,
            EXISTS (
                SELECT 1
                FROM view_normalizada v
                WHERE s.celular_limpo IS NOT NULL
                  AND v.celular_limpo = s.celular_limpo
            ) AS existe_celular,
            EXISTS (
                SELECT 1
                FROM view_normalizada v
                WHERE s.cpf_limpo IS NOT NULL
                  AND v.cpf_limpo = s.cpf_limpo
            ) AS existe_cpf,
            ROW_NUMBER() OVER (
                PARTITION BY s.celular_limpo
                ORDER BY s.linha_arquivo NULLS LAST, s.row_ctid
            ) AS rn_celular,
            ROW_NUMBER() OVER (
                PARTITION BY s.cpf_limpo
                ORDER BY s.linha_arquivo NULLS LAST, s.row_ctid
            ) AS rn_cpf
        FROM staging_normalizada s
    )
    SELECT
        v.row_ctid,
        v.linha_arquivo,
        v.celular_limpo,
        v.cpf_limpo,
        CASE
            WHEN v.celular_limpo IS NULL AND v.cpf_limpo IS NULL
                THEN 'SEM_IDENTIFICADOR'

            -- A regra principal é telefone primeiro.
            WHEN v.existe_celular
                THEN 'EXISTENTE_CELULAR'

            -- CPF só é consultado como segunda opção.
            WHEN NOT v.existe_celular AND v.existe_cpf
                THEN 'EXISTENTE_CPF'

            -- Deduplicação dentro do próprio arquivo.
            WHEN v.celular_limpo IS NOT NULL AND v.rn_celular > 1
                THEN 'DUPLICADO_ARQUIVO'

            WHEN v.cpf_limpo IS NOT NULL AND v.rn_cpf > 1
                THEN 'DUPLICADO_ARQUIVO'

            ELSE 'NOVO'
        END AS classificacao
    FROM verificada v;

    SELECT
        COUNT(*) FILTER (WHERE classificacao = 'NOVO'),
        COUNT(*) FILTER (WHERE classificacao = 'EXISTENTE_CELULAR'),
        COUNT(*) FILTER (WHERE classificacao = 'EXISTENTE_CPF'),
        COUNT(*) FILTER (WHERE classificacao = 'DUPLICADO_ARQUIVO'),
        COUNT(*) FILTER (WHERE classificacao = 'SEM_IDENTIFICADOR')
    INTO
        v_novos,
        v_existentes_celular,
        v_existentes_cpf,
        v_duplicados,
        v_sem_identificador
    FROM tmp_massivo_somente_novos;

    -- Tudo que não é novo fica bloqueado para a procedure oficial.
    -- Nenhum registro existente é atualizado.
    UPDATE modelo_estrela.stg_leads_site s
       SET processado = TRUE
      FROM tmp_massivo_somente_novos c
     WHERE s.ctid = c.row_ctid
       AND c.classificacao <> 'NOVO';

    -- Somente os novos ficam disponíveis para a procedure oficial.
    UPDATE modelo_estrela.stg_leads_site s
       SET processado = FALSE
      FROM tmp_massivo_somente_novos c
     WHERE s.ctid = c.row_ctid
       AND c.classificacao = 'NOVO';

    IF v_novos > 0 THEN
        PERFORM *
        FROM modelo_estrela.sp_processar_stg_leads_site(p_upload_id);
    ELSE
        v_status := 'CONCLUIDO_SEM_NOVOS';
    END IF;

    -- Busca o resultado gerado pela procedure oficial quando o log existir.
    BEGIN
        SELECT
            COALESCE(li.linhas_inseridas, 0),
            COALESCE(li.linhas_rejeitadas, 0),
            COALESCE(li.status, v_status)
        INTO
            v_inseridas,
            v_rejeitadas,
            v_status
        FROM modelo_estrela.logs_importacoes li
        WHERE li.upload_id = p_upload_id
        ORDER BY li.criado_em DESC
        LIMIT 1;
    EXCEPTION
        WHEN undefined_table OR undefined_column THEN
            v_inseridas := v_novos;
            v_rejeitadas := 0;
    END;

    v_inseridas := COALESCE(v_inseridas, 0);
    v_rejeitadas := COALESCE(v_rejeitadas, 0);
    v_status := COALESCE(v_status, 'CONCLUIDO');

    v_mensagem := format(
        '%s recebida(s); %s nova(s); %s ignorada(s) por celular; %s ignorada(s) por CPF; %s duplicada(s) no arquivo; %s sem identificador; %s inserida(s); %s rejeitada(s). Nenhum lead existente foi atualizado.',
        v_total,
        v_novos,
        v_existentes_celular,
        v_existentes_cpf,
        v_duplicados,
        v_sem_identificador,
        v_inseridas,
        v_rejeitadas
    );

    RETURN QUERY
    SELECT
        p_upload_id,
        v_total,
        v_novos,
        v_existentes_celular,
        v_existentes_cpf,
        v_duplicados,
        v_sem_identificador,
        v_novos,
        v_inseridas,
        v_rejeitadas,
        v_status,
        v_mensagem;
END;
$$;

COMMENT ON FUNCTION modelo_estrela.sp_importar_somente_leads_novos(text)
IS 'Carga massiva temporária. Compara primeiro pelo celular e depois pelo CPF na view modelo_estrela.vw_leads_painel_lite. Insere somente leads novos e nunca atualiza registros existentes. Limite de 10.000 linhas.';

-- Validação da instalação:
-- SELECT to_regprocedure(
--     'modelo_estrela.sp_importar_somente_leads_novos(text)'
-- );
