-- Otimiza a carga massiva temporária para evitar comparações linha a linha
CREATE OR REPLACE FUNCTION modelo_estrela.sp_importar_somente_leads_novos(p_upload_id text)
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

    SELECT COUNT(*) INTO v_total
    FROM modelo_estrela.stg_leads_site
    WHERE upload_id = p_upload_id;

    IF v_total = 0 THEN
        RAISE EXCEPTION 'Nenhuma linha encontrada para o upload_id %', p_upload_id;
    END IF;

    IF v_total > 10000 THEN
        RAISE EXCEPTION 'Limite de 10.000 linhas excedido. Upload % possui % linhas.', p_upload_id, v_total;
    END IF;

    DROP TABLE IF EXISTS pg_temp.tmp_massivo_entrada;
    DROP TABLE IF EXISTS pg_temp.tmp_massivo_match_celular;
    DROP TABLE IF EXISTS pg_temp.tmp_massivo_match_cpf;
    DROP TABLE IF EXISTS pg_temp.tmp_massivo_classificacao;

    CREATE TEMP TABLE tmp_massivo_entrada ON COMMIT DROP AS
    SELECT
        s.ctid AS row_ctid,
        s.linha_arquivo,
        CASE
            WHEN LENGTH(regexp_replace(COALESCE(s.celular::text,''),'[^0-9]','','g')) IN (12,13)
             AND LEFT(regexp_replace(COALESCE(s.celular::text,''),'[^0-9]','','g'),2)='55'
            THEN SUBSTRING(regexp_replace(COALESCE(s.celular::text,''),'[^0-9]','','g') FROM 3)
            ELSE NULLIF(regexp_replace(COALESCE(s.celular::text,''),'[^0-9]','','g'),'')
        END AS celular_limpo,
        NULLIF(regexp_replace(COALESCE(s.cpf::text,''),'[^0-9]','','g'),'') AS cpf_limpo
    FROM modelo_estrela.stg_leads_site s
    WHERE s.upload_id = p_upload_id;

    CREATE INDEX ON tmp_massivo_entrada (celular_limpo);
    CREATE INDEX ON tmp_massivo_entrada (cpf_limpo);

    CREATE TEMP TABLE tmp_massivo_match_celular ON COMMIT DROP AS
    SELECT celular_limpo, COUNT(*)::bigint AS qtd
    FROM (
        SELECT DISTINCT
            CASE
                WHEN LENGTH(regexp_replace(COALESCE(v.celular::text,''),'[^0-9]','','g')) IN (12,13)
                 AND LEFT(regexp_replace(COALESCE(v.celular::text,''),'[^0-9]','','g'),2)='55'
                THEN SUBSTRING(regexp_replace(COALESCE(v.celular::text,''),'[^0-9]','','g') FROM 3)
                ELSE NULLIF(regexp_replace(COALESCE(v.celular::text,''),'[^0-9]','','g'),'')
            END AS celular_limpo,
            v.sk_pessoa
        FROM modelo_estrela.vw_leads_painel_lite v
        WHERE regexp_replace(COALESCE(v.celular::text,''),'[^0-9]','','g') <> ''
    ) x
    WHERE celular_limpo IN (SELECT celular_limpo FROM tmp_massivo_entrada WHERE celular_limpo IS NOT NULL)
    GROUP BY celular_limpo;

    CREATE INDEX ON tmp_massivo_match_celular (celular_limpo);

    CREATE TEMP TABLE tmp_massivo_match_cpf ON COMMIT DROP AS
    SELECT cpf_limpo, COUNT(*)::bigint AS qtd
    FROM (
        SELECT DISTINCT
            NULLIF(regexp_replace(COALESCE(v.cpf::text,''),'[^0-9]','','g'),'') AS cpf_limpo,
            v.sk_pessoa
        FROM modelo_estrela.vw_leads_painel_lite v
        WHERE regexp_replace(COALESCE(v.cpf::text,''),'[^0-9]','','g') <> ''
    ) x
    WHERE cpf_limpo IN (SELECT cpf_limpo FROM tmp_massivo_entrada WHERE cpf_limpo IS NOT NULL)
    GROUP BY cpf_limpo;

    CREATE INDEX ON tmp_massivo_match_cpf (cpf_limpo);

    CREATE TEMP TABLE tmp_massivo_classificacao ON COMMIT DROP AS
    WITH ranked AS (
        SELECT
            e.*,
            COALESCE(mc.qtd,0) AS qtd_celular,
            COALESCE(mp.qtd,0) AS qtd_cpf,
            ROW_NUMBER() OVER (PARTITION BY e.celular_limpo ORDER BY e.linha_arquivo NULLS LAST, e.row_ctid) AS rn_celular,
            ROW_NUMBER() OVER (PARTITION BY e.cpf_limpo ORDER BY e.linha_arquivo NULLS LAST, e.row_ctid) AS rn_cpf
        FROM tmp_massivo_entrada e
        LEFT JOIN tmp_massivo_match_celular mc ON mc.celular_limpo = e.celular_limpo
        LEFT JOIN tmp_massivo_match_cpf mp ON mp.cpf_limpo = e.cpf_limpo
    )
    SELECT
        row_ctid,
        CASE
            WHEN celular_limpo IS NULL AND cpf_limpo IS NULL THEN 'SEM_IDENTIFICADOR'
            WHEN qtd_celular > 0 THEN 'EXISTENTE_CELULAR'
            WHEN qtd_celular = 0 AND qtd_cpf > 0 THEN 'EXISTENTE_CPF'
            WHEN celular_limpo IS NOT NULL AND rn_celular > 1 THEN 'DUPLICADO_ARQUIVO'
            WHEN cpf_limpo IS NOT NULL AND rn_cpf > 1 THEN 'DUPLICADO_ARQUIVO'
            ELSE 'NOVO'
        END AS classificacao
    FROM ranked;

    SELECT
        COUNT(*) FILTER (WHERE classificacao='NOVO'),
        COUNT(*) FILTER (WHERE classificacao='EXISTENTE_CELULAR'),
        COUNT(*) FILTER (WHERE classificacao='EXISTENTE_CPF'),
        COUNT(*) FILTER (WHERE classificacao='DUPLICADO_ARQUIVO'),
        COUNT(*) FILTER (WHERE classificacao='SEM_IDENTIFICADOR')
    INTO v_novos, v_existentes_celular, v_existentes_cpf, v_duplicados, v_sem_identificador
    FROM tmp_massivo_classificacao;

    UPDATE modelo_estrela.stg_leads_site s
       SET processado = (c.classificacao <> 'NOVO')
      FROM tmp_massivo_classificacao c
     WHERE s.ctid = c.row_ctid;

    IF v_novos > 0 THEN
        PERFORM * FROM modelo_estrela.sp_processar_stg_leads_site(p_upload_id);
    ELSE
        v_status := 'CONCLUIDO_SEM_NOVOS';
    END IF;

    BEGIN
        SELECT COALESCE(linhas_inseridas,0), COALESCE(linhas_rejeitadas,0), COALESCE(status,v_status)
        INTO v_inseridas, v_rejeitadas, v_status
        FROM modelo_estrela.logs_importacoes
        WHERE upload_id = p_upload_id
        ORDER BY criado_em DESC
        LIMIT 1;
    EXCEPTION WHEN undefined_table OR undefined_column THEN
        v_inseridas := v_novos;
        v_rejeitadas := 0;
    END;

    v_mensagem := format(
        '%s recebidas; %s novas; %s ignoradas por celular; %s ignoradas por CPF; %s duplicadas no arquivo; %s sem identificador; %s inseridas; %s rejeitadas.',
        v_total, v_novos, v_existentes_celular, v_existentes_cpf, v_duplicados, v_sem_identificador, v_inseridas, v_rejeitadas
    );

    RETURN QUERY SELECT p_upload_id, v_total, v_novos, v_existentes_celular, v_existentes_cpf,
        v_duplicados, v_sem_identificador, v_novos, COALESCE(v_inseridas,0), COALESCE(v_rejeitadas,0),
        COALESCE(v_status,'CONCLUIDO'), v_mensagem;
END;
$$;
