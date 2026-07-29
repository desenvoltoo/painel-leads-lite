-- Corrige a identidade da importacao:
-- 1) CPF valido identifica a pessoa antes do celular;
-- 2) mesmo CPF nunca e enviado novamente para insercao;
-- 3) celular diferente de CPF existente preenche telefone2 quando disponivel;
-- 4) deduplicacao dentro do arquivo usa CPF primeiro e celular como fallback;
-- 5) importacoes "somente novos" sao serializadas para evitar corrida entre uploads.

BEGIN;

CREATE OR REPLACE FUNCTION modelo_estrela.sp_importar_somente_leads_novos(
    p_upload_id text
)
RETURNS TABLE(
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
AS $function$
DECLARE
    v_total bigint := 0;
    v_novos bigint := 0;
    v_existentes_telefone bigint := 0;
    v_existentes_cpf bigint := 0;
    v_cpf_telefone2 bigint := 0;
    v_conflitos bigint := 0;
    v_duplicados bigint := 0;
    v_sem_identificador bigint := 0;
    v_inseridas bigint := 0;
    v_rejeitadas bigint := 0;
    v_status text := 'CONCLUIDO';
    v_mensagem text := '';
BEGIN
    IF NULLIF(BTRIM(p_upload_id), '') IS NULL THEN
        RAISE EXCEPTION 'upload_id e obrigatorio';
    END IF;

    -- Lock global: impede duas importacoes simultaneas de consultarem a pessoa
    -- como inexistente e inserirem o mesmo CPF em paralelo.
    PERFORM pg_advisory_xact_lock(hashtext('SP_IMPORTAR_SOMENTE_NOVOS_GLOBAL'));

    SELECT COUNT(*) INTO v_total
    FROM modelo_estrela.stg_leads_site
    WHERE upload_id = p_upload_id;

    IF v_total = 0 THEN
        RAISE EXCEPTION 'Nenhuma linha encontrada para o upload_id %', p_upload_id;
    END IF;
    IF v_total > 15000 THEN
        RAISE EXCEPTION 'O upload % possui % linhas. O limite e 15.000.', p_upload_id, v_total;
    END IF;

    DROP TABLE IF EXISTS pg_temp.tmp_triagem_somente_novos;

    CREATE TEMP TABLE tmp_triagem_somente_novos
    ON COMMIT DROP AS
    WITH normalizado AS (
        SELECT
            s.ctid AS row_ctid,
            s.linha_arquivo,
            modelo_estrela.fn_somente_numeros(s.cpf) AS cpf_limpo,
            modelo_estrela.fn_somente_numeros(s.celular) AS telefone_limpo,
            COALESCE(
                CASE
                    WHEN modelo_estrela.fn_somente_numeros(s.cpf) IS NOT NULL
                    THEN 'CPF:' || modelo_estrela.fn_somente_numeros(s.cpf)
                END,
                CASE
                    WHEN modelo_estrela.fn_somente_numeros(s.celular) IS NOT NULL
                    THEN 'TEL:' || modelo_estrela.fn_somente_numeros(s.celular)
                END,
                'LINHA:' || s.ctid::text
            ) AS chave_identidade
        FROM modelo_estrela.stg_leads_site s
        WHERE s.upload_id = p_upload_id
    ),
    classificados AS (
        SELECT
            n.*,
            ROW_NUMBER() OVER (
                PARTITION BY n.chave_identidade
                ORDER BY n.linha_arquivo NULLS LAST, n.row_ctid
            ) AS rn,
            pc.sk_pessoa AS pessoa_cpf,
            modelo_estrela.fn_somente_numeros(pc.celular) AS celular_atual,
            modelo_estrela.fn_somente_numeros(pc.telefone2) AS telefone2_atual,
            pt.sk_pessoa AS pessoa_telefone
        FROM normalizado n
        LEFT JOIN LATERAL (
            SELECT p.sk_pessoa, p.celular, p.telefone2
            FROM modelo_estrela.dim_pessoa p
            WHERE n.cpf_limpo IS NOT NULL
              AND regexp_replace(COALESCE(p.cpf, ''), '[^0-9]', '', 'g') = n.cpf_limpo
            ORDER BY p.updated_at DESC NULLS LAST, p.sk_pessoa
            LIMIT 1
        ) pc ON TRUE
        LEFT JOIN LATERAL (
            SELECT p.sk_pessoa
            FROM modelo_estrela.dim_pessoa p
            WHERE n.telefone_limpo IS NOT NULL
              AND (
                    regexp_replace(COALESCE(p.celular, ''), '[^0-9]', '', 'g') = n.telefone_limpo
                 OR regexp_replace(COALESCE(p.telefone2, ''), '[^0-9]', '', 'g') = n.telefone_limpo
              )
            ORDER BY p.updated_at DESC NULLS LAST, p.sk_pessoa
            LIMIT 1
        ) pt ON TRUE
    )
    SELECT
        c.*,
        CASE
            WHEN c.cpf_limpo IS NULL AND c.telefone_limpo IS NULL
                THEN 'SEM_IDENTIFICADOR'
            WHEN c.rn > 1
                THEN 'DUPLICADO_ARQUIVO'

            -- CPF sempre tem prioridade sobre telefone.
            WHEN c.pessoa_cpf IS NOT NULL
             AND c.telefone_limpo IS NOT NULL
             AND c.telefone_limpo = c.celular_atual
                THEN 'EXISTENTE_CPF'
            WHEN c.pessoa_cpf IS NOT NULL
             AND c.telefone_limpo IS NOT NULL
             AND c.telefone_limpo = c.telefone2_atual
                THEN 'EXISTENTE_CPF'
            WHEN c.pessoa_cpf IS NOT NULL
             AND c.telefone_limpo IS NOT NULL
             AND c.telefone_limpo IS DISTINCT FROM c.celular_atual
             AND c.telefone2_atual IS NULL
                THEN 'PREENCHER_TELEFONE2'
            WHEN c.pessoa_cpf IS NOT NULL
             AND c.telefone_limpo IS NOT NULL
             AND c.telefone_limpo IS DISTINCT FROM c.celular_atual
             AND c.telefone_limpo IS DISTINCT FROM c.telefone2_atual
                THEN 'CONFLITO_TELEFONE2'
            WHEN c.pessoa_cpf IS NOT NULL
                THEN 'EXISTENTE_CPF'

            -- Celular so identifica quando o CPF nao existe no banco.
            WHEN c.pessoa_telefone IS NOT NULL
                THEN 'EXISTENTE_TELEFONE'
            ELSE 'NOVO'
        END AS classificacao
    FROM classificados c;

    CREATE INDEX ON tmp_triagem_somente_novos (classificacao);
    ANALYZE tmp_triagem_somente_novos;

    -- Mesmo CPF + celular novo: preserva celular principal e grava o novo em telefone2.
    UPDATE modelo_estrela.dim_pessoa p
    SET telefone2 = t.telefone_limpo,
        updated_at = now()
    FROM tmp_triagem_somente_novos t
    WHERE t.classificacao = 'PREENCHER_TELEFONE2'
      AND p.sk_pessoa = t.pessoa_cpf
      AND modelo_estrela.fn_somente_numeros(p.celular)
          IS DISTINCT FROM t.telefone_limpo
      AND modelo_estrela.fn_somente_numeros(p.telefone2) IS NULL;

    INSERT INTO modelo_estrela.logs_rejeicoes_import (
        ts, motivo, cpf_raw, celular_raw, nome_raw, email_raw,
        upload_id, linha, campo, valor_mascarado, criado_em
    )
    SELECT
        now(),
        CASE
            WHEN t.classificacao = 'SEM_IDENTIFICADOR' THEN 'SEM_IDENTIFICADOR'
            ELSE 'CONFLITO_TELEFONE2'
        END,
        s.cpf, s.celular, s.nome, s.email,
        p_upload_id, s.linha_arquivo, 'celular', NULL, now()
    FROM tmp_triagem_somente_novos t
    JOIN modelo_estrela.stg_leads_site s ON s.ctid = t.row_ctid
    WHERE t.classificacao IN ('SEM_IDENTIFICADOR', 'CONFLITO_TELEFONE2');

    SELECT
        COUNT(*) FILTER (WHERE classificacao = 'NOVO'),
        COUNT(*) FILTER (WHERE classificacao = 'EXISTENTE_TELEFONE'),
        COUNT(*) FILTER (WHERE classificacao = 'EXISTENTE_CPF'),
        COUNT(*) FILTER (WHERE classificacao = 'PREENCHER_TELEFONE2'),
        COUNT(*) FILTER (WHERE classificacao = 'CONFLITO_TELEFONE2'),
        COUNT(*) FILTER (WHERE classificacao = 'DUPLICADO_ARQUIVO'),
        COUNT(*) FILTER (WHERE classificacao = 'SEM_IDENTIFICADOR')
    INTO
        v_novos,
        v_existentes_telefone,
        v_existentes_cpf,
        v_cpf_telefone2,
        v_conflitos,
        v_duplicados,
        v_sem_identificador
    FROM tmp_triagem_somente_novos;

    -- Somente classificacao NOVO segue para a procedure de insercao.
    -- Todo CPF ja existente e marcado como processado, inclusive quando telefone2 foi preenchido.
    UPDATE modelo_estrela.stg_leads_site s
    SET processado = (t.classificacao <> 'NOVO')
    FROM tmp_triagem_somente_novos t
    WHERE s.ctid = t.row_ctid;

    IF v_novos > 0 THEN
        PERFORM *
        FROM modelo_estrela.sp_processar_stg_leads_site(p_upload_id);
    ELSE
        v_status := 'CONCLUIDO_SEM_NOVOS';
    END IF;

    BEGIN
        SELECT COALESCE(linhas_inseridas, 0),
               COALESCE(linhas_rejeitadas, 0),
               COALESCE(status, v_status)
        INTO v_inseridas, v_rejeitadas, v_status
        FROM modelo_estrela.logs_importacoes
        WHERE upload_id = p_upload_id
        ORDER BY criado_em DESC
        LIMIT 1;
    EXCEPTION
        WHEN undefined_table OR undefined_column THEN
            v_inseridas := v_novos;
            v_rejeitadas := v_sem_identificador + v_conflitos;
    END;

    v_rejeitadas := GREATEST(
        COALESCE(v_rejeitadas, 0),
        v_sem_identificador + v_conflitos
    );

    v_mensagem := format(
        '%s recebida(s); %s nova(s); %s CPF(s) existente(s); %s telefone(s) existente(s); %s telefone2 preenchido(s); %s conflito(s) de telefone2; %s duplicada(s) no arquivo; %s sem identificador; %s inserida(s).',
        v_total, v_novos, v_existentes_cpf, v_existentes_telefone,
        v_cpf_telefone2, v_conflitos, v_duplicados,
        v_sem_identificador, v_inseridas
    );

    RETURN QUERY SELECT
        p_upload_id,
        v_total,
        v_novos,
        v_existentes_telefone,
        v_existentes_cpf + v_cpf_telefone2 + v_conflitos,
        v_duplicados,
        v_sem_identificador,
        v_novos,
        COALESCE(v_inseridas, 0),
        v_rejeitadas,
        COALESCE(v_status, 'CONCLUIDO'),
        v_mensagem;
END;
$function$;

COMMENT ON FUNCTION modelo_estrela.sp_importar_somente_leads_novos(text) IS
'Importacao somente novos com CPF prioritario. Mesmo CPF nao duplica; celular diferente preenche telefone2 quando disponivel.';

COMMIT;

-- Validacao da definicao instalada.
SELECT
    position('CPF sempre tem prioridade sobre telefone' IN pg_get_functiondef(
        'modelo_estrela.sp_importar_somente_leads_novos(text)'::regprocedure
    )) > 0 AS cpf_prioritario_ativo,
    position('SP_IMPORTAR_SOMENTE_NOVOS_GLOBAL' IN pg_get_functiondef(
        'modelo_estrela.sp_importar_somente_leads_novos(text)'::regprocedure
    )) > 0 AS lock_global_ativo;
