-- Importacao por telefone com suporte a telefone secundario.
-- Reaplicavel e compativel com os schemas Anhanguera e UniFECAF.

BEGIN;

ALTER TABLE modelo_estrela.dim_pessoa
    ADD COLUMN IF NOT EXISTS telefone2 text;
ALTER TABLE unifecaf.dim_pessoa
    ADD COLUMN IF NOT EXISTS telefone2 text;

CREATE INDEX IF NOT EXISTS idx_modelo_dim_pessoa_telefone2_normalizado
ON modelo_estrela.dim_pessoa (
    regexp_replace(COALESCE(telefone2, ''), '[^0-9]', '', 'g')
);
CREATE INDEX IF NOT EXISTS idx_unifecaf_dim_pessoa_telefone2_normalizado
ON unifecaf.dim_pessoa (
    regexp_replace(COALESCE(telefone2, ''), '[^0-9]', '', 'g')
);

COMMENT ON COLUMN modelo_estrela.dim_pessoa.telefone2 IS
    'Telefone secundario preenchido quando um CPF existente recebe outro numero.';
COMMENT ON COLUMN unifecaf.dim_pessoa.telefone2 IS
    'Telefone secundario preenchido quando um CPF existente recebe outro numero.';

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
    v_cpf_telefone2 bigint := 0;
    v_conflitos bigint := 0;
    v_duplicados bigint := 0;
    v_sem_telefone bigint := 0;
    v_inseridas bigint := 0;
    v_rejeitadas bigint := 0;
    v_status text := 'CONCLUIDO';
    v_mensagem text := '';
BEGIN
    IF NULLIF(BTRIM(p_upload_id), '') IS NULL THEN
        RAISE EXCEPTION 'upload_id e obrigatorio';
    END IF;

    PERFORM pg_advisory_xact_lock(
        hashtext('SP_IMPORTAR_SOMENTE_NOVOS:' || p_upload_id)
    );

    SELECT COUNT(*) INTO v_total
    FROM modelo_estrela.stg_leads_site
    WHERE upload_id = p_upload_id;

    IF v_total = 0 THEN
        RAISE EXCEPTION 'Nenhuma linha encontrada para o upload_id %', p_upload_id;
    END IF;
    IF v_total > 15000 THEN
        RAISE EXCEPTION 'O upload % possui % linhas. O limite e 15.000.', p_upload_id, v_total;
    END IF;

    DROP TABLE IF EXISTS pg_temp.tmp_telefones_existentes;
    DROP TABLE IF EXISTS pg_temp.tmp_triagem_somente_novos;

    CREATE TEMP TABLE tmp_telefones_existentes
    ON COMMIT DROP AS
    SELECT sk_pessoa, telefone_limpo
    FROM (
        SELECT sk_pessoa,
               modelo_estrela.fn_somente_numeros(celular) AS telefone_limpo
        FROM modelo_estrela.dim_pessoa
        UNION ALL
        SELECT sk_pessoa,
               modelo_estrela.fn_somente_numeros(telefone2) AS telefone_limpo
        FROM modelo_estrela.dim_pessoa
    ) x
    WHERE telefone_limpo IS NOT NULL;

    CREATE INDEX ON tmp_telefones_existentes (telefone_limpo);
    ANALYZE tmp_telefones_existentes;

    CREATE TEMP TABLE tmp_triagem_somente_novos
    ON COMMIT DROP AS
    WITH n AS (
        SELECT
            s.ctid AS row_ctid,
            s.linha_arquivo,
            modelo_estrela.fn_somente_numeros(s.celular) AS telefone_limpo,
            modelo_estrela.fn_somente_numeros(s.cpf) AS cpf_limpo
        FROM modelo_estrela.stg_leads_site s
        WHERE s.upload_id = p_upload_id
    ),
    r AS (
        SELECT
            n.*,
            ROW_NUMBER() OVER (
                PARTITION BY n.telefone_limpo
                ORDER BY n.linha_arquivo NULLS LAST, n.row_ctid
            ) AS rn,
            te.sk_pessoa AS pessoa_telefone,
            pc.sk_pessoa AS pessoa_cpf,
            modelo_estrela.fn_somente_numeros(pc.celular) AS celular_atual,
            modelo_estrela.fn_somente_numeros(pc.telefone2) AS telefone2_atual
        FROM n
        LEFT JOIN LATERAL (
            SELECT sk_pessoa
            FROM tmp_telefones_existentes t
            WHERE t.telefone_limpo = n.telefone_limpo
            ORDER BY sk_pessoa
            LIMIT 1
        ) te ON n.telefone_limpo IS NOT NULL
        LEFT JOIN LATERAL (
            SELECT p.sk_pessoa, p.celular, p.telefone2
            FROM modelo_estrela.dim_pessoa p
            WHERE n.cpf_limpo IS NOT NULL
              AND modelo_estrela.fn_somente_numeros(p.cpf) = n.cpf_limpo
            ORDER BY p.sk_pessoa
            LIMIT 1
        ) pc ON TRUE
    )
    SELECT
        r.*,
        CASE
            WHEN telefone_limpo IS NULL THEN 'SEM_TELEFONE'
            WHEN rn > 1 THEN 'DUPLICADO_ARQUIVO'
            WHEN pessoa_telefone IS NOT NULL THEN 'EXISTENTE_TELEFONE'
            WHEN pessoa_cpf IS NOT NULL
             AND (telefone2_atual IS NULL OR telefone2_atual = telefone_limpo)
                THEN 'PREENCHER_TELEFONE2'
            WHEN pessoa_cpf IS NOT NULL THEN 'CONFLITO_TELEFONE2'
            ELSE 'NOVO'
        END AS classificacao
    FROM r;

    CREATE INDEX ON tmp_triagem_somente_novos (classificacao);
    ANALYZE tmp_triagem_somente_novos;

    UPDATE modelo_estrela.dim_pessoa p
    SET telefone2 = t.telefone_limpo,
        updated_at = now()
    FROM tmp_triagem_somente_novos t
    WHERE t.classificacao = 'PREENCHER_TELEFONE2'
      AND p.sk_pessoa = t.pessoa_cpf
      AND modelo_estrela.fn_somente_numeros(p.celular)
          IS DISTINCT FROM t.telefone_limpo
      AND modelo_estrela.fn_somente_numeros(p.telefone2)
          IS DISTINCT FROM t.telefone_limpo;

    INSERT INTO modelo_estrela.logs_rejeicoes_import (
        ts, motivo, cpf_raw, celular_raw, nome_raw, email_raw,
        upload_id, linha, campo, valor_mascarado, criado_em
    )
    SELECT
        now(),
        CASE WHEN t.classificacao = 'SEM_TELEFONE'
             THEN 'SEM_TELEFONE'
             ELSE 'CONFLITO_TELEFONE2' END,
        s.cpf, s.celular, s.nome, s.email,
        p_upload_id, s.linha_arquivo, 'celular', NULL, now()
    FROM tmp_triagem_somente_novos t
    JOIN modelo_estrela.stg_leads_site s ON s.ctid = t.row_ctid
    WHERE t.classificacao IN ('SEM_TELEFONE', 'CONFLITO_TELEFONE2');

    SELECT
        COUNT(*) FILTER (WHERE classificacao = 'NOVO'),
        COUNT(*) FILTER (WHERE classificacao = 'EXISTENTE_TELEFONE'),
        COUNT(*) FILTER (WHERE classificacao = 'PREENCHER_TELEFONE2'),
        COUNT(*) FILTER (WHERE classificacao = 'CONFLITO_TELEFONE2'),
        COUNT(*) FILTER (WHERE classificacao = 'DUPLICADO_ARQUIVO'),
        COUNT(*) FILTER (WHERE classificacao = 'SEM_TELEFONE')
    INTO v_novos, v_existentes_telefone, v_cpf_telefone2,
         v_conflitos, v_duplicados, v_sem_telefone
    FROM tmp_triagem_somente_novos;

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
            v_rejeitadas := v_sem_telefone + v_conflitos;
    END;

    v_rejeitadas := GREATEST(
        COALESCE(v_rejeitadas, 0),
        v_sem_telefone + v_conflitos
    );

    v_mensagem := format(
        '%s recebida(s); %s nova(s); %s telefone(s) existente(s); %s telefone2 preenchido(s); %s conflito(s) de telefone2; %s duplicada(s); %s sem telefone; %s inserida(s).',
        v_total, v_novos, v_existentes_telefone, v_cpf_telefone2,
        v_conflitos, v_duplicados, v_sem_telefone, v_inseridas
    );

    RETURN QUERY SELECT
        p_upload_id,
        v_total,
        v_novos,
        v_existentes_telefone,
        v_cpf_telefone2 + v_conflitos,
        v_duplicados,
        v_sem_telefone,
        v_novos,
        COALESCE(v_inseridas, 0),
        v_rejeitadas,
        COALESCE(v_status, 'CONCLUIDO'),
        v_mensagem;
END;
$function$;

-- Ajustes conservadores nas procedures principais existentes.
-- Os replaces sao intencionais e falham caso a estrutura esperada nao exista.
DO $patch_modelo$
DECLARE
    d text;
    antes text;
BEGIN
    SELECT pg_get_functiondef(
        'modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure
    ) INTO d;
    antes := d;

    d := replace(
        d,
        'modelo_estrela.fn_somente_numeros(p.celular) = t.celular_limpo',
        '(modelo_estrela.fn_somente_numeros(p.celular) = t.celular_limpo OR modelo_estrela.fn_somente_numeros(p.telefone2) = t.celular_limpo)'
    );

    d := replace(
        d,
        E'celular = CASE\n            WHEN modelo_estrela.fn_celular_valido_basico(t.celular_limpo)\n            THEN t.celular_limpo\n            ELSE p.celular\n        END,',
        E'celular = CASE\n            WHEN NOT modelo_estrela.fn_celular_valido_basico(p.celular)\n             AND modelo_estrela.fn_celular_valido_basico(t.celular_limpo)\n            THEN t.celular_limpo\n            ELSE p.celular\n        END,\n        telefone2 = CASE\n            WHEN modelo_estrela.fn_celular_valido_basico(t.celular_limpo)\n             AND modelo_estrela.fn_somente_numeros(p.celular) IS DISTINCT FROM t.celular_limpo\n             AND NOT modelo_estrela.fn_celular_valido_basico(p.telefone2)\n            THEN t.celular_limpo\n            ELSE p.telefone2\n        END,'
    );

    IF d = antes THEN
        RAISE EXCEPTION 'Nao foi possivel ajustar sp_processar_stg_leads_site';
    END IF;
    EXECUTE d;
END;
$patch_modelo$;

DO $patch_unifecaf$
DECLARE
    d text;
    antes text;
BEGIN
    SELECT pg_get_functiondef(
        'unifecaf.sp_processar_upload(text,boolean)'::regprocedure
    ) INTO d;
    antes := d;

    d := replace(
        d,
        'unifecaf.fn_digits(p.celular) = t.cel_norm',
        '(unifecaf.fn_digits(p.celular) = t.cel_norm OR unifecaf.fn_digits(p.telefone2) = t.cel_norm)'
    );
    d := replace(
        d,
        'unifecaf.fn_digits(p.celular)=t.cel_norm',
        '(unifecaf.fn_digits(p.celular)=t.cel_norm OR unifecaf.fn_digits(p.telefone2)=t.cel_norm)'
    );

    d := replace(
        d,
        'celular=COALESCE(NULLIF(trim(t.celular),''''),p.celular), email=',
        'celular=CASE WHEN unifecaf.fn_digits(p.celular) IS NULL THEN NULLIF(trim(t.celular),'''') ELSE p.celular END, telefone2=CASE WHEN t.cel_norm IS NOT NULL AND unifecaf.fn_digits(p.celular) IS DISTINCT FROM t.cel_norm AND unifecaf.fn_digits(p.telefone2) IS NULL THEN t.cel_norm ELSE p.telefone2 END, email='
    );

    IF d = antes THEN
        RAISE EXCEPTION 'Nao foi possivel ajustar unifecaf.sp_processar_upload';
    END IF;
    EXECUTE d;
END;
$patch_unifecaf$;

ANALYZE modelo_estrela.dim_pessoa;
ANALYZE unifecaf.dim_pessoa;

COMMIT;
