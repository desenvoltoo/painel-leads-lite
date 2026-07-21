-- Política de importação:
-- 1) dados pessoais/acadêmicos vazios preservam o valor atual;
-- 2) dados operacionais/variáveis refletem exatamente o novo arquivo,
--    inclusive NULL quando a célula vier vazia.
--
-- Esta migration envolve a função atual em uma função core e executa
-- a sincronização operacional após o processamento dimensional existente.

BEGIN;

DO $migration$
BEGIN
    IF to_regprocedure('modelo_estrela.sp_processar_stg_leads_site_core(text)') IS NULL
       AND to_regprocedure('modelo_estrela.sp_processar_stg_leads_site(text)') IS NOT NULL THEN
        ALTER FUNCTION modelo_estrela.sp_processar_stg_leads_site(text)
        RENAME TO sp_processar_stg_leads_site_core;
    END IF;
END;
$migration$;

CREATE OR REPLACE FUNCTION modelo_estrela.sp_processar_stg_leads_site(
    p_upload_id text
)
RETURNS TABLE (
    linhas_recebidas integer,
    linhas_processadas integer,
    linhas_rejeitadas integer,
    linhas_gravadas_staging integer,
    duplicados_arquivo integer,
    duplicados_banco integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = modelo_estrela, public, pg_temp
AS $function$
DECLARE
    v_result record;
    v_variaveis_alteradas integer := 0;
BEGIN
    SELECT *
    INTO v_result
    FROM modelo_estrela.sp_processar_stg_leads_site_core(p_upload_id);

    /*
     * Seleciona uma linha por identificador do arquivo e resolve a pessoa:
     * CPF tem prioridade; celular só é aceito quando pertence a uma única pessoa.
     */
    DROP TABLE IF EXISTS pg_temp.tmp_variaveis_upload;

    CREATE TEMP TABLE tmp_variaveis_upload
    ON COMMIT DROP
    AS
    WITH base AS (
        SELECT
            s.id,
            s.linha_arquivo,
            modelo_estrela.fn_somente_numeros(s.cpf) AS cpf_limpo,
            modelo_estrela.fn_somente_numeros(s.celular) AS celular_limpo,

            NULLIF(btrim(s.consultor_comercial), '') AS consultor_comercial,
            NULLIF(btrim(s.consultor_disparo), '') AS consultor_disparo,
            NULLIF(btrim(s.status), '') AS status,
            NULLIF(btrim(s.status_inscricao), '') AS status_inscricao,
            NULLIF(btrim(s.campanha), '') AS campanha,
            NULLIF(btrim(s.canal), '') AS canal,
            NULLIF(btrim(s.acao_comercial), '') AS acao_comercial,
            NULLIF(btrim(s.tipo_disparo), '') AS tipo_disparo,
            NULLIF(btrim(s.peca_disparo), '') AS peca_disparo,
            NULLIF(btrim(s.texto_disparo), '') AS texto_disparo,
            NULLIF(btrim(s.observacao), '') AS observacao,

            CASE
                WHEN NULLIF(regexp_replace(COALESCE(s.qtd_acionamentos, ''), '[^0-9]', '', 'g'), '') IS NULL
                THEN NULL
                ELSE regexp_replace(s.qtd_acionamentos, '[^0-9]', '', 'g')::integer
            END AS qtd_acionamentos,

            CASE
                WHEN NULLIF(btrim(COALESCE(s.matriculado, s.flag_matriculado)), '') IS NULL
                THEN NULL
                ELSE modelo_estrela.to_bool_any(COALESCE(s.matriculado, s.flag_matriculado))
            END AS flag_matriculado,

            modelo_estrela.parse_ts_any(s.data_ultima_acao) AS data_ultima_acao,
            modelo_estrela.parse_ts_any(s.data_disparo) AS data_disparo,
            COALESCE(s.dt_upload, now()) AS dt_upload,

            ROW_NUMBER() OVER (
                PARTITION BY COALESCE(
                    CASE
                        WHEN modelo_estrela.fn_cpf_valido_basico(s.cpf)
                        THEN 'CPF:' || modelo_estrela.fn_somente_numeros(s.cpf)
                    END,
                    CASE
                        WHEN modelo_estrela.fn_celular_valido_basico(s.celular)
                        THEN 'CEL:' || modelo_estrela.fn_somente_numeros(s.celular)
                    END,
                    'LINHA:' || s.id::text
                )
                ORDER BY s.dt_upload DESC NULLS LAST,
                         s.linha_arquivo DESC NULLS LAST,
                         s.id DESC
            ) AS rn
        FROM modelo_estrela.stg_leads_site s
        WHERE s.upload_id = p_upload_id
    ),
    celulares_unicos AS (
        SELECT
            modelo_estrela.fn_somente_numeros(celular) AS celular_limpo,
            MIN(sk_pessoa) AS sk_pessoa
        FROM modelo_estrela.dim_pessoa
        WHERE modelo_estrela.fn_celular_valido_basico(celular)
        GROUP BY 1
        HAVING COUNT(*) = 1
    )
    SELECT
        b.*,
        COALESCE(pcpf.sk_pessoa, pcel.sk_pessoa) AS sk_pessoa_dim
    FROM base b
    LEFT JOIN modelo_estrela.dim_pessoa pcpf
      ON modelo_estrela.fn_cpf_valido_basico(b.cpf_limpo)
     AND modelo_estrela.fn_somente_numeros(pcpf.cpf) = b.cpf_limpo
    LEFT JOIN celulares_unicos pcel
      ON pcpf.sk_pessoa IS NULL
     AND b.celular_limpo = pcel.celular_limpo
    WHERE b.rn = 1;

    /*
     * Atualiza os campos variáveis diretamente.
     * Não há COALESCE com o valor antigo: vazio no arquivo deve limpar.
     */
    UPDATE modelo_estrela.leads_painel_lite l
    SET
        consultor_comercial = t.consultor_comercial,
        consultor_disparo = t.consultor_disparo,
        status = t.status,
        status_inscricao = t.status_inscricao,
        campanha = t.campanha,
        canal = t.canal,
        acao_comercial = t.acao_comercial,
        tipo_disparo = t.tipo_disparo,
        peca_disparo = t.peca_disparo,
        texto_disparo = t.texto_disparo,
        observacao = t.observacao,
        qtd_acionamentos = t.qtd_acionamentos,
        flag_matriculado = t.flag_matriculado,
        data_ultima_acao = t.data_ultima_acao,
        data_disparo = t.data_disparo,
        data_atualizacao = GREATEST(
            COALESCE(l.data_atualizacao, timestamp '1900-01-01'),
            COALESCE(t.dt_upload::timestamp, now()::timestamp)
        ),
        dt_upload = GREATEST(COALESCE(l.dt_upload, t.dt_upload), t.dt_upload)
    FROM tmp_variaveis_upload t
    WHERE t.sk_pessoa_dim IS NOT NULL
      AND l.sk_pessoa_dim = t.sk_pessoa_dim
      AND ROW(
          l.consultor_comercial,
          l.consultor_disparo,
          l.status,
          l.status_inscricao,
          l.campanha,
          l.canal,
          l.acao_comercial,
          l.tipo_disparo,
          l.peca_disparo,
          l.texto_disparo,
          l.observacao,
          l.qtd_acionamentos,
          l.flag_matriculado,
          l.data_ultima_acao,
          l.data_disparo
      ) IS DISTINCT FROM ROW(
          t.consultor_comercial,
          t.consultor_disparo,
          t.status,
          t.status_inscricao,
          t.campanha,
          t.canal,
          t.acao_comercial,
          t.tipo_disparo,
          t.peca_disparo,
          t.texto_disparo,
          t.observacao,
          t.qtd_acionamentos,
          t.flag_matriculado,
          t.data_ultima_acao,
          t.data_disparo
      );

    GET DIAGNOSTICS v_variaveis_alteradas = ROW_COUNT;

    /* Mantém dim_consultor e f_lead coerentes com a tabela operacional. */
    INSERT INTO modelo_estrela.dim_consultor (
        consultor_comercial,
        consultor_disparo,
        updated_at
    )
    SELECT DISTINCT
        t.consultor_comercial,
        t.consultor_disparo,
        now()
    FROM tmp_variaveis_upload t
    WHERE t.sk_pessoa_dim IS NOT NULL
      AND (t.consultor_comercial IS NOT NULL OR t.consultor_disparo IS NOT NULL)
      AND NOT EXISTS (
          SELECT 1
          FROM modelo_estrela.dim_consultor dc
          WHERE dc.consultor_comercial IS NOT DISTINCT FROM t.consultor_comercial
            AND dc.consultor_disparo IS NOT DISTINCT FROM t.consultor_disparo
      );

    UPDATE modelo_estrela.f_lead f
    SET
        sk_consultor = dc.sk_consultor,
        data_ultima_acao = t.data_ultima_acao,
        data_disparo = t.data_disparo,
        qtd_acionamentos = COALESCE(t.qtd_acionamentos, 0),
        data_atualizacao = GREATEST(
            COALESCE(f.data_atualizacao, timestamp '1900-01-01'),
            COALESCE(t.dt_upload::timestamp, now()::timestamp)
        )
    FROM tmp_variaveis_upload t
    LEFT JOIN modelo_estrela.dim_consultor dc
      ON dc.consultor_comercial IS NOT DISTINCT FROM t.consultor_comercial
     AND dc.consultor_disparo IS NOT DISTINCT FROM t.consultor_disparo
    WHERE t.sk_pessoa_dim IS NOT NULL
      AND f.sk_pessoa = t.sk_pessoa_dim;

    UPDATE modelo_estrela.logs_importacoes li
    SET
        mensagem = CONCAT_WS(
            ' ',
            NULLIF(li.mensagem, ''),
            v_variaveis_alteradas || ' lead(s) com campos operacionais realmente alterados.'
        ),
        atualizado_em = now()
    WHERE li.upload_id = p_upload_id;

    RETURN QUERY
    SELECT
        v_result.linhas_recebidas,
        v_result.linhas_processadas,
        v_result.linhas_rejeitadas,
        v_result.linhas_gravadas_staging,
        v_result.duplicados_arquivo,
        v_result.duplicados_banco;
END;
$function$;

COMMENT ON FUNCTION modelo_estrela.sp_processar_stg_leads_site(text) IS
'Wrapper oficial: preserva dados pessoais/acadêmicos e substitui campos operacionais conforme o novo arquivo, inclusive limpando valores vazios.';

COMMIT;
