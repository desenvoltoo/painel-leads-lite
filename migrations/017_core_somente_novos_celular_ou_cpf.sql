-- Core exclusivo do modo SOMENTE NOVOS.
-- Regras:
-- 1. celular tem prioridade;
-- 2. CPF é usado quando celular não existe;
-- 3. rejeita apenas quando celular e CPF estão ausentes;
-- 4. não atualiza leads antigos porque a função externa filtra existentes antes.

CREATE OR REPLACE FUNCTION modelo_estrela.sp_processar_somente_novos_core(p_upload_id text)
RETURNS TABLE(
    linhas_recebidas integer,
    linhas_processadas integer,
    linhas_rejeitadas integer,
    linhas_gravadas_staging integer,
    duplicados_arquivo integer,
    duplicados_banco integer
)
LANGUAGE plpgsql
AS $function$
DECLARE
    v_recebidas integer := 0;
    v_processadas integer := 0;
    v_rejeitadas integer := 0;
    v_duplicados_arquivo integer := 0;
BEGIN
    SELECT COUNT(*)::integer
      INTO v_recebidas
      FROM modelo_estrela.stg_leads_site
     WHERE upload_id = p_upload_id
       AND COALESCE(processado, false) = false;

    DELETE FROM modelo_estrela.logs_rejeicoes_import
     WHERE upload_id = p_upload_id;

    INSERT INTO modelo_estrela.logs_rejeicoes_import(
        upload_id, linha, motivo, campo, valor_mascarado
    )
    SELECT
        p_upload_id,
        linha_arquivo,
        'SEM_IDENTIFICADOR',
        'celular/cpf',
        NULL
    FROM modelo_estrela.stg_leads_site
    WHERE upload_id = p_upload_id
      AND COALESCE(processado, false) = false
      AND modelo_estrela.only_digits(celular) IS NULL
      AND modelo_estrela.only_digits(cpf) IS NULL;

    GET DIAGNOSTICS v_rejeitadas = ROW_COUNT;

    WITH src AS (
        SELECT
            s.*,
            modelo_estrela.only_digits(s.celular) AS celular_limpo,
            modelo_estrela.only_digits(s.cpf) AS cpf_limpo,
            NULLIF(lower(trim(s.email)), '') AS email_limpo,
            COALESCE(
                modelo_estrela.only_digits(s.celular),
                modelo_estrela.only_digits(s.cpf),
                NULLIF(lower(trim(s.email)), ''),
                md5(COALESCE(s.nome, '') || '-' || COALESCE(s.linha_arquivo::text, '') || '-' || COALESCE(p_upload_id, ''))
            ) AS chave_lead
        FROM modelo_estrela.stg_leads_site s
        WHERE s.upload_id = p_upload_id
          AND COALESCE(s.processado, false) = false
          AND (
              modelo_estrela.only_digits(s.celular) IS NOT NULL
              OR modelo_estrela.only_digits(s.cpf) IS NOT NULL
          )
    ),
    ranked AS (
        SELECT
            src.*,
            ROW_NUMBER() OVER (
                PARTITION BY chave_lead
                ORDER BY
                    COALESCE(
                        modelo_estrela.parse_ts_any(data_atualizacao),
                        modelo_estrela.parse_ts_any(data_inscricao),
                        dt_upload,
                        now()
                    ) DESC,
                    id DESC
            ) AS rn,
            COUNT(*) OVER (PARTITION BY chave_lead) AS qtd_dup
        FROM src
    ),
    final AS (
        SELECT
            md5(chave_lead) AS sk_pessoa,
            cpf_limpo AS cpf,
            celular_limpo AS celular,
            NULLIF(trim(nome), '') AS nome,
            NULLIF(trim(email), '') AS email,
            NULLIF(trim(curso), '') AS curso,
            NULLIF(trim(modalidade), '') AS modalidade,
            NULLIF(trim(turno), '') AS turno,
            NULLIF(trim(COALESCE(polo, unidade)), '') AS polo,
            NULLIF(trim(origem), '') AS origem,
            NULLIF(trim(tipo_negocio), '') AS tipo_negocio,
            NULLIF(trim(consultor_comercial), '') AS consultor_comercial,
            NULLIF(trim(consultor_disparo), '') AS consultor_disparo,
            NULLIF(trim(campanha), '') AS campanha,
            NULLIF(trim(canal), '') AS canal,
            NULLIF(trim(acao_comercial), '') AS acao_comercial,
            NULLIF(trim(tipo_disparo), '') AS tipo_disparo,
            NULLIF(trim(peca_disparo), '') AS peca_disparo,
            NULLIF(trim(texto_disparo), '') AS texto_disparo,
            NULLIF(regexp_replace(COALESCE(qtd_acionamentos, ''), '[^0-9]', '', 'g'), '')::integer AS qtd_acionamentos,
            NULLIF(trim(status), '') AS status,
            NULLIF(trim(status_inscricao), '') AS status_inscricao,
            NULLIF(trim(observacao), '') AS observacao,
            (
                modelo_estrela.to_bool_any(flag_matriculado)
                OR modelo_estrela.to_bool_any(matriculado)
                OR upper(trim(COALESCE(status, ''))) = 'MAT'
                OR upper(trim(COALESCE(status_inscricao, ''))) IN ('MATRICULADO', 'MATRÍCULADO', 'MATRICULADOS')
            ) AS flag_matriculado,
            modelo_estrela.parse_ts_any(data_inscricao) AS data_inscricao,
            modelo_estrela.parse_ts_any(data_matricula) AS data_matricula,
            COALESCE(modelo_estrela.parse_ts_any(data_atualizacao), now()) AS data_atualizacao,
            modelo_estrela.parse_ts_any(data_ultima_acao) AS data_ultima_acao,
            modelo_estrela.parse_ts_any(data_disparo) AS data_disparo,
            COALESCE(dt_upload, now()) AS dt_upload,
            qtd_dup
        FROM ranked
        WHERE rn = 1
    ),
    inserted AS (
        INSERT INTO modelo_estrela.leads_painel_lite(
            sk_pessoa, cpf, celular, nome, email, curso, modalidade, turno, polo, origem,
            tipo_negocio, consultor_comercial, consultor_disparo, campanha, canal,
            acao_comercial, tipo_disparo, peca_disparo, texto_disparo, qtd_acionamentos,
            status, status_inscricao, observacao, flag_matriculado, data_inscricao,
            data_matricula, data_atualizacao, data_ultima_acao, data_disparo, dt_upload
        )
        SELECT
            sk_pessoa, cpf, celular, nome, email, curso, modalidade, turno, polo, origem,
            tipo_negocio, consultor_comercial, consultor_disparo, campanha, canal,
            acao_comercial, tipo_disparo, peca_disparo, texto_disparo, COALESCE(qtd_acionamentos, 0),
            status, status_inscricao, observacao, flag_matriculado, data_inscricao,
            data_matricula, data_atualizacao, data_ultima_acao, data_disparo, dt_upload
        FROM final
        ON CONFLICT (sk_pessoa) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*)::integer
      INTO v_processadas
      FROM inserted;

    SELECT COALESCE(SUM(qtd_dup - 1), 0)::integer
      INTO v_duplicados_arquivo
      FROM (
          SELECT
              COALESCE(
                  modelo_estrela.only_digits(celular),
                  modelo_estrela.only_digits(cpf),
                  NULLIF(lower(trim(email)), '')
              ) AS chave,
              COUNT(*) AS qtd_dup
          FROM modelo_estrela.stg_leads_site
          WHERE upload_id = p_upload_id
            AND COALESCE(processado, false) = false
            AND (
                modelo_estrela.only_digits(celular) IS NOT NULL
                OR modelo_estrela.only_digits(cpf) IS NOT NULL
            )
          GROUP BY 1
          HAVING COUNT(*) > 1
      ) d;

    RETURN QUERY
    SELECT
        v_recebidas,
        v_processadas,
        v_rejeitadas,
        v_recebidas,
        v_duplicados_arquivo,
        0;
END;
$function$;

-- Troca somente a chamada interna da função temporária.
-- A função externa continua responsável por classificar celular primeiro,
-- CPF depois e marcar existentes como processados.
DO $migration$
DECLARE
    v_definition text;
BEGIN
    SELECT pg_get_functiondef('modelo_estrela.sp_importar_somente_leads_novos(text)'::regprocedure)
      INTO v_definition;

    IF v_definition IS NULL THEN
        RAISE EXCEPTION 'Função sp_importar_somente_leads_novos(text) não encontrada.';
    END IF;

    v_definition := replace(
        v_definition,
        'modelo_estrela.sp_processar_stg_leads_site_core(p_upload_id)',
        'modelo_estrela.sp_processar_somente_novos_core(p_upload_id)'
    );

    IF position('sp_processar_somente_novos_core' in v_definition) = 0 THEN
        RAISE EXCEPTION 'Não foi possível substituir a chamada core na função somente novos.';
    END IF;

    EXECUTE v_definition;
END;
$migration$;

COMMENT ON FUNCTION modelo_estrela.sp_processar_somente_novos_core(text) IS
'Core exclusivo da importação somente novos: celular primeiro, CPF como fallback e rejeição apenas sem ambos.';
