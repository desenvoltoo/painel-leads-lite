-- Corrige o fluxo "somente novos" e reduz leituras globais da dim_pessoa.
-- Requer fila parada e nenhuma importacao ativa.

BEGIN;

INSERT INTO modelo_estrela.backup_funcoes_otimizacao (rotina, definicao, motivo)
SELECT
    'modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure,
    pg_get_functiondef('modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure),
    'Antes da migration 028: filtrar somente linhas pendentes e restringir celulares ao upload';

DO $patch$
DECLARE
    d text;
BEGIN
    SELECT pg_get_functiondef(
        'modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure
    ) INTO d;

    -- A rotina wrapper marca como processadas as linhas que nao sao novas.
    -- A principal deve trabalhar apenas com as linhas ainda pendentes.
    d := replace(
        d,
        'FROM modelo_estrela.stg_leads_site' || chr(10) ||
        '    WHERE upload_id = p_upload_id;',
        'FROM modelo_estrela.stg_leads_site' || chr(10) ||
        '    WHERE upload_id = p_upload_id' || chr(10) ||
        '      AND COALESCE(processado, false) = false;'
    );

    d := replace(
        d,
        'FROM modelo_estrela.stg_leads_site s' || chr(10) ||
        '        WHERE s.upload_id = p_upload_id',
        'FROM modelo_estrela.stg_leads_site s' || chr(10) ||
        '        WHERE s.upload_id = p_upload_id' || chr(10) ||
        '          AND COALESCE(s.processado, false) = false'
    );

    d := replace(
        d,
        'FROM modelo_estrela.stg_leads_site s' || chr(10) ||
        '    WHERE s.upload_id = p_upload_id' || chr(10) ||
        '      AND NOT modelo_estrela.fn_cpf_valido_basico(s.cpf)',
        'FROM modelo_estrela.stg_leads_site s' || chr(10) ||
        '    WHERE s.upload_id = p_upload_id' || chr(10) ||
        '      AND COALESCE(s.processado, false) = false' || chr(10) ||
        '      AND NOT modelo_estrela.fn_cpf_valido_basico(s.cpf)'
    );

    d := replace(
        d,
        'WHERE upload_id = p_upload_id;' || chr(10) || chr(10) ||
        '    UPDATE modelo_estrela.logs_importacoes',
        'WHERE upload_id = p_upload_id' || chr(10) ||
        '      AND COALESCE(processado, false) = false;' || chr(10) || chr(10) ||
        '    UPDATE modelo_estrela.logs_importacoes'
    );

    -- Restringe a verificacao de unicidade de celular apenas aos celulares
    -- presentes no upload atual, evitando GROUP BY sobre toda dim_pessoa.
    d := replace(
        d,
        'WITH celulares_unicos AS (' || chr(10) ||
        '         SELECT' || chr(10) ||
        '             modelo_estrela.fn_somente_numeros(celular)' || chr(10) ||
        '                 AS celular_limpo,' || chr(10) ||
        '             MIN(sk_pessoa) AS sk_pessoa' || chr(10) ||
        '         FROM modelo_estrela.dim_pessoa' || chr(10) ||
        '         WHERE modelo_estrela.fn_celular_valido_basico(celular)' || chr(10) ||
        '         GROUP BY 1' || chr(10) ||
        '         HAVING COUNT(*) = 1' || chr(10) ||
        '     )',
        'WITH celulares_upload AS (' || chr(10) ||
        '         SELECT DISTINCT celular_limpo' || chr(10) ||
        '         FROM tmp_leads_importacao' || chr(10) ||
        '         WHERE modelo_estrela.fn_celular_valido_basico(celular_limpo)' || chr(10) ||
        '     ), celulares_unicos AS (' || chr(10) ||
        '         SELECT u.celular_limpo, MIN(p.sk_pessoa) AS sk_pessoa' || chr(10) ||
        '         FROM celulares_upload u' || chr(10) ||
        '         JOIN modelo_estrela.dim_pessoa p' || chr(10) ||
        '           ON regexp_replace(COALESCE(p.celular, ''''), ''[^0-9]'', '''', ''g'') = u.celular_limpo' || chr(10) ||
        '         GROUP BY u.celular_limpo' || chr(10) ||
        '         HAVING COUNT(*) = 1' || chr(10) ||
        '     )'
    );

    d := replace(
        d,
        'WITH celulares_unicos_finais AS (' || chr(10) ||
        '         SELECT' || chr(10) ||
        '             modelo_estrela.fn_somente_numeros(celular) AS celular_limpo,' || chr(10) ||
        '             MIN(sk_pessoa) AS sk_pessoa' || chr(10) ||
        '         FROM modelo_estrela.dim_pessoa' || chr(10) ||
        '         WHERE modelo_estrela.fn_celular_valido_basico(celular)' || chr(10) ||
        '         GROUP BY 1' || chr(10) ||
        '         HAVING COUNT(*) = 1' || chr(10) ||
        '     )',
        'WITH celulares_upload_finais AS (' || chr(10) ||
        '         SELECT DISTINCT celular_limpo' || chr(10) ||
        '         FROM tmp_leads_importacao' || chr(10) ||
        '         WHERE modelo_estrela.fn_celular_valido_basico(celular_limpo)' || chr(10) ||
        '     ), celulares_unicos_finais AS (' || chr(10) ||
        '         SELECT u.celular_limpo, MIN(p.sk_pessoa) AS sk_pessoa' || chr(10) ||
        '         FROM celulares_upload_finais u' || chr(10) ||
        '         JOIN modelo_estrela.dim_pessoa p' || chr(10) ||
        '           ON regexp_replace(COALESCE(p.celular, ''''), ''[^0-9]'', '''', ''g'') = u.celular_limpo' || chr(10) ||
        '         GROUP BY u.celular_limpo' || chr(10) ||
        '         HAVING COUNT(*) = 1' || chr(10) ||
        '     )'
    );

    EXECUTE d;
END;
$patch$;

COMMIT;

SELECT
    position(
        'AND COALESCE(s.processado, false) = false'
        IN pg_get_functiondef(
            'modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure
        )
    ) > 0 AS somente_pendentes_ativo,
    position(
        'WITH celulares_upload AS'
        IN pg_get_functiondef(
            'modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure
        )
    ) > 0 AS celulares_restritos_ao_upload,
    position(
        'WITH celulares_upload_finais AS'
        IN pg_get_functiondef(
            'modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure
        )
    ) > 0 AS celulares_finais_restritos;
