-- Otimizacao segura do pipeline modelo_estrela.
-- Requer fila parada e nenhuma sessao de importacao ativa.

BEGIN;

CREATE TABLE IF NOT EXISTS modelo_estrela.backup_funcoes_otimizacao (
    backup_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rotina regprocedure NOT NULL,
    definicao text NOT NULL,
    motivo text NOT NULL,
    criado_em timestamptz NOT NULL DEFAULT now()
);

INSERT INTO modelo_estrela.backup_funcoes_otimizacao (rotina, definicao, motivo)
SELECT
    'modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure,
    pg_get_functiondef('modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure),
    'Antes da migration 027: remove deduplicacao global por upload'
UNION ALL
SELECT
    'modelo_estrela.sp_importar_somente_leads_novos(text)'::regprocedure,
    pg_get_functiondef('modelo_estrela.sp_importar_somente_leads_novos(text)'::regprocedure),
    'Antes da migration 027: restringe mapa temporario aos telefones do upload';

-- A tabela operacional ja possui uma chave unica por pessoa. Sem ela, a
-- deduplicacao global nao pode ser retirada com seguranca.
DO $check_unique$
BEGIN
    IF to_regclass('modelo_estrela.ux_leads_painel_sk_pessoa_dim') IS NULL THEN
        RAISE EXCEPTION
            'Indice ux_leads_painel_sk_pessoa_dim ausente. Execute a manutencao estrutural antes da migration 027.';
    END IF;
END;
$check_unique$;

-- Remove da funcao principal a deduplicacao de TODA leads_painel_lite e o
-- CREATE INDEX executado em cada upload. A chave unica permanente ja impede
-- novas duplicidades. Mantemos apenas a sincronizacao de sk_pessoa limitada
-- as pessoas do upload atual.
DO $patch_principal$
DECLARE
    d text;
    p_inicio integer;
    p_fim integer;
    trecho text;
BEGIN
    SELECT pg_get_functiondef(
        'modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure
    ) INTO d;

    p_inicio := position(
        '    /*' || chr(10) ||
        '     * Blindagem estrutural da tabela operacional.'
        IN d
    );

    p_fim := position(
        '    /*' || chr(10) ||
        '     * Completa a importação parcial com os dados atuais.'
        IN d
    );

    IF p_inicio = 0 OR p_fim = 0 OR p_fim <= p_inicio THEN
        RAISE EXCEPTION
            'Nao foi possivel localizar o bloco de deduplicacao global na funcao principal.';
    END IF;

    trecho := $replacement$    /*
     * A unicidade estrutural ja e garantida pelo indice permanente
     * ux_leads_painel_sk_pessoa_dim. Atualiza somente registros envolvidos
     * neste upload, evitando varrer toda leads_painel_lite.
     */
    UPDATE modelo_estrela.leads_painel_lite l
    SET sk_pessoa = md5('DIM:' || l.sk_pessoa_dim::text)
    FROM (
        SELECT DISTINCT sk_pessoa_dim
        FROM tmp_leads_importacao
        WHERE sk_pessoa_dim IS NOT NULL
    ) u
    WHERE l.sk_pessoa_dim = u.sk_pessoa_dim
      AND l.sk_pessoa IS DISTINCT FROM md5('DIM:' || l.sk_pessoa_dim::text);

$replacement$;

    d := overlay(d placing trecho from p_inicio for (p_fim - p_inicio));
    EXECUTE d;
END;
$patch_principal$;

-- A funcao "somente novos" montava uma copia de todos os celulares da
-- dim_pessoa em cada upload. Agora materializa somente pessoas cujos telefones
-- aparecem no arquivo atual, usando os indices permanentes normalizados.
DO $patch_somente_novos$
DECLARE
    d text;
    p_inicio integer;
    p_fim integer;
    trecho text;
BEGIN
    SELECT pg_get_functiondef(
        'modelo_estrela.sp_importar_somente_leads_novos(text)'::regprocedure
    ) INTO d;

    p_inicio := position(
        '    CREATE TEMP TABLE tmp_telefones_existentes'
        IN d
    );

    p_fim := position(
        '    CREATE TEMP TABLE tmp_triagem_somente_novos'
        IN d
    );

    IF p_inicio = 0 OR p_fim = 0 OR p_fim <= p_inicio THEN
        RAISE EXCEPTION
            'Nao foi possivel localizar o mapa temporario de telefones na funcao somente novos.';
    END IF;

    trecho := $replacement$    CREATE TEMP TABLE tmp_telefones_existentes
    ON COMMIT DROP AS
    WITH telefones_upload AS (
        SELECT DISTINCT
            modelo_estrela.fn_somente_numeros(s.celular) AS telefone_limpo
        FROM modelo_estrela.stg_leads_site s
        WHERE s.upload_id = p_upload_id
          AND modelo_estrela.fn_somente_numeros(s.celular) IS NOT NULL
    )
    SELECT p.sk_pessoa, u.telefone_limpo
    FROM telefones_upload u
    JOIN modelo_estrela.dim_pessoa p
      ON regexp_replace(COALESCE(p.celular, ''), '[^0-9]', '', 'g') = u.telefone_limpo
    UNION
    SELECT p.sk_pessoa, u.telefone_limpo
    FROM telefones_upload u
    JOIN modelo_estrela.dim_pessoa p
      ON regexp_replace(COALESCE(p.telefone2, ''), '[^0-9]', '', 'g') = u.telefone_limpo;

    CREATE INDEX ON tmp_telefones_existentes (telefone_limpo, sk_pessoa);
    ANALYZE tmp_telefones_existentes;

$replacement$;

    d := overlay(d placing trecho from p_inicio for (p_fim - p_inicio));

    -- Faz a consulta de CPF usar exatamente a expressao do indice permanente.
    d := replace(
        d,
        'modelo_estrela.fn_somente_numeros(p.cpf) = n.cpf_limpo',
        'regexp_replace(COALESCE(p.cpf, ''''), ''[^0-9]'', '''', ''g'') = n.cpf_limpo'
    );

    EXECUTE d;
END;
$patch_somente_novos$;

-- Ajustes de autovacuum para tabelas com alta rotatividade.
ALTER TABLE modelo_estrela.stg_leads_site SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.005,
    autovacuum_vacuum_threshold = 300,
    autovacuum_analyze_threshold = 150
);

ALTER TABLE modelo_estrela.leads_painel_lite SET (
    autovacuum_vacuum_scale_factor = 0.03,
    autovacuum_analyze_scale_factor = 0.015,
    autovacuum_vacuum_threshold = 500,
    autovacuum_analyze_threshold = 250
);

ALTER TABLE modelo_estrela.f_lead SET (
    autovacuum_vacuum_scale_factor = 0.03,
    autovacuum_analyze_scale_factor = 0.015,
    autovacuum_vacuum_threshold = 500,
    autovacuum_analyze_threshold = 250
);

COMMIT;

-- Validacoes rapidas.
SELECT
    position(
        'Blindagem estrutural da tabela operacional'
        IN pg_get_functiondef(
            'modelo_estrela.sp_processar_stg_leads_site(text)'::regprocedure
        )
    ) = 0 AS deduplicacao_global_removida,
    position(
        'WITH telefones_upload AS'
        IN pg_get_functiondef(
            'modelo_estrela.sp_importar_somente_leads_novos(text)'::regprocedure
        )
    ) > 0 AS triagem_restrita_ao_upload,
    to_regclass('modelo_estrela.ux_leads_painel_sk_pessoa_dim') IS NOT NULL
        AS unicidade_operacional_ativa;
