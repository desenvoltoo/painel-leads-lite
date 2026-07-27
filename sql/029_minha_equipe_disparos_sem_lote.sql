BEGIN;

CREATE OR REPLACE VIEW modelo_estrela.vw_op_consultor_momento AS
WITH base AS (
    SELECT
        COALESCE(NULLIF(BTRIM(dc.consultor_disparo), ''), 'SEM CONSULTOR') AS consultor_disparo,
        COALESCE(NULLIF(BTRIM(dd.tipo_disparo), ''), 'SEM TIPO') AS tipo_disparo,
        COALESCE(NULLIF(BTRIM(dca.campanha), ''), 'SEM CAMPANHA') AS campanha,
        COALESCE(NULLIF(BTRIM(dca.canal), ''), 'SEM CANAL') AS canal,
        COALESCE(NULLIF(BTRIM(dd.peca_disparo), ''), 'SEM PEÇA') AS peca_disparo,
        f.data_disparo,
        f.data_ultima_acao,
        COALESCE(ds.status, '') AS status,
        COALESCE(ds.status_inscricao, '') AS status_inscricao,
        COALESCE(ds.matriculado, false) AS matriculado
    FROM modelo_estrela.f_lead f
    LEFT JOIN modelo_estrela.dim_consultor dc
           ON dc.sk_consultor = f.sk_consultor
    LEFT JOIN modelo_estrela.dim_disparo dd
           ON dd.sk_disparo = f.sk_disparo
    LEFT JOIN modelo_estrela.dim_campanha dca
           ON dca.sk_campanha = f.sk_campanha
    LEFT JOIN modelo_estrela.dim_status ds
           ON ds.sk_status = f.sk_status
    WHERE f.data_disparo IS NOT NULL
),
resumo AS (
    SELECT
        consultor_disparo,
        COUNT(*)::bigint AS total_disparado,
        COUNT(*) FILTER (
            WHERE data_disparo::date = CURRENT_DATE
        )::bigint AS disparado_hoje,
        COUNT(*) FILTER (
            WHERE data_disparo >= date_trunc('week', CURRENT_TIMESTAMP)
              AND data_disparo < CURRENT_TIMESTAMP
        )::bigint AS disparado_semana,
        COUNT(*) FILTER (
            WHERE data_disparo >= date_trunc('month', CURRENT_TIMESTAMP)
              AND data_disparo < CURRENT_TIMESTAMP
        )::bigint AS disparado_mes,
        COUNT(*) FILTER (
            WHERE UPPER(status) ~ '(RETORNO|RETORNOU|EM RETORNO)'
               OR UPPER(status_inscricao) ~ '(RETORNO|RETORNOU|EM RETORNO)'
        )::bigint AS retornos,
        COUNT(*) FILTER (
            WHERE UPPER(status) ~ '(POSITIVO|INTERESSADO|MATRICULADO|MATRÍCULA)'
               OR UPPER(status_inscricao) ~ '(POSITIVO|INTERESSADO|MATRICULADO|MATRÍCULA)'
               OR matriculado
        )::bigint AS positivos,
        COUNT(*) FILTER (
            WHERE UPPER(status) ~ '(NEGATIVO|SEM INTERESSE|NÃO INTERESSADO|NAO INTERESSADO)'
               OR UPPER(status_inscricao) ~ '(NEGATIVO|SEM INTERESSE|NÃO INTERESSADO|NAO INTERESSADO)'
        )::bigint AS negativos,
        COUNT(*) FILTER (WHERE matriculado)::bigint AS matriculas,
        MAX(data_disparo) AS ultimo_disparo,
        MAX(COALESCE(data_ultima_acao, data_disparo)) AS ultima_movimentacao
    FROM base
    GROUP BY consultor_disparo
),
detalhes AS (
    SELECT
        consultor_disparo,
        jsonb_agg(
            jsonb_build_object(
                'tipo_disparo', tipo_disparo,
                'campanha', campanha,
                'canal', canal,
                'peca_disparo', peca_disparo,
                'total_disparado', total_disparado,
                'disparado_semana', disparado_semana,
                'ultimo_disparo', ultimo_disparo
            )
            ORDER BY total_disparado DESC, tipo_disparo, campanha
        ) AS detalhes_disparos
    FROM (
        SELECT
            consultor_disparo,
            tipo_disparo,
            campanha,
            canal,
            peca_disparo,
            COUNT(*)::bigint AS total_disparado,
            COUNT(*) FILTER (
                WHERE data_disparo >= date_trunc('week', CURRENT_TIMESTAMP)
                  AND data_disparo < CURRENT_TIMESTAMP
            )::bigint AS disparado_semana,
            MAX(data_disparo) AS ultimo_disparo
        FROM base
        GROUP BY consultor_disparo, tipo_disparo, campanha, canal, peca_disparo
    ) agrupado
    GROUP BY consultor_disparo
)
SELECT
    r.consultor_disparo,
    r.total_disparado,
    r.disparado_hoje,
    r.disparado_semana,
    r.disparado_mes,
    r.retornos,
    r.positivos,
    r.negativos,
    r.matriculas,
    ROUND(100.0 * r.retornos / NULLIF(r.total_disparado, 0), 2) AS taxa_retorno_pct,
    ROUND(100.0 * r.matriculas / NULLIF(r.total_disparado, 0), 2) AS taxa_matricula_pct,
    r.ultimo_disparo,
    r.ultima_movimentacao,
    COALESCE(d.detalhes_disparos, '[]'::jsonb) AS detalhes_disparos,

    -- Compatibilidade temporária com a interface anterior.
    r.total_disparado AS total_leads_em_lote,
    r.total_disparado AS trabalhados,
    0::bigint AS pendentes,
    0::bigint AS em_atendimento,
    100::numeric AS percentual_trabalhado
FROM resumo r
LEFT JOIN detalhes d USING (consultor_disparo);

COMMENT ON VIEW modelo_estrela.vw_op_consultor_momento IS
'Métricas da equipe calculadas por data_disparo diretamente no modelo estrela, sem dependência de lotes.';

COMMIT;
