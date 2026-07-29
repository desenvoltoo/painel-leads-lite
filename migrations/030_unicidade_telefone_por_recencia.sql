-- Regras:
-- 1) Mesmo CPF: manter uma linha (a mais recente) e mover um celular alternativo para telefone2.
-- 2) Mesmo telefone em CPFs diferentes: manter somente o registro mais recente e apagar os antigos.
-- 3) Impedir que novas importacoes recriem o conflito.

BEGIN;

ALTER TABLE modelo_estrela.leads_painel_lite
    ADD COLUMN IF NOT EXISTS telefone2 text;

LOCK TABLE modelo_estrela.leads_painel_lite IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE IF NOT EXISTS modelo_estrela.backup_leads_conflito_telefone_030
(LIKE modelo_estrela.leads_painel_lite INCLUDING ALL);

INSERT INTO modelo_estrela.backup_leads_conflito_telefone_030
SELECT l.*
FROM modelo_estrela.leads_painel_lite l
WHERE EXISTS (
    SELECT 1
    FROM modelo_estrela.leads_painel_lite x
    WHERE x.ctid <> l.ctid
      AND (
          NULLIF(regexp_replace(COALESCE(l.celular::text, ''), '[^0-9]', '', 'g'), '')
              IN (
                  NULLIF(regexp_replace(COALESCE(x.celular::text, ''), '[^0-9]', '', 'g'), ''),
                  NULLIF(regexp_replace(COALESCE(x.telefone2::text, ''), '[^0-9]', '', 'g'), '')
              )
          OR
          NULLIF(regexp_replace(COALESCE(l.telefone2::text, ''), '[^0-9]', '', 'g'), '')
              IN (
                  NULLIF(regexp_replace(COALESCE(x.celular::text, ''), '[^0-9]', '', 'g'), ''),
                  NULLIF(regexp_replace(COALESCE(x.telefone2::text, ''), '[^0-9]', '', 'g'), '')
              )
      )
)
ON CONFLICT DO NOTHING;

-- Mesmo CPF: captura um telefone alternativo antes de apagar duplicados.
WITH base AS (
    SELECT
        ctid,
        regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g') AS cpf_limpo,
        NULLIF(regexp_replace(COALESCE(celular::text, ''), '[^0-9]', '', 'g'), '') AS celular_limpo,
        NULLIF(regexp_replace(COALESCE(telefone2::text, ''), '[^0-9]', '', 'g'), '') AS telefone2_limpo,
        ROW_NUMBER() OVER (
            PARTITION BY regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g')
            ORDER BY
                data_atualizacao DESC NULLS LAST,
                dt_upload DESC NULLS LAST,
                data_matricula DESC NULLS LAST,
                data_inscricao DESC NULLS LAST,
                ctid DESC
        ) AS rn
    FROM modelo_estrela.leads_painel_lite
    WHERE NULLIF(regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g'), '') IS NOT NULL
), alternativo AS (
    SELECT
        p.ctid AS principal_ctid,
        MIN(t.telefone) FILTER (
            WHERE t.telefone IS NOT NULL
              AND t.telefone IS DISTINCT FROM p.celular_limpo
        ) AS telefone_alternativo
    FROM base p
    JOIN base d
      ON d.cpf_limpo = p.cpf_limpo
     AND d.rn > 1
    CROSS JOIN LATERAL (
        VALUES (d.celular_limpo), (d.telefone2_limpo)
    ) AS t(telefone)
    WHERE p.rn = 1
    GROUP BY p.ctid, p.celular_limpo
)
UPDATE modelo_estrela.leads_painel_lite l
SET telefone2 = a.telefone_alternativo
FROM alternativo a
WHERE l.ctid = a.principal_ctid
  AND NULLIF(regexp_replace(COALESCE(l.telefone2::text, ''), '[^0-9]', '', 'g'), '') IS NULL
  AND a.telefone_alternativo IS NOT NULL;

-- Mesmo CPF: apaga duplicados e mantém o registro mais recente.
WITH classificados AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g')
            ORDER BY
                data_atualizacao DESC NULLS LAST,
                dt_upload DESC NULLS LAST,
                data_matricula DESC NULLS LAST,
                data_inscricao DESC NULLS LAST,
                ctid DESC
        ) AS rn
    FROM modelo_estrela.leads_painel_lite
    WHERE NULLIF(regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g'), '') IS NOT NULL
)
DELETE FROM modelo_estrela.leads_painel_lite l
USING classificados c
WHERE l.ctid = c.ctid
  AND c.rn > 1;

-- Remove telefone2 igual ao celular da própria pessoa.
UPDATE modelo_estrela.leads_painel_lite
SET telefone2 = NULL
WHERE NULLIF(regexp_replace(COALESCE(celular::text, ''), '[^0-9]', '', 'g'), '') IS NOT NULL
  AND regexp_replace(COALESCE(celular::text, ''), '[^0-9]', '', 'g')
      = regexp_replace(COALESCE(telefone2::text, ''), '[^0-9]', '', 'g');

-- Mesmo telefone em CPFs diferentes: apaga a linha antiga inteira.
WITH ocorrencias AS (
    SELECT
        l.ctid,
        regexp_replace(COALESCE(l.cpf::text, ''), '[^0-9]', '', 'g') AS cpf_limpo,
        t.telefone,
        l.data_atualizacao,
        l.dt_upload,
        l.data_matricula,
        l.data_inscricao,
        ROW_NUMBER() OVER (
            PARTITION BY t.telefone
            ORDER BY
                l.data_atualizacao DESC NULLS LAST,
                l.dt_upload DESC NULLS LAST,
                l.data_matricula DESC NULLS LAST,
                l.data_inscricao DESC NULLS LAST,
                l.ctid DESC
        ) AS rn
    FROM modelo_estrela.leads_painel_lite l
    CROSS JOIN LATERAL (
        VALUES
            (NULLIF(regexp_replace(COALESCE(l.celular::text, ''), '[^0-9]', '', 'g'), '')),
            (NULLIF(regexp_replace(COALESCE(l.telefone2::text, ''), '[^0-9]', '', 'g'), ''))
    ) AS t(telefone)
    WHERE t.telefone IS NOT NULL
), perdedores AS (
    SELECT DISTINCT o.ctid
    FROM ocorrencias o
    WHERE o.rn > 1
      AND EXISTS (
          SELECT 1
          FROM ocorrencias vencedor
          WHERE vencedor.telefone = o.telefone
            AND vencedor.rn = 1
            AND vencedor.cpf_limpo IS DISTINCT FROM o.cpf_limpo
      )
)
DELETE FROM modelo_estrela.leads_painel_lite l
USING perdedores p
WHERE l.ctid = p.ctid;

-- Trigger: em nova inserção/alteração, o telefone fica com o CPF mais recente.
CREATE OR REPLACE FUNCTION modelo_estrela.fn_resolver_telefone_por_recencia()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    v_cpf text;
    v_data_nova timestamptz;
    v_tel text;
    v_conflito record;
BEGIN
    v_cpf := NULLIF(regexp_replace(COALESCE(NEW.cpf::text, ''), '[^0-9]', '', 'g'), '');
    v_data_nova := COALESCE(
        NEW.data_atualizacao::timestamptz,
        NEW.dt_upload,
        NEW.data_matricula::timestamptz,
        NEW.data_inscricao::timestamptz,
        now()
    );

    FOR v_tel IN
        SELECT DISTINCT telefone
        FROM (
            VALUES
                (NULLIF(regexp_replace(COALESCE(NEW.celular::text, ''), '[^0-9]', '', 'g'), '')),
                (NULLIF(regexp_replace(COALESCE(NEW.telefone2::text, ''), '[^0-9]', '', 'g'), ''))
        ) AS n(telefone)
        WHERE telefone IS NOT NULL
    LOOP
        PERFORM pg_advisory_xact_lock(hashtext('TEL:' || v_tel));

        FOR v_conflito IN
            SELECT
                x.ctid,
                NULLIF(regexp_replace(COALESCE(x.cpf::text, ''), '[^0-9]', '', 'g'), '') AS cpf_limpo,
                COALESCE(
                    x.data_atualizacao::timestamptz,
                    x.dt_upload,
                    x.data_matricula::timestamptz,
                    x.data_inscricao::timestamptz,
                    '-infinity'::timestamptz
                ) AS data_registro
            FROM modelo_estrela.leads_painel_lite x
            WHERE (TG_OP = 'INSERT' OR x.ctid <> OLD.ctid)
              AND (
                    NULLIF(regexp_replace(COALESCE(x.celular::text, ''), '[^0-9]', '', 'g'), '') = v_tel
                 OR NULLIF(regexp_replace(COALESCE(x.telefone2::text, ''), '[^0-9]', '', 'g'), '') = v_tel
              )
        LOOP
            IF v_conflito.cpf_limpo IS NOT DISTINCT FROM v_cpf THEN
                CONTINUE;
            END IF;

            IF v_data_nova >= v_conflito.data_registro THEN
                DELETE FROM modelo_estrela.leads_painel_lite
                WHERE ctid = v_conflito.ctid;
            ELSE
                -- O registro atual é mais antigo: cancela a nova linha/alteração.
                RETURN NULL;
            END IF;
        END LOOP;
    END LOOP;

    RETURN NEW;
END;
$function$;

DROP TRIGGER IF EXISTS trg_resolver_telefone_por_recencia
ON modelo_estrela.leads_painel_lite;

CREATE TRIGGER trg_resolver_telefone_por_recencia
BEFORE INSERT OR UPDATE OF cpf, celular, telefone2, data_atualizacao, dt_upload
ON modelo_estrela.leads_painel_lite
FOR EACH ROW
EXECUTE FUNCTION modelo_estrela.fn_resolver_telefone_por_recencia();

COMMIT;

WITH telefones AS (
    SELECT
        regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g') AS cpf_limpo,
        t.telefone
    FROM modelo_estrela.leads_painel_lite l
    CROSS JOIN LATERAL (
        VALUES
            (NULLIF(regexp_replace(COALESCE(l.celular::text, ''), '[^0-9]', '', 'g'), '')),
            (NULLIF(regexp_replace(COALESCE(l.telefone2::text, ''), '[^0-9]', '', 'g'), ''))
    ) AS t(telefone)
    WHERE t.telefone IS NOT NULL
)
SELECT
    EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_resolver_telefone_por_recencia'
          AND NOT tgisinternal
    ) AS trigger_telefone_ativo,
    COUNT(*) AS telefones_em_cpfs_diferentes
FROM (
    SELECT telefone
    FROM telefones
    GROUP BY telefone
    HAVING COUNT(DISTINCT cpf_limpo) > 1
) d;