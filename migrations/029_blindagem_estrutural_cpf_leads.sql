-- Blindagem estrutural contra CPF duplicado na tabela operacional.
-- Funciona independentemente de qual procedure/rota execute a importacao.

BEGIN;

-- Garante compatibilidade com bancos onde a migration de telefone2 ainda nao foi aplicada.
ALTER TABLE modelo_estrela.leads_painel_lite
    ADD COLUMN IF NOT EXISTS telefone2 text;

LOCK TABLE modelo_estrela.leads_painel_lite IN SHARE ROW EXCLUSIVE MODE;

-- 1) Consolida telefone alternativo antes de remover duplicados existentes.
WITH classificados AS (
    SELECT
        ctid,
        regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g') AS cpf_limpo,
        celular,
        telefone2,
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
), alternativos AS (
    SELECT
        principal.ctid AS principal_ctid,
        MIN(
            CASE
                WHEN regexp_replace(COALESCE(outro.celular::text, ''), '[^0-9]', '', 'g') <> ''
                 AND regexp_replace(COALESCE(outro.celular::text, ''), '[^0-9]', '', 'g')
                     IS DISTINCT FROM regexp_replace(COALESCE(principal.celular::text, ''), '[^0-9]', '', 'g')
                THEN regexp_replace(COALESCE(outro.celular::text, ''), '[^0-9]', '', 'g')
            END
        ) AS telefone_alternativo
    FROM classificados principal
    JOIN classificados outro
      ON outro.cpf_limpo = principal.cpf_limpo
     AND outro.rn > 1
    WHERE principal.rn = 1
    GROUP BY principal.ctid
)
UPDATE modelo_estrela.leads_painel_lite l
SET telefone2 = a.telefone_alternativo
FROM alternativos a
WHERE l.ctid = a.principal_ctid
  AND NULLIF(regexp_replace(COALESCE(l.telefone2::text, ''), '[^0-9]', '', 'g'), '') IS NULL
  AND a.telefone_alternativo IS NOT NULL;

-- 2) Remove duplicados atuais, mantendo o registro mais recente por CPF.
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

-- 3) Trigger defensivo: intercepta qualquer INSERT, mesmo fora das SPs oficiais.
CREATE OR REPLACE FUNCTION modelo_estrela.fn_bloquear_cpf_duplicado_leads()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    v_cpf text;
    v_existente modelo_estrela.leads_painel_lite%ROWTYPE;
    v_celular_novo text;
BEGIN
    v_cpf := NULLIF(regexp_replace(COALESCE(NEW.cpf::text, ''), '[^0-9]', '', 'g'), '');

    IF v_cpf IS NULL THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF EXISTS (
            SELECT 1
            FROM modelo_estrela.leads_painel_lite x
            WHERE x.ctid <> OLD.ctid
              AND regexp_replace(COALESCE(x.cpf::text, ''), '[^0-9]', '', 'g') = v_cpf
        ) THEN
            RAISE EXCEPTION 'CPF duplicado bloqueado: %', v_cpf
                USING ERRCODE = 'unique_violation';
        END IF;
        RETURN NEW;
    END IF;

    SELECT x.*
      INTO v_existente
      FROM modelo_estrela.leads_painel_lite x
     WHERE regexp_replace(COALESCE(x.cpf::text, ''), '[^0-9]', '', 'g') = v_cpf
     ORDER BY x.data_atualizacao DESC NULLS LAST, x.dt_upload DESC NULLS LAST, x.ctid DESC
     LIMIT 1
     FOR UPDATE;

    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    v_celular_novo := NULLIF(regexp_replace(COALESCE(NEW.celular::text, ''), '[^0-9]', '', 'g'), '');

    UPDATE modelo_estrela.leads_painel_lite x
       SET telefone2 = CASE
               WHEN NULLIF(regexp_replace(COALESCE(x.telefone2::text, ''), '[^0-9]', '', 'g'), '') IS NULL
                AND v_celular_novo IS NOT NULL
                AND v_celular_novo IS DISTINCT FROM NULLIF(regexp_replace(COALESCE(x.celular::text, ''), '[^0-9]', '', 'g'), '')
               THEN v_celular_novo
               ELSE x.telefone2
           END,
           data_atualizacao = GREATEST(
               COALESCE(x.data_atualizacao, timestamp '1900-01-01'),
               COALESCE(NEW.data_atualizacao, NEW.dt_upload::timestamp, now()::timestamp)
           ),
           dt_upload = GREATEST(COALESCE(x.dt_upload, NEW.dt_upload), NEW.dt_upload)
     WHERE x.ctid = v_existente.ctid;

    -- Cancela fisicamente a nova linha: o CPF ja existe.
    RETURN NULL;
END;
$function$;

DROP TRIGGER IF EXISTS trg_bloquear_cpf_duplicado_leads
ON modelo_estrela.leads_painel_lite;

CREATE TRIGGER trg_bloquear_cpf_duplicado_leads
BEFORE INSERT OR UPDATE OF cpf
ON modelo_estrela.leads_painel_lite
FOR EACH ROW
EXECUTE FUNCTION modelo_estrela.fn_bloquear_cpf_duplicado_leads();

-- 4) Ultima barreira: o proprio PostgreSQL recusa CPF normalizado repetido.
DROP INDEX IF EXISTS modelo_estrela.ux_leads_painel_cpf_normalizado;

CREATE UNIQUE INDEX ux_leads_painel_cpf_normalizado
ON modelo_estrela.leads_painel_lite (
    regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g')
)
WHERE NULLIF(regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g'), '') IS NOT NULL;

COMMIT;

SELECT
    to_regclass('modelo_estrela.ux_leads_painel_cpf_normalizado') IS NOT NULL AS indice_unico_cpf_ativo,
    EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'trg_bloquear_cpf_duplicado_leads'
          AND NOT tgisinternal
    ) AS trigger_cpf_ativo,
    COUNT(*) AS cpfs_duplicados_restantes
FROM (
    SELECT regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g') AS cpf_limpo
    FROM modelo_estrela.leads_painel_lite
    WHERE NULLIF(regexp_replace(COALESCE(cpf::text, ''), '[^0-9]', '', 'g'), '') IS NOT NULL
    GROUP BY 1
    HAVING COUNT(*) > 1
) d;