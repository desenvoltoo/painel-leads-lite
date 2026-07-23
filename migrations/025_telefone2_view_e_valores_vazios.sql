-- Expondo telefone2 nas views oficiais sem regravar grandes volumes.
-- Os marcadores de vazio sao tratados na aplicacao para evitar UPDATE massivo.

BEGIN;

ALTER TABLE modelo_estrela.dim_pessoa
    ADD COLUMN IF NOT EXISTS telefone2 text;

ALTER TABLE unifecaf.dim_pessoa
    ADD COLUMN IF NOT EXISTS telefone2 text;

DO $migration$
DECLARE
    v_def text;
BEGIN
    IF to_regclass('modelo_estrela.vw_leads_painel_lite') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema = 'modelo_estrela'
             AND table_name = 'vw_leads_painel_lite'
             AND column_name = 'telefone2'
       ) THEN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'modelo_estrela'
              AND table_name = 'vw_leads_painel_lite'
              AND column_name = 'sk_pessoa_dim'
        ) THEN
            RAISE EXCEPTION 'A view modelo_estrela.vw_leads_painel_lite nao possui sk_pessoa_dim';
        END IF;

        SELECT pg_get_viewdef('modelo_estrela.vw_leads_painel_lite'::regclass, true)
          INTO v_def;

        EXECUTE format(
            'CREATE OR REPLACE VIEW modelo_estrela.vw_leads_painel_lite AS
             SELECT base.*, NULLIF(NULLIF(BTRIM(p.telefone2), E''\\N''), E''\\\\N'') AS telefone2
             FROM (%s) base
             LEFT JOIN modelo_estrela.dim_pessoa p
               ON p.sk_pessoa = base.sk_pessoa_dim',
            v_def
        );
    END IF;
END;
$migration$;

DO $migration$
DECLARE
    v_def text;
BEGIN
    IF to_regclass('unifecaf.vw_leads_painel_lite') IS NOT NULL
       AND NOT EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema = 'unifecaf'
             AND table_name = 'vw_leads_painel_lite'
             AND column_name = 'telefone2'
       ) THEN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'unifecaf'
              AND table_name = 'vw_leads_painel_lite'
              AND column_name = 'sk_pessoa_dim'
        ) THEN
            RAISE EXCEPTION 'A view unifecaf.vw_leads_painel_lite nao possui sk_pessoa_dim';
        END IF;

        SELECT pg_get_viewdef('unifecaf.vw_leads_painel_lite'::regclass, true)
          INTO v_def;

        EXECUTE format(
            'CREATE OR REPLACE VIEW unifecaf.vw_leads_painel_lite AS
             SELECT base.*, NULLIF(NULLIF(BTRIM(p.telefone2), E''\\N''), E''\\\\N'') AS telefone2
             FROM (%s) base
             LEFT JOIN unifecaf.dim_pessoa p
               ON p.sk_pessoa = base.sk_pessoa_dim',
            v_def
        );
    END IF;
END;
$migration$;

COMMIT;
