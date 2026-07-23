BEGIN;

ALTER TABLE modelo_estrela.stg_leads_site ADD COLUMN IF NOT EXISTS telefone2 text;
ALTER TABLE modelo_estrela.leads_painel_lite ADD COLUMN IF NOT EXISTS telefone2 text;
ALTER TABLE unifecaf.stg_leads ADD COLUMN IF NOT EXISTS telefone2 text;

UPDATE modelo_estrela.leads_painel_lite l
SET telefone2 = p.telefone2
FROM modelo_estrela.dim_pessoa p
WHERE l.sk_pessoa_dim = p.sk_pessoa
  AND l.telefone2 IS DISTINCT FROM p.telefone2;

CREATE OR REPLACE FUNCTION modelo_estrela.fn_sync_telefone2_painel()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.telefone2 IS DISTINCT FROM OLD.telefone2 THEN
        UPDATE modelo_estrela.leads_painel_lite
        SET telefone2 = NEW.telefone2
        WHERE sk_pessoa_dim = NEW.sk_pessoa;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_telefone2_painel ON modelo_estrela.dim_pessoa;
CREATE TRIGGER trg_sync_telefone2_painel
AFTER UPDATE OF telefone2 ON modelo_estrela.dim_pessoa
FOR EACH ROW EXECUTE FUNCTION modelo_estrela.fn_sync_telefone2_painel();

CREATE OR REPLACE FUNCTION modelo_estrela.fn_staging_telefone2_existente()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_cpf text;
    v_tel2 text;
BEGIN
    v_cpf := modelo_estrela.fn_somente_numeros(NEW.cpf);
    v_tel2 := modelo_estrela.fn_somente_numeros(NEW.telefone2);
    IF v_cpf IS NOT NULL AND v_tel2 IS NOT NULL THEN
        UPDATE modelo_estrela.dim_pessoa p
        SET telefone2 = v_tel2,
            updated_at = now()
        WHERE modelo_estrela.fn_somente_numeros(p.cpf) = v_cpf
          AND modelo_estrela.fn_somente_numeros(p.celular) IS DISTINCT FROM v_tel2
          AND (p.telefone2 IS NULL OR modelo_estrela.fn_somente_numeros(p.telefone2) = v_tel2);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_staging_telefone2_existente ON modelo_estrela.stg_leads_site;
CREATE TRIGGER trg_staging_telefone2_existente
AFTER INSERT OR UPDATE OF telefone2, cpf ON modelo_estrela.stg_leads_site
FOR EACH ROW EXECUTE FUNCTION modelo_estrela.fn_staging_telefone2_existente();

CREATE OR REPLACE FUNCTION unifecaf.fn_staging_telefone2_existente()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_cpf text;
    v_tel2 text;
BEGIN
    v_cpf := unifecaf.fn_digits(NEW.cpf);
    v_tel2 := unifecaf.fn_digits(NEW.telefone2);
    IF v_cpf IS NOT NULL AND v_tel2 IS NOT NULL THEN
        UPDATE unifecaf.dim_pessoa p
        SET telefone2 = v_tel2,
            atualizado_em = now()
        WHERE unifecaf.fn_digits(p.cpf) = v_cpf
          AND unifecaf.fn_digits(p.celular) IS DISTINCT FROM v_tel2
          AND (p.telefone2 IS NULL OR unifecaf.fn_digits(p.telefone2) = v_tel2);
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_staging_telefone2_existente ON unifecaf.stg_leads;
CREATE TRIGGER trg_staging_telefone2_existente
AFTER INSERT OR UPDATE OF telefone2, cpf ON unifecaf.stg_leads
FOR EACH ROW EXECUTE FUNCTION unifecaf.fn_staging_telefone2_existente();

COMMIT;
