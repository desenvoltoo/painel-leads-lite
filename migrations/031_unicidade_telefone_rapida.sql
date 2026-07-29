BEGIN;

ALTER TABLE modelo_estrela.leads_painel_lite
  ADD COLUMN IF NOT EXISTS telefone2 text;

LOCK TABLE modelo_estrela.leads_painel_lite IN SHARE ROW EXCLUSIVE MODE;

-- Backup linear e rapido da tabela antes da limpeza.
DROP TABLE IF EXISTS modelo_estrela.backup_leads_antes_telefone_031;
CREATE TABLE modelo_estrela.backup_leads_antes_telefone_031 AS
SELECT * FROM modelo_estrela.leads_painel_lite;

-- Materializa cada telefone uma unica vez.
CREATE TEMP TABLE tmp_tel_ocorrencias ON COMMIT DROP AS
SELECT
  l.ctid,
  regexp_replace(COALESCE(l.cpf::text,''),'[^0-9]','','g') AS cpf_limpo,
  t.telefone,
  COALESCE(
    l.data_atualizacao::timestamptz,
    l.dt_upload,
    l.data_matricula::timestamptz,
    l.data_inscricao::timestamptz,
    '-infinity'::timestamptz
  ) AS data_ref
FROM modelo_estrela.leads_painel_lite l
CROSS JOIN LATERAL (
  VALUES
    (NULLIF(regexp_replace(COALESCE(l.celular::text,''),'[^0-9]','','g'),'')),
    (NULLIF(regexp_replace(COALESCE(l.telefone2::text,''),'[^0-9]','','g'),''))
) t(telefone)
WHERE t.telefone IS NOT NULL;

CREATE INDEX ON tmp_tel_ocorrencias (telefone, data_ref DESC, ctid);
ANALYZE tmp_tel_ocorrencias;

-- Mesmo CPF: remove telefone2 igual ao celular.
UPDATE modelo_estrela.leads_painel_lite
SET telefone2 = NULL
WHERE NULLIF(regexp_replace(COALESCE(celular::text,''),'[^0-9]','','g'),'')
    = NULLIF(regexp_replace(COALESCE(telefone2::text,''),'[^0-9]','','g'),'');

-- CPF diferente com mesmo telefone: mantém a linha mais recente e remove as antigas.
WITH ranqueados AS (
  SELECT
    ctid,
    telefone,
    cpf_limpo,
    ROW_NUMBER() OVER (
      PARTITION BY telefone
      ORDER BY data_ref DESC, ctid DESC
    ) AS rn,
    FIRST_VALUE(cpf_limpo) OVER (
      PARTITION BY telefone
      ORDER BY data_ref DESC, ctid DESC
    ) AS cpf_vencedor
  FROM tmp_tel_ocorrencias
), perdedores AS (
  SELECT DISTINCT ctid
  FROM ranqueados
  WHERE rn > 1
    AND cpf_limpo IS DISTINCT FROM cpf_vencedor
)
DELETE FROM modelo_estrela.leads_painel_lite l
USING perdedores p
WHERE l.ctid = p.ctid;

-- Indices para a blindagem futura ser rapida.
CREATE INDEX IF NOT EXISTS idx_leads_celular_normalizado
ON modelo_estrela.leads_painel_lite
((regexp_replace(COALESCE(celular::text,''),'[^0-9]','','g')));

CREATE INDEX IF NOT EXISTS idx_leads_telefone2_normalizado
ON modelo_estrela.leads_painel_lite
((regexp_replace(COALESCE(telefone2::text,''),'[^0-9]','','g')));

CREATE OR REPLACE FUNCTION modelo_estrela.fn_blindar_telefone_duplicado()
RETURNS trigger
LANGUAGE plpgsql
AS $fn$
DECLARE
  v_cpf text;
  v_tel text;
  v_data_nova timestamptz;
  v_old record;
BEGIN
  v_cpf := NULLIF(regexp_replace(COALESCE(NEW.cpf::text,''),'[^0-9]','','g'),'');
  v_data_nova := COALESCE(NEW.data_atualizacao::timestamptz, NEW.dt_upload, NEW.data_matricula::timestamptz, NEW.data_inscricao::timestamptz, now());

  FOR v_tel IN
    SELECT DISTINCT x.telefone
    FROM (VALUES
      (NULLIF(regexp_replace(COALESCE(NEW.celular::text,''),'[^0-9]','','g'),'')),
      (NULLIF(regexp_replace(COALESCE(NEW.telefone2::text,''),'[^0-9]','','g'),''))
    ) x(telefone)
    WHERE x.telefone IS NOT NULL
  LOOP
    PERFORM pg_advisory_xact_lock(hashtext('TEL:' || v_tel));

    SELECT l.ctid,
           NULLIF(regexp_replace(COALESCE(l.cpf::text,''),'[^0-9]','','g'),'') AS cpf_limpo,
           COALESCE(l.data_atualizacao::timestamptz,l.dt_upload,l.data_matricula::timestamptz,l.data_inscricao::timestamptz,'-infinity'::timestamptz) AS data_ref
      INTO v_old
      FROM modelo_estrela.leads_painel_lite l
     WHERE (TG_OP = 'INSERT' OR l.ctid <> OLD.ctid)
       AND (
         regexp_replace(COALESCE(l.celular::text,''),'[^0-9]','','g') = v_tel
         OR regexp_replace(COALESCE(l.telefone2::text,''),'[^0-9]','','g') = v_tel
       )
     ORDER BY data_ref DESC, l.ctid DESC
     LIMIT 1;

    IF FOUND AND v_old.cpf_limpo IS DISTINCT FROM v_cpf THEN
      IF v_data_nova >= v_old.data_ref THEN
        DELETE FROM modelo_estrela.leads_painel_lite WHERE ctid = v_old.ctid;
      ELSE
        RETURN NULL;
      END IF;
    END IF;
  END LOOP;

  RETURN NEW;
END;
$fn$;

DROP TRIGGER IF EXISTS trg_resolver_telefone_por_recencia ON modelo_estrela.leads_painel_lite;
DROP TRIGGER IF EXISTS trg_blindar_telefone_duplicado ON modelo_estrela.leads_painel_lite;

CREATE TRIGGER trg_blindar_telefone_duplicado
BEFORE INSERT OR UPDATE OF cpf, celular, telefone2, data_atualizacao, dt_upload
ON modelo_estrela.leads_painel_lite
FOR EACH ROW
EXECUTE FUNCTION modelo_estrela.fn_blindar_telefone_duplicado();

COMMIT;

WITH telefones AS (
  SELECT regexp_replace(COALESCE(cpf::text,''),'[^0-9]','','g') cpf_limpo, t.telefone
  FROM modelo_estrela.leads_painel_lite l
  CROSS JOIN LATERAL (VALUES
    (NULLIF(regexp_replace(COALESCE(l.celular::text,''),'[^0-9]','','g'),'')),
    (NULLIF(regexp_replace(COALESCE(l.telefone2::text,''),'[^0-9]','','g'),''))
  ) t(telefone)
  WHERE t.telefone IS NOT NULL
)
SELECT COUNT(*) AS telefones_em_cpfs_diferentes
FROM (
  SELECT telefone
  FROM telefones
  GROUP BY telefone
  HAVING COUNT(DISTINCT cpf_limpo) > 1
) x;