-- Corrige referencias ambiguas ao parametro/coluna upload_id na funcao
-- modelo_estrela.sp_importar_somente_leads_novos(text).

BEGIN;

DO $fix$
DECLARE
    d text;
    antes text;
BEGIN
    SELECT pg_get_functiondef(
        'modelo_estrela.sp_importar_somente_leads_novos(text)'::regprocedure
    ) INTO d;

    antes := d;

    d := replace(
        d,
        E'FROM modelo_estrela.stg_leads_site\n    WHERE upload_id = p_upload_id',
        E'FROM modelo_estrela.stg_leads_site s\n    WHERE s.upload_id = p_upload_id'
    );

    d := replace(
        d,
        E'FROM modelo_estrela.logs_importacoes\n        WHERE upload_id = p_upload_id',
        E'FROM modelo_estrela.logs_importacoes li\n        WHERE li.upload_id = p_upload_id'
    );

    IF d = antes THEN
        RAISE EXCEPTION 'Nenhuma referencia ambigua foi encontrada para corrigir';
    END IF;

    EXECUTE d;
END;
$fix$;

COMMIT;
