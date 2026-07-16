-- Limpa a staging somente depois que a função oficial concluir com sucesso.
-- Preserva a função original como *_core e mantém a mesma assinatura pública.

BEGIN;

DO $$
BEGIN
  IF to_regprocedure('modelo_estrela.sp_processar_stg_leads_site_core(text)') IS NULL THEN
    ALTER FUNCTION modelo_estrela.sp_processar_stg_leads_site(text)
      RENAME TO sp_processar_stg_leads_site_core;
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION modelo_estrela.sp_processar_stg_leads_site(p_upload_id text)
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
  v_gravadas_staging integer := 0;
  v_duplicados_arquivo integer := 0;
  v_duplicados_banco integer := 0;
BEGIN
  SELECT
    r.linhas_recebidas,
    r.linhas_processadas,
    r.linhas_rejeitadas,
    r.linhas_gravadas_staging,
    r.duplicados_arquivo,
    r.duplicados_banco
  INTO
    v_recebidas,
    v_processadas,
    v_rejeitadas,
    v_gravadas_staging,
    v_duplicados_arquivo,
    v_duplicados_banco
  FROM modelo_estrela.sp_processar_stg_leads_site_core(p_upload_id) r;

  -- Só chega aqui quando o processamento principal terminou sem exceção.
  DELETE FROM modelo_estrela.stg_leads_site
  WHERE upload_id = p_upload_id;

  RETURN QUERY SELECT
    v_recebidas,
    v_processadas,
    v_rejeitadas,
    v_gravadas_staging,
    v_duplicados_arquivo,
    v_duplicados_banco;
END;
$function$;

COMMIT;
