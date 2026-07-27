# -*- coding: utf-8 -*-
"""Endurecimento da importação PostgreSQL.

Aplica correções idempotentes na rotina oficial:
- deduplicação por sk_pessoa_dim antes do UPSERT no mapa;
- remoção de referência inválida a dt_upload na temp table;
- índices de apoio;
- função segura para reprocessar upload parado com advisory lock;
- view de diagnóstico de cargas travadas.
"""
from __future__ import annotations

from . import database as db


def apply_import_hardening() -> dict:
    schema = db._safe_ident((getattr(db, "DB_SCHEMA", None) or "modelo_estrela").strip())

    ddl = f"""
    CREATE INDEX IF NOT EXISTS idx_stg_leads_site_upload_id
      ON {schema}.stg_leads_site (upload_id);

    CREATE INDEX IF NOT EXISTS idx_stg_leads_site_upload_cpf
      ON {schema}.stg_leads_site (upload_id, cpf);

    CREATE INDEX IF NOT EXISTS idx_stg_leads_site_upload_celular
      ON {schema}.stg_leads_site (upload_id, celular);

    CREATE UNIQUE INDEX IF NOT EXISTS ux_mapa_pessoa_painel_sk
      ON {schema}.mapa_pessoa_painel (sk_pessoa_painel);

    DO $hardening$
    DECLARE
      v_oid oid;
      v_def text;
      v_old text;
      v_bad text;
      v_new text;
    BEGIN
      SELECT p.oid
        INTO v_oid
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = '{schema}'
        AND p.proname = 'sp_processar_stg_leads_site'
        AND pg_get_function_identity_arguments(p.oid) = 'p_upload_id text'
      LIMIT 1;

      IF v_oid IS NULL THEN
        RAISE NOTICE 'SP %.sp_processar_stg_leads_site(text) não encontrada; patch ignorado.', '{schema}';
        RETURN;
      END IF;

      v_def := pg_get_functiondef(v_oid);

      v_old := E'    FROM tmp_leads_importacao t\n    ON CONFLICT (sk_pessoa_painel)';
      v_bad := E'    FROM (\n        SELECT DISTINCT ON (sk_pessoa_dim)\n            *\n        FROM tmp_leads_importacao\n        WHERE sk_pessoa_dim IS NOT NULL\n        ORDER BY\n            sk_pessoa_dim,\n            dt_upload DESC NULLS LAST,\n            linha_arquivo DESC NULLS LAST\n    ) t\n    ON CONFLICT (sk_pessoa_painel)';
      v_new := E'    FROM (\n        SELECT DISTINCT ON (sk_pessoa_dim)\n            *\n        FROM tmp_leads_importacao\n        WHERE sk_pessoa_dim IS NOT NULL\n        ORDER BY\n            sk_pessoa_dim,\n            linha_arquivo DESC NULLS LAST\n    ) t\n    ON CONFLICT (sk_pessoa_painel)';

      IF position(v_bad IN v_def) > 0 THEN
        v_def := replace(v_def, v_bad, v_new);
        EXECUTE v_def;
        RAISE NOTICE 'SP corrigida: removida referência inválida a dt_upload.';
      ELSIF position(v_old IN v_def) > 0 THEN
        v_def := replace(v_def, v_old, v_new);
        EXECUTE v_def;
        RAISE NOTICE 'SP corrigida: deduplicação por sk_pessoa_dim aplicada.';
      ELSE
        RAISE NOTICE 'SP já endurecida ou com estrutura diferente; nenhuma substituição necessária.';
      END IF;
    END;
    $hardening$;

    CREATE OR REPLACE FUNCTION {schema}.fn_reprocessar_upload_pendente(p_upload_id text)
    RETURNS jsonb
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = {schema}, public, pg_temp
    AS $fn$
    DECLARE
      v_total bigint;
      v_lock boolean;
      v_result jsonb;
    BEGIN
      IF NULLIF(btrim(p_upload_id), '') IS NULL THEN
        RAISE EXCEPTION 'upload_id obrigatório';
      END IF;

      SELECT pg_try_advisory_xact_lock(hashtext('reprocessar-upload:' || p_upload_id))
        INTO v_lock;
      IF NOT v_lock THEN
        RAISE EXCEPTION 'Upload % já está sendo processado', p_upload_id;
      END IF;

      SELECT count(*) INTO v_total
      FROM {schema}.stg_leads_site
      WHERE upload_id = p_upload_id;

      IF v_total = 0 THEN
        RAISE EXCEPTION 'Nenhuma linha encontrada na staging para o upload_id %', p_upload_id;
      END IF;

      INSERT INTO {schema}.logs_importacoes (
        upload_id, status, etapa, mensagem, criado_em, atualizado_em
      )
      VALUES (
        p_upload_id, 'PROCESSANDO', 'REPROCESSAMENTO_SEGURO',
        'Reprocessamento seguro iniciado.', now(), now()
      )
      ON CONFLICT (upload_id) DO UPDATE SET
        status = 'PROCESSANDO',
        etapa = 'REPROCESSAMENTO_SEGURO',
        mensagem = concat_ws(' ', nullif({schema}.logs_importacoes.mensagem, ''), 'Reprocessamento seguro iniciado.'),
        atualizado_em = now();

      SELECT to_jsonb(r) INTO v_result
      FROM {schema}.sp_processar_stg_leads_site(p_upload_id) r;

      RETURN jsonb_build_object(
        'ok', true,
        'upload_id', p_upload_id,
        'linhas_staging', v_total,
        'resultado', coalesce(v_result, '{{}}'::jsonb)
      );
    EXCEPTION WHEN OTHERS THEN
      UPDATE {schema}.logs_importacoes
         SET status = 'ERRO',
             etapa = 'REPROCESSAMENTO_SEGURO',
             mensagem = concat_ws(' ', nullif(mensagem, ''), 'Erro:', SQLERRM),
             atualizado_em = now()
       WHERE upload_id = p_upload_id;
      RAISE;
    END;
    $fn$;

    CREATE OR REPLACE VIEW {schema}.vw_importacoes_travadas AS
    SELECT
      s.upload_id,
      count(*)::bigint AS linhas_staging,
      max(s.dt_upload) AS ultimo_upload_em,
      l.status,
      l.etapa,
      l.atualizado_em,
      l.mensagem
    FROM {schema}.stg_leads_site s
    LEFT JOIN {schema}.logs_importacoes l ON l.upload_id = s.upload_id
    WHERE s.upload_id IS NOT NULL
      AND btrim(s.upload_id) <> ''
      AND (
        l.upload_id IS NULL
        OR upper(coalesce(l.status, '')) IN ('AGUARDANDO','RECEBIDO','STAGING','STAGING_CONCLUIDA','PENDENTE','ERRO')
        OR (
          upper(coalesce(l.status, '')) = 'PROCESSANDO'
          AND coalesce(l.atualizado_em, l.criado_em, now()) < now() - interval '20 minutes'
        )
      )
    GROUP BY s.upload_id, l.status, l.etapa, l.atualizado_em, l.mensagem;
    """

    db._run_gestao_query(ddl, {}, "apply_import_hardening")
    return {
        "schema": schema,
        "status": "aplicado",
        "recuperacao": f"{schema}.fn_reprocessar_upload_pendente(text)",
        "diagnostico": f"{schema}.vw_importacoes_travadas",
    }
