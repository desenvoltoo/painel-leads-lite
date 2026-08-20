# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any

from flask import session

from . import database as db
from .upload_async import enqueue_upload_dataframe


def _institution() -> str:
    value = str(session.get("active_institution") or "anhanguera").strip().lower()
    return "unifecaf" if value == "unifecaf" else "anhanguera"


def _history_items(limit: int = 100) -> list[dict[str, Any]]:
    institution = _institution()
    if institution == "unifecaf":
        schema = db._safe_ident("unifecaf")
        staging = db._safe_ident("stg_leads")
    else:
        schema_name = str(getattr(db, "DB_SCHEMA", None) or "modelo_estrela").strip()
        schema = db._safe_ident(schema_name)
        staging = db._safe_ident("stg_leads_site")

    rows = db._run_gestao_query(
        f"""
        SELECT
            p.upload_id,
            COALESCE(NULLIF(BTRIM(p.arquivo), ''), 'Importação sem nome') AS nome_arquivo,
            'SISTEMA'::text AS usuario,
            COALESCE(p.status, 'AGUARDANDO') AS status,
            COALESCE(p.etapa, 'AGUARDANDO') AS etapa,
            COALESCE(p.linhas_total, 0)::bigint AS linhas_recebidas,
            COALESCE(p.linhas_processadas, 0)::bigint AS linhas_validas,
            COALESCE(p.linhas_inseridas, 0)::bigint AS linhas_inseridas,
            GREATEST(
                COALESCE(p.linhas_processadas, 0)
                - COALESCE(p.linhas_inseridas, 0)
                - COALESCE(p.linhas_ignoradas, 0)
                - COALESCE(p.linhas_rejeitadas, 0),
                0
            )::bigint AS linhas_atualizadas,
            COALESCE(p.linhas_rejeitadas, 0)::bigint AS linhas_rejeitadas,
            COALESCE(NULLIF(BTRIM(p.mensagem), ''), NULLIF(BTRIM(p.erro), ''), 'Sem mensagem') AS mensagem,
            p.atualizado_em AS criado_em,
            COALESCE(s.linhas_staging, 0)::bigint AS linhas_staging
        FROM {schema}.op_importacao_progresso p
        LEFT JOIN (
            SELECT upload_id, COUNT(*)::bigint AS linhas_staging
            FROM {schema}.{staging}
            GROUP BY upload_id
        ) s ON s.upload_id = p.upload_id
        ORDER BY p.atualizado_em DESC NULLS LAST
        LIMIT :limit
        """,
        {"limit": max(1, min(int(limit or 100), 500))},
        f"management_import_history_{institution}",
    )
    return list(rows or [])


def apply_management_import_compat() -> dict[str, Any]:
    import app as app_module

    original_logs_loader = app_module.gestao_op_get_logs_auditoria

    def process_upload_dataframe_compat(df, filename: str = "upload", upload_id: str | None = None, routine_name: str | None = None):
        institution = _institution()
        if institution == "unifecaf":
            routine = (
                routine_name
                or str(os.getenv("UNIFECAF_IMPORT_ROUTINE") or "").strip()
                or "sp_processar_stg_leads"
            )
        else:
            routine = (
                routine_name
                or str(os.getenv("LEADS_IMPORT_ROUTINE") or "").strip()
                or "sp_importar_leads_diario"
            )

        return enqueue_upload_dataframe(
            df,
            filename=filename,
            mode="ATUALIZAR_EXISTENTES",
            routine_name=routine,
            institution=institution,
        )

    def logs_loader_compat(kind, args, user_context):
        if str(kind or "").strip().lower() != "importacoes":
            return original_logs_loader(kind, args, user_context)
        try:
            limit = int(args.get("limit", 100))
        except Exception:
            limit = 100
        items = _history_items(limit)
        return {
            "success": True,
            "ok": True,
            "data": {
                "items": items,
                "data": items,
                "total": len(items),
                "institution": _institution(),
            },
        }, False

    app_module.process_upload_dataframe = process_upload_dataframe_compat
    app_module.gestao_op_get_logs_auditoria = logs_loader_compat
    return {"status": "aplicado", "upload": "assincrono", "historico": "op_importacao_progresso"}
