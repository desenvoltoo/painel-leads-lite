# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import jsonify, session

from services import database as db


def _institution_config() -> tuple[str, str, str]:
    institution = str(session.get("active_institution") or "anhanguera").strip().lower()
    if institution == "unifecaf":
        return institution, db._safe_ident("unifecaf"), "stg_leads"
    schema = str(getattr(db, "DB_SCHEMA", None) or "modelo_estrela").strip()
    return "anhanguera", db._safe_ident(schema), "stg_leads_site"


def register_management_import_routes(app) -> None:
    if "api_management_import_history" in app.view_functions:
        return

    def api_management_import_history():
        institution, schema, staging = _institution_config()
        try:
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
                    FROM {schema}.{db._safe_ident(staging)}
                    GROUP BY upload_id
                ) s ON s.upload_id = p.upload_id
                ORDER BY p.atualizado_em DESC NULLS LAST
                LIMIT 100
                """,
                {},
                f"management_import_history_{institution}",
            )
            return jsonify({"ok": True, "institution": institution, "items": rows or []})
        except Exception as exc:
            app.logger.exception("management_import_history_error institution=%s", institution)
            return jsonify({"ok": False, "error": {"message": "Não foi possível carregar o histórico de importações.", "details": str(exc)}}), 500

    app.add_url_rule(
        "/api/gestao/importacoes/ativas",
        endpoint="api_management_import_history",
        view_func=api_management_import_history,
        methods=["GET"],
    )
