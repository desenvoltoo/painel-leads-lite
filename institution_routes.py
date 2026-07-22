# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from flask import jsonify, request, session


def register_institution_routes(app) -> None:
    from services import database as db

    sources = {
        "anhanguera": {
            "label": "Anhanguera",
            "schema": str(os.getenv("DB_SCHEMA") or "modelo_estrela").strip(),
            "view": str(os.getenv("LEADS_VIEW") or "vw_leads_painel_lite").strip(),
            "import_enabled": True,
        },
        "unifecaf": {
            "label": "UniFECAF",
            "schema": str(os.getenv("UNIFECAF_DB_SCHEMA") or "unifecaf").strip(),
            "view": str(os.getenv("UNIFECAF_LEADS_VIEW") or "vw_leads_painel_lite").strip(),
            "import_enabled": bool(str(os.getenv("UNIFECAF_IMPORT_ENABLED") or "false").lower() in {"1", "true", "yes", "sim"}),
        },
    }

    original_view_table_id = db._view_table_id
    original_view_columns = db._view_columns
    view_columns_cache: dict[str, set[str]] = {}

    def active_key() -> str:
        key = str(session.get("active_institution") or "anhanguera").strip().lower()
        return key if key in sources else "anhanguera"

    def dynamic_view_table_id() -> str:
        source = sources[active_key()]
        return f"{db._safe_ident(source['schema'])}.{db._safe_ident(source['view'])}"

    def dynamic_view_columns() -> set[str]:
        table_id = dynamic_view_table_id()
        if table_id in view_columns_cache:
            return view_columns_cache[table_id]
        schema, view = table_id.split(".", 1)
        rows = db._run_gestao_query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=:schema AND table_name=:view",
            {"schema": schema, "view": view},
            f"view_columns_{active_key()}",
        )
        cols = {row["column_name"] for row in rows}
        if not cols:
            raise RuntimeError(f"View de leads não encontrada: {table_id}")
        view_columns_cache[table_id] = cols
        return cols

    db._view_table_id = dynamic_view_table_id
    db._view_columns = dynamic_view_columns

    @app.get("/api/instituicao")
    def api_get_institution():
        key = active_key()
        source = sources[key]
        return jsonify({
            "ok": True,
            "institution": key,
            "label": source["label"],
            "schema": source["schema"],
            "view": source["view"],
            "import_enabled": source["import_enabled"],
            "available": [
                {"value": item_key, "label": item["label"]}
                for item_key, item in sources.items()
            ],
        })

    @app.post("/api/instituicao")
    def api_set_institution():
        payload = request.get_json(silent=True) or {}
        key = str(payload.get("institution") or "").strip().lower()
        if key not in sources:
            return jsonify({"ok": False, "error": {"message": "Instituição inválida."}}), 400
        session["active_institution"] = key
        session.modified = True
        source = sources[key]
        app.logger.info(
            "institution_changed institution=%s schema=%s view=%s user=%s",
            key,
            source["schema"],
            source["view"],
            session.get("user_email") or session.get("email") or "unknown",
        )
        return jsonify({
            "ok": True,
            "institution": key,
            "label": source["label"],
            "import_enabled": source["import_enabled"],
        })

    @app.before_request
    def protect_cross_institution_upload():
        if active_key() != "unifecaf":
            return None
        if not request.path.startswith("/api/upload"):
            return None
        if request.path.startswith("/api/upload/preview") or request.path.startswith("/api/upload/progresso"):
            return None
        if sources["unifecaf"]["import_enabled"]:
            return None
        return jsonify({
            "ok": False,
            "error": {
                "code": "UNIFECAF_IMPORT_NOT_CONFIGURED",
                "message": "A consulta da UniFECAF está ativa, mas a importação está bloqueada até as SPs específicas da UniFECAF serem configuradas.",
            },
        }), 409
