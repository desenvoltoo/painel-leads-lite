# -*- coding: utf-8 -*-
"""Ajustes operacionais carregados automaticamente pelo Python.

Centraliza a ordenação da fila de disparos sem alterar o contrato das rotas
existentes. O módulo é carregado pelo mecanismo padrão ``sitecustomize`` antes
de ``app.py``.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _install_database_overrides() -> None:
    try:
        from services import database as db
    except Exception:
        logger.exception("Não foi possível carregar services.database para aplicar a fila de disparos.")
        return

    def dispatch_priority_order_clause() -> str:
        """Prioriza nunca disparados e depois inscrições mais recentes."""
        parts: list[str] = []

        if db._has_view_col("data_disparo"):
            parts.append("CASE WHEN v.data_disparo IS NULL THEN 0 ELSE 1 END ASC")
        if db._has_view_col("data_inscricao"):
            parts.append("v.data_inscricao DESC NULLS LAST")
        if db._has_view_col("data_atualizacao"):
            parts.append("v.data_atualizacao DESC NULLS LAST")
        if db._has_view_col("sk_pessoa"):
            parts.append("v.sk_pessoa ASC")

        return " ORDER BY " + ", ".join(parts or ["1"])

    def query_leads(
        filters=None,
        limit=100,
        offset=0,
        order_by=None,
        order_dir="asc",
    ):
        params = []
        select_cols = ", ".join("v." + column for column in db.LEADS_COLUMNS)
        sql = db._apply_filters(
            f"SELECT {select_cols} FROM {db._view_table_id()} v WHERE 1=1",
            filters,
            params,
        )

        if not order_by or order_by in {"prioridade_disparo", "data_disparo"}:
            sql += dispatch_priority_order_clause()
        else:
            column, direction = db._safe_order(order_by, order_dir)
            sql += f" ORDER BY v.{column} {direction} NULLS LAST"
            if column != "sk_pessoa" and db._has_view_col("sk_pessoa"):
                sql += ", v.sk_pessoa ASC"

        sql += " LIMIT @limit OFFSET @offset"
        db._add_param(params, "limit", "INT64", int(limit))
        db._add_param(params, "offset", "INT64", int(offset))

        return db._run_gestao_query(
            db._postgres_sql(sql),
            db._params_to_dict(params),
            "leads_list_dispatch_priority",
        )

    db._dispatch_priority_order_clause = dispatch_priority_order_clause
    db.query_leads = query_leads
    db._export_order_clause = dispatch_priority_order_clause

    logger.info("Prioridade da fila de disparos instalada.")


def _install_html_assets() -> None:
    try:
        import app as app_module
    except Exception:
        logger.exception("Não foi possível carregar app.py para injetar os recursos visuais.")
        return

    flask_app = getattr(app_module, "app", None)
    if flask_app is None or getattr(flask_app, "_dispatch_assets_installed", False):
        return

    @flask_app.after_request
    def inject_dispatch_assets(response):
        content_type = response.headers.get("Content-Type", "")
        if "text/html" not in content_type.lower():
            return response

        try:
            html = response.get_data(as_text=True)
        except Exception:
            return response

        css_tag = '<link rel="stylesheet" href="/static/css/dispatch-priority.css?v=20260721-1">'
        js_tag = '<script defer src="/static/js/dispatch-priority.js?v=20260721-1"></script>'

        if css_tag not in html and "</head>" in html:
            html = html.replace("</head>", f"  {css_tag}\n</head>", 1)
        if js_tag not in html and "</body>" in html:
            html = html.replace("</body>", f"  {js_tag}\n</body>", 1)

        response.set_data(html)
        response.headers["Content-Length"] = str(len(response.get_data()))
        return response

    flask_app._dispatch_assets_installed = True
    logger.info("Recursos visuais da fila de disparos instalados.")


_install_database_overrides()
_install_html_assets()
