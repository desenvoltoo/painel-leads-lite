# -*- coding: utf-8 -*-
"""Extensões operacionais carregadas automaticamente pelo Python.

Mantém a fila de disparos priorizada e registra KPIs educacionais sem alterar o
contrato das rotas existentes do painel.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _install_database_overrides() -> None:
    try:
        from services import database as db
    except Exception:
        logger.exception("Não foi possível carregar services.database.")
        return

    def dispatch_priority_order_clause() -> str:
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

    def query_leads(filters=None, limit=100, offset=0, order_by=None, order_dir="asc"):
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
    logger.info("Prioridade operacional da fila instalada.")


def _install_app_extensions() -> None:
    try:
        import app as app_module
        from flask import jsonify, request
        from services import database as db
    except Exception:
        logger.exception("Não foi possível carregar o app para registrar extensões.")
        return

    flask_app = getattr(app_module, "app", None)
    if flask_app is None:
        return

    if "api_kpis_education" not in flask_app.view_functions:
        @flask_app.post("/api/kpis/education", endpoint="api_kpis_education")
        def api_kpis_education():
            try:
                filters = request.get_json(silent=True) or {}
                params = []
                filtered_sql = db._apply_filters(
                    f"SELECT v.* FROM {db._view_table_id()} v WHERE 1=1",
                    filters,
                    params,
                )
                sql = f"""
                WITH filtered AS (
                    {filtered_sql}
                ),
                resumo AS (
                    SELECT
                        COUNT(*)::bigint AS total,
                        COUNT(*) FILTER (WHERE data_disparo IS NULL)::bigint AS fila_disparo,
                        COUNT(*) FILTER (WHERE data_inscricao::date = CURRENT_DATE)::bigint AS inscritos_hoje,
                        COUNT(*) FILTER (WHERE data_inscricao >= CURRENT_DATE - INTERVAL '6 days')::bigint AS inscritos_7_dias,
                        COUNT(*) FILTER (WHERE data_disparo::date = CURRENT_DATE)::bigint AS disparados_hoje,
                        COUNT(*) FILTER (WHERE flag_matriculado IS TRUE)::bigint AS matriculas,
                        COUNT(*) FILTER (
                            WHERE data_disparo IS NULL
                              AND data_inscricao < CURRENT_DATE - INTERVAL '3 days'
                        )::bigint AS backlog_3_dias,
                        COALESCE(
                            AVG(EXTRACT(EPOCH FROM (data_disparo - data_inscricao)) / 86400.0)
                            FILTER (
                                WHERE data_disparo IS NOT NULL
                                  AND data_inscricao IS NOT NULL
                                  AND data_disparo >= data_inscricao
                            ),
                            0
                        )::numeric(12,2) AS tempo_medio_disparo_dias
                    FROM filtered
                ),
                top_curso AS (
                    SELECT NULLIF(TRIM(curso::text), '') AS nome, COUNT(*)::bigint AS total
                    FROM filtered
                    WHERE NULLIF(TRIM(curso::text), '') IS NOT NULL
                    GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 1
                ),
                top_origem AS (
                    SELECT NULLIF(TRIM(origem::text), '') AS nome, COUNT(*)::bigint AS total
                    FROM filtered
                    WHERE NULLIF(TRIM(origem::text), '') IS NOT NULL
                    GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 1
                ),
                top_modalidade AS (
                    SELECT NULLIF(TRIM(modalidade::text), '') AS nome, COUNT(*)::bigint AS total
                    FROM filtered
                    WHERE NULLIF(TRIM(modalidade::text), '') IS NOT NULL
                    GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 1
                )
                SELECT
                    r.*,
                    CASE WHEN r.total > 0 THEN ROUND((r.matriculas::numeric * 100) / r.total, 2) ELSE 0 END AS taxa_matricula,
                    tc.nome AS top_curso_nome, tc.total AS top_curso_total,
                    tor.nome AS top_origem_nome, tor.total AS top_origem_total,
                    tm.nome AS top_modalidade_nome, tm.total AS top_modalidade_total
                FROM resumo r
                LEFT JOIN top_curso tc ON TRUE
                LEFT JOIN top_origem tor ON TRUE
                LEFT JOIN top_modalidade tm ON TRUE
                """
                rows = db._run_gestao_query(
                    db._postgres_sql(sql),
                    db._params_to_dict(params),
                    "education_kpis",
                )
                row = (rows or [{}])[0]
                data = {
                    "total": int(row.get("total") or 0),
                    "fila_disparo": int(row.get("fila_disparo") or 0),
                    "inscritos_hoje": int(row.get("inscritos_hoje") or 0),
                    "inscritos_7_dias": int(row.get("inscritos_7_dias") or 0),
                    "disparados_hoje": int(row.get("disparados_hoje") or 0),
                    "matriculas": int(row.get("matriculas") or 0),
                    "backlog_3_dias": int(row.get("backlog_3_dias") or 0),
                    "taxa_matricula": float(row.get("taxa_matricula") or 0),
                    "tempo_medio_disparo_dias": float(row.get("tempo_medio_disparo_dias") or 0),
                    "top_curso": {"nome": row.get("top_curso_nome"), "total": int(row.get("top_curso_total") or 0)},
                    "top_origem": {"nome": row.get("top_origem_nome"), "total": int(row.get("top_origem_total") or 0)},
                    "top_modalidade": {"nome": row.get("top_modalidade_nome"), "total": int(row.get("top_modalidade_total") or 0)},
                }
                return jsonify({"ok": True, "data": data})
            except Exception as exc:
                logger.exception("Falha ao calcular KPIs educacionais.")
                return jsonify({"ok": False, "error": str(exc)}), 500

    logger.info("Endpoint de KPIs educacionais instalado.")


_install_database_overrides()
_install_app_extensions()
