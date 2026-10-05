# -*- coding: utf-8 -*-
"""KPIs educacionais do Painel de Leads.

Registrados explicitamente pelo WSGI para não depender do carregamento
automático de sitecustomize.py, que pode variar entre imagens Python.
"""
from __future__ import annotations

import logging

from flask import jsonify, request

logger = logging.getLogger(__name__)


def register_education_kpi_routes(app):
    """Registra /api/kpis/education de forma idempotente."""
    if "api_kpis_education" in app.view_functions:
        return {"registered": False, "reason": "already_registered"}

    from services import database as db

    def _metric_sql(columns: set[str]) -> dict[str, str]:
        has_disparo = "data_disparo" in columns
        has_inscricao = "data_inscricao" in columns
        has_matricula = "flag_matriculado" in columns or "matriculado" in columns

        no_disparo = (
            "(data_disparo IS NULL OR data_disparo::text = '-infinity')"
            if has_disparo else "TRUE"
        )
        com_disparo = (
            "(data_disparo IS NOT NULL AND data_disparo::text <> '-infinity')"
            if has_disparo else "FALSE"
        )

        if "flag_matriculado" in columns:
            matriculado = "flag_matriculado IS TRUE"
        elif "matriculado" in columns:
            matriculado = (
                "LOWER(BTRIM(COALESCE(matriculado::text, ''))) "
                "IN ('true','t','1','sim','s','yes')"
            )
        else:
            matriculado = "FALSE"

        return {
            "no_disparo": no_disparo,
            "com_disparo": com_disparo,
            "inscritos_hoje": (
                "data_inscricao::date = CURRENT_DATE" if has_inscricao else "FALSE"
            ),
            "inscritos_7_dias": (
                "data_inscricao::date >= CURRENT_DATE - 6" if has_inscricao else "FALSE"
            ),
            "disparados_hoje": (
                f"{com_disparo} AND data_disparo::date = CURRENT_DATE"
                if has_disparo else "FALSE"
            ),
            "matriculado": matriculado if has_matricula else "FALSE",
            "backlog": (
                f"{no_disparo} AND data_inscricao IS NOT NULL "
                "AND data_inscricao::date < CURRENT_DATE - 3"
                if has_inscricao else "FALSE"
            ),
            "avg_delay": (
                "COALESCE(AVG(EXTRACT(EPOCH FROM "
                "(data_disparo - data_inscricao::timestamp)) / 86400.0) "
                f"FILTER (WHERE {com_disparo} "
                "AND data_inscricao IS NOT NULL "
                "AND data_disparo >= data_inscricao::timestamp), 0)"
                if has_disparo and has_inscricao else "0"
            ),
        }

    def _top_cte(cte_name: str, column: str, columns: set[str]) -> tuple[str, str, str]:
        alias = {
            "curso": "tc",
            "origem": "tor",
            "modalidade": "tm",
        }.get(column, cte_name[:2])

        if column not in columns:
            return "", f"NULL::text AS top_{column}_nome", f"0::bigint AS top_{column}_total"

        safe_col = db._safe_ident(column)
        cte = f"""
        {cte_name} AS (
            SELECT
                NULLIF(BTRIM({safe_col}::text), '') AS nome,
                COUNT(*)::bigint AS total
            FROM filtered
            WHERE NULLIF(BTRIM({safe_col}::text), '') IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC, 1
            LIMIT 1
        )
        """
        return (
            cte,
            f"{alias}.nome AS top_{column}_nome",
            f"COALESCE({alias}.total, 0)::bigint AS top_{column}_total",
        )

    def api_kpis_education():
        try:
            filters = request.get_json(silent=True) or {}
            params = []
            columns = set(db._view_columns())
            metrics = _metric_sql(columns)

            filtered_sql = db._apply_filters(
                f"SELECT v.* FROM {db._view_table_id()} v WHERE 1=1",
                filters,
                params,
            )

            top_specs = [
                ("top_curso", "curso", "tc"),
                ("top_origem", "origem", "tor"),
                ("top_modalidade", "modalidade", "tm"),
            ]
            ctes = []
            select_parts = []
            joins = []

            for cte_name, column, alias in top_specs:
                cte, name_sql, total_sql = _top_cte(cte_name, column, columns)
                if cte:
                    ctes.append(cte)
                    joins.append(f"LEFT JOIN {cte_name} {alias} ON TRUE")
                select_parts.extend([name_sql, total_sql])

            extra_ctes = (",\n" + ",\n".join(ctes)) if ctes else ""
            extra_joins = "\n".join(joins)
            extra_selects = ",\n                    " + ",\n                    ".join(select_parts)

            sql = f"""
            WITH filtered AS (
                {filtered_sql}
            ),
            resumo AS (
                SELECT
                    COUNT(*)::bigint AS total,
                    COUNT(*) FILTER (WHERE {metrics['no_disparo']})::bigint AS fila_disparo,
                    COUNT(*) FILTER (WHERE {metrics['inscritos_hoje']})::bigint AS inscritos_hoje,
                    COUNT(*) FILTER (WHERE {metrics['inscritos_7_dias']})::bigint AS inscritos_7_dias,
                    COUNT(*) FILTER (WHERE {metrics['disparados_hoje']})::bigint AS disparados_hoje,
                    COUNT(*) FILTER (WHERE {metrics['matriculado']})::bigint AS matriculas,
                    COUNT(*) FILTER (WHERE {metrics['backlog']})::bigint AS backlog_3_dias,
                    ({metrics['avg_delay']})::numeric(12,2) AS tempo_medio_disparo_dias
                FROM filtered
            )
            {extra_ctes}
            SELECT
                r.*,
                CASE
                    WHEN r.total > 0
                    THEN ROUND((r.matriculas::numeric * 100) / r.total, 2)
                    ELSE 0
                END AS taxa_matricula
                {extra_selects}
            FROM resumo r
            {extra_joins}
            """

            rows = db._run_gestao_query(
                db._postgres_sql(sql),
                db._params_to_dict(params),
                "education_kpis_explicit",
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
                "top_curso": {
                    "nome": row.get("top_curso_nome"),
                    "total": int(row.get("top_curso_total") or 0),
                },
                "top_origem": {
                    "nome": row.get("top_origem_nome"),
                    "total": int(row.get("top_origem_total") or 0),
                },
                "top_modalidade": {
                    "nome": row.get("top_modalidade_nome"),
                    "total": int(row.get("top_modalidade_total") or 0),
                },
            }
            return jsonify({"ok": True, "data": data})
        except Exception as exc:
            logger.exception("Falha ao calcular KPIs educacionais.")
            return jsonify({
                "ok": False,
                "error": "Erro ao calcular KPIs educacionais.",
                "details": str(exc),
                "error_type": type(exc).__name__,
            }), 500

    app.add_url_rule(
        "/api/kpis/education",
        endpoint="api_kpis_education",
        view_func=api_kpis_education,
        methods=["POST"],
    )
    return {"registered": True}
