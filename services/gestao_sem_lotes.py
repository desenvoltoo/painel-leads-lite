# -*- coding: utf-8 -*-
"""Gestão de produtividade baseada diretamente nos disparos, sem lotes."""
from __future__ import annotations

from typing import Any, Dict

from flask import jsonify, request

from . import database as db

INVALID_CONSULTANTS = {"", "N", "NULL", "N/A", "NA", "NONE", "UNDEFINED", "-"}


def _rows(sql: str, params: Dict[str, Any] | None = None, name: str = "gestao_sem_lotes"):
    return db._run_gestao_query(sql, params or {}, name)


def _schema() -> str:
    return (getattr(db, "DB_SCHEMA", None) or "modelo_estrela").strip()


def _relation() -> str:
    schema = _schema()
    found = _rows(
        """
        SELECT table_name
        FROM (
          SELECT table_name, 0 AS prioridade
          FROM information_schema.views
          WHERE table_schema = :schema
            AND table_name IN ('vw_leads_painel_lite', 'leads_painel_lite')
          UNION ALL
          SELECT table_name, 1 AS prioridade
          FROM information_schema.tables
          WHERE table_schema = :schema
            AND table_name IN ('vw_leads_painel_lite', 'leads_painel_lite')
        ) x
        ORDER BY prioridade
        LIMIT 1
        """,
        {"schema": schema},
        "gestao_sem_lotes_relation",
    )
    if not found:
        raise RuntimeError("View de leads não encontrada no banco.")
    return f"{db._safe_ident(schema)}.{db._safe_ident(str(found[0]['table_name']))}"


def _consultants_payload() -> Dict[str, Any]:
    relation = _relation()
    items = _rows(
        f"""
        WITH base AS (
          SELECT
            CASE
              WHEN UPPER(BTRIM(REPLACE(COALESCE(consultor_disparo::text, ''), chr(92), ''))) IN
                   ('', 'N', 'NULL', 'N/A', 'NA', 'NONE', 'UNDEFINED', '-')
                THEN NULL
              ELSE BTRIM(consultor_disparo::text)
            END AS consultor_disparo,
            data_disparo,
            data_matricula,
            matriculado,
            status,
            observacao,
            acao_comercial,
            tipo_disparo,
            campanha,
            canal,
            peca_disparo
          FROM {relation}
          WHERE data_disparo IS NOT NULL
        ), valid AS (
          SELECT * FROM base WHERE consultor_disparo IS NOT NULL
        )
        SELECT
          consultor_disparo,
          COUNT(*)::bigint AS total_disparado,
          COUNT(*) FILTER (WHERE data_disparo::date = CURRENT_DATE)::bigint AS disparado_hoje,
          COUNT(*) FILTER (WHERE data_disparo >= date_trunc('week', CURRENT_TIMESTAMP))::bigint AS disparado_semana,
          COUNT(*) FILTER (WHERE data_disparo >= date_trunc('month', CURRENT_TIMESTAMP))::bigint AS disparado_mes,
          COUNT(*) FILTER (
            WHERE UPPER(COALESCE(status::text,'') || ' ' || COALESCE(observacao::text,'') || ' ' || COALESCE(acao_comercial::text,''))
                  ~ '(RETOR|CONTATO|RESPONDEU)'
          )::bigint AS retornos,
          COUNT(*) FILTER (
            WHERE UPPER(COALESCE(status::text,'') || ' ' || COALESCE(observacao::text,'') || ' ' || COALESCE(acao_comercial::text,''))
                  ~ '(POSIT|INTERESS|MATRIC|CONVERT|FECHOU)'
          )::bigint AS positivos,
          COUNT(*) FILTER (
            WHERE UPPER(COALESCE(status::text,'') || ' ' || COALESCE(observacao::text,'') || ' ' || COALESCE(acao_comercial::text,''))
                  ~ '(NEGAT|SEM INTERESSE|NAO INTERESS|NÃO INTERESS)'
          )::bigint AS negativos,
          COUNT(*) FILTER (
            WHERE COALESCE(matriculado::text,'') ~* '^(true|t|1|sim|s)$' OR data_matricula IS NOT NULL
          )::bigint AS matriculas,
          MAX(data_disparo) AS ultimo_disparo
        FROM valid
        GROUP BY consultor_disparo
        ORDER BY disparado_semana DESC, total_disparado DESC, consultor_disparo
        """,
        name="gestao_sem_lotes_consultores",
    ) or []

    breakdown = _rows(
        f"""
        SELECT
          BTRIM(consultor_disparo::text) AS consultor_disparo,
          COALESCE(NULLIF(BTRIM(tipo_disparo::text), ''), 'SEM TIPO') AS tipo_disparo,
          COALESCE(NULLIF(BTRIM(campanha::text), ''), 'SEM CAMPANHA') AS campanha,
          COALESCE(NULLIF(BTRIM(canal::text), ''), 'SEM CANAL') AS canal,
          COALESCE(NULLIF(BTRIM(peca_disparo::text), ''), 'SEM PEÇA') AS peca_disparo,
          COUNT(*)::bigint AS total,
          COUNT(*) FILTER (WHERE data_disparo >= date_trunc('week', CURRENT_TIMESTAMP))::bigint AS semana,
          MAX(data_disparo) AS ultimo_disparo
        FROM {relation}
        WHERE data_disparo IS NOT NULL
          AND UPPER(BTRIM(REPLACE(COALESCE(consultor_disparo::text, ''), chr(92), ''))) NOT IN
              ('', 'N', 'NULL', 'N/A', 'NA', 'NONE', 'UNDEFINED', '-')
        GROUP BY 1,2,3,4,5
        ORDER BY semana DESC, total DESC
        LIMIT 1000
        """,
        name="gestao_sem_lotes_breakdown",
    ) or []

    for row in items:
        total = int(row.get("total_disparado") or 0)
        retornos = int(row.get("retornos") or 0)
        matriculas = int(row.get("matriculas") or 0)
        row["taxa_retorno_pct"] = round((retornos / total * 100), 2) if total else 0
        row["taxa_matricula_pct"] = round((matriculas / total * 100), 2) if total else 0

    return {"items": items, "breakdown": breakdown, "total": len(items)}


def register_gestao_sem_lotes(app) -> None:
    def consultores_sem_lotes():
        try:
            return jsonify({"ok": True, "data": _consultants_payload()})
        except Exception as exc:
            app.logger.exception("Falha ao carregar produtividade sem lotes")
            return jsonify({"ok": False, "error": {"message": str(exc)}}), 500

    target_rule = "/api/gestao/operacional/consultores"
    for rule in app.url_map.iter_rules():
        if rule.rule == target_rule and "GET" in rule.methods:
            app.view_functions[rule.endpoint] = consultores_sem_lotes
            break
    else:
        app.add_url_rule(target_rule, "consultores_sem_lotes", consultores_sem_lotes, methods=["GET"])
