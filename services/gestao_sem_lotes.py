# -*- coding: utf-8 -*-
"""Gestão executiva baseada diretamente nos disparos, sem dependência de lotes."""
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


def _where_filters() -> tuple[str, Dict[str, Any]]:
    clauses = ["data_disparo IS NOT NULL"]
    params: Dict[str, Any] = {}
    mapping = {
        "consultor_disparo": "consultor_disparo",
        "tipo_negocio": "tipo_negocio",
        "tipo_disparo": "tipo_disparo",
        "campanha": "campanha",
        "canal": "canal",
        "curso": "curso",
        "unidade": "unidade",
    }
    for arg, column in mapping.items():
        value = str(request.args.get(arg) or "").strip()
        if value:
            clauses.append(f"COALESCE({db._safe_ident(column)}::text, '') ILIKE :{arg}")
            params[arg] = f"%{value}%"

    start = str(request.args.get("data_ini") or "").strip()
    end = str(request.args.get("data_fim") or "").strip()
    if start:
        clauses.append("data_disparo >= CAST(:data_ini AS timestamp)")
        params["data_ini"] = start
    if end:
        clauses.append("data_disparo < (CAST(:data_fim AS date) + INTERVAL '1 day')")
        params["data_fim"] = end
    return " AND ".join(clauses), params


def _matriculated_sql() -> str:
    # Regra oficial: somente a flag booleana verdadeira confirma matrícula atual.
    # data_matricula, status e textos como "sim" não contam como matrícula.
    return "matriculado IS TRUE"


def _consultants_payload() -> Dict[str, Any]:
    relation = _relation()
    where_sql, params = _where_filters()
    matriculated = _matriculated_sql()
    valid_consultant = "UPPER(BTRIM(REPLACE(COALESCE(consultor_disparo::text, ''), chr(92), ''))) NOT IN ('', 'N', 'NULL', 'N/A', 'NA', 'NONE', 'UNDEFINED', '-')"
    text_status = "UPPER(COALESCE(status::text,'') || ' ' || COALESCE(status_inscricao::text,'') || ' ' || COALESCE(observacao::text,'') || ' ' || COALESCE(acao_comercial::text,''))"

    items = _rows(
        f"""
        WITH valid AS (
          SELECT *
          FROM {relation}
          WHERE {where_sql}
            AND {valid_consultant}
        )
        SELECT
          BTRIM(consultor_disparo::text) AS consultor_disparo,
          COUNT(*)::bigint AS total_disparado,
          COUNT(*) FILTER (WHERE data_disparo::date = CURRENT_DATE)::bigint AS disparado_hoje,
          COUNT(*) FILTER (WHERE data_disparo >= date_trunc('week', CURRENT_TIMESTAMP))::bigint AS disparado_semana,
          COUNT(*) FILTER (WHERE data_disparo >= date_trunc('month', CURRENT_TIMESTAMP))::bigint AS disparado_mes,
          COUNT(*) FILTER (WHERE {text_status} ~ '(RETOR|CONTATO|RESPONDEU)')::bigint AS retornos,
          COUNT(*) FILTER (WHERE {text_status} ~ '(POSIT|INTERESS|CONVERT|FECHOU)' OR {matriculated})::bigint AS positivos,
          COUNT(*) FILTER (WHERE {text_status} ~ '(NEGAT|SEM INTERESSE|NAO INTERESS|NÃO INTERESS)')::bigint AS negativos,
          COUNT(*) FILTER (WHERE {matriculated})::bigint AS matriculas,
          COUNT(*) FILTER (WHERE {text_status} ~ '(NEGOCIA|EM ANDAMENTO|PROPOSTA)')::bigint AS em_negociacao,
          MAX(data_disparo) AS ultimo_disparo
        FROM valid
        GROUP BY BTRIM(consultor_disparo::text)
        ORDER BY disparado_semana DESC, total_disparado DESC, consultor_disparo
        """,
        params,
        "gestao_sem_lotes_consultores",
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
          COUNT(*) FILTER (WHERE {matriculated})::bigint AS matriculas,
          MAX(data_disparo) AS ultimo_disparo
        FROM {relation}
        WHERE {where_sql} AND {valid_consultant}
        GROUP BY 1,2,3,4,5
        ORDER BY semana DESC, total DESC
        LIMIT 1500
        """,
        params,
        "gestao_sem_lotes_breakdown",
    ) or []

    summary_rows = _rows(
        f"""
        SELECT
          COUNT(*)::bigint AS total_disparado,
          COUNT(*) FILTER (WHERE data_disparo::date = CURRENT_DATE)::bigint AS disparado_hoje,
          COUNT(*) FILTER (WHERE data_disparo >= date_trunc('week', CURRENT_TIMESTAMP))::bigint AS disparado_semana,
          COUNT(*) FILTER (WHERE data_disparo >= date_trunc('week', CURRENT_TIMESTAMP) - INTERVAL '7 days' AND data_disparo < date_trunc('week', CURRENT_TIMESTAMP))::bigint AS semana_anterior,
          COUNT(*) FILTER (WHERE data_disparo >= date_trunc('month', CURRENT_TIMESTAMP))::bigint AS disparado_mes,
          COUNT(*) FILTER (WHERE data_disparo >= date_trunc('month', CURRENT_TIMESTAMP) - INTERVAL '1 month' AND data_disparo < date_trunc('month', CURRENT_TIMESTAMP))::bigint AS mes_anterior,
          COUNT(*) FILTER (WHERE {text_status} ~ '(RETOR|CONTATO|RESPONDEU)')::bigint AS retornos,
          COUNT(*) FILTER (WHERE {text_status} ~ '(POSIT|INTERESS|CONVERT|FECHOU)' OR {matriculated})::bigint AS positivos,
          COUNT(*) FILTER (WHERE {text_status} ~ '(NEGOCIA|EM ANDAMENTO|PROPOSTA)')::bigint AS em_negociacao,
          COUNT(*) FILTER (WHERE {text_status} ~ '(NEGAT|SEM INTERESSE|NAO INTERESS|NÃO INTERESS)')::bigint AS negativos,
          COUNT(*) FILTER (WHERE {matriculated})::bigint AS matriculas
        FROM {relation}
        WHERE {where_sql}
        """,
        params,
        "gestao_sem_lotes_summary",
    ) or [{}]
    summary = summary_rows[0] if summary_rows else {}

    by_business = _rows(
        f"""
        SELECT COALESCE(NULLIF(BTRIM(tipo_negocio::text),''),'SEM TIPO DE NEGÓCIO') AS nome,
               COUNT(*)::bigint AS disparos,
               COUNT(*) FILTER (WHERE {text_status} ~ '(RETOR|CONTATO|RESPONDEU)')::bigint AS retornos,
               COUNT(*) FILTER (WHERE {text_status} ~ '(POSIT|INTERESS|CONVERT|FECHOU)' OR {matriculated})::bigint AS positivos,
               COUNT(*) FILTER (WHERE {matriculated})::bigint AS matriculas
        FROM {relation}
        WHERE {where_sql}
        GROUP BY 1 ORDER BY disparos DESC LIMIT 30
        """,
        params,
        "gestao_sem_lotes_business",
    ) or []

    by_campaign = _rows(
        f"""
        SELECT COALESCE(NULLIF(BTRIM(campanha::text),''),'SEM CAMPANHA') AS nome,
               COUNT(*)::bigint AS disparos,
               COUNT(*) FILTER (WHERE {text_status} ~ '(RETOR|CONTATO|RESPONDEU)')::bigint AS retornos,
               COUNT(*) FILTER (WHERE {text_status} ~ '(POSIT|INTERESS|CONVERT|FECHOU)' OR {matriculated})::bigint AS positivos,
               COUNT(*) FILTER (WHERE {matriculated})::bigint AS matriculas
        FROM {relation}
        WHERE {where_sql}
        GROUP BY 1 ORDER BY disparos DESC LIMIT 30
        """,
        params,
        "gestao_sem_lotes_campaign",
    ) or []

    options = _rows(
        f"""
        SELECT
          ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(BTRIM(tipo_negocio::text),'')), NULL) AS tipos_negocio,
          ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(BTRIM(tipo_disparo::text),'')), NULL) AS tipos_disparo,
          ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(BTRIM(canal::text),'')), NULL) AS canais,
          ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(BTRIM(unidade::text),'')), NULL) AS unidades
        FROM {relation}
        WHERE data_disparo IS NOT NULL
        """,
        name="gestao_sem_lotes_options",
    ) or [{}]

    for row in items:
        total = int(row.get("total_disparado") or 0)
        retornos = int(row.get("retornos") or 0)
        matriculas = int(row.get("matriculas") or 0)
        row["taxa_retorno_pct"] = round((retornos / total * 100), 2) if total else 0
        row["taxa_matricula_pct"] = round((matriculas / total * 100), 2) if total else 0

    total = int(summary.get("total_disparado") or 0)
    summary["taxa_retorno_pct"] = round(int(summary.get("retornos") or 0) / total * 100, 2) if total else 0
    summary["taxa_matricula_pct"] = round(int(summary.get("matriculas") or 0) / total * 100, 2) if total else 0
    current_week = int(summary.get("disparado_semana") or 0)
    previous_week = int(summary.get("semana_anterior") or 0)
    current_month = int(summary.get("disparado_mes") or 0)
    previous_month = int(summary.get("mes_anterior") or 0)
    summary["variacao_semana_pct"] = round((current_week - previous_week) / previous_week * 100, 2) if previous_week else None
    summary["variacao_mes_pct"] = round((current_month - previous_month) / previous_month * 100, 2) if previous_month else None

    return {
        "items": items,
        "breakdown": breakdown,
        "summary": summary,
        "funnel": {
            "disparados": total,
            "retornos": int(summary.get("retornos") or 0),
            "positivos": int(summary.get("positivos") or 0),
            "em_negociacao": int(summary.get("em_negociacao") or 0),
            "matriculas": int(summary.get("matriculas") or 0),
            "negativos": int(summary.get("negativos") or 0),
        },
        "by_business": by_business,
        "by_campaign": by_campaign,
        "options": options[0] if options else {},
        "total": len(items),
    }


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
