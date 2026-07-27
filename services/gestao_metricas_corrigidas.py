# -*- coding: utf-8 -*-
"""Correções finais das métricas da gestão.

- remove consultores artificiais/sem identificação;
- mantém disparos no período de data_disparo;
- contabiliza matrícula somente quando matriculado IS TRUE e data_matricula está no período;
- recalcula totais, funil, campanha, negócio e detalhamento de forma consistente.
"""
from __future__ import annotations

from typing import Any, Dict

from flask import jsonify, request

from . import database as db
from . import gestao_sem_lotes as base_service


INVALID_NAMES = {
    "", "N", "NULL", "N/A", "NA", "NONE", "UNDEFINED", "-",
    "SEM CONSULTOR", "SEM_CONSULTOR", "SEM RESPONSAVEL", "SEM RESPONSÁVEL",
    "NAO INFORMADO", "NÃO INFORMADO",
}


def _rows(sql: str, params: Dict[str, Any] | None = None, name: str = "gestao_metricas_corrigidas"):
    return db._run_gestao_query(sql, params or {}, name)


def _valid_name(value: Any) -> bool:
    normalized = str(value or "").replace("\\", "").strip().upper()
    return normalized not in INVALID_NAMES


def _dimension_filters() -> tuple[str, Dict[str, Any]]:
    clauses = [
        "UPPER(BTRIM(REPLACE(COALESCE(consultor_disparo::text, ''), chr(92), ''))) NOT IN "
        "('', 'N', 'NULL', 'N/A', 'NA', 'NONE', 'UNDEFINED', '-', 'SEM CONSULTOR', "
        "'SEM_CONSULTOR', 'SEM RESPONSAVEL', 'SEM RESPONSÁVEL', 'NAO INFORMADO', 'NÃO INFORMADO')"
    ]
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
    return " AND ".join(clauses), params


def _matriculation_period() -> tuple[str, Dict[str, Any]]:
    clauses = [
        "matriculado IS TRUE",
        "data_matricula IS NOT NULL",
    ]
    params: Dict[str, Any] = {}
    start = str(request.args.get("data_ini") or "").strip()
    end = str(request.args.get("data_fim") or "").strip()
    if start:
        clauses.append("data_matricula >= CAST(:mat_data_ini AS timestamp)")
        params["mat_data_ini"] = start
    if end:
        clauses.append("data_matricula < (CAST(:mat_data_fim AS date) + INTERVAL '1 day')")
        params["mat_data_fim"] = end
    return " AND ".join(clauses), params


def _correct_payload() -> Dict[str, Any]:
    payload = base_service._consultants_payload()
    relation = base_service._relation()
    dim_where, dim_params = _dimension_filters()
    mat_where, mat_params = _matriculation_period()
    params = {**dim_params, **mat_params}

    items = [row for row in (payload.get("items") or []) if _valid_name(row.get("consultor_disparo"))]
    payload["items"] = items
    payload["total"] = len(items)

    consultant_rows = _rows(
        f"""
        SELECT BTRIM(consultor_disparo::text) AS consultor_disparo,
               COUNT(*)::bigint AS matriculas
        FROM {relation}
        WHERE {dim_where} AND {mat_where}
        GROUP BY BTRIM(consultor_disparo::text)
        """,
        params,
        "metricas_matriculas_consultor",
    ) or []
    by_consultant = {str(row.get("consultor_disparo")): int(row.get("matriculas") or 0) for row in consultant_rows}

    total_matriculas = 0
    for row in items:
        name = str(row.get("consultor_disparo") or "")
        matriculas = by_consultant.get(name, 0)
        row["matriculas"] = matriculas
        total = int(row.get("total_disparado") or 0)
        row["taxa_matricula_pct"] = round(matriculas / total * 100, 2) if total else 0
        total_matriculas += matriculas

    detail_rows = _rows(
        f"""
        SELECT BTRIM(consultor_disparo::text) AS consultor_disparo,
               COALESCE(NULLIF(BTRIM(tipo_disparo::text), ''), 'SEM TIPO') AS tipo_disparo,
               COALESCE(NULLIF(BTRIM(campanha::text), ''), 'SEM CAMPANHA') AS campanha,
               COALESCE(NULLIF(BTRIM(canal::text), ''), 'SEM CANAL') AS canal,
               COALESCE(NULLIF(BTRIM(peca_disparo::text), ''), 'SEM PEÇA') AS peca_disparo,
               COUNT(*)::bigint AS matriculas
        FROM {relation}
        WHERE {dim_where} AND {mat_where}
        GROUP BY 1,2,3,4,5
        """,
        params,
        "metricas_matriculas_detalhe",
    ) or []
    detail_map = {
        (str(r.get("consultor_disparo")), str(r.get("tipo_disparo")), str(r.get("campanha")), str(r.get("canal")), str(r.get("peca_disparo"))): int(r.get("matriculas") or 0)
        for r in detail_rows
    }
    clean_breakdown = []
    for row in payload.get("breakdown") or []:
        if not _valid_name(row.get("consultor_disparo")):
            continue
        key = (
            str(row.get("consultor_disparo")), str(row.get("tipo_disparo")),
            str(row.get("campanha")), str(row.get("canal")), str(row.get("peca_disparo")),
        )
        row["matriculas"] = detail_map.get(key, 0)
        clean_breakdown.append(row)
    payload["breakdown"] = clean_breakdown

    business_rows = _rows(
        f"""
        SELECT COALESCE(NULLIF(BTRIM(tipo_negocio::text),''),'SEM TIPO DE NEGÓCIO') AS nome,
               COUNT(*)::bigint AS matriculas
        FROM {relation}
        WHERE {dim_where} AND {mat_where}
        GROUP BY 1
        """,
        params,
        "metricas_matriculas_negocio",
    ) or []
    business_map = {str(r.get("nome")): int(r.get("matriculas") or 0) for r in business_rows}
    for row in payload.get("by_business") or []:
        row["matriculas"] = business_map.get(str(row.get("nome")), 0)
        disparos = int(row.get("disparos") or 0)
        row["conversao"] = round(int(row["matriculas"]) / disparos * 100, 2) if disparos else 0

    campaign_rows = _rows(
        f"""
        SELECT COALESCE(NULLIF(BTRIM(campanha::text),''),'SEM CAMPANHA') AS nome,
               COUNT(*)::bigint AS matriculas
        FROM {relation}
        WHERE {dim_where} AND {mat_where}
        GROUP BY 1
        """,
        params,
        "metricas_matriculas_campanha",
    ) or []
    campaign_map = {str(r.get("nome")): int(r.get("matriculas") or 0) for r in campaign_rows}
    for row in payload.get("by_campaign") or []:
        row["matriculas"] = campaign_map.get(str(row.get("nome")), 0)
        disparos = int(row.get("disparos") or 0)
        row["conversao"] = round(int(row["matriculas"]) / disparos * 100, 2) if disparos else 0

    summary = payload.get("summary") or {}
    summary["matriculas"] = total_matriculas
    total_disparado = int(summary.get("total_disparado") or 0)
    summary["taxa_matricula_pct"] = round(total_matriculas / total_disparado * 100, 2) if total_disparado else 0
    payload["summary"] = summary
    if payload.get("funnel") is not None:
        payload["funnel"]["matriculas"] = total_matriculas

    return payload


def register_metricas_corrigidas(app) -> None:
    def endpoint():
        try:
            return jsonify({"ok": True, "data": _correct_payload()})
        except Exception as exc:
            app.logger.exception("Falha ao carregar métricas corrigidas")
            return jsonify({"ok": False, "error": {"message": str(exc)}}), 500

    target = "/api/gestao/operacional/consultores"
    for rule in app.url_map.iter_rules():
        if rule.rule == target and "GET" in rule.methods:
            app.view_functions[rule.endpoint] = endpoint
            return
    app.add_url_rule(target, "metricas_corrigidas", endpoint, methods=["GET"])
