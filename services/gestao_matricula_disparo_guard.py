# -*- coding: utf-8 -*-
"""Aplica a regra oficial de matrícula na Gestão Executiva.

Uma matrícula só é contabilizada quando:
- matriculado IS TRUE
- data_disparo IS NOT NULL
"""
from __future__ import annotations

from typing import Any, Dict

from flask import request

from . import database as db
from . import gestao_sem_lotes


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
    return "(matriculado IS TRUE AND data_disparo IS NOT NULL)"


def apply_gestao_matricula_disparo_guard() -> dict[str, str]:
    gestao_sem_lotes._where_filters = _where_filters
    gestao_sem_lotes._matriculated_sql = _matriculated_sql
    return {
        "status": "aplicado",
        "regra": "matriculado_is_true_e_data_disparo_preenchida",
    }
