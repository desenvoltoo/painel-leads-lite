# -*- coding: utf-8 -*-
"""Diagnóstico de qualidade dos dados para o módulo de gestão."""
from __future__ import annotations

from typing import Any, Dict

from flask import jsonify, request

from . import database as db

FIELDS = [
    "status_inscricao", "data_inscricao", "origem", "unidade", "tipo_negocio",
    "curso", "modalidade", "turno", "nome", "cpf", "celular", "email",
    "data_ultima_acao", "qtd_acionamentos", "status", "data_disparo",
    "peca_disparo", "texto_disparo", "consultor_disparo", "tipo_disparo",
    "campanha", "observacao", "data_matricula", "matriculado", "canal",
    "acao_comercial", "consultor_comercial",
]

MARKERS = ["\\N", "\\\\N", "NULL", "N/A", "NA", "NONE", "UNDEFINED", "-"]


def _rows(sql: str, params: Dict[str, Any] | None = None, name: str = "qualidade_dados"):
    return db._run_gestao_query(sql, params or {}, name)


def _schema() -> str:
    return (getattr(db, "DB_SCHEMA", None) or "modelo_estrela").strip()


def _relation() -> tuple[str, set[str]]:
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
        "qualidade_relation",
    )
    if not found:
        raise RuntimeError("View de leads não encontrada no banco.")
    name = str(found[0]["table_name"])
    cols = _rows(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :name
        """,
        {"schema": schema, "name": name},
        "qualidade_columns",
    )
    relation = f"{db._safe_ident(schema)}.{db._safe_ident(name)}"
    return relation, {str(row["column_name"]) for row in cols}


def _issue_queries(columns: set[str]) -> list[tuple[str, str, str]]:
    issues: list[tuple[str, str, str]] = []
    marker_sql = ", ".join("'" + marker.replace("'", "''") + "'" for marker in MARKERS)

    for field in FIELDS:
        if field not in columns:
            continue
        ident = db._safe_ident(field)
        text = f"COALESCE({ident}::text, '')"
        trimmed = f"BTRIM({text})"
        issues.append((field, "MARCADOR_LITERAL", f"UPPER({trimmed}) IN ({marker_sql})"))
        issues.append((field, "ESPACOS_EXTRAS", f"{text} <> '' AND {text} <> BTRIM({text})"))

    if "cpf" in columns:
        issues.append(("cpf", "CPF_INVALIDO", "BTRIM(COALESCE(cpf::text,'')) <> '' AND length(regexp_replace(cpf::text, '[^0-9]', '', 'g')) <> 11"))
    if "celular" in columns:
        issues.append(("celular", "CELULAR_INVALIDO", "BTRIM(COALESCE(celular::text,'')) <> '' AND length(regexp_replace(celular::text, '[^0-9]', '', 'g')) NOT IN (10,11)"))
    if "email" in columns:
        issues.append(("email", "EMAIL_INVALIDO", "BTRIM(COALESCE(email::text,'')) <> '' AND email::text !~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}$'"))
    if "nome" in columns:
        issues.append(("nome", "NOME_CURTO", "BTRIM(COALESCE(nome::text,'')) <> '' AND length(BTRIM(nome::text)) < 3"))
    for field in ("data_disparo", "data_matricula", "data_inscricao"):
        if field in columns:
            issues.append((field, "DATA_FUTURA", f"{db._safe_ident(field)}::date > CURRENT_DATE + 1"))
    if "matriculado" in columns:
        issues.append(("matriculado", "BOOLEANO_INVALIDO", "BTRIM(COALESCE(matriculado::text,'')) <> '' AND UPPER(BTRIM(matriculado::text)) NOT IN ('TRUE','FALSE','T','F','1','0','SIM','NAO','NÃO','S','N')"))
    if "matriculado" in columns and "data_matricula" in columns:
        issues.append(("matriculado", "MATRICULA_SEM_DATA", "UPPER(BTRIM(COALESCE(matriculado::text,''))) IN ('TRUE','T','1','SIM','S') AND data_matricula IS NULL"))
        issues.append(("data_matricula", "DATA_SEM_MATRICULA", "data_matricula IS NOT NULL AND UPPER(BTRIM(COALESCE(matriculado::text,''))) IN ('FALSE','F','0','NAO','NÃO','N')"))
    return issues


def _diagnose(limit: int) -> Dict[str, Any]:
    relation, columns = _relation()
    definitions = _issue_queries(columns)
    identity_columns = [column for column in ("nome", "cpf", "celular", "email", "consultor_disparo") if column in columns]
    identity_select = ", ".join(f"COALESCE({db._safe_ident(column)}::text, '') AS {db._safe_ident(column)}" for column in identity_columns)
    items = []
    total = 0

    for field, issue, condition in definitions:
        ident = db._safe_ident(field)
        result = _rows(
            f"SELECT COUNT(*)::bigint AS quantidade, MIN({ident}::text) FILTER (WHERE {ident} IS NOT NULL) AS exemplo FROM {relation} WHERE {condition}",
            name=f"qualidade_{field}_{issue}"[:60],
        )
        quantity = int((result[0].get("quantidade") if result else 0) or 0)
        if quantity <= 0:
            continue
        total += quantity
        select_parts = [f"{ident}::text AS valor"]
        if identity_select:
            select_parts.append(identity_select)
        samples = _rows(
            f"SELECT {', '.join(select_parts)} FROM {relation} WHERE {condition} LIMIT :limit",
            {"limit": limit},
            f"qualidade_amostra_{field}_{issue}"[:60],
        )
        items.append({
            "campo": field,
            "problema": issue,
            "quantidade": quantity,
            "exemplo": result[0].get("exemplo") if result else None,
            "amostras": samples or [],
        })

    items.sort(key=lambda item: (-item["quantidade"], item["campo"], item["problema"]))
    return {
        "items": items,
        "total_inconsistencias": total,
        "campos_analisados": len([field for field in FIELDS if field in columns]),
        "campos_com_problema": len({item["campo"] for item in items}),
        "fonte": relation,
    }


def register_qualidade_dados(app) -> None:
    if "qualidade_dados_inconsistencias" in app.view_functions:
        return

    @app.get("/api/gestao/qualidade-dados/inconsistencias")
    def qualidade_dados_inconsistencias():
        try:
            limit = max(1, min(int(request.args.get("amostras") or 5), 20))
            return jsonify({"ok": True, "data": _diagnose(limit)})
        except Exception as exc:
            app.logger.exception("Falha ao analisar qualidade dos dados")
            return jsonify({"ok": False, "error": str(exc)}), 500
