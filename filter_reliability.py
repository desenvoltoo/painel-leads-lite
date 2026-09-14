# -*- coding: utf-8 -*-
"""Camada de confiabilidade dos filtros do Painel de Leads.

Mantém dropdowns e consultas fiéis ao PostgreSQL, tratando diferenças apenas
de formatação (caixa/espaços) como o mesmo valor e marcadores técnicos de vazio
como ausência de conteúdo.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from services import database as db

EMPTY_MARKERS = {
    "",
    r"\n",
    r"\\n",
    r"\N",
    r"\\N",
    "null",
    "none",
    "nan",
    "nat",
}


def _normalized_text(value: Any) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    return text.upper()


def _is_empty_marker(value: Any) -> bool:
    return str(value or "").strip().casefold() in {item.casefold() for item in EMPTY_MARKERS}


def _as_filter_list(value: Any) -> list[str]:
    if value in (None, "", []):
        return []
    raw = value if isinstance(value, list) else str(value).split(" || ")
    return [str(item).strip() for item in raw if str(item).strip()]


def _apply_text_multi_filter(sql: str, params: list, column: str, value: Any, param_name: str) -> str:
    values = _as_filter_list(value)
    if not values:
        return sql

    wants_empty = db.EMPTY_FILTER_TOKEN in values or any(_is_empty_marker(item) for item in values)
    real_values = [
        item
        for item in values
        if item != db.EMPTY_FILTER_TOKEN and not _is_empty_marker(item)
    ]

    clauses: list[str] = []

    if real_values:
        pname = f"f_{param_name}"
        normalized_values = sorted({_normalized_text(item) for item in real_values})
        clauses.append(
            "UPPER(REGEXP_REPLACE(BTRIM(COALESCE("
            f"v.{column}::text, '')), '\\s+', ' ', 'g')) = ANY(@{pname})"
        )
        db._add_param(params, pname, "ARRAY", normalized_values)

    if wants_empty:
        blank_name = f"blank_{param_name}"
        clauses.append(
            "LOWER(BTRIM(COALESCE("
            f"v.{column}::text, ''))) = ANY(@{blank_name})"
        )
        db._add_param(
            params,
            blank_name,
            "ARRAY",
            sorted({item.casefold() for item in EMPTY_MARKERS}),
        )

    if clauses:
        sql += " AND (" + " OR ".join(clauses) + ")"

    return sql


def _apply_filters(sql: str, filters, params: list) -> str:
    filters = dict(filters or {})

    if filters.get("consultor") and not filters.get("consultor_disparo"):
        filters["consultor_disparo"] = filters.get("consultor")

    filter_cols = [
        "status",
        "curso",
        "graduacao",
        "conclusao",
        "modalidade",
        "turno",
        "polo",
        "origem",
        "consultor_disparo",
        "consultor_comercial",
        "canal",
        "campanha",
        "tipo_disparo",
        "tipo_negocio",
    ]

    for key in filter_cols:
        if not db._has_view_col(key):
            db.logger.warning(
                "Filtro ignorado: coluna não existe na view col=%s view=%s",
                key,
                db._view_table_id(),
            )
            continue
        sql = _apply_text_multi_filter(sql, params, key, filters.get(key), key)

    busca = str(filters.get("busca") or "").strip()
    if busca:
        busca_num = re.sub(r"[^0-9]", "", busca)
        clauses = []
        if db._has_view_col("nome"):
            clauses.append("COALESCE(v.nome::text, '') ILIKE @busca")
        if db._has_view_col("email"):
            clauses.append("COALESCE(v.email::text, '') ILIKE @busca")
        if busca_num and db._has_view_col("cpf"):
            clauses.append(
                "regexp_replace(COALESCE(v.cpf::text, ''), '[^0-9]', '', 'g') LIKE @busca_num"
            )
        if busca_num and db._has_view_col("celular"):
            clauses.append(
                "regexp_replace(COALESCE(v.celular::text, ''), '[^0-9]', '', 'g') LIKE @busca_num"
            )
        if clauses:
            sql += " AND (" + " OR ".join(clauses) + ")"
            db._add_param(params, "busca", "STRING", f"%{busca}%")
            db._add_param(params, "busca_num", "STRING", f"%{busca_num}%")

    nome = str(filters.get("nome") or "").strip()
    if nome and db._has_view_col("nome"):
        sql += " AND COALESCE(v.nome::text, '') ILIKE @nome"
        db._add_param(params, "nome", "STRING", f"%{nome}%")

    email = str(filters.get("email") or "").strip()
    if email and db._has_view_col("email"):
        sql += " AND COALESCE(v.email::text, '') ILIKE @email"
        db._add_param(params, "email", "STRING", f"%{email}%")

    cpf = re.sub(r"[^0-9]", "", str(filters.get("cpf") or ""))
    if cpf and db._has_view_col("cpf"):
        sql += (
            " AND regexp_replace(COALESCE(v.cpf::text, ''), '[^0-9]', '', 'g') LIKE @cpf"
        )
        db._add_param(params, "cpf", "STRING", f"%{cpf}%")

    celular = re.sub(r"[^0-9]", "", str(filters.get("celular") or ""))
    if celular and db._has_view_col("celular"):
        sql += (
            " AND regexp_replace(COALESCE(v.celular::text, ''), '[^0-9]', '', 'g') LIKE @celular"
        )
        db._add_param(params, "celular", "STRING", f"%{celular}%")

    matriculado = str(filters.get("matriculado") or "").strip().lower()
    if matriculado and db._has_view_col("flag_matriculado"):
        if matriculado in ("true", "1", "sim", "s", "yes"):
            sql += " AND v.flag_matriculado IS TRUE"
        elif matriculado in ("false", "0", "nao", "não", "n", "no"):
            sql += " AND (v.flag_matriculado IS FALSE OR v.flag_matriculado IS NULL)"

    if filters.get("data_inicio") or filters.get("data_ini"):
        value = str(filters.get("data_inicio") or filters.get("data_ini"))
        if db._has_view_col("data_inscricao"):
            db._add_param(params, "data_inicio", "DATE", date.fromisoformat(value))
            sql += " AND DATE(v.data_inscricao) >= @data_inicio"

    if filters.get("data_fim"):
        value = str(filters.get("data_fim"))
        if db._has_view_col("data_inscricao"):
            db._add_param(params, "data_fim", "DATE", date.fromisoformat(value))
            sql += " AND DATE(v.data_inscricao) <= @data_fim"

    sit = str(filters.get("data_disparo_situacao") or "").strip().lower()
    if db._has_view_col("data_disparo"):
        no_disparo = "(v.data_disparo IS NULL OR v.data_disparo::text = '-infinity')"
        com_disparo = "(v.data_disparo IS NOT NULL AND v.data_disparo::text <> '-infinity')"

        if sit == "vazias":
            sql += f" AND {no_disparo}"
        elif sit == "preenchidas":
            sql += f" AND {com_disparo}"

        if filters.get("data_disparo_mes") and sit != "vazias":
            y, m = map(int, str(filters["data_disparo_mes"]).split("-"))
            ini = date(y, m, 1)
            fim = date(y + (m == 12), 1 if m == 12 else m + 1, 1)
            sql += (
                f" AND {com_disparo}"
                " AND DATE(v.data_disparo) >= @data_disparo_ini"
                " AND DATE(v.data_disparo) < @data_disparo_fim"
            )
            db._add_param(params, "data_disparo_ini", "DATE", ini)
            db._add_param(params, "data_disparo_fim", "DATE", fim)

    return sql


def query_options():
    """Retorna um valor por opção lógica, usando a grafia mais frequente no banco."""
    option_map = {
        "status": ("status", "status"),
        "curso": ("curso", "cursos"),
        "graduacao": ("graduacao", "graduacoes"),
        "conclusao": ("conclusao", "conclusoes"),
        "modalidade": ("modalidade", "modalidades"),
        "turno": ("turno", "turnos"),
        "polo": ("polo", "polos"),
        "origem": ("origem", "origens"),
        "consultor_disparo": ("consultor_disparo", "consultores_disparo"),
        "consultor_comercial": ("consultor_comercial", "consultores_comercial"),
        "canal": ("canal", "canais"),
        "campanha": ("campanha", "campanhas"),
        "tipo_disparo": ("tipo_disparo", "tipos_disparo"),
        "tipo_negocio": ("tipo_negocio", "tipos_negocio"),
    }

    opts = {}
    blank_markers = sorted({item.casefold() for item in EMPTY_MARKERS if item})

    for col, (singular_key, plural_key) in option_map.items():
        if not db._has_view_col(col):
            values = []
        else:
            safe_col = db._safe_ident(col)
            rows = db._run_gestao_query(
                f"""
                WITH valores AS (
                    SELECT
                        REGEXP_REPLACE(BTRIM({safe_col}::text), '\\s+', ' ', 'g') AS value
                    FROM {db._view_table_id()}
                ),
                validos AS (
                    SELECT
                        value,
                        UPPER(value) AS normalized,
                        COUNT(*)::bigint AS quantidade
                    FROM valores
                    WHERE NULLIF(value, '') IS NOT NULL
                      AND NOT (LOWER(value) = ANY(:blank_markers))
                    GROUP BY value, UPPER(value)
                ),
                ranqueados AS (
                    SELECT
                        value,
                        normalized,
                        quantidade,
                        ROW_NUMBER() OVER (
                            PARTITION BY normalized
                            ORDER BY quantidade DESC, value ASC
                        ) AS rn
                    FROM validos
                )
                SELECT value
                FROM ranqueados
                WHERE rn = 1
                ORDER BY value
                """,
                {"blank_markers": blank_markers},
                f"options_{col}_dominant",
            )
            values = [
                row["value"]
                for row in rows
                if row.get("value") not in (None, "")
            ]

        opts[singular_key] = values
        opts[plural_key] = values

    return opts


_original_json_safe_value = db._json_safe_value
_original_query_leads = db.query_leads


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, str) and value.strip().lower() == "-infinity":
        return None
    if isinstance(value, (datetime, date)) and getattr(value, "year", 2) <= 1:
        return None
    return _original_json_safe_value(value)


def _normalize_quick_search_filters(filters):
    normalized = dict(filters or {})
    cpf_digits = re.sub(r"[^0-9]", "", str(normalized.get("cpf") or ""))
    if cpf_digits and not normalized.get("busca") and not normalized.get("celular"):
        normalized.pop("cpf", None)
        normalized["busca"] = cpf_digits
    return normalized


def query_leads(filters=None, *args, **kwargs):
    return _original_query_leads(_normalize_quick_search_filters(filters), *args, **kwargs)


db._apply_text_multi_filter = _apply_text_multi_filter
db._apply_filters = _apply_filters
db.query_options = query_options
db._json_safe_value = _json_safe_value
db.query_leads = query_leads

db.logger.info("Camada de filtros fiéis ao banco instalada.")
