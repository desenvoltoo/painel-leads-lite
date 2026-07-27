# -*- coding: utf-8 -*-
"""Unifica Anhanguera e UniFECAF nas métricas e exportações da Gestão."""
from __future__ import annotations

from functools import lru_cache
from typing import Any

from . import database as db

REQUIRED_COLUMNS = [
    "status_inscricao", "data_inscricao", "origem", "unidade", "tipo_negocio",
    "curso", "modalidade", "turno", "nome", "cpf", "celular", "email",
    "data_ultima_acao", "qtd_acionamentos", "status", "data_disparo",
    "peca_disparo", "texto_disparo", "consultor_disparo", "tipo_disparo",
    "campanha", "observacao", "data_matricula", "matriculado", "canal",
    "acao_comercial", "consultor_comercial",
]

DATE_COLUMNS = {"data_inscricao", "data_ultima_acao", "data_disparo", "data_matricula"}
NATIVE_DATE_TYPES = {
    "date",
    "timestamp without time zone",
    "timestamp with time zone",
}

ALIASES = {
    "unidade": ("unidade", "polo"),
    "data_ultima_acao": ("data_ultima_acao", "data_ultima_interacao"),
    "matriculado": ("matriculado", "flag_matriculado"),
}


def _rows(sql: str, params: dict[str, Any] | None = None, name: str = "gestao_multi_instituicao"):
    return db._run_gestao_query(sql, params or {}, name)


@lru_cache(maxsize=8)
def _find_relation(schema: str) -> tuple[str, dict[str, str]] | None:
    rows = _rows(
        """
        SELECT table_name
        FROM (
          SELECT table_name, 0 AS prioridade
          FROM information_schema.views
          WHERE table_schema=:schema
            AND table_name IN ('vw_leads_painel_lite','leads_painel_lite')
          UNION ALL
          SELECT table_name, 1 AS prioridade
          FROM information_schema.tables
          WHERE table_schema=:schema
            AND table_name IN ('vw_leads_painel_lite','leads_painel_lite')
        ) x
        ORDER BY prioridade, table_name
        LIMIT 1
        """,
        {"schema": schema},
        f"multi_relation_{schema}",
    )
    if not rows:
        return None
    table = str(rows[0]["table_name"])
    cols = _rows(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema=:schema AND table_name=:table
        """,
        {"schema": schema, "table": table},
        f"multi_columns_{schema}",
    )
    column_types = {str(r["column_name"]): str(r.get("data_type") or "") for r in cols}
    return f"{db._safe_ident(schema)}.{db._safe_ident(table)}", column_types


def _source_for(target: str, available: dict[str, str]) -> str | None:
    for candidate in ALIASES.get(target, (target,)):
        if candidate in available:
            return candidate
    return None


def _select_part(relation: str, available: dict[str, str], institution: str) -> str:
    parts: list[str] = []
    for target in REQUIRED_COLUMNS:
        source = _source_for(target, available)
        alias = db._safe_ident(target)
        if not source:
            if target in DATE_COLUMNS:
                expr = "NULL::timestamp"
            elif target == "qtd_acionamentos":
                expr = "NULL::bigint"
            elif target == "matriculado":
                expr = "FALSE::boolean"
            else:
                expr = "NULL::text"
        else:
            col = db._safe_ident(source)
            data_type = available.get(source, "")
            if target in DATE_COLUMNS:
                if data_type in NATIVE_DATE_TYPES:
                    expr = f"{col}::timestamp"
                else:
                    expr = f"modelo_estrela.parse_ts_any({col}::text)"
            elif target == "qtd_acionamentos":
                if data_type in {"smallint", "integer", "bigint", "numeric"}:
                    expr = f"{col}::bigint"
                else:
                    expr = f"NULLIF(regexp_replace(COALESCE({col}::text,''),'[^0-9]','','g'),'')::bigint"
            elif target == "matriculado":
                if data_type == "boolean":
                    expr = f"COALESCE({col}, FALSE)"
                else:
                    expr = f"CASE WHEN UPPER(BTRIM(COALESCE({col}::text,''))) IN ('TRUE','T','1','SIM','S') THEN TRUE ELSE FALSE END"
            else:
                expr = f"NULLIF(BTRIM({col}::text),'')"
        parts.append(f"{expr} AS {alias}")
    parts.append(f"'{institution}'::text AS instituicao")
    return f"SELECT {', '.join(parts)} FROM {relation}"


@lru_cache(maxsize=1)
def combined_relation() -> str:
    selects: list[str] = []
    for schema, institution in (("modelo_estrela", "ANHANGUERA"), ("unifecaf", "UNIFECAF")):
        found = _find_relation(schema)
        if not found:
            continue
        relation, columns = found
        selects.append(_select_part(relation, columns, institution))
    if not selects:
        raise RuntimeError("Nenhuma fonte de leads encontrada para Anhanguera ou UniFECAF.")
    return "(" + " UNION ALL ".join(selects) + ") AS leads_unificados"


def clear_relation_cache() -> None:
    _find_relation.cache_clear()
    combined_relation.cache_clear()


def _patch_export_dates(produtividade_export) -> None:
    current = produtividade_export._make_workbook
    if getattr(current, "_date_export_guard", False):
        return

    def guarded_make_workbook(rows, start, month_label):
        normalized = []
        for original in rows:
            row = dict(original)
            for field in DATE_COLUMNS:
                parsed = produtividade_export._as_date(row.get(field))
                row[field] = parsed
            normalized.append(row)
        return current(normalized, start, month_label)

    guarded_make_workbook._date_export_guard = True
    produtividade_export._make_workbook = guarded_make_workbook


def apply_multi_institution_metrics() -> dict[str, Any]:
    from . import gestao_sem_lotes
    from . import produtividade_export

    clear_relation_cache()
    gestao_sem_lotes._relation = combined_relation
    produtividade_export._relation = combined_relation
    _patch_export_dates(produtividade_export)

    return {
        "status": "aplicado",
        "fontes": ["modelo_estrela", "unifecaf"],
        "uso": ["gestao", "ranking", "funil", "campanhas", "exportacao"],
        "datas_exportacao": "DD/MM/AAAA",
        "otimizacoes": ["cache_relacao", "tipos_nativos", "menos_parse_por_linha"],
    }
