# -*- coding: utf-8 -*-
"""Unifica Anhanguera e UniFECAF nas métricas e exportações da Gestão."""
from __future__ import annotations

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
TEXT_COLUMNS = set(REQUIRED_COLUMNS) - DATE_COLUMNS - {"qtd_acionamentos", "matriculado"}

ALIASES = {
    "unidade": ("unidade", "polo"),
    "data_ultima_acao": ("data_ultima_acao", "data_ultima_interacao"),
    "matriculado": ("matriculado", "flag_matriculado"),
}


def _rows(sql: str, params: dict[str, Any] | None = None, name: str = "gestao_multi_instituicao"):
    return db._run_gestao_query(sql, params or {}, name)


def _find_relation(schema: str) -> tuple[str, set[str]] | None:
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
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=:schema AND table_name=:table
        """,
        {"schema": schema, "table": table},
        f"multi_columns_{schema}",
    )
    return f"{db._safe_ident(schema)}.{db._safe_ident(table)}", {str(r["column_name"]) for r in cols}


def _source_for(target: str, available: set[str]) -> str | None:
    for candidate in ALIASES.get(target, (target,)):
        if candidate in available:
            return candidate
    return None


def _select_part(relation: str, available: set[str], institution: str) -> str:
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
            if target in DATE_COLUMNS:
                expr = f"modelo_estrela.parse_ts_any({col}::text)"
            elif target == "qtd_acionamentos":
                expr = f"NULLIF(regexp_replace(COALESCE({col}::text,''),'[^0-9]','','g'),'')::bigint"
            elif target == "matriculado":
                expr = f"CASE WHEN UPPER(BTRIM(COALESCE({col}::text,''))) IN ('TRUE','T','1','SIM','S') THEN TRUE ELSE FALSE END"
            else:
                expr = f"NULLIF(BTRIM({col}::text),'')"
        parts.append(f"{expr} AS {alias}")
    parts.append(f"'{institution}'::text AS instituicao")
    return f"SELECT {', '.join(parts)} FROM {relation}"


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
    return "(" + " UNION ALL ".join(selects) + ")"


def apply_multi_institution_metrics() -> dict[str, Any]:
    from . import gestao_sem_lotes
    from . import produtividade_export

    gestao_sem_lotes._relation = combined_relation
    produtividade_export._relation = combined_relation

    return {
        "status": "aplicado",
        "fontes": ["modelo_estrela", "unifecaf"],
        "uso": ["gestao", "ranking", "funil", "campanhas", "exportacao"],
    }
