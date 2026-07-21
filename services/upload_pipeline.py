# -*- coding: utf-8 -*-
"""Pipeline robusto de upload para PostgreSQL/Supabase."""
from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Dict

from sqlalchemy import text

from . import database as db

logger = logging.getLogger(__name__)


def _routine_candidates(preferred_routine: str | None = None) -> list[str]:
    configured = str(os.getenv("LEADS_IMPORT_ROUTINE") or "").strip()
    names = [str(preferred_routine or "").strip()]
    if not preferred_routine:
        names.extend([
            configured,
            "sp_processar_stg_leads_site",
            "sp_importar_leads_site",
            "sp_import_leads",
            "sp_import_star_from_site",
        ])
    result: list[str] = []
    for name in names:
        if name and name not in result and db.IDENT_RE.fullmatch(name):
            result.append(name)
    return result


def _find_routine(schema: str, preferred_routine: str | None = None) -> Dict[str, Any] | None:
    names = _routine_candidates(preferred_routine)
    if not names:
        return None
    rows = db._run_gestao_query(
        """
        SELECT p.proname AS routine_name,
               p.prokind,
               pg_get_function_identity_arguments(p.oid) AS identity_args
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = :schema
          AND p.proname = ANY(:names)
        ORDER BY array_position(:names, p.proname),
                 CASE WHEN pg_get_function_identity_arguments(p.oid) = 'text' THEN 0 ELSE 1 END
        """,
        {"schema": schema, "names": names},
        "find_import_routine",
    )
    for row in rows or []:
        args = str(row.get("identity_args") or "").strip().lower()
        if args in {"text", "character varying", "varchar"}:
            return row
    return rows[0] if rows else None


def _staging_count(schema_ident: str, upload_id: str) -> int:
    rows = db._run_gestao_query(
        f"SELECT COUNT(*) AS total FROM {schema_ident}.stg_leads_site WHERE upload_id = :upload_id",
        {"upload_id": upload_id},
        "upload_staging_retained",
    )
    return int((rows or [{}])[0].get("total") or 0)


def _log_snapshot(schema_ident: str, upload_id: str) -> Dict[str, Any]:
    rows = db._run_gestao_query(
        f"""
        SELECT *
        FROM {schema_ident}.logs_importacoes
        WHERE upload_id = :upload_id OR id_importacao = :upload_id
        ORDER BY criado_em DESC
        LIMIT 1
        """,
        {"upload_id": upload_id},
        "upload_log_snapshot",
    )
    return (rows or [{}])[0]


def _execute_routine(schema_ident: str, routine: Dict[str, Any], upload_id: str) -> Dict[str, Any]:
    name = db._safe_ident(str(routine.get("routine_name") or ""))
    prokind = str(routine.get("prokind") or "f").lower()
    if prokind == "p":
        with db.get_engine().begin() as conn:
            conn.execute(text(f"CALL {schema_ident}.{name}(:upload_id)"), {"upload_id": upload_id})
        return {}
    rows = db._run_gestao_query(
        f"SELECT * FROM {schema_ident}.{name}(:upload_id)",
        {"upload_id": upload_id},
        f"execute_{name}",
    )
    return (rows or [{}])[0]


def process_upload_dataframe(df, filename: str = "upload", upload_id: str | None = None, routine_name: str | None = None):
    schema = str(os.getenv("DB_SCHEMA", db.DB_SCHEMA) or "modelo_estrela").strip()
    schema_ident = db._safe_ident(schema)
    upload_id = upload_id or uuid.uuid4().hex
    prepared = db._prepare_upload_dataframe(df, filename, upload_id)

    if prepared.empty:
        return {"job_id": upload_id, "status": "DONE", "done": True, "report": {"linhas_recebidas": 0, "linhas_processadas": 0, "linhas_rejeitadas": 0, "linhas_gravadas_staging": 0, "linhas_pendentes_staging": 0, "staging_retida": False, "duplicados_arquivo": 0, "duplicados_banco": 0}}

    stg_cols = set(db._table_columns(schema, "stg_leads_site"))
    if not stg_cols:
        raise RuntimeError(f"Tabela {schema}.stg_leads_site não encontrada.")

    prepared = prepared[[column for column in prepared.columns if column in stg_cols]]
    for required in ("upload_id", "dt_upload"):
        if required not in prepared.columns:
            raise RuntimeError(f"Coluna {required} não existe na staging.")

    logger.info("upload_staging_insert inicio upload_id=%s arquivo=%s linhas=%s rotina_preferida=%s", upload_id, filename, len(prepared), routine_name or "padrao")
    with db.get_engine().begin() as conn:
        prepared.to_sql("stg_leads_site", con=conn, schema=schema_ident, if_exists="append", index=False, method="multi", chunksize=1000)

    routine = _find_routine(schema, routine_name)
    if not routine:
        candidates = ", ".join(_routine_candidates(routine_name))
        raise RuntimeError(f"Nenhuma rotina de consolidação encontrada no schema {schema}. Rotinas procuradas: {candidates}.")

    logger.info("upload_routine_execute upload_id=%s routine=%s kind=%s", upload_id, routine.get("routine_name"), routine.get("prokind"))
    report = _execute_routine(schema_ident, routine, upload_id)
    retained = _staging_count(schema_ident, upload_id)
    log_row = _log_snapshot(schema_ident, upload_id)

    processed = int(report.get("linhas_processadas") or report.get("linhas_validas") or report.get("linhas_novas") or log_row.get("linhas_validas") or len(prepared))
    rejected = int(report.get("linhas_rejeitadas") or log_row.get("linhas_rejeitadas") or 0)

    return {
        "job_id": upload_id,
        "status": "DONE",
        "done": True,
        "routine": {"name": routine.get("routine_name"), "kind": "PROCEDURE" if str(routine.get("prokind")) == "p" else "FUNCTION"},
        "report": {
            "linhas_recebidas": int(report.get("linhas_recebidas") or len(prepared)),
            "linhas_processadas": processed,
            "linhas_rejeitadas": rejected,
            "linhas_gravadas_staging": len(prepared),
            "linhas_pendentes_staging": retained,
            "staging_retida": bool(retained),
            "linhas_novas": int(report.get("linhas_novas") or 0),
            "existentes_por_celular": int(report.get("existentes_por_celular") or 0),
            "existentes_por_cpf": int(report.get("existentes_por_cpf") or 0),
            "duplicados_arquivo": int(report.get("duplicados_no_arquivo") or report.get("duplicados_arquivo") or log_row.get("duplicados_arquivo") or 0),
            "duplicados_banco": int(report.get("duplicados_banco") or log_row.get("duplicados_banco") or 0),
            "mensagem": report.get("mensagem") or "",
        },
    }
