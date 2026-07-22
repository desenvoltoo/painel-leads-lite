# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from typing import Any, Dict

from sqlalchemy import text

from . import database as db
from .upload_pipeline import _execute_routine, _find_routine, _log_snapshot, _staging_count

logger = logging.getLogger(__name__)


def _schema() -> tuple[str, str]:
    schema = str(os.getenv("DB_SCHEMA", db.DB_SCHEMA) or "modelo_estrela").strip()
    return schema, db._safe_ident(schema)


def _progress_row(upload_id: str) -> Dict[str, Any]:
    _, schema_ident = _schema()
    rows = db._run_gestao_query(
        f"SELECT * FROM {schema_ident}.op_importacao_progresso WHERE upload_id = :upload_id",
        {"upload_id": upload_id},
        "upload_progress_get",
    )
    return (rows or [{}])[0]


def get_upload_progress(upload_id: str) -> Dict[str, Any]:
    row = _progress_row(upload_id)
    if not row:
        raise LookupError("Importação não encontrada.")
    return row


def _set_progress(upload_id: str, status: str, etapa: str, progresso: float, **metrics: Any) -> None:
    _, schema_ident = _schema()
    params = {
        "upload_id": upload_id,
        "status": status,
        "etapa": etapa,
        "progresso": progresso,
        "linhas_processadas": metrics.get("linhas_processadas"),
        "linhas_inseridas": metrics.get("linhas_inseridas"),
        "linhas_ignoradas": metrics.get("linhas_ignoradas"),
        "linhas_rejeitadas": metrics.get("linhas_rejeitadas"),
        "mensagem": metrics.get("mensagem"),
        "erro": metrics.get("erro"),
    }
    db._run_gestao_query(
        f"SELECT {schema_ident}.fn_atualizar_progresso_importacao("
        ":upload_id,:status,:etapa,:progresso,:linhas_processadas,:linhas_inseridas,"
        ":linhas_ignoradas,:linhas_rejeitadas,:mensagem,:erro)",
        params,
        "upload_progress_update",
    )


def _worker(upload_id: str, routine_name: str, total_rows: int) -> None:
    schema, schema_ident = _schema()
    started = time.monotonic()
    try:
        _set_progress(upload_id, "PROCESSANDO", "LOCALIZANDO_ROTINA", 25)
        routine = _find_routine(schema, routine_name)
        if not routine:
            raise RuntimeError(f"Rotina {routine_name} não encontrada no schema {schema}.")

        _set_progress(upload_id, "PROCESSANDO", "EXECUTANDO_SP", 35)
        report = _execute_routine(schema_ident, routine, upload_id, total_rows)
        retained = _staging_count(schema_ident, upload_id)
        log_row = _log_snapshot(schema_ident, upload_id)

        inserted = int(report.get("linhas_inseridas") or log_row.get("linhas_inseridas") or 0)
        rejected = int(report.get("linhas_rejeitadas") or log_row.get("linhas_rejeitadas") or 0)
        existing_phone = int(report.get("existentes_por_celular") or 0)
        existing_cpf = int(report.get("existentes_por_cpf") or 0)
        duplicates_file = int(report.get("duplicados_no_arquivo") or report.get("duplicados_arquivo") or 0)
        no_identifier = int(report.get("linhas_sem_identificador") or 0)
        ignored = existing_phone + existing_cpf + duplicates_file + no_identifier
        processed = inserted + ignored + rejected
        message = report.get("mensagem") or log_row.get("mensagem") or "Importação concluída."

        with db.get_engine().begin() as conn:
            conn.execute(text(f"""
                UPDATE {schema_ident}.op_importacao_progresso
                   SET status='CONCLUIDO', etapa='CONCLUIDO', progresso=100,
                       linhas_processadas=:processed, linhas_inseridas=:inserted,
                       linhas_ignoradas=:ignored, linhas_rejeitadas=:rejected,
                       duplicados_arquivo=:duplicates_file,
                       existentes_por_celular=:existing_phone,
                       existentes_por_cpf=:existing_cpf,
                       mensagem=:message, atualizado_em=now(), finalizado_em=now()
                 WHERE upload_id=:upload_id
            """), {
                "processed": processed,
                "inserted": inserted,
                "ignored": ignored,
                "rejected": rejected,
                "duplicates_file": duplicates_file,
                "existing_phone": existing_phone,
                "existing_cpf": existing_cpf,
                "message": message,
                "upload_id": upload_id,
            })
        logger.info(
            "upload_async_complete upload_id=%s rotina=%s total=%s processadas=%s inseridas=%s ignoradas=%s rejeitadas=%s staging_pendente=%s elapsed_s=%.2f",
            upload_id, routine_name, total_rows, processed, inserted, ignored, rejected, retained,
            time.monotonic() - started,
        )
    except Exception as exc:
        logger.exception("upload_async_error upload_id=%s rotina=%s", upload_id, routine_name)
        try:
            _set_progress(upload_id, "ERRO", "ERRO", 100, erro=str(exc), mensagem="Falha ao processar importação.")
        except Exception:
            logger.exception("upload_async_progress_error upload_id=%s", upload_id)


def enqueue_upload_dataframe(df, filename: str, mode: str, routine_name: str) -> Dict[str, Any]:
    schema, schema_ident = _schema()
    upload_id = uuid.uuid4().hex
    prepared = db._prepare_upload_dataframe(df, filename, upload_id)
    total_rows = len(prepared)
    if total_rows <= 0:
        raise ValueError("A planilha não possui linhas para importar.")

    stg_cols = set(db._table_columns(schema, "stg_leads_site"))
    prepared = prepared[[column for column in prepared.columns if column in stg_cols]]

    with db.get_engine().begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {schema_ident}.op_importacao_progresso
                (upload_id, modo, rotina, arquivo, status, etapa, linhas_total, progresso)
            VALUES (:upload_id, :modo, :rotina, :arquivo, 'STAGING', 'GRAVANDO_STAGING', :total, 10)
        """), {
            "upload_id": upload_id,
            "modo": mode,
            "rotina": routine_name,
            "arquivo": filename,
            "total": total_rows,
        })
        prepared.to_sql(
            "stg_leads_site", con=conn, schema=schema_ident,
            if_exists="append", index=False, method="multi", chunksize=1000,
        )
        conn.execute(text(f"""
            UPDATE {schema_ident}.op_importacao_progresso
               SET status='AGUARDANDO', etapa='STAGING_CONCLUIDA', progresso=20,
                   linhas_processadas=0, atualizado_em=now()
             WHERE upload_id=:upload_id
        """), {"upload_id": upload_id})

    thread = threading.Thread(
        target=_worker,
        args=(upload_id, routine_name, total_rows),
        daemon=True,
        name=f"upload-worker-{upload_id[:8]}",
    )
    thread.start()
    logger.info("upload_async_queued upload_id=%s modo=%s rotina=%s linhas=%s", upload_id, mode, routine_name, total_rows)
    return {
        "job_id": upload_id,
        "upload_id": upload_id,
        "status": "AGUARDANDO",
        "done": False,
        "mode": "somente_novos" if mode == "SOMENTE_NOVOS" else "atualizar_existentes",
        "progress_url": f"/api/upload/progresso/{upload_id}",
        "report": {"linhas_recebidas": total_rows, "linhas_gravadas_staging": total_rows},
    }
