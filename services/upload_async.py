# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import io
import logging
import os
import threading
import time
import uuid
from typing import Any, Dict

from sqlalchemy import text

from . import database as db
from .upload_pipeline import _execute_routine, _find_routine

logger = logging.getLogger(__name__)

STAGING_COLUMNS = {
    "anhanguera": {
        "status_inscricao", "data_inscricao", "origem", "unidade", "polo", "tipo_negocio",
        "curso", "modalidade", "turno", "nome", "cpf", "celular", "email",
        "graduacao", "conclusao",
        "data_ultima_acao", "qtd_acionamentos", "status", "data_disparo",
        "peca_disparo", "texto_disparo", "consultor_disparo", "tipo_disparo",
        "campanha", "observacao", "data_matricula", "matriculado", "canal",
        "acao_comercial", "consultor_comercial", "upload_id", "linha_arquivo",
        "nome_arquivo", "dt_upload",
    },
    "unifecaf": {
        "data_inscricao", "origem", "unidade", "tipo_negocio", "curso",
        "modalidade", "nome", "cpf", "celular", "email",
        "graduacao", "conclusao",
        "data_ultima_interacao", "qtd_acionamentos", "status", "data_disparo",
        "peca_disparo", "texto_disparo", "consultor_disparo", "tipo_disparo",
        "campanha", "data_matricula", "matriculado", "consultor_comercial",
        "observacao", "upload_id", "linha_arquivo", "nome_arquivo",
    },
}

UNIFECAF_ROUTINE_CANDIDATES = (
    "sp_processar_stg_leads",
    "sp_processar_stg_leads_site",
    "sp_importar_somente_leads_novos",
    "sp_importar_leads",
    "sp_processar_leads",
)


def _config(institution: str = "anhanguera") -> Dict[str, str]:
    key = str(institution or "anhanguera").strip().lower()
    if key == "unifecaf":
        schema = str(os.getenv("UNIFECAF_DB_SCHEMA") or "unifecaf").strip()
        return {"institution": "unifecaf", "schema": schema, "schema_ident": db._safe_ident(schema), "staging": "stg_leads", "progress": "op_importacao_progresso", "logs": "logs_importacoes"}
    schema = str(os.getenv("DB_SCHEMA", db.DB_SCHEMA) or "modelo_estrela").strip()
    return {"institution": "anhanguera", "schema": schema, "schema_ident": db._safe_ident(schema), "staging": "stg_leads_site", "progress": "op_importacao_progresso", "logs": "logs_importacoes"}


def _progress_row(upload_id: str) -> Dict[str, Any]:
    for institution in ("anhanguera", "unifecaf"):
        cfg = _config(institution)
        try:
            rows = db._run_gestao_query(f"SELECT * FROM {cfg['schema_ident']}.{cfg['progress']} WHERE upload_id=:upload_id", {"upload_id": upload_id}, f"upload_progress_get_{institution}")
            if rows:
                row = rows[0]
                row["institution"] = institution
                return row
        except Exception:
            logger.debug("progress table unavailable institution=%s", institution, exc_info=True)
    return {}


def get_upload_progress(upload_id: str) -> Dict[str, Any]:
    row = _progress_row(upload_id)
    if not row:
        raise LookupError("Importação não encontrada.")
    return row


def _set_progress(cfg: Dict[str, str], upload_id: str, status: str, etapa: str, progresso: float, **metrics: Any) -> None:
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
        f"SELECT {cfg['schema_ident']}.fn_atualizar_progresso_importacao(:upload_id,:status,:etapa,:progresso,:linhas_processadas,:linhas_inseridas,:linhas_ignoradas,:linhas_rejeitadas,:mensagem,:erro)",
        params,
        f"upload_progress_update_{cfg['institution']}",
    )


def _safe_set_progress(cfg: Dict[str, str], upload_id: str, status: str, etapa: str, progresso: float, **metrics: Any) -> None:
    try:
        _set_progress(cfg, upload_id, status, etapa, progresso, **metrics)
    except Exception:
        logger.warning("upload_progress_nonfatal institution=%s upload_id=%s status=%s etapa=%s", cfg["institution"], upload_id, status, etapa, exc_info=True)


def _resolve_routine(cfg: Dict[str, str], preferred: str):
    routine = _find_routine(cfg["schema"], preferred)
    if routine or cfg["institution"] != "unifecaf":
        return routine
    configured = [
        str(os.getenv("UNIFECAF_IMPORT_ROUTINE") or "").strip(),
        str(os.getenv("UNIFECAF_IMPORT_ROUTINE_MASSIVA") or "").strip(),
        *UNIFECAF_ROUTINE_CANDIDATES,
    ]
    for candidate in configured:
        if not candidate or candidate == preferred:
            continue
        routine = _find_routine(cfg["schema"], candidate)
        if routine:
            logger.warning("unifecaf_routine_fallback preferred=%s selected=%s", preferred, routine.get("routine_name"))
            return routine
    return None


def _cleanup_staging(cfg: Dict[str, str], upload_id: str) -> int:
    enabled = str(os.getenv("IMPORT_CLEAN_STAGING_AFTER_SUCCESS", "true")).strip().lower() in {"1", "true", "yes", "sim"}
    if not enabled:
        return 0
    with db.get_engine().begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {cfg['schema_ident']}.{db._safe_ident(cfg['staging'])} WHERE upload_id=:upload_id"),
            {"upload_id": upload_id},
        )
        return int(result.rowcount or 0)


def _worker(cfg: Dict[str, str], upload_id: str, routine_name: str, total_rows: int) -> None:
    started = time.monotonic()
    try:
        _safe_set_progress(cfg, upload_id, "PROCESSANDO", "LOCALIZANDO_ROTINA", 25)
        routine = _resolve_routine(cfg, routine_name)
        if not routine:
            raise RuntimeError(f"Nenhuma rotina compatível encontrada no schema {cfg['schema']} para processar {upload_id}.")

        selected_name = str(routine.get("routine_name") or routine_name)
        _safe_set_progress(cfg, upload_id, "PROCESSANDO", "EXECUTANDO_SP", 35, mensagem=f"Executando {selected_name}.")
        logger.warning("upload_sp_call institution=%s upload_id=%s rotina=%s linhas=%s", cfg["institution"], upload_id, selected_name, total_rows)

        report = _execute_routine(cfg["schema_ident"], routine, upload_id, total_rows)
        logger.warning("upload_sp_return institution=%s upload_id=%s rotina=%s report=%s", cfg["institution"], upload_id, selected_name, report)

        inserted = int(report.get("linhas_inseridas") or 0)
        updated = int(report.get("linhas_atualizadas") or 0)
        rejected = int(report.get("linhas_rejeitadas") or 0)
        existing_phone = int(report.get("existentes_por_celular") or 0)
        existing_cpf = int(report.get("existentes_por_cpf") or 0)
        duplicates_file = int(report.get("duplicados_no_arquivo") or report.get("duplicados_arquivo") or 0)
        no_identifier = int(report.get("linhas_sem_identificador") or 0)
        ignored = existing_phone + existing_cpf + duplicates_file + no_identifier
        processed = min(total_rows, inserted + updated + ignored + rejected)
        message = report.get("mensagem") or f"Importação concluída por {selected_name}."

        removed = 0
        try:
            removed = _cleanup_staging(cfg, upload_id)
            logger.warning("upload_staging_cleaned institution=%s upload_id=%s removidas=%s", cfg["institution"], upload_id, removed)
        except Exception:
            logger.warning("upload_staging_cleanup_nonfatal institution=%s upload_id=%s", cfg["institution"], upload_id, exc_info=True)

        try:
            with db.get_engine().begin() as conn:
                conn.execute(
                    text(f"""
                        UPDATE {cfg['schema_ident']}.{cfg['progress']}
                           SET status='CONCLUIDO', etapa='CONCLUIDO', progresso=100,
                               linhas_processadas=:processed, linhas_inseridas=:inserted,
                               linhas_ignoradas=:ignored, linhas_rejeitadas=:rejected,
                               duplicados_arquivo=:duplicates_file,
                               existentes_por_celular=:existing_phone,
                               existentes_por_cpf=:existing_cpf,
                               mensagem=:message, atualizado_em=now(), finalizado_em=now()
                         WHERE upload_id=:upload_id
                    """),
                    {
                        "processed": processed,
                        "inserted": inserted,
                        "ignored": ignored,
                        "rejected": rejected,
                        "duplicates_file": duplicates_file,
                        "existing_phone": existing_phone,
                        "existing_cpf": existing_cpf,
                        "message": message,
                        "upload_id": upload_id,
                    },
                )
        except Exception:
            logger.warning("upload_finalize_progress_nonfatal institution=%s upload_id=%s", cfg["institution"], upload_id, exc_info=True)

        logger.info(
            "upload_async_complete institution=%s upload_id=%s rotina=%s total=%s inseridas=%s atualizadas=%s staging_removidas=%s elapsed_s=%.2f",
            cfg["institution"], upload_id, selected_name, total_rows, inserted, updated, removed, time.monotonic() - started,
        )
    except Exception as exc:
        logger.exception("upload_async_error institution=%s upload_id=%s", cfg["institution"], upload_id)
        _safe_set_progress(cfg, upload_id, "ERRO", "ERRO", 100, erro=str(exc), mensagem="Falha ao processar importação.")


def start_upload_worker(institution: str, upload_id: str, routine_name: str, total_rows: int) -> threading.Thread:
    cfg = _config(institution)
    thread = threading.Thread(target=_worker, args=(cfg, upload_id, routine_name, total_rows), daemon=True, name=f"upload-{cfg['institution']}-{upload_id[:8]}")
    thread.start()
    return thread


def _prepare_for_unifecaf(prepared):
    rename = {"data_ultima_acao": "data_ultima_interacao", "polo": "unidade", "flag_matriculado": "matriculado"}
    return prepared.rename(columns={k: v for k, v in rename.items() if k in prepared.columns})


def _copy_dataframe_to_staging(cfg: Dict[str, str], prepared, upload_id: str, mode: str, routine_name: str, filename: str) -> None:
    engine = db.get_engine()
    raw = engine.raw_connection()
    cursor = None
    try:
        cursor = raw.cursor()
        timeout_seconds = max(180, int(os.getenv("LEADS_IMPORT_STAGING_TIMEOUT_SECONDS", "900") or 900))
        batch_rows = max(1000, min(20000, int(os.getenv("LEADS_IMPORT_COPY_BATCH_ROWS", "5000") or 5000)))

        cursor.execute("SET LOCAL lock_timeout = '10s'")
        cursor.execute(f"SET LOCAL statement_timeout = '{timeout_seconds}s'")
        cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (f"upload-staging:{cfg['institution']}",))
        if not bool(cursor.fetchone()[0]):
            raise RuntimeError(f"Já existe um arquivo sendo gravado na staging da {cfg['institution']}. Aguarde a conclusão e tente novamente.")

        cursor.execute(
            f"INSERT INTO {cfg['schema_ident']}.{cfg['progress']} (upload_id, modo, rotina, arquivo, status, etapa, linhas_total, progresso) VALUES (%s,%s,%s,%s,'STAGING','GRAVANDO_STAGING',%s,10)",
            (upload_id, mode, routine_name, filename, len(prepared)),
        )
        if cfg["institution"] == "unifecaf":
            cursor.execute(
                f"INSERT INTO {cfg['schema_ident']}.logs_importacoes (upload_id,nome_arquivo,status,etapa,total_linhas,linhas_recebidas) VALUES (%s,%s,'RECEBIDO','STAGING',%s,%s) ON CONFLICT (upload_id) DO NOTHING",
                (upload_id, filename, len(prepared), len(prepared)),
            )

        columns = list(prepared.columns)
        column_sql = ",".join(db._safe_ident(column) for column in columns)
        copy_sql = f"COPY {cfg['schema_ident']}.{db._safe_ident(cfg['staging'])} ({column_sql}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N', QUOTE '\"', ESCAPE '\"')"

        total_rows = len(prepared)
        copied = 0
        for start in range(0, total_rows, batch_rows):
            end = min(start + batch_rows, total_rows)
            batch = prepared.iloc[start:end]
            buffer = io.StringIO()
            batch.to_csv(
                buffer,
                index=False,
                header=False,
                sep="\t",
                na_rep="\\N",
                quoting=csv.QUOTE_MINIMAL,
                quotechar='"',
                escapechar="\\",
                lineterminator="\n",
            )
            buffer.seek(0)

            if hasattr(cursor, "copy_expert"):
                cursor.copy_expert(copy_sql, buffer)
            elif hasattr(cursor, "copy"):
                with cursor.copy(copy_sql) as copy:
                    while True:
                        chunk = buffer.read(1024 * 1024)
                        if not chunk:
                            break
                        copy.write(chunk)
            else:
                raise RuntimeError("O driver PostgreSQL não oferece suporte a COPY FROM STDIN.")

            copied = end
            progress = 10 + min(9, int((copied / total_rows) * 9)) if total_rows else 19
            cursor.execute(
                f"UPDATE {cfg['schema_ident']}.{cfg['progress']} SET progresso=%s, atualizado_em=now() WHERE upload_id=%s",
                (progress, upload_id),
            )
            logger.info(
                "upload_staging_batch institution=%s upload_id=%s copied=%s total=%s batch=%s",
                cfg["institution"], upload_id, copied, total_rows, batch_rows,
            )

        cursor.execute(f"UPDATE {cfg['schema_ident']}.{cfg['progress']} SET status='AGUARDANDO', etapa='STAGING_CONCLUIDA', progresso=20, atualizado_em=now() WHERE upload_id=%s", (upload_id,))
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        if cursor is not None:
            cursor.close()
        raw.close()


def enqueue_upload_dataframe(df, filename: str, mode: str, routine_name: str, institution: str = "anhanguera") -> Dict[str, Any]:
    cfg = _config(institution)
    upload_id = uuid.uuid4().hex
    started = time.monotonic()
    prepared = db._prepare_upload_dataframe(df, filename, upload_id)
    if cfg["institution"] == "unifecaf":
        prepared = _prepare_for_unifecaf(prepared)
    total_rows = len(prepared)
    if total_rows <= 0:
        raise ValueError("A planilha não possui linhas para importar.")
    if total_rows > 100000:
        raise ValueError("O limite é de 100000 linhas por arquivo.")

    stg_cols = STAGING_COLUMNS[cfg["institution"]]
    prepared = prepared[[c for c in prepared.columns if c in stg_cols]].copy()
    if "upload_id" not in prepared.columns:
        prepared["upload_id"] = upload_id
    if "nome_arquivo" in stg_cols and "nome_arquivo" not in prepared.columns:
        prepared["nome_arquivo"] = filename
    if "linha_arquivo" in stg_cols and "linha_arquivo" not in prepared.columns:
        prepared["linha_arquivo"] = range(2, total_rows + 2)

    _copy_dataframe_to_staging(cfg, prepared, upload_id, mode, routine_name, filename)
    worker = start_upload_worker(cfg["institution"], upload_id, routine_name, total_rows)
    logger.warning("upload_worker_started institution=%s upload_id=%s rotina=%s thread_alive=%s", cfg["institution"], upload_id, routine_name, worker.is_alive())
    logger.info("upload_async_queued institution=%s upload_id=%s rotina=%s linhas=%s elapsed_s=%.2f", cfg["institution"], upload_id, routine_name, total_rows, time.monotonic() - started)
    return {
        "job_id": upload_id,
        "upload_id": upload_id,
        "institution": cfg["institution"],
        "status": "AGUARDANDO",
        "done": False,
        "mode": "somente_novos" if mode == "SOMENTE_NOVOS" else "atualizar_existentes",
        "progress_url": f"/api/upload/progresso/{upload_id}",
        "report": {"linhas_recebidas": total_rows, "linhas_gravadas_staging": total_rows},
    }
