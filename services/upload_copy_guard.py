# -*- coding: utf-8 -*-
"""Blindagem do COPY para staging.

Mantém o fluxo atual, mas adiciona:
- application_name por upload;
- PID do backend PostgreSQL nos logs;
- watchdog externo para COPY excessivamente longo;
- rollback/close explícitos e diagnóstico de erro.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import threading
import time
from typing import Any, Dict

from sqlalchemy import text

from . import database as db

logger = logging.getLogger(__name__)


def _int_env(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(str(os.getenv(name, default)).strip()))
    except Exception:
        return default


def _watch_copy(backend_pid: int, upload_id: str, institution: str, done: threading.Event) -> None:
    timeout = _int_env("UPLOAD_COPY_WATCHDOG_SECONDS", 210, 30)
    terminate_grace = _int_env("UPLOAD_COPY_TERMINATE_GRACE_SECONDS", 10, 1)
    if done.wait(timeout):
        return

    logger.error(
        "upload_copy_watchdog_timeout institution=%s upload_id=%s backend_pid=%s timeout_s=%s",
        institution,
        upload_id,
        backend_pid,
        timeout,
    )

    try:
        with db.get_engine().begin() as conn:
            cancelled = bool(
                conn.execute(
                    text("SELECT pg_cancel_backend(:pid)"),
                    {"pid": backend_pid},
                ).scalar()
            )
        logger.error(
            "upload_copy_watchdog_cancel institution=%s upload_id=%s backend_pid=%s result=%s",
            institution,
            upload_id,
            backend_pid,
            cancelled,
        )
    except Exception:
        logger.exception(
            "upload_copy_watchdog_cancel_error institution=%s upload_id=%s backend_pid=%s",
            institution,
            upload_id,
            backend_pid,
        )

    if done.wait(terminate_grace):
        return

    try:
        with db.get_engine().begin() as conn:
            terminated = bool(
                conn.execute(
                    text("SELECT pg_terminate_backend(:pid)"),
                    {"pid": backend_pid},
                ).scalar()
            )
        logger.critical(
            "upload_copy_watchdog_terminate institution=%s upload_id=%s backend_pid=%s result=%s",
            institution,
            upload_id,
            backend_pid,
            terminated,
        )
    except Exception:
        logger.exception(
            "upload_copy_watchdog_terminate_error institution=%s upload_id=%s backend_pid=%s",
            institution,
            upload_id,
            backend_pid,
        )


def _guarded_copy_dataframe_to_staging(
    cfg: Dict[str, str],
    prepared: Any,
    upload_id: str,
    mode: str,
    routine_name: str,
    filename: str,
) -> None:
    engine = db.get_engine()
    raw = engine.raw_connection()
    cursor = None
    done = threading.Event()
    backend_pid = 0
    started = time.monotonic()

    try:
        cursor = raw.cursor()
        cursor.execute("SET LOCAL lock_timeout = '5s'")
        cursor.execute("SET LOCAL statement_timeout = '180s'")
        cursor.execute("SET LOCAL idle_in_transaction_session_timeout = '240s'")
        cursor.execute(
            "SELECT set_config('application_name', %s, true)",
            (f"painel-upload:{cfg['institution']}:{upload_id[:8]}",),
        )
        cursor.execute("SELECT pg_backend_pid()")
        backend_pid = int(cursor.fetchone()[0])

        cursor.execute(
            "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
            (f"upload-staging:{cfg['institution']}",),
        )
        if not bool(cursor.fetchone()[0]):
            raise RuntimeError(
                f"Já existe um arquivo sendo gravado na staging da {cfg['institution']}. Aguarde a conclusão e tente novamente."
            )

        cursor.execute(
            f"INSERT INTO {cfg['schema_ident']}.{cfg['progress']} "
            "(upload_id, modo, rotina, arquivo, status, etapa, linhas_total, progresso) "
            "VALUES (%s,%s,%s,%s,'STAGING','GRAVANDO_STAGING',%s,10)",
            (upload_id, mode, routine_name, filename, len(prepared)),
        )
        if cfg["institution"] == "unifecaf":
            cursor.execute(
                f"INSERT INTO {cfg['schema_ident']}.logs_importacoes "
                "(upload_id,nome_arquivo,status,etapa,total_linhas,linhas_recebidas) "
                "VALUES (%s,%s,'RECEBIDO','STAGING',%s,%s) ON CONFLICT (upload_id) DO NOTHING",
                (upload_id, filename, len(prepared), len(prepared)),
            )

        columns = list(prepared.columns)
        buffer = io.StringIO()
        prepared.to_csv(
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
        column_sql = ",".join(db._safe_ident(column) for column in columns)
        copy_sql = (
            f"COPY {cfg['schema_ident']}.{db._safe_ident(cfg['staging'])} ({column_sql}) "
            "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N', QUOTE '\"', ESCAPE '\"')"
        )

        logger.warning(
            "upload_copy_start institution=%s upload_id=%s backend_pid=%s rows=%s chars=%s",
            cfg["institution"],
            upload_id,
            backend_pid,
            len(prepared),
            len(buffer.getvalue()),
        )

        watchdog = threading.Thread(
            target=_watch_copy,
            args=(backend_pid, upload_id, cfg["institution"], done),
            daemon=True,
            name=f"copy-watchdog-{upload_id[:8]}",
        )
        watchdog.start()

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

        done.set()
        logger.warning(
            "upload_copy_complete institution=%s upload_id=%s backend_pid=%s rows=%s elapsed_s=%.2f",
            cfg["institution"],
            upload_id,
            backend_pid,
            len(prepared),
            time.monotonic() - started,
        )

        cursor.execute(
            f"UPDATE {cfg['schema_ident']}.{cfg['progress']} "
            "SET status='AGUARDANDO', etapa='STAGING_CONCLUIDA', progresso=20, atualizado_em=now() "
            "WHERE upload_id=%s",
            (upload_id,),
        )
        raw.commit()
    except Exception:
        done.set()
        logger.exception(
            "upload_copy_error institution=%s upload_id=%s backend_pid=%s elapsed_s=%.2f",
            cfg.get("institution"),
            upload_id,
            backend_pid,
            time.monotonic() - started,
        )
        try:
            raw.rollback()
        except Exception:
            logger.exception(
                "upload_copy_rollback_error institution=%s upload_id=%s backend_pid=%s",
                cfg.get("institution"),
                upload_id,
                backend_pid,
            )
        raise
    finally:
        done.set()
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                logger.warning("upload_copy_cursor_close_error upload_id=%s", upload_id, exc_info=True)
        try:
            raw.close()
        except Exception:
            logger.warning("upload_copy_connection_close_error upload_id=%s", upload_id, exc_info=True)


def apply_upload_copy_guard() -> Dict[str, Any]:
    from . import upload_async

    upload_async._copy_dataframe_to_staging = _guarded_copy_dataframe_to_staging
    return {
        "status": "aplicado",
        "watchdog_seconds": _int_env("UPLOAD_COPY_WATCHDOG_SECONDS", 210, 30),
        "terminate_grace_seconds": _int_env("UPLOAD_COPY_TERMINATE_GRACE_SECONDS", 10, 1),
        "backend_diagnostics": True,
    }
