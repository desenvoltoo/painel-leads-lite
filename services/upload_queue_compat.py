# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict

from . import database as db
from . import upload_async

logger = logging.getLogger(__name__)
_APPLIED = False
_ORIGINAL_WORKER: Callable[..., Any] | None = None


def _queued_worker(cfg: Dict[str, str], upload_id: str, routine_name: str, total_rows: int) -> None:
    """Executa apenas uma procedure por instituição de cada vez.

    O lock é de sessão e fica em uma conexão dedicada enquanto a procedure roda.
    Anhanguera e UniFECAF usam chaves diferentes e podem processar em paralelo.
    """
    assert _ORIGINAL_WORKER is not None

    lock_key = f"upload-processing:{cfg['institution']}"
    raw = db.get_engine().raw_connection()
    cursor = None
    queued_at = time.monotonic()
    try:
        cursor = raw.cursor()
        try:
            upload_async._set_progress(
                cfg,
                upload_id,
                "AGUARDANDO_PROCESSAMENTO",
                "FILA_PROCESSAMENTO",
                20,
                mensagem="Upload concluído. Aguardando processamento interno.",
            )
        except Exception:
            logger.exception("upload_queue_progress_error upload_id=%s", upload_id)

        logger.info(
            "upload_processing_queue_wait institution=%s upload_id=%s lock_key=%s",
            cfg['institution'], upload_id, lock_key,
        )
        cursor.execute("SELECT pg_advisory_lock(hashtext(%s))", (lock_key,))
        logger.info(
            "upload_processing_queue_acquired institution=%s upload_id=%s wait_s=%.2f",
            cfg['institution'], upload_id, time.monotonic() - queued_at,
        )

        _ORIGINAL_WORKER(cfg, upload_id, routine_name, total_rows)
    finally:
        if cursor is not None:
            try:
                cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_key,))
            except Exception:
                logger.exception("upload_processing_unlock_error upload_id=%s", upload_id)
            cursor.close()
        raw.close()


def apply_upload_queue_compat() -> None:
    global _APPLIED, _ORIGINAL_WORKER
    if _APPLIED:
        return

    _ORIGINAL_WORKER = upload_async._worker
    upload_async._worker = _queued_worker
    _APPLIED = True
    logger.info("upload_queue_compat_applied mode=one-procedure-per-institution")
