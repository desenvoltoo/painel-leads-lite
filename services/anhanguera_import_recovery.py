# -*- coding: utf-8 -*-
"""Recupera continuamente cargas Anhanguera que ficaram paradas na staging."""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

from . import database as db
from .upload_async import start_upload_worker

logger = logging.getLogger(__name__)
_started = False
_lock = threading.Lock()


def _schema() -> str:
    return str(os.getenv("DB_SCHEMA") or getattr(db, "DB_SCHEMA", None) or "modelo_estrela").strip()


def _pending_uploads() -> list[dict[str, Any]]:
    schema = db._safe_ident(_schema())
    rows = db._run_gestao_query(
        f"""
        SELECT
            s.upload_id,
            COUNT(*)::bigint AS total_rows,
            COALESCE(MAX(p.rotina), 'sp_processar_stg_leads_site') AS routine_name
        FROM {schema}.stg_leads_site s
        LEFT JOIN {schema}.op_importacao_progresso p
          ON p.upload_id = s.upload_id
        WHERE NULLIF(BTRIM(s.upload_id::text), '') IS NOT NULL
          AND (
            p.upload_id IS NULL
            OR UPPER(COALESCE(p.status, '')) IN ('AGUARDANDO','STAGING','PENDENTE','ERRO')
            OR (
              UPPER(COALESCE(p.status, '')) = 'PROCESSANDO'
              AND COALESCE(p.atualizado_em, now()) < now() - interval '20 minutes'
            )
          )
        GROUP BY s.upload_id
        ORDER BY MIN(s.linha_arquivo) NULLS LAST, s.upload_id
        """,
        {},
        "anhanguera_pending_imports",
    )
    return list(rows or [])


def _ensure_progress_row(upload_id: str, total_rows: int, routine_name: str) -> None:
    schema = db._safe_ident(_schema())
    db._run_gestao_query(
        f"""
        INSERT INTO {schema}.op_importacao_progresso
            (upload_id, modo, rotina, arquivo, status, etapa, linhas_total, progresso, atualizado_em)
        VALUES
            (:upload_id, 'ATUALIZAR_EXISTENTES', :routine_name, 'RECUPERACAO_AUTOMATICA',
             'AGUARDANDO', 'RECUPERACAO_AUTOMATICA', :total_rows, 20, now())
        ON CONFLICT (upload_id) DO UPDATE SET
            rotina = EXCLUDED.rotina,
            status = 'AGUARDANDO',
            etapa = 'RECUPERACAO_AUTOMATICA',
            linhas_total = EXCLUDED.linhas_total,
            progresso = 20,
            atualizado_em = now()
        """,
        {"upload_id": upload_id, "routine_name": routine_name, "total_rows": total_rows},
        "anhanguera_recovery_progress",
    )


def _run_recovery() -> None:
    delay = max(2, int(os.getenv("ANHANGUERA_RECOVERY_START_DELAY_SECONDS", "8") or 8))
    interval = max(10, int(os.getenv("ANHANGUERA_RECOVERY_INTERVAL_SECONDS", "30") or 30))
    time.sleep(delay)
    while True:
        try:
            pending = _pending_uploads()
            if pending:
                logger.warning(
                    "anhanguera_recovery_found uploads=%s total_rows=%s",
                    len(pending),
                    sum(int(row.get("total_rows") or 0) for row in pending),
                )
            for row in pending:
                upload_id = str(row.get("upload_id") or "").strip()
                total_rows = int(row.get("total_rows") or 0)
                routine_name = str(row.get("routine_name") or "sp_processar_stg_leads_site").strip()
                if not upload_id or total_rows <= 0:
                    continue
                _ensure_progress_row(upload_id, total_rows, routine_name)
                worker = start_upload_worker("anhanguera", upload_id, routine_name, total_rows)
                worker.join()
        except Exception:
            logger.exception("anhanguera_recovery_error")
        time.sleep(interval)


def start_anhanguera_import_recovery() -> dict[str, Any]:
    global _started
    enabled = str(os.getenv("ANHANGUERA_AUTO_RECOVERY_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "sim"}
    if not enabled:
        return {"status": "desabilitado"}
    with _lock:
        if _started:
            return {"status": "ja_iniciado"}
        _started = True
        thread = threading.Thread(target=_run_recovery, daemon=True, name="anhanguera-import-recovery")
        thread.start()
    return {
        "status": "iniciado",
        "delay_seconds": int(os.getenv("ANHANGUERA_RECOVERY_START_DELAY_SECONDS", "8") or 8),
        "interval_seconds": int(os.getenv("ANHANGUERA_RECOVERY_INTERVAL_SECONDS", "30") or 30),
    }
