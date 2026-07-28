# -*- coding: utf-8 -*-
"""Recupera continuamente cargas UniFECAF que ficaram paradas na staging."""
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


def _pending_uploads() -> list[dict[str, Any]]:
    schema = db._safe_ident(str(os.getenv("UNIFECAF_DB_SCHEMA") or "unifecaf").strip())
    rows = db._run_gestao_query(
        f"""
        SELECT
            s.upload_id,
            COUNT(*)::bigint AS total_rows,
            COALESCE(MAX(NULLIF(BTRIM(p.rotina), '')), 'sp_processar_stg_leads') AS routine_name,
            MAX(p.atualizado_em) AS progresso_atualizado_em,
            MAX(l.atualizado_em) AS log_atualizado_em
        FROM {schema}.stg_leads s
        LEFT JOIN {schema}.op_importacao_progresso p
          ON p.upload_id = s.upload_id
        LEFT JOIN {schema}.logs_importacoes l
          ON l.upload_id = s.upload_id
        WHERE NULLIF(BTRIM(s.upload_id), '') IS NOT NULL
          AND COALESCE(s.processado, false) = false
          AND (
            p.upload_id IS NULL
            OR UPPER(COALESCE(p.status, '')) IN (
                'AGUARDANDO','STAGING','PENDENTE','ERRO','CONCLUIDO','CONCLUIDO_COM_REJEICOES'
            )
            OR (
              UPPER(COALESCE(p.status, '')) = 'PROCESSANDO'
              AND COALESCE(p.atualizado_em, timestamp '1900-01-01') < now() - interval '20 minutes'
            )
          )
        GROUP BY s.upload_id
        ORDER BY MIN(s.linha_arquivo) NULLS LAST, s.upload_id
        """,
        {},
        "unifecaf_pending_imports",
    )
    return list(rows or [])


def _ensure_progress_row(upload_id: str, total_rows: int, routine_name: str) -> None:
    schema = db._safe_ident(str(os.getenv("UNIFECAF_DB_SCHEMA") or "unifecaf").strip())
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
            erro = NULL,
            atualizado_em = now(),
            finalizado_em = NULL
        """,
        {"upload_id": upload_id, "routine_name": routine_name, "total_rows": total_rows},
        "unifecaf_recovery_progress",
    )

    # Reabre o log quando ainda há linhas não processadas na staging.
    db._run_gestao_query(
        f"""
        UPDATE {schema}.logs_importacoes
           SET status = 'RECEBIDO',
               etapa = 'RECUPERACAO_AUTOMATICA',
               mensagem = 'Carga reaberta automaticamente porque ainda possui linhas pendentes na staging.',
               linhas_recebidas = :total_rows,
               total_linhas = :total_rows,
               atualizado_em = now(),
               finalizado_em = NULL,
               erros = 0
         WHERE upload_id = :upload_id
        """,
        {"upload_id": upload_id, "total_rows": total_rows},
        "unifecaf_recovery_reopen_log",
    )


def _run_recovery() -> None:
    delay = max(2, int(os.getenv("UNIFECAF_RECOVERY_START_DELAY_SECONDS", "5") or 5))
    interval = max(10, int(os.getenv("UNIFECAF_RECOVERY_INTERVAL_SECONDS", "30") or 30))
    time.sleep(delay)
    while True:
        try:
            pending = _pending_uploads()
            if pending:
                logger.warning(
                    "unifecaf_recovery_found uploads=%s total_rows=%s",
                    len(pending),
                    sum(int(r.get("total_rows") or 0) for r in pending),
                )
            for row in pending:
                upload_id = str(row.get("upload_id") or "").strip()
                total_rows = int(row.get("total_rows") or 0)
                routine_name = str(row.get("routine_name") or "sp_processar_stg_leads").strip()
                if not upload_id or total_rows <= 0:
                    continue
                _ensure_progress_row(upload_id, total_rows, routine_name)
                worker = start_upload_worker("unifecaf", upload_id, routine_name, total_rows)
                worker.join()
        except Exception:
            logger.exception("unifecaf_recovery_error")
        time.sleep(interval)


def start_unifecaf_import_recovery() -> dict[str, Any]:
    global _started
    enabled = str(os.getenv("UNIFECAF_AUTO_RECOVERY_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "sim"}
    if not enabled:
        return {"status": "desabilitado"}
    with _lock:
        if _started:
            return {"status": "ja_iniciado"}
        _started = True
        thread = threading.Thread(target=_run_recovery, daemon=True, name="unifecaf-import-recovery")
        thread.start()
    return {
        "status": "iniciado",
        "delay_seconds": int(os.getenv("UNIFECAF_RECOVERY_START_DELAY_SECONDS", "5") or 5),
        "interval_seconds": int(os.getenv("UNIFECAF_RECOVERY_INTERVAL_SECONDS", "30") or 30),
    }
