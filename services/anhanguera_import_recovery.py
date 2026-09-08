# -*- coding: utf-8 -*-
"""Recupera continuamente cargas Anhanguera pendentes na staging."""
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
            COALESCE(MAX(p.modo), 'ATUALIZAR_EXISTENTES') AS mode,
            CASE
                WHEN UPPER(COALESCE(MAX(p.modo), '')) IN ('SOMENTE_NOVOS', 'SOMENTE NOVOS', 'NOVOS')
                    THEN 'sp_importar_leads_novos'
                ELSE 'sp_importar_leads_diario'
            END AS routine_name
        FROM {schema}.stg_leads_site s
        LEFT JOIN {schema}.op_importacao_progresso p
          ON p.upload_id = s.upload_id
        WHERE NULLIF(BTRIM(s.upload_id::text), '') IS NOT NULL
          AND COALESCE(s.processado, false) = false
          AND (
                p.upload_id IS NULL
                OR UPPER(COALESCE(p.status, '')) IN ('ERRO', 'AGUARDANDO', 'STAGING')
              )
          AND NOT (
                UPPER(COALESCE(p.status, '')) = 'PROCESSANDO'
                AND COALESCE(p.atualizado_em, p.criado_em, now()) >= now() - interval '20 minutes'
              )
        GROUP BY s.upload_id
        ORDER BY MIN(s.linha_arquivo) NULLS LAST, s.upload_id
        """,
        {},
        "anhanguera_pending_imports",
    )
    return list(rows or [])


def _still_pending(upload_id: str) -> bool:
    schema = db._safe_ident(_schema())
    rows = db._run_gestao_query(
        f"""
        SELECT EXISTS (
            SELECT 1
            FROM {schema}.stg_leads_site
            WHERE upload_id = :upload_id
              AND COALESCE(processado, false) = false
        ) AS pendente
        """,
        {"upload_id": upload_id},
        "anhanguera_recovery_recheck_pending",
    )
    return bool(rows and rows[0].get("pendente"))


def _mark_completed_if_empty(upload_id: str) -> None:
    schema = db._safe_ident(_schema())
    db._run_gestao_query(
        f"""
        UPDATE {schema}.op_importacao_progresso p
           SET status = 'CONCLUIDO',
               etapa = 'CONCLUIDO',
               progresso = 100,
               erro = NULL,
               mensagem = COALESCE(NULLIF(p.mensagem, ''), 'Importação concluída.'),
               atualizado_em = now(),
               finalizado_em = COALESCE(p.finalizado_em, now())
         WHERE p.upload_id = :upload_id
           AND NOT EXISTS (
                SELECT 1
                FROM {schema}.stg_leads_site s
                WHERE s.upload_id = p.upload_id
                  AND COALESCE(s.processado, false) = false
           )
        """,
        {"upload_id": upload_id},
        "anhanguera_recovery_mark_completed_if_empty",
    )


def _claim_upload(upload_id: str, total_rows: int, routine_name: str) -> bool:
    """Reserva atomicamente um upload para uma única instância do recovery."""
    schema = db._safe_ident(_schema())
    rows = db._run_gestao_query(
        f"""
        INSERT INTO {schema}.op_importacao_progresso
            (upload_id, modo, rotina, arquivo, status, etapa, linhas_total, progresso, atualizado_em)
        VALUES
            (:upload_id,
             CASE WHEN :routine_name = 'sp_importar_leads_novos' THEN 'SOMENTE_NOVOS' ELSE 'ATUALIZAR_EXISTENTES' END,
             :routine_name,
             'RECUPERACAO_AUTOMATICA',
             'PROCESSANDO', 'RECUPERACAO_AUTOMATICA', :total_rows, 25, now())
        ON CONFLICT (upload_id) DO UPDATE SET
            rotina = EXCLUDED.rotina,
            status = 'PROCESSANDO',
            etapa = 'RECUPERACAO_AUTOMATICA',
            linhas_total = EXCLUDED.linhas_total,
            progresso = 25,
            erro = NULL,
            atualizado_em = now(),
            finalizado_em = NULL
        WHERE UPPER(COALESCE({schema}.op_importacao_progresso.status, '')) <> 'PROCESSANDO'
           OR COALESCE({schema}.op_importacao_progresso.atualizado_em,
                       {schema}.op_importacao_progresso.criado_em,
                       now()) < now() - interval '20 minutes'
        RETURNING upload_id
        """,
        {"upload_id": upload_id, "routine_name": routine_name, "total_rows": total_rows},
        "anhanguera_recovery_claim",
    )
    return bool(rows)


def _reopen_log(upload_id: str) -> None:
    schema = db._safe_ident(_schema())
    db._run_gestao_query(
        f"""
        UPDATE {schema}.logs_importacoes
           SET status = 'PROCESSANDO',
               etapa = 'RECUPERACAO_AUTOMATICA',
               mensagem = 'Carga pendente retomada automaticamente.',
               atualizado_em = now(),
               finalizado_em = NULL
         WHERE upload_id = :upload_id
        """,
        {"upload_id": upload_id},
        "anhanguera_recovery_reopen_log",
    )


def _run_recovery() -> None:
    delay = max(2, int(os.getenv("ANHANGUERA_RECOVERY_START_DELAY_SECONDS", "5") or 5))
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
                routine_name = str(row.get("routine_name") or "sp_importar_leads_diario").strip()
                if not upload_id or total_rows <= 0:
                    continue

                if not _still_pending(upload_id):
                    logger.info("anhanguera_recovery_skip_empty upload_id=%s", upload_id)
                    _mark_completed_if_empty(upload_id)
                    continue

                # Apenas uma instância pode transformar o status em PROCESSANDO.
                if not _claim_upload(upload_id, total_rows, routine_name):
                    logger.info("anhanguera_recovery_skip_claimed upload_id=%s", upload_id)
                    continue

                # Outra execução manual pode ter concluído entre o claim e o worker.
                if not _still_pending(upload_id):
                    logger.info("anhanguera_recovery_skip_after_claim upload_id=%s", upload_id)
                    _mark_completed_if_empty(upload_id)
                    continue

                _reopen_log(upload_id)
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
        "delay_seconds": int(os.getenv("ANHANGUERA_RECOVERY_START_DELAY_SECONDS", "5") or 5),
        "interval_seconds": int(os.getenv("ANHANGUERA_RECOVERY_INTERVAL_SECONDS", "30") or 30),
    }
