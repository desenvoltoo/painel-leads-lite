# -*- coding: utf-8 -*-
"""Aplica a atualização prioritária de matriculados antes de limpar a staging."""
from __future__ import annotations

import logging
import threading
from typing import Any

from . import database as db
from . import upload_async
from .matriculado_full_update import atualizar_matriculados_do_upload

logger = logging.getLogger(__name__)
_PATCHED = False
_RESULTS: dict[str, int] = {}
_RESULTS_LOCK = threading.Lock()


def apply_matriculado_full_update_patch() -> None:
    """Garante que a carga de matrícula prevaleça sobre os dados existentes.

    A atualização prioritária roda imediatamente antes da limpeza da staging.
    A mensagem de matrículas só é anexada quando o worker realmente conclui;
    em caso de falha, não registra "0 matriculados" como se fosse resultado válido.
    """
    global _PATCHED
    if _PATCHED:
        return

    original_cleanup = upload_async._cleanup_staging
    original_worker = upload_async._worker

    def cleanup_com_prioridade_matriculado(cfg: dict[str, str], upload_id: str) -> int:
        atualizados = 0

        if cfg.get("institution") == "anhanguera":
            try:
                report = atualizar_matriculados_do_upload(cfg["schema"], upload_id)
                atualizados = int(report.get("atualizados") or 0)
                logger.info(
                    "upload_matriculados_priority_before_cleanup upload_id=%s atualizados=%s",
                    upload_id,
                    atualizados,
                )
            except Exception:
                logger.exception(
                    "Falha na atualização prioritária dos matriculados upload_id=%s",
                    upload_id,
                )

        with _RESULTS_LOCK:
            _RESULTS[upload_id] = atualizados

        return original_cleanup(cfg, upload_id)

    def worker_com_matriculados(
        cfg: dict[str, str],
        upload_id: str,
        routine_name: str,
        total_rows: int,
    ) -> Any:
        result = original_worker(cfg, upload_id, routine_name, total_rows)

        if cfg.get("institution") != "anhanguera":
            return result

        with _RESULTS_LOCK:
            atualizados = int(_RESULTS.pop(upload_id, 0) or 0)

        try:
            rows = db._run_gestao_query(
                f"""
                SELECT status, etapa
                  FROM {cfg['schema_ident']}.{cfg['progress']}
                 WHERE upload_id = :upload_id
                 LIMIT 1
                """,
                {"upload_id": upload_id},
                "upload_matriculados_status",
            ) or []
            progresso = rows[0] if rows else {}
            status = str(progresso.get("status") or "").upper()

            if status != "CONCLUIDO":
                logger.warning(
                    "upload_matriculados_message_skipped upload_id=%s status=%s atualizados=%s",
                    upload_id,
                    status or "DESCONHECIDO",
                    atualizados,
                )
                return result

            mensagem = (
                f"{atualizados} matriculado(s) receberam os dados prioritários "
                "do arquivo, incluindo status e matrícula."
            )

            db._run_gestao_query(
                f"""
                UPDATE {cfg['schema_ident']}.{cfg['progress']}
                   SET mensagem = CONCAT_WS(
                         ' ', NULLIF(mensagem, ''),
                         :mensagem
                       ),
                       atualizado_em = now()
                 WHERE upload_id = :upload_id
                   AND status = 'CONCLUIDO'
                """,
                {
                    "upload_id": upload_id,
                    "mensagem": mensagem,
                },
                "upload_matriculados_progress",
            )
        except Exception:
            logger.exception(
                "Falha ao registrar resultado dos matriculados upload_id=%s",
                upload_id,
            )

        return result

    upload_async._cleanup_staging = cleanup_com_prioridade_matriculado
    upload_async._worker = worker_com_matriculados
    _PATCHED = True
