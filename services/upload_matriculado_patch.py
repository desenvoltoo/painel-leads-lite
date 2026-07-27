# -*- coding: utf-8 -*-
"""Integra a atualização completa de matriculados ao worker assíncrono."""
from __future__ import annotations

import logging
from typing import Any

from . import database as db
from . import upload_async
from .matriculado_full_update import atualizar_matriculados_do_upload

logger = logging.getLogger(__name__)
_PATCHED = False


def apply_matriculado_full_update_patch() -> None:
    global _PATCHED
    if _PATCHED:
        return

    original_worker = upload_async._worker

    def worker_com_matriculados(cfg: dict[str, str], upload_id: str, routine_name: str, total_rows: int) -> Any:
        result = original_worker(cfg, upload_id, routine_name, total_rows)

        # A regra desta camada se aplica à base principal, cuja staging é stg_leads_site.
        if cfg.get("institution") != "anhanguera":
            return result

        try:
            report = atualizar_matriculados_do_upload(cfg["schema"], upload_id)
            atualizados = int(report.get("atualizados") or 0)
            logger.info(
                "upload_matriculados_full_update upload_id=%s atualizados=%s",
                upload_id,
                atualizados,
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
                """,
                {
                    "upload_id": upload_id,
                    "mensagem": f"{atualizados} matriculado(s) tiveram todos os dados preenchidos atualizados.",
                },
                "upload_matriculados_progress",
            )
        except Exception:
            logger.exception(
                "Falha na atualização completa dos matriculados upload_id=%s",
                upload_id,
            )
        return result

    upload_async._worker = worker_com_matriculados
    _PATCHED = True
