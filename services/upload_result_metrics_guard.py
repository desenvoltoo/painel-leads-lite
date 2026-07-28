# -*- coding: utf-8 -*-
"""Faz o worker usar os contadores gravados pela SP em logs_importacoes.

Algumas funções retornam apenas contadores genéricos, mas gravam inseridas,
atualizadas e ignoradas corretamente no log. Sem esta camada o worker encerrava
o progresso com atualizadas=0, mesmo após o UPSERT.
"""
from __future__ import annotations

import logging
from typing import Any

from . import database as db
from . import upload_async

logger = logging.getLogger(__name__)
_PATCHED = False


def _log_result(schema_ident: str, upload_id: str) -> dict[str, Any]:
    rows = db._run_gestao_query(
        f"""
        SELECT
            linhas_recebidas,
            linhas_validas,
            linhas_inseridas,
            linhas_atualizadas,
            linhas_ignoradas,
            linhas_rejeitadas,
            duplicados_arquivo,
            duplicados_banco,
            mensagem,
            status,
            etapa
        FROM {schema_ident}.logs_importacoes
        WHERE upload_id = :upload_id OR id_importacao = :upload_id
        ORDER BY atualizado_em DESC NULLS LAST, criado_em DESC NULLS LAST
        LIMIT 1
        """,
        {"upload_id": upload_id},
        "upload_result_metrics_log",
    )
    return dict((rows or [{}])[0])


def apply_upload_result_metrics_guard() -> dict[str, Any]:
    global _PATCHED
    if _PATCHED:
        return {"status": "ja_aplicado"}

    original_execute = upload_async._execute_routine

    def execute_com_metricas(schema_ident: str, routine: dict[str, Any], upload_id: str, total_rows: int) -> dict[str, Any]:
        report = dict(original_execute(schema_ident, routine, upload_id, total_rows) or {})
        try:
            log = _log_result(schema_ident, upload_id)
            # O log da SP é a fonte de verdade para estes contadores.
            for key in (
                "linhas_recebidas",
                "linhas_validas",
                "linhas_inseridas",
                "linhas_atualizadas",
                "linhas_ignoradas",
                "linhas_rejeitadas",
                "duplicados_arquivo",
                "duplicados_banco",
                "mensagem",
                "status",
                "etapa",
            ):
                value = log.get(key)
                if value is not None:
                    report[key] = value
            logger.warning(
                "upload_result_metrics_merged upload_id=%s inseridas=%s atualizadas=%s ignoradas=%s rejeitadas=%s",
                upload_id,
                report.get("linhas_inseridas"),
                report.get("linhas_atualizadas"),
                report.get("linhas_ignoradas"),
                report.get("linhas_rejeitadas"),
            )
        except Exception:
            logger.exception("upload_result_metrics_merge_failed upload_id=%s", upload_id)
        return report

    upload_async._execute_routine = execute_com_metricas
    _PATCHED = True
    return {"status": "aplicado", "fonte_metricas": "logs_importacoes"}
