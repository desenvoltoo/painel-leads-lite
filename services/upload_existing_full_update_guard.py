# -*- coding: utf-8 -*-
"""Garante atualização completa antes da limpeza da staging.

O worker assíncrono limpava a staging antes da camada de atualização completa de
matriculados. Assim, a rotina posterior encontrava zero linhas. Este guard move
a atualização para imediatamente antes do DELETE da staging.
"""
from __future__ import annotations

import logging
from typing import Any

from . import upload_async
from .matriculado_full_update import atualizar_matriculados_do_upload

logger = logging.getLogger(__name__)
_PATCHED = False


def apply_upload_existing_full_update_guard() -> dict[str, Any]:
    global _PATCHED
    if _PATCHED:
        return {"status": "ja_aplicado"}

    original_cleanup = upload_async._cleanup_staging

    def cleanup_com_atualizacao(cfg: dict[str, str], upload_id: str) -> int:
        if cfg.get("institution") == "anhanguera":
            report = atualizar_matriculados_do_upload(cfg["schema"], upload_id)
            atualizados = int(report.get("atualizados") or 0)
            logger.warning(
                "upload_existing_full_update_before_cleanup institution=%s upload_id=%s atualizados=%s",
                cfg.get("institution"),
                upload_id,
                atualizados,
            )
        return original_cleanup(cfg, upload_id)

    upload_async._cleanup_staging = cleanup_com_atualizacao
    _PATCHED = True
    return {
        "status": "aplicado",
        "ordem": "sp_atualizacao_completa_cleanup",
        "regra": "matriculado_true_atualiza_todos_os_campos_presentes",
    }
