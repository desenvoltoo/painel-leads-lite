# -*- coding: utf-8 -*-
"""Evita concorrência entre importações da UniFECAF na SP de consolidação."""
from __future__ import annotations

import logging
from typing import Any, Dict

from sqlalchemy import text

from . import database as db
from . import upload_async

logger = logging.getLogger(__name__)


def apply_unifecaf_serial_guard() -> dict[str, Any]:
    current = upload_async._execute_routine
    if getattr(current, "_unifecaf_serial_guard", False):
        return {"status": "ja_aplicado"}

    def guarded_execute(schema_ident: str, routine: Dict[str, Any], upload_id: str, total_rows: int):
        schema_name = str(schema_ident).replace('"', '').strip().lower()
        if schema_name != "unifecaf":
            return current(schema_ident, routine, upload_id, total_rows)

        lock_key = "unifecaf:sp_processar_stg_leads:global"
        logger.info(
            "unifecaf_serial_wait upload_id=%s rotina=%s linhas=%s",
            upload_id,
            routine.get("routine_name"),
            total_rows,
        )

        with db.get_engine().begin() as conn:
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"), {"lock_key": lock_key})
            logger.info("unifecaf_serial_acquired upload_id=%s", upload_id)
            return current(schema_ident, routine, upload_id, total_rows)

    guarded_execute._unifecaf_serial_guard = True
    upload_async._execute_routine = guarded_execute
    return {
        "status": "aplicado",
        "lock": "global_por_instituicao",
        "schema": "unifecaf",
    }
