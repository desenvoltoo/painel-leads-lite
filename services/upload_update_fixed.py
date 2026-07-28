# -*- coding: utf-8 -*-
"""Upload de atualização com um único upload_id em todas as etapas."""
from __future__ import annotations

import time
import uuid
from typing import Any, Dict

from . import database as db
from .upload_async import (
    STAGING_COLUMNS,
    _config,
    _copy_dataframe_to_staging,
    _prepare_for_unifecaf,
    start_upload_worker,
)


def enqueue_update_existing_dataframe(
    df,
    filename: str,
    routine_name: str,
    institution: str = "anhanguera",
) -> Dict[str, Any]:
    """Grava log, progresso e staging com o mesmo identificador."""
    cfg = _config(institution)
    upload_id = uuid.uuid4().hex
    started = time.monotonic()

    prepared = db._prepare_upload_dataframe(df, filename, upload_id)
    if cfg["institution"] == "unifecaf":
        prepared = _prepare_for_unifecaf(prepared)

    total_rows = len(prepared)
    if total_rows <= 0:
        raise ValueError("A planilha não possui linhas para importar.")

    staging_columns = STAGING_COLUMNS[cfg["institution"]]
    prepared = prepared[[column for column in prepared.columns if column in staging_columns]].copy()
    prepared["upload_id"] = upload_id

    if "nome_arquivo" in staging_columns:
        prepared["nome_arquivo"] = filename
    if "linha_arquivo" in staging_columns and "linha_arquivo" not in prepared.columns:
        prepared["linha_arquivo"] = range(2, total_rows + 2)

    _copy_dataframe_to_staging(
        cfg,
        prepared,
        upload_id,
        "ATUALIZAR_EXISTENTES",
        routine_name,
        filename,
    )

    # A SP da Anhanguera cria o log quando ele não existe. Criamos antes para
    # preservar o nome do arquivo e impedir que o painel acompanhe outro ID.
    db._run_gestao_query(
        f"""
        INSERT INTO {cfg['schema_ident']}.{cfg['logs']} (
            upload_id,
            nome_arquivo,
            status,
            etapa,
            total_linhas,
            linhas_recebidas,
            criado_em,
            atualizado_em
        )
        VALUES (
            :upload_id,
            :filename,
            'RECEBIDO',
            'STAGING_CONCLUIDA',
            :total_rows,
            :total_rows,
            NOW(),
            NOW()
        )
        ON CONFLICT (upload_id)
        DO UPDATE SET
            nome_arquivo = COALESCE(EXCLUDED.nome_arquivo, {cfg['logs']}.nome_arquivo),
            total_linhas = EXCLUDED.total_linhas,
            linhas_recebidas = EXCLUDED.linhas_recebidas,
            atualizado_em = NOW()
        """,
        {
            "upload_id": upload_id,
            "filename": filename,
            "total_rows": total_rows,
        },
        f"upload_log_same_id_{cfg['institution']}",
    )

    worker = start_upload_worker(
        cfg["institution"],
        upload_id,
        routine_name,
        total_rows,
    )

    return {
        "job_id": upload_id,
        "upload_id": upload_id,
        "institution": cfg["institution"],
        "status": "AGUARDANDO",
        "done": False,
        "mode": "atualizar_existentes",
        "progress_url": f"/api/upload/progresso/{upload_id}",
        "report": {
            "linhas_recebidas": total_rows,
            "linhas_gravadas_staging": total_rows,
        },
        "worker_started": worker.is_alive(),
        "queued_in_seconds": round(time.monotonic() - started, 3),
    }
