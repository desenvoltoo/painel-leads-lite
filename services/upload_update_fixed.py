# -*- coding: utf-8 -*-
"""Upload de atualização com um único upload_id em todas as etapas."""
from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)


_ACADEMIC_UPLOAD_ALIASES = {
    "graduacao": {
        "graduacao",
        "graduação",
        "formacao",
        "formação",
        "curso_graduacao",
        "curso_graduação",
        "graduacao_cursada",
        "graduação_cursada",
    },
    "conclusao": {
        "conclusao",
        "conclusão",
        "ano_conclusao",
        "ano_conclusão",
        "data_conclusao",
        "data_conclusão",
        "conclusao_graduacao",
        "conclusão_graduação",
    },
}


def _restore_academic_columns_from_source(df, prepared):
    """Garante graduação/conclusão diretamente a partir da planilha original.

    Esta proteção é intencionalmente independente de ``UPLOAD_ALIASES``. Assim,
    mesmo que uma instância antiga do módulo database permaneça carregada ou
    algum patch histórico altere o parser, os dois campos acadêmicos chegam à
    staging quando estiverem presentes no arquivo recebido.
    """
    if df is None or getattr(df, "empty", True) or prepared is None or getattr(prepared, "empty", True):
        return prepared

    normalized_source = {
        db._normalize_upload_col(column): column
        for column in df.columns
    }

    for target, aliases in _ACADEMIC_UPLOAD_ALIASES.items():
        source_column = None
        for alias in aliases:
            normalized_alias = db._normalize_upload_col(alias)
            if normalized_alias in normalized_source:
                source_column = normalized_source[normalized_alias]
                break

        if source_column is None:
            continue

        source_values = df[source_column].reset_index(drop=True)
        if len(source_values) != len(prepared):
            raise RuntimeError(
                f"Quantidade de linhas divergente ao preservar {target}: "
                f"origem={len(source_values)} preparado={len(prepared)}."
            )

        prepared = prepared.copy()
        prepared[target] = source_values.astype(object).where(source_values.notna(), None).values

        source_has_value = source_values.notna() & source_values.astype(str).str.strip().ne("")
        prepared_has_value = prepared[target].notna() & prepared[target].astype(str).str.strip().ne("")
        if bool(source_has_value.any()) and not bool(prepared_has_value.any()):
            raise RuntimeError(
                f"A coluna {target} possui dados na planilha, mas ficou vazia antes da staging."
            )

    return prepared


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

    # Proteção explícita para os novos campos acadêmicos. A planilha original é
    # a fonte de verdade para graduação e conclusão antes do COPY para a staging.
    if cfg["institution"] == "anhanguera":
        prepared = _restore_academic_columns_from_source(df, prepared)

    if cfg["institution"] == "unifecaf":
        prepared = _prepare_for_unifecaf(prepared)

    total_rows = len(prepared)
    if total_rows <= 0:
        raise ValueError("A planilha não possui linhas para importar.")

    staging_columns = STAGING_COLUMNS[cfg["institution"]]
    prepared = prepared[[column for column in prepared.columns if column in staging_columns]].copy()
    prepared["upload_id"] = upload_id

    if cfg["institution"] == "anhanguera":
        for field in ("graduacao", "conclusao"):
            if field in prepared.columns:
                non_empty = int(
                    (
                        prepared[field].notna()
                        & prepared[field].astype(str).str.strip().ne("")
                    ).sum()
                )
                logger.info(
                    "upload_academic_field upload_id=%s field=%s preenchidos=%s total=%s",
                    upload_id,
                    field,
                    non_empty,
                    total_rows,
                )

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
