# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from typing import Any, Dict

from sqlalchemy import text

from . import database as db
from .upload_pipeline import _execute_routine, _find_routine

logger = logging.getLogger(__name__)


STAGING_COLUMNS = {
    "anhanguera": {
        "status_inscricao", "data_inscricao", "origem", "unidade", "tipo_negocio",
        "curso", "modalidade", "turno", "nome", "cpf", "celular", "email",
        "data_ultima_acao", "qtd_acionamentos", "status", "data_disparo",
        "peca_disparo", "texto_disparo", "consultor_disparo", "tipo_disparo",
        "campanha", "observacao", "data_matricula", "matriculado", "canal",
        "acao_comercial", "consultor_comercial", "upload_id", "linha_arquivo",
        "nome_arquivo", "dt_upload",
    },
    "unifecaf": {
        "data_inscricao", "origem", "unidade", "tipo_negocio", "curso",
        "modalidade", "nome", "cpf", "celular", "email",
        "data_ultima_interacao", "qtd_acionamentos", "status", "data_disparo",
        "peca_disparo", "texto_disparo", "consultor_disparo", "tipo_disparo",
        "campanha", "data_matricula", "matriculado", "consultor_comercial",
        "observacao", "upload_id", "linha_arquivo", "nome_arquivo",
    },
}


def _config(institution: str = "anhanguera") -> Dict[str, str]:
    key = str(institution or "anhanguera").strip().lower()
    if key == "unifecaf":
        schema = str(os.getenv("UNIFECAF_DB_SCHEMA") or "unifecaf").strip()
        return {
            "institution": "unifecaf",
            "schema": schema,
            "schema_ident": db._safe_ident(schema),
            "staging": "stg_leads",
            "progress": "op_importacao_progresso",
            "logs": "logs_importacoes",
        }
    schema = str(os.getenv("DB_SCHEMA", db.DB_SCHEMA) or "modelo_estrela").strip()
    return {
        "institution": "anhanguera",
        "schema": schema,
        "schema_ident": db._safe_ident(schema),
        "staging": "stg_leads_site",
        "progress": "op_importacao_progresso",
        "logs": "logs_importacoes",
    }


def _progress_row(upload_id: str) -> Dict[str, Any]:
    for institution in ("anhanguera", "unifecaf"):
        cfg = _config(institution)
        try:
            rows = db._run_gestao_query(
                f"SELECT * FROM {cfg['schema_ident']}.{cfg['progress']} WHERE upload_id=:upload_id",
                {"upload_id": upload_id},
                f"upload_progress_get_{institution}",
            )
            if rows:
                row = rows[0]
                row["institution"] = institution
                return row
        except Exception:
            logger.debug("progress table unavailable institution=%s", institution, exc_info=True)
    return {}


def get_upload_progress(upload_id: str) -> Dict[str, Any]:
    row = _progress_row(upload_id)
    if not row:
        raise LookupError("Importação não encontrada.")
    return row


def _set_progress(cfg: Dict[str, str], upload_id: str, status: str, etapa: str, progresso: float, **metrics: Any) -> None:
    params = {
        "upload_id": upload_id, "status": status, "etapa": etapa, "progresso": progresso,
        "linhas_processadas": metrics.get("linhas_processadas"),
        "linhas_inseridas": metrics.get("linhas_inseridas"),
        "linhas_ignoradas": metrics.get("linhas_ignoradas"),
        "linhas_rejeitadas": metrics.get("linhas_rejeitadas"),
        "mensagem": metrics.get("mensagem"), "erro": metrics.get("erro"),
    }
    db._run_gestao_query(
        f"SELECT {cfg['schema_ident']}.fn_atualizar_progresso_importacao("
        ":upload_id,:status,:etapa,:progresso,:linhas_processadas,:linhas_inseridas,"
        ":linhas_ignoradas,:linhas_rejeitadas,:mensagem,:erro)",
        params,
        f"upload_progress_update_{cfg['institution']}",
    )


def _worker(cfg: Dict[str, str], upload_id: str, routine_name: str, total_rows: int) -> None:
    started = time.monotonic()
    try:
        _set_progress(cfg, upload_id, "PROCESSANDO", "LOCALIZANDO_ROTINA", 25)
        routine = _find_routine(cfg["schema"], routine_name)
        if not routine:
            raise RuntimeError(f"Rotina {routine_name} não encontrada no schema {cfg['schema']}.")
        _set_progress(cfg, upload_id, "PROCESSANDO", "EXECUTANDO_SP", 35)
        report = _execute_routine(cfg["schema_ident"], routine, upload_id, total_rows)

        inserted = int(report.get("linhas_inseridas") or 0)
        updated = int(report.get("linhas_atualizadas") or 0)
        rejected = int(report.get("linhas_rejeitadas") or 0)
        existing_phone = int(report.get("existentes_por_celular") or 0)
        existing_cpf = int(report.get("existentes_por_cpf") or 0)
        duplicates_file = int(report.get("duplicados_no_arquivo") or report.get("duplicados_arquivo") or 0)
        no_identifier = int(report.get("linhas_sem_identificador") or 0)
        ignored = existing_phone + existing_cpf + duplicates_file + no_identifier
        processed = min(total_rows, inserted + updated + ignored + rejected)
        message = report.get("mensagem") or "Importação concluída."

        with db.get_engine().begin() as conn:
            conn.execute(text(f"""
                UPDATE {cfg['schema_ident']}.{cfg['progress']}
                   SET status='CONCLUIDO', etapa='CONCLUIDO', progresso=100,
                       linhas_processadas=:processed, linhas_inseridas=:inserted,
                       linhas_ignoradas=:ignored, linhas_rejeitadas=:rejected,
                       duplicados_arquivo=:duplicates_file,
                       existentes_por_celular=:existing_phone,
                       existentes_por_cpf=:existing_cpf,
                       mensagem=:message, atualizado_em=now(), finalizado_em=now()
                 WHERE upload_id=:upload_id
            """), {"processed": processed, "inserted": inserted, "ignored": ignored,
                    "rejected": rejected, "duplicates_file": duplicates_file,
                    "existing_phone": existing_phone, "existing_cpf": existing_cpf,
                    "message": message, "upload_id": upload_id})
        logger.info("upload_async_complete institution=%s upload_id=%s rotina=%s total=%s elapsed_s=%.2f",
                    cfg["institution"], upload_id, routine_name, total_rows, time.monotonic()-started)
    except Exception as exc:
        logger.exception("upload_async_error institution=%s upload_id=%s", cfg["institution"], upload_id)
        try:
            _set_progress(cfg, upload_id, "ERRO", "ERRO", 100, erro=str(exc), mensagem="Falha ao processar importação.")
        except Exception:
            logger.exception("upload_async_progress_error upload_id=%s", upload_id)


def _prepare_for_unifecaf(prepared):
    rename = {
        "data_ultima_acao": "data_ultima_interacao",
        "polo": "unidade",
        "flag_matriculado": "matriculado",
    }
    return prepared.rename(columns={k: v for k, v in rename.items() if k in prepared.columns})


def enqueue_upload_dataframe(df, filename: str, mode: str, routine_name: str, institution: str = "anhanguera") -> Dict[str, Any]:
    cfg = _config(institution)
    upload_id = uuid.uuid4().hex
    started = time.monotonic()
    logger.info(
        "upload_stage_prepare_start institution=%s upload_id=%s arquivo=%s linhas_entrada=%s",
        cfg["institution"], upload_id, filename, len(df),
    )

    prepared = db._prepare_upload_dataframe(df, filename, upload_id)
    if cfg["institution"] == "unifecaf":
        prepared = _prepare_for_unifecaf(prepared)
    total_rows = len(prepared)
    if total_rows <= 0:
        raise ValueError("A planilha não possui linhas para importar.")

    # Evita consulta a information_schema nesta etapa crítica. As colunas são
    # conhecidas e versionadas junto com cada staging.
    stg_cols = STAGING_COLUMNS[cfg["institution"]]
    selected_columns = [column for column in prepared.columns if column in stg_cols]
    prepared = prepared[selected_columns].copy()

    if "upload_id" not in prepared.columns:
        prepared["upload_id"] = upload_id
    if "nome_arquivo" in stg_cols and "nome_arquivo" not in prepared.columns:
        prepared["nome_arquivo"] = filename
    if "linha_arquivo" in stg_cols and "linha_arquivo" not in prepared.columns:
        prepared["linha_arquivo"] = range(2, total_rows + 2)

    logger.info(
        "upload_stage_columns_ready institution=%s upload_id=%s staging=%s colunas=%s elapsed_s=%.2f",
        cfg["institution"], upload_id, cfg["staging"], len(prepared.columns), time.monotonic() - started,
    )

    with db.get_engine().begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {cfg['schema_ident']}.{cfg['progress']}
                (upload_id, modo, rotina, arquivo, status, etapa, linhas_total, progresso)
            VALUES (:upload_id,:modo,:rotina,:arquivo,'STAGING','GRAVANDO_STAGING',:total,10)
        """), {"upload_id": upload_id, "modo": mode, "rotina": routine_name, "arquivo": filename, "total": total_rows})

        if cfg["institution"] == "unifecaf":
            conn.execute(text(f"""
                INSERT INTO {cfg['schema_ident']}.logs_importacoes
                  (upload_id,nome_arquivo,status,etapa,total_linhas,linhas_recebidas)
                VALUES (:upload_id,:arquivo,'RECEBIDO','STAGING',:total,:total)
                ON CONFLICT (upload_id) DO NOTHING
            """), {"upload_id": upload_id, "arquivo": filename, "total": total_rows})

        logger.info(
            "upload_stage_insert_start institution=%s upload_id=%s staging=%s linhas=%s",
            cfg["institution"], upload_id, cfg["staging"], total_rows,
        )
        # method=None usa executemany do driver e evita uma instrução SQL gigante.
        prepared.to_sql(
            cfg["staging"],
            con=conn,
            schema=cfg["schema_ident"],
            if_exists="append",
            index=False,
            method=None,
            chunksize=500,
        )
        logger.info(
            "upload_stage_insert_complete institution=%s upload_id=%s staging=%s linhas=%s elapsed_s=%.2f",
            cfg["institution"], upload_id, cfg["staging"], total_rows, time.monotonic() - started,
        )
        conn.execute(text(
            f"UPDATE {cfg['schema_ident']}.{cfg['progress']} "
            "SET status='AGUARDANDO',etapa='STAGING_CONCLUIDA',progresso=20,atualizado_em=now() "
            "WHERE upload_id=:upload_id"
        ), {"upload_id": upload_id})

    thread = threading.Thread(
        target=_worker,
        args=(cfg, upload_id, routine_name, total_rows),
        daemon=True,
        name=f"upload-{cfg['institution']}-{upload_id[:8]}",
    )
    thread.start()
    logger.info(
        "upload_async_queued institution=%s upload_id=%s rotina=%s linhas=%s elapsed_s=%.2f",
        cfg["institution"], upload_id, routine_name, total_rows, time.monotonic() - started,
    )
    return {
        "job_id": upload_id,
        "upload_id": upload_id,
        "institution": cfg["institution"],
        "status": "AGUARDANDO",
        "done": False,
        "mode": "somente_novos" if mode == "SOMENTE_NOVOS" else "atualizar_existentes",
        "progress_url": f"/api/upload/progresso/{upload_id}",
        "report": {"linhas_recebidas": total_rows, "linhas_gravadas_staging": total_rows},
    }
