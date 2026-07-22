# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import uuid
from time import perf_counter

from flask import jsonify, request

from app import _read_upload_to_df, _validate_upload_filename
from services.upload_pipeline import process_upload_dataframe


def register_upload_update_existing_routes(app) -> None:
    if "api_upload_atualizar_existentes" in app.view_functions:
        return

    def api_upload_atualizar_existentes():
        if "file" not in request.files:
            return jsonify({"ok": False, "error": {"code": "NO_FILE", "message": "Nenhum arquivo enviado."}}), 400

        file_storage = request.files["file"]
        filename = (file_storage.filename or "").strip()
        if not filename:
            return jsonify({"ok": False, "error": {"code": "NO_FILENAME", "message": "Nome do arquivo é obrigatório."}}), 400
        if not _validate_upload_filename(filename):
            return jsonify({"ok": False, "error": {"code": "INVALID_FILE", "message": "Formato inválido. Envie CSV, XLSX ou XLS."}}), 400

        started = perf_counter()
        upload_id = uuid.uuid4().hex
        try:
            df = _read_upload_to_df(file_storage)
            routine_name = str(
                os.getenv("LEADS_IMPORT_ROUTINE")
                or "sp_processar_stg_leads_site"
            ).strip()

            if routine_name == str(os.getenv("LEADS_IMPORT_ROUTINE_MASSIVA") or "sp_importar_somente_leads_novos").strip():
                raise RuntimeError(
                    "Configuração inválida: a rotina normal não pode ser igual à rotina de somente novos."
                )

            app.logger.info(
                "upload_mode_selected upload_id=%s mode=atualizar_existentes routine=%s linhas=%s",
                upload_id,
                routine_name,
                len(df),
            )
            result = process_upload_dataframe(
                df,
                filename=filename,
                upload_id=upload_id,
                routine_name=routine_name,
            )
            return jsonify({
                "ok": True,
                "mode": "atualizar_existentes",
                "message": "Carga concluída. Novos registros foram incluídos e existentes foram atualizados.",
                "duration_s": round(perf_counter() - started, 3),
                **result,
            }), 200
        except Exception as exc:
            app.logger.exception("upload_atualizar_existentes_error upload_id=%s", upload_id)
            return jsonify({
                "ok": False,
                "error": {
                    "code": exc.__class__.__name__,
                    "message": "Falha ao atualizar a base existente.",
                    "details": str(exc),
                },
            }), 500

    app.add_url_rule(
        "/api/upload/atualizar-existentes",
        endpoint="api_upload_atualizar_existentes",
        view_func=api_upload_atualizar_existentes,
        methods=["POST"],
    )
