# -*- coding: utf-8 -*-
from __future__ import annotations

import os

from flask import jsonify, request

from app import _read_upload_to_df, _validate_upload_filename
from services.upload_async import enqueue_upload_dataframe


def register_upload_update_existing_routes(app) -> None:
    if "api_upload_atualizar_existentes" in app.view_functions:
        return

    def api_upload_atualizar_existentes():
        if "file" not in request.files:
            return jsonify({"ok": False, "error": {"code": "NO_FILE", "message": "Nenhum arquivo enviado."}}), 400

        file_storage = request.files["file"]
        filename = (file_storage.filename or "").strip()
        if not filename or not _validate_upload_filename(filename):
            return jsonify({"ok": False, "error": {"code": "INVALID_FILE", "message": "Envie um arquivo CSV, XLS ou XLSX válido."}}), 400

        try:
            df = _read_upload_to_df(file_storage)
            routine_name = str(os.getenv("LEADS_IMPORT_ROUTINE") or "sp_processar_stg_leads_site").strip()
            mass_routine = str(os.getenv("LEADS_IMPORT_ROUTINE_MASSIVA") or "sp_importar_somente_leads_novos").strip()
            if routine_name == mass_routine:
                raise RuntimeError("As rotinas dos dois modos não podem ser iguais.")

            result = enqueue_upload_dataframe(
                df,
                filename=filename,
                mode="ATUALIZAR_EXISTENTES",
                routine_name=routine_name,
            )
            return jsonify({
                "ok": True,
                "mode": "atualizar_existentes",
                "message": "Arquivo gravado na staging. Atualização iniciada em segundo plano.",
                **result,
            }), 202
        except Exception as exc:
            app.logger.exception("upload_atualizar_existentes_error")
            return jsonify({"ok": False, "error": {"code": exc.__class__.__name__, "message": "Falha ao iniciar a atualização da base.", "details": str(exc)}}), 500

    app.add_url_rule(
        "/api/upload/atualizar-existentes",
        endpoint="api_upload_atualizar_existentes",
        view_func=api_upload_atualizar_existentes,
        methods=["POST"],
    )
