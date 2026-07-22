# -*- coding: utf-8 -*-
from __future__ import annotations

import os

from flask import jsonify, request, session

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
            institution = str(session.get("active_institution") or "anhanguera").strip().lower()
            df = _read_upload_to_df(file_storage)
            max_rows = int(os.getenv("LEADS_IMPORT_MAX_ROWS", "15000") or 15000)
            if len(df) > max_rows:
                return jsonify({"ok": False, "error": {"code": "UPLOAD_ROW_LIMIT", "message": f"O limite é de {max_rows} linhas por arquivo."}}), 400

            if institution == "unifecaf":
                enabled = str(os.getenv("UNIFECAF_IMPORT_ENABLED") or "false").lower() in {"1", "true", "yes", "sim"}
                if not enabled:
                    return jsonify({"ok": False, "error": {"code": "UNIFECAF_IMPORT_DISABLED", "message": "A importação da UniFECAF está desabilitada."}}), 409
                routine_name = str(os.getenv("UNIFECAF_IMPORT_ROUTINE") or "sp_processar_stg_leads").strip()
            else:
                routine_name = str(os.getenv("LEADS_IMPORT_ROUTINE") or "sp_processar_stg_leads_site").strip()

            result = enqueue_upload_dataframe(df, filename=filename, mode="ATUALIZAR_EXISTENTES", routine_name=routine_name, institution=institution)
            return jsonify({"ok": True, "mode": "atualizar_existentes", "message": "Arquivo gravado na staging da instituição ativa. Atualização iniciada.", **result}), 202
        except Exception as exc:
            app.logger.exception("upload_atualizar_existentes_error")
            return jsonify({"ok": False, "error": {"code": exc.__class__.__name__, "message": "Falha ao iniciar a atualização da base.", "details": str(exc)}}), 500

    app.add_url_rule("/api/upload/atualizar-existentes", endpoint="api_upload_atualizar_existentes", view_func=api_upload_atualizar_existentes, methods=["POST"])
