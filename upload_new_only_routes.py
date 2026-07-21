# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import uuid
from pathlib import Path
from time import perf_counter

from flask import jsonify, request

from app import _read_upload_to_df, _validate_upload_filename
from services.upload_pipeline import process_upload_dataframe


def register_upload_new_only_routes(app) -> None:
    if "api_upload_somente_novos" in app.view_functions:
        return

    def api_upload_somente_novos():
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
            max_rows = int(os.getenv("LEADS_IMPORT_MAX_ROWS", "10000") or 10000)
            if len(df) > max_rows:
                return jsonify({
                    "ok": False,
                    "error": {
                        "code": "UPLOAD_ROW_LIMIT",
                        "message": f"O limite do modo somente novos é de {max_rows:,} linhas por arquivo.".replace(",", "."),
                    },
                }), 400

            routine_name = str(
                os.getenv("LEADS_IMPORT_ROUTINE_MASSIVA")
                or "sp_importar_somente_leads_novos"
            ).strip()
            result = process_upload_dataframe(
                df,
                filename=filename,
                upload_id=upload_id,
                routine_name=routine_name,
            )
            return jsonify({
                "ok": True,
                "mode": "somente_novos",
                "message": "Carga concluída. Leads já existentes foram ignorados.",
                "duration_s": round(perf_counter() - started, 3),
                **result,
            }), 202
        except Exception as exc:
            app.logger.exception("upload_somente_novos_error upload_id=%s", upload_id)
            return jsonify({
                "ok": False,
                "error": {
                    "code": exc.__class__.__name__,
                    "message": "Falha ao executar a importação somente de leads novos.",
                    "details": str(exc),
                },
            }), 500

    app.add_url_rule(
        "/api/upload/somente-novos",
        endpoint="api_upload_somente_novos",
        view_func=api_upload_somente_novos,
        methods=["POST"],
    )
