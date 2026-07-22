# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import jsonify

from services.upload_async import get_upload_progress


def register_upload_progress_routes(app) -> None:
    if "api_upload_progresso" in app.view_functions:
        return

    def api_upload_progresso(upload_id: str):
        try:
            row = get_upload_progress(upload_id)
            status = str(row.get("status") or "AGUARDANDO").upper()
            done = status in {"CONCLUIDO", "ERRO"}
            ok = status != "ERRO"
            return jsonify({
                "ok": ok,
                "done": done,
                "upload_id": upload_id,
                "mode": "somente_novos" if row.get("modo") == "SOMENTE_NOVOS" else "atualizar_existentes",
                "status": status,
                "stage": row.get("etapa"),
                "progress": float(row.get("progresso") or 0),
                "message": row.get("mensagem") or "",
                "error": row.get("erro") or "",
                "report": {
                    "linhas_recebidas": int(row.get("linhas_total") or 0),
                    "linhas_processadas": int(row.get("linhas_processadas") or 0),
                    "linhas_inseridas": int(row.get("linhas_inseridas") or 0),
                    "linhas_ignoradas": int(row.get("linhas_ignoradas") or 0),
                    "linhas_rejeitadas": int(row.get("linhas_rejeitadas") or 0),
                    "duplicados_arquivo": int(row.get("duplicados_arquivo") or 0),
                    "existentes_por_celular": int(row.get("existentes_por_celular") or 0),
                    "existentes_por_cpf": int(row.get("existentes_por_cpf") or 0),
                },
            }), 200
        except LookupError as exc:
            return jsonify({"ok": False, "error": {"code": "NOT_FOUND", "message": str(exc)}}), 404
        except Exception as exc:
            app.logger.exception("upload_progress_error upload_id=%s", upload_id)
            return jsonify({"ok": False, "error": {"code": exc.__class__.__name__, "message": "Falha ao consultar o progresso.", "details": str(exc)}}), 500

    app.add_url_rule(
        "/api/upload/progresso/<upload_id>",
        endpoint="api_upload_progresso",
        view_func=api_upload_progresso,
        methods=["GET"],
    )
