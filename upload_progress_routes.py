# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import jsonify

from services.upload_async import get_upload_progress


# Para a interface, o upload termina quando o arquivo foi integralmente gravado
# na staging. A procedure continua como processamento interno e não deve manter
# a barra de upload presa em 35% por vários minutos.
UPLOAD_COMPLETE_STATUSES = {"AGUARDANDO", "PROCESSANDO", "CONCLUIDO"}
UPLOAD_COMPLETE_STAGES = {
    "STAGING_CONCLUIDA",
    "LOCALIZANDO_ROTINA",
    "EXECUTANDO_SP",
    "CONCLUIDO",
}


def register_upload_progress_routes(app) -> None:
    if "api_upload_progresso" in app.view_functions:
        return

    def api_upload_progresso(upload_id: str):
        try:
            row = get_upload_progress(upload_id)
            internal_status = str(row.get("status") or "AGUARDANDO").upper()
            internal_stage = str(row.get("etapa") or "").upper()
            upload_complete = (
                internal_status in UPLOAD_COMPLETE_STATUSES
                or internal_stage in UPLOAD_COMPLETE_STAGES
            )
            failed_before_staging = internal_status == "ERRO" and internal_stage not in UPLOAD_COMPLETE_STAGES

            # Semântica exibida ao usuário: staging confirmada = upload 100%.
            # Mantemos os campos internal_* para diagnóstico e acompanhamento
            # do processamento que segue no PostgreSQL.
            if upload_complete:
                status = "UPLOAD_CONCLUIDO"
                stage = "STAGING_CONCLUIDA"
                progress = 100.0
                done = True
                ok = True
                message = row.get("mensagem") or "Arquivo gravado na staging. Processamento interno iniciado."
            elif failed_before_staging:
                status = "ERRO"
                stage = internal_stage or "ERRO"
                progress = 100.0
                done = True
                ok = False
                message = row.get("mensagem") or "Falha ao gravar o arquivo na staging."
            else:
                status = internal_status
                stage = internal_stage
                progress = float(row.get("progresso") or 0)
                done = False
                ok = True
                message = row.get("mensagem") or ""

            return jsonify({
                "ok": ok,
                "done": done,
                "upload_complete": upload_complete,
                "upload_id": upload_id,
                "mode": "somente_novos" if row.get("modo") == "SOMENTE_NOVOS" else "atualizar_existentes",
                "status": status,
                "stage": stage,
                "progress": progress,
                "message": message,
                "error": row.get("erro") or "",
                "internal_status": internal_status,
                "internal_stage": internal_stage,
                "internal_progress": float(row.get("progresso") or 0),
                "processing_in_background": upload_complete and internal_status != "CONCLUIDO",
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
