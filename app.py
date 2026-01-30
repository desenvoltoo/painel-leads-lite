# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)
Versão: 2.0 - Modelo Estrela
"""

import os
import traceback
from flask import Flask, render_template, request, jsonify

from services.bigquery import (
    query_leads,
    query_kpis,
    query_options,
    ingest_upload_file,
    debug_count,
    debug_sample,
)

# ============================================================
# HELPERS
# ============================================================
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) else v if v else default

def _required_envs_ok():
    # Adicionada BQ_PROMOTE_PROC como boa prática de verificação
    missing = []
    for k in ("GCP_PROJECT_ID", "BQ_DATASET", "BQ_VIEW_LEADS"):
        if not _env(k):
            missing.append(k)
    return (len(missing) == 0, missing)

def _get_filters_from_request():
    def n(v):
        v = (v or "").strip()
        return v if v else None

    # Mapeia os argumentos da URL vindos do app.js
    return {
        "status": n(request.args.get("status")),
        "curso": n(request.args.get("curso")),
        "polo": n(request.args.get("polo")),
        "origem": n(request.args.get("origem")),
        "data_ini": n(request.args.get("data_ini")),  # YYYY-MM-DD
        "data_fim": n(request.args.get("data_fim")),   # YYYY-MM-DD
        "limit": int(request.args.get("limit") or 500),
    }

def _source_ref():
    return {
        "project": _env("GCP_PROJECT_ID", "painel-universidade"),
        "dataset": _env("BQ_DATASET", "modelo_estrela"),
        "view": _env("BQ_VIEW_LEADS", "vw_leads_painel_lite"),
        "upload_table": _env("BQ_UPLOAD_TABLE", "stg_leads_upload"),
        "promote_proc": _env("BQ_PROMOTE_PROC", "sp_v9_run_pipeline"), # Procedure nova
    }

def _error_payload(e: Exception, public_msg: str):
    return {
        "ok": False,
        "error": public_msg,
        "details": str(e),
        "trace": traceback.format_exc(limit=6),
        "source": _source_ref(),
    }

# ============================================================
# APP FACTORY
# ============================================================
def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024  # Aumentado para 30MB para arquivos maiores

    @app.get("/")
    def index():
        ok, missing = _required_envs_ok()
        if not ok:
            print(f"⚠️ Alerta: Variáveis de ambiente faltando: {missing}")
        return render_template("index.html")

    @app.get("/health")
    def health():
        ok, missing = _required_envs_ok()
        return jsonify({
            "status": "ok" if ok else "missing_env",
            "missing_env": missing,
            "source": _source_ref(),
        })

    @app.get("/api/leads")
    def api_leads():
        ok, missing = _required_envs_ok()
        if not ok:
            return jsonify({"error": f"Faltam ENVs: {missing}"}), 500

        filters = _get_filters_from_request()
        try:
            rows = query_leads(filters)
            return jsonify({"count": len(rows), "rows": rows, "source": _source_ref()})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao buscar leads no BigQuery.")), 500

    @app.get("/api/kpis")
    def api_kpis():
        ok, missing = _required_envs_ok()
        if not ok:
            return jsonify({"error": f"Faltam ENVs: {missing}"}), 500

        filters = _get_filters_from_request()
        try:
            # O novo bigquery.py já retorna o dicionário no formato correto
            data = query_kpis(filters)
            data["source"] = _source_ref()
            return jsonify(data)
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao calcular KPIs.")), 500

    @app.get("/api/options")
    def api_options():
        try:
            data = query_options()
            return jsonify(data)
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao carregar opções dos filtros.")), 500

    @app.post("/api/upload")
    def api_upload():
        if "file" not in request.files:
            return jsonify({"ok": False, "error": "Arquivo não encontrado no request."}), 400

        f = request.files["file"]
        if not f.filename:
            return jsonify({"ok": False, "error": "Nome do arquivo inválido."}), 400

        try:
            source = request.form.get("source", "UPLOAD_WEB_APP")
            result = ingest_upload_file(f, source=source)
            
            return jsonify({
                "ok": True, 
                "message": "Upload e processamento concluídos!",
                "details": result
            })
        except Exception as e:
            return jsonify(_error_payload(e, "Falha crítica na ingestão/pipeline.")), 500

    # Rotas de Debug
    @app.get("/api/debug/count")
    def api_debug_count():
        try:
            return jsonify({"ok": True, "count": debug_count()})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro no count.")), 500

    return app

if __name__ == "__main__":
    port = int(_env("PORT", "8080"))
    app = create_app()
    # Debug=True apenas localmente
    app.run(host="0.0.0.0", port=port, debug=True)
