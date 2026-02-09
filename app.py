# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)
Versão: 3.0 - Modelo Estrela Consolidado V14
"""

import os
import traceback
from flask import Flask, render_template, request, jsonify

from services.bigquery import (
    query_leads,
    query_options,
    ingest_upload_file
)

# ============================================================
# HELPERS
# ============================================================
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) else v if v else default

def _required_envs_ok():
    # Verificação mínima para o app rodar com segurança
    missing = []
    for k in ("GCP_PROJECT_ID", "BQ_DATASET"):
        if not _env(k):
            missing.append(k)
    return (len(missing) == 0, missing)

def _get_filters_from_request():
    def n(v):
        v = (v or "").strip()
        return v if v else None

    # Captura os filtros enviados pelo app.js (AJAX)
    return {
        "status": n(request.args.get("status")),
        "curso": n(request.args.get("curso")), # Pode vir como string separada por ||
        "polo": n(request.args.get("polo")),
        "data_ini": n(request.args.get("data_ini")),
        "data_fim": n(request.args.get("data_fim")),
        "limit": int(request.args.get("limit") or 500),
    }

def _error_payload(e: Exception, public_msg: str):
    return {
        "ok": False,
        "error": public_msg,
        "details": str(e),
        "trace": traceback.format_exc(limit=3)
    }

# ============================================================
# APP FACTORY
# ============================================================
def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024 # Limite de 30MB para uploads

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/health")
    def health():
        ok, missing = _required_envs_ok()
        return jsonify({"status": "ok" if ok else "unhealthy", "missing": missing})

    @app.get("/api/leads")
    def api_leads():
        try:
            filters = _get_filters_from_request()
            rows = query_leads(filters)
            # Retorna diretamente a lista para facilitar o .map() no JS
            return jsonify(rows)
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao buscar leads no BigQuery.")), 500

    @app.get("/api/kpis")
    def api_kpis():
        try:
            filters = _get_filters_from_request()
            # Calculamos KPIs básicos a partir da query de leads para evitar 2 chamadas pesadas
            rows = query_leads(filters)
            
            total = len(rows)
            # Exemplo simples de KPI de status predominante
            status_counts = {}
            for r in rows:
                st = r.get('status', 'Lead')
                status_counts[st] = status_counts.get(st, 0) + 1
            
            top_status = None
            if status_counts:
                best = max(status_counts, key=status_counts.get)
                top_status = {"status": best, "cnt": status_counts[best]}

            return jsonify({
                "total": total,
                "top_status": top_status
            })
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
            return jsonify({"ok": False, "error": "Arquivo não encontrado."}), 400

        f = request.files["file"]
        if not f.filename:
            return jsonify({"ok": False, "error": "Nome de arquivo vazio."}), 400

        try:
            source = request.form.get("source", "PAINEL_V14")
            result = ingest_upload_file(f, source=source)
            return jsonify({"ok": True, "message": "Processado com sucesso!", "details": result})
        except Exception as e:
            return jsonify(_error_payload(e, "Falha na ingestão V14.")), 500

    return app

if __name__ == "__main__":
    app = create_app()
    port = int(_env("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
