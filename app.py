# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)
- Frontend leve (HTML+JS) com tabela e filtros
- Backend mínimo: só recebe filtros, consulta uma VIEW no BigQuery e devolve JSON

Como rodar local:
    pip install -r requirements.txt
    setx GOOGLE_APPLICATION_CREDENTIALS "C:\caminho\svc.json"
    setx GCP_PROJECT_ID "seu-projeto"
    setx BQ_DATASET "marts"
    setx BQ_VIEW_LEADS "vw_leads_painel"
    python app.py

Se variáveis/credenciais não estiverem configuradas, a API retorna dados de exemplo.
"""
import os
from flask import Flask, render_template, request, jsonify
from services.bigquery import query_leads

def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/health")
    def health():
        return jsonify({"status": "ok"})

    @app.get("/api/leads")
    def api_leads():
        # Filtros simples (expanda conforme precisar)
        filters = {
            "status": request.args.get("status") or None,
            "curso": request.args.get("curso") or None,
            "polo": request.args.get("polo") or None,
            "origem": request.args.get("origem") or None,
            "data_ini": request.args.get("data_ini") or None,  # YYYY-MM-DD
            "data_fim": request.args.get("data_fim") or None,  # YYYY-MM-DD
            "limit": int(request.args.get("limit") or 500),
        }

        rows = query_leads(filters)
        return jsonify({"count": len(rows), "rows": rows})

    @app.get("/api/options")
    def api_options():
        """
        Opcional: pode virar uma consulta real no BigQuery (distinct).
        Por enquanto devolve listas vazias (frontend lida com isso).
        """
        return jsonify({
            "status": [],
            "curso": [],
            "polo": [],
            "origem": []
        })

    return app

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app = create_app()
    app.run(host="0.0.0.0", port=port, debug=True)
