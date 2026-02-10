# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)
Versão: 3.0 - Modelo Estrela Consolidado V14 (MULTI filtros)
"""

import os
import traceback
import pandas as pd
from flask import Flask, render_template, request, jsonify

from services.bigquery import (
    query_leads,
    query_leads_count,
    query_options,
    process_upload_dataframe
)

# ============================================================
# HELPERS
# ============================================================
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) else v if v else default

def _required_envs_ok():
    missing = []
    for k in ("GCP_PROJECT_ID", "BQ_DATASET"):
        if not _env(k):
            missing.append(k)
    return (len(missing) == 0, missing)

def _n(v):
    v = (v or "").strip()
    return v if v else None

def _split_multi(v):
    """
    Recebe:
      - "A || B" -> ["A","B"]
      - "A" -> ["A"]
      - ""/None -> None
    """
    s = _n(v)
    if not s:
        return None
    if "||" in s:
        parts = [p.strip() for p in s.split("||")]
        parts = [p for p in parts if p]
        return parts if parts else None
    return [s]

def _get_filters_from_request():
    # MULTI
    status = _split_multi(request.args.get("status"))
    curso = _split_multi(request.args.get("curso"))
    polo = _split_multi(request.args.get("polo"))
    consultor = _split_multi(request.args.get("consultor"))

    filters = {
        "status": status,
        "curso": curso,
        "polo": polo,
        "consultor": consultor,

        # SINGLE
        "cpf": _n(request.args.get("cpf")),
        "celular": _n(request.args.get("celular")),
        "email": _n(request.args.get("email")),
        "nome": _n(request.args.get("nome")),

        # date range
        "data_ini": _n(request.args.get("data_ini")),
        "data_fim": _n(request.args.get("data_fim")),
    }

    # remove None / vazios
    filters = {k: v for k, v in filters.items() if v}

    meta = {
        "limit": int(request.args.get("limit") or 500),
        "offset": int(request.args.get("offset") or 0),
        "order_by": _n(request.args.get("order_by")) or "data_inscricao_dt",
        "order_dir": _n(request.args.get("order_dir")) or "DESC",
    }
    return filters, meta

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
    app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024  # 30MB

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/health")
    def health():
        ok, missing = _required_envs_ok()
        return jsonify({"status": "ok" if ok else "unhealthy", "missing": missing})

    @app.get("/api/options")
    def api_options():
        try:
            data = query_options()
            return jsonify({"ok": True, "data": data})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao carregar opções dos filtros.")), 500

    @app.get("/api/leads")
    def api_leads():
        try:
            filters, meta = _get_filters_from_request()
            rows = query_leads(
                filters=filters,
                limit=meta["limit"],
                offset=meta["offset"],
                order_by=meta["order_by"],
                order_dir=meta["order_dir"],
            )
            total = query_leads_count(filters=filters)
            return jsonify({"ok": True, "total": total, "data": rows})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao buscar leads no BigQuery.")), 500

    @app.get("/api/kpis")
    def api_kpis():
        """
        KPI simples (para produção, recomendo query agregada dedicada).
        """
        try:
            filters, meta = _get_filters_from_request()

            # para KPI, evitar paginação (pode pesar). Mantive o limit atual.
            rows = query_leads(
                filters=filters,
                limit=meta["limit"],
                offset=meta["offset"],
                order_by=meta["order_by"],
                order_dir=meta["order_dir"],
            )

            total = len(rows)
            status_counts = {}
            for r in rows:
                st = r.get("status") or "Lead"
                status_counts[st] = status_counts.get(st, 0) + 1

            top_status = None
            if status_counts:
                best = max(status_counts, key=status_counts.get)
                top_status = {"status": best, "cnt": status_counts[best]}

            return jsonify({"ok": True, "total": total, "top_status": top_status})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao calcular KPIs.")), 500

    @app.post("/api/upload")
    def api_upload():
        if "file" not in request.files:
            return jsonify({"ok": False, "error": "Arquivo não encontrado."}), 400

        f = request.files["file"]
        if not f.filename:
            return jsonify({"ok": False, "error": "Nome de arquivo vazio."}), 400

        try:
            filename = (f.filename or "").lower()

            if filename.endswith(".csv"):
                df = pd.read_csv(f)
            elif filename.endswith(".xlsx") or filename.endswith(".xls"):
                df = pd.read_excel(f)
            else:
                return jsonify({"ok": False, "error": "Formato inválido. Envie CSV ou XLSX."}), 400

            process_upload_dataframe(df)

            return jsonify({"ok": True, "message": "Processado com sucesso! (staging + procedure V14)"}), 200
        except Exception as e:
            return jsonify(_error_payload(e, "Falha na ingestão V14.")), 500

    return app


if __name__ == "__main__":
    app = create_app()
    port = int(_env("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
