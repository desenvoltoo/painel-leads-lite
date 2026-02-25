# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)
Versão: 4.0 - Novo Modelo Estrela (vw_leads_painel_lite + sp_import_star_from_site)
"""

import os
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import Flask, render_template, request, jsonify, send_file

from services.bigquery import (
    query_leads,
    query_leads_count,
    query_options,
    process_upload_dataframe,
    export_leads_rows,
    df_to_xlsx,         # ✅ salva cópia do upload
    rows_to_xlsx,       # ✅ gera export XLSX no servidor
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

def _get_filters_from_request():
    def n(v):
        v = (v or "").strip()
        return v if v else None

    # filtros (mantém compat e adiciona novos do star)
    filters = {
        "status": n(request.args.get("status")),  # pode ser status_inscricao também (bigquery.py trata)
        "curso": n(request.args.get("curso")),
        "polo": n(request.args.get("polo")),
        "origem": n(request.args.get("origem")),                 # ✅ novo
        "consultor": n(request.args.get("consultor")),           # compat -> consultor_disparo
        "consultor_disparo": n(request.args.get("consultor_disparo")),
        "consultor_comercial": n(request.args.get("consultor_comercial")),  # ✅ novo
        "modalidade": n(request.args.get("modalidade")),
        "turno": n(request.args.get("turno")),                   # ✅ novo
        "canal": n(request.args.get("canal")),
        "campanha": n(request.args.get("campanha")),
        "tipo_disparo": n(request.args.get("tipo_disparo")),     # ✅ novo
        "tipo_negocio": n(request.args.get("tipo_negocio")),     # ✅ novo
        "cpf": n(request.args.get("cpf")),
        "celular": n(request.args.get("celular")),
        "email": n(request.args.get("email")),
        "nome": n(request.args.get("nome")),
        "matriculado": n(request.args.get("matriculado")),       # ✅ novo
        "data_ini": n(request.args.get("data_ini")),
        "data_fim": n(request.args.get("data_fim")),
    }
    filters = {k: v for k, v in filters.items() if v is not None and str(v).strip() != ""}

    meta = {
        "limit": int(request.args.get("limit") or 500),
        "offset": int(request.args.get("offset") or 0),
        "order_by": n(request.args.get("order_by")) or "data_inscricao",  # ✅ novo padrão
        "order_dir": n(request.args.get("order_dir")) or "DESC",
    }
    return filters, meta

def _error_payload(e: Exception, public_msg: str):
    return {
        "ok": False,
        "error": public_msg,
        "details": str(e),
        "trace": traceback.format_exc(limit=3),
    }

def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

# ============================================================
# APP FACTORY
# ============================================================
def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024

    asset_version = _env("ASSET_VERSION", "20260225-star-v1")
    ui_version = _env("UI_VERSION", f"v{asset_version}")

    # pastas locais (mantém XLSX)
    UPLOAD_DIR = Path(_env("UPLOAD_DIR", "enviados"))
    EXPORT_DIR = Path(_env("EXPORT_DIR", "exportados"))
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    @app.get("/")
    def index():
        return render_template(
            "index.html",
            asset_version=asset_version,
            ui_version=ui_version,
        )

    @app.get("/health")
    def health():
        ok, missing = _required_envs_ok()
        return jsonify(
            {
                "status": "ok" if ok else "unhealthy",
                "missing": missing,
                "ui_version": ui_version,
                "asset_version": asset_version,
            }
        )

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
        Melhorado: usa status_inscricao quando existir (novo star).
        """
        try:
            filters, meta = _get_filters_from_request()
            rows = query_leads(
                filters=filters,
                limit=min(int(meta["limit"]), 5000),  # evita estourar
                offset=meta["offset"],
                order_by=meta["order_by"],
                order_dir=meta["order_dir"],
            )

            total = len(rows)
            status_counts: dict = {}
            for r in rows:
                st = r.get("status_inscricao") or r.get("status") or "LEAD"
                status_counts[st] = status_counts.get(st, 0) + 1

            top_status = None
            if status_counts:
                best = max(status_counts, key=status_counts.get)
                top_status = {"status": best, "cnt": status_counts[best]}

            return jsonify({"ok": True, "total": total, "top_status": top_status})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao calcular KPIs.")), 500

    @app.get("/api/options")
    def api_options():
        try:
            data = query_options()
            return jsonify({"ok": True, "data": data})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao carregar opções dos filtros.")), 500

    # ============================================================
    # ✅ EXPORT XLSX (gera arquivo em exportados/ e também envia)
    # ============================================================
    @app.get("/api/export/xlsx")
    def api_export_xlsx():
        try:
            filters, meta = _get_filters_from_request()

            limit = min(int(meta.get("limit") or 50000), 100000)
            rows = export_leads_rows(
                filters=filters,
                limit=limit,
                offset=int(meta.get("offset") or 0),
                order_by=meta.get("order_by") or "data_inscricao",
                order_dir=meta.get("order_dir") or "DESC",
            )

            fname = f"leads_export_{_stamp()}.xlsx"
            out_path = str(EXPORT_DIR / fname)

            rows_to_xlsx(rows or [], out_path, sheet_name="Leads")

            return send_file(
                out_path,
                as_attachment=True,
                download_name="leads_export.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao exportar XLSX.")), 500

    # ============================================================
    # ✅ UPLOAD (CSV/XLSX) + salva cópia XLSX em enviados/
    # ============================================================
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

            # ✅ salva cópia SEMPRE em XLSX
            saved_name = f"upload_{_stamp()}.xlsx"
            saved_path = str(UPLOAD_DIR / saved_name)
            df_to_xlsx(df, saved_path, sheet_name="Upload")

            # ✅ staging + SP nova (amarrada no star)
            process_upload_dataframe(df)

            return jsonify(
                {
                    "ok": True,
                    "message": "Processado com sucesso! (staging + procedure do novo Modelo Estrela)",
                    "saved_xlsx": saved_name,
                }
            ), 200

        except Exception as e:
            return jsonify(_error_payload(e, "Falha na ingestão.")), 500

    return app


if __name__ == "__main__":
    app = create_app()
    port = int(_env("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
