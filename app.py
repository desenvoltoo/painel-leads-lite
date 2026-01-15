# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)

Rotas:
- /                     -> tela
- /health               -> status do app + envs
- /api/leads            -> lista leads (BigQuery)
- /api/kpis             -> KPIs (BigQuery)
- /api/options          -> distincts para datalist
- /api/upload           -> upload CSV/XLSX -> staging -> CALL procedure
- /api/debug/source     -> mostra de onde está lendo (dataset/view)
- /api/debug/sample     -> retorna 5 linhas da view (pra validar colunas)
- /api/debug/count      -> count(*) na view (pra provar que tem dados)

ENV obrigatórias:
- GCP_PROJECT_ID
- BQ_DATASET
- BQ_VIEW_LEADS

ENV opcionais:
- BQ_UPLOAD_TABLE (default: stg_leads_upload)
- BQ_PROMOTE_PROC (default: sp_promote_stg_leads_upload)
- PORT (default: 8080)
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
    return v if v else default


def _required_envs_ok():
    missing = []
    for k in ("GCP_PROJECT_ID", "BQ_DATASET", "BQ_VIEW_LEADS"):
        if not _env(k):
            missing.append(k)
    return (len(missing) == 0, missing)


def _get_filters_from_request():
    # UI manda strings vazias -> normaliza para None
    def n(v):
        v = (v or "").strip()
        return v if v else None

    return {
        "status": n(request.args.get("status")),
        "curso": n(request.args.get("curso")),
        "polo": n(request.args.get("polo")),
        "origem": n(request.args.get("origem")),
        "data_ini": n(request.args.get("data_ini")),  # YYYY-MM-DD
        "data_fim": n(request.args.get("data_fim")),  # YYYY-MM-DD
        "limit": int(request.args.get("limit") or 500),
    }


def _source_ref():
    return {
        "project": _env("GCP_PROJECT_ID"),
        "dataset": _env("BQ_DATASET"),
        "view": _env("BQ_VIEW_LEADS"),
        "upload_table": _env("BQ_UPLOAD_TABLE", "stg_leads_upload"),
        "promote_proc": _env("BQ_PROMOTE_PROC", "sp_promote_stg_leads_upload"),
    }


def _error_payload(e: Exception, public_msg: str):
    # útil demais no seu cenário (Cloud Run / logs)
    return {
        "ok": False,
        "error": public_msg,
        "details": str(e),
        "trace": traceback.format_exc(limit=6),
        "source": _source_ref(),
    }


# ============================================================
# APP
# ============================================================
def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024  # 25MB

    # ------------------- páginas -------------------
    @app.get("/")
    def index():
        ok, missing = _required_envs_ok()
        if not ok:
            # renderiza mesmo assim, mas vai falhar nas APIs
            print("⚠️ ENV faltando:", missing)
        return render_template("index.html")

    @app.get("/health")
    def health():
        ok, missing = _required_envs_ok()
        return jsonify({
            "status": "ok" if ok else "missing_env",
            "missing_env": missing,
            "source": _source_ref(),
        })

    # ------------------- api: leads -------------------
    @app.get("/api/leads")
    def api_leads():
        ok, missing = _required_envs_ok()
        if not ok:
            return jsonify({
                "count": 0,
                "rows": [],
                "error": f"ENV obrigatórias faltando: {missing}",
                "source": _source_ref(),
            }), 500

        filters = _get_filters_from_request()
        try:
            rows = query_leads(filters)
            return jsonify({"count": len(rows), "rows": rows, "source": _source_ref()})
        except Exception as e:
            print("🚨 /api/leads ERROR:", repr(e))
            return jsonify(_error_payload(e, "Falha ao consultar o BigQuery (leads).")), 500

    # ------------------- api: kpis -------------------
    @app.get("/api/kpis")
    def api_kpis():
        ok, missing = _required_envs_ok()
        if not ok:
            return jsonify({
                "total": 0,
                "top_status": None,
                "last_date": None,
                "by_status": [],
                "error": f"ENV obrigatórias faltando: {missing}",
                "source": _source_ref(),
            }), 500

        filters = _get_filters_from_request()
        try:
            data = query_kpis(filters)
            data["source"] = _source_ref()
            return jsonify(data)
        except Exception as e:
            print("🚨 /api/kpis ERROR:", repr(e))
            return jsonify(_error_payload(e, "Falha ao consultar o BigQuery (KPIs).")), 500

    # ------------------- api: options -------------------
    @app.get("/api/options")
    def api_options():
        ok, missing = _required_envs_ok()
        if not ok:
            return jsonify({
                "status": [],
                "curso": [],
                "polo": [],
                "origem": [],
                "error": f"ENV obrigatórias faltando: {missing}",
                "source": _source_ref(),
            }), 500

        try:
            data = query_options()
            data["source"] = _source_ref()
            return jsonify(data)
        except Exception as e:
            print("🚨 /api/options ERROR:", repr(e))
            return jsonify(_error_payload(e, "Falha ao consultar o BigQuery (options).")), 500

    # ------------------- api: upload -------------------
    @app.post("/api/upload")
    def api_upload():
        ok, missing = _required_envs_ok()
        if not ok:
            # ✅ Aqui é erro (e não sucesso). E não pode referenciar result/f
            return jsonify({
                "ok": False,
                "error": f"ENV obrigatórias faltando: {missing}",
                "source": _source_ref(),
            }), 500

        try:
            if "file" not in request.files:
                return jsonify({
                    "ok": False,
                    "error": "Nenhum arquivo enviado (campo 'file').",
                    "source": _source_ref(),
                }), 400

            f = request.files["file"]
            if not f.filename:
                return jsonify({
                    "ok": False,
                    "error": "Nome de arquivo vazio.",
                    "source": _source_ref(),
                }), 400

            source = (request.form.get("source") or "UPLOAD_PAINEL").strip()

            # ✅ Faz ingestão (staging + procedure)
            result = ingest_upload_file(f, source=source)

            return jsonify({
                "ok": True,
                **result,
                "filename": f.filename,
                "source": _source_ref(),
            })

        except Exception as e:
            print("🚨 /api/upload ERROR:", repr(e))
            return jsonify(_error_payload(e, "Falha no upload/ingestão.")), 500


    # ============================================================
    # DEBUG (pra você nunca ficar no escuro)
    # ============================================================
    @app.get("/api/debug/source")
    def api_debug_source():
        return jsonify({"ok": True, "source": _source_ref()})

    @app.get("/api/debug/count")
    def api_debug_count():
        ok, missing = _required_envs_ok()
        if not ok:
            return jsonify({"ok": False, "error": f"ENV faltando: {missing}", "source": _source_ref()}), 500
        try:
            c = debug_count()
            return jsonify({"ok": True, "count": c, "source": _source_ref()})
        except Exception as e:
            return jsonify(_error_payload(e, "Falha ao contar registros na view.")), 500

    @app.get("/api/debug/sample")
    def api_debug_sample():
        ok, missing = _required_envs_ok()
        if not ok:
            return jsonify({"ok": False, "error": f"ENV faltando: {missing}", "source": _source_ref()}), 500
        try:
            rows = debug_sample(limit=5)
            return jsonify({"ok": True, "rows": rows, "source": _source_ref()})
        except Exception as e:
            return jsonify(_error_payload(e, "Falha ao coletar amostra da view.")), 500

    return app


if __name__ == "__main__":
    port = int(_env("PORT", "8080"))
    app = create_app()
    app.run(host="0.0.0.0", port=port, debug=True)
