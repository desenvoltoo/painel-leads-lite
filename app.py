# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)

Rotas:
- /                      -> tela
- /health                -> status do app + envs
- /api/leads             -> lista leads (BigQuery)
- /api/kpis              -> KPIs (BigQuery)
- /api/options           -> distincts (curso/polo/status/origem)
- /api/upload            -> upload CSV/XLSX -> staging -> CALL procedure
- /api/export            -> export CSV (UTF-8 BOM) sem quebrar acentos
- /download/modelo       -> baixa modelo XLSX de importação
- /api/debug/source      -> mostra de onde está lendo
- /api/debug/sample      -> retorna 5 linhas da view
- /api/debug/count       -> count(*) na view

ENV obrigatórias:
- GCP_PROJECT_ID
- BQ_DATASET
- BQ_VIEW_LEADS

ENV opcionais:
- BQ_UPLOAD_TABLE (default: stg_leads_upload)
- BQ_PIPELINE_PROC (default: sp_v9_run_pipeline)
- BQ_LOCATION (default: us-central1)
- BQ_OPTIONS_LIMIT (default: 50000)
- PORT (default: 8080)
"""

import os
import io
import traceback
import pandas as pd

from flask import Flask, render_template, request, jsonify, send_file

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


def _source_ref():
    return {
        "project": _env("GCP_PROJECT_ID"),
        "dataset": _env("BQ_DATASET"),
        "view": _env("BQ_VIEW_LEADS"),
        "upload_table": _env("BQ_UPLOAD_TABLE", "stg_leads_upload"),
        "pipeline_proc": _env("BQ_PIPELINE_PROC", _env("BQ_PIPELINE_PROC", "sp_v9_run_pipeline")),
        "location": _env("BQ_LOCATION", "us-central1"),
        "options_limit": int(_env("BQ_OPTIONS_LIMIT", "50000")),
    }


def _error_payload(e: Exception, public_msg: str):
    return {
        "ok": False,
        "error": public_msg,
        "details": str(e),
        "trace": traceback.format_exc(limit=6),
        "source": _source_ref(),
    }


def _n(v):
    v = (v or "").strip()
    return v if v else None


def _get_list(name: str):
    """
    Aceita:
    - ?curso=ABC&curso=DEF (getlist nativo)
    - ou hidden "||" (caso você mande assim do front): ?curso_multi=ABC||DEF
    """
    items = [x.strip() for x in request.args.getlist(name) if (x or "").strip()]

    # fallback: curso_multi/polo_multi no formato "A||B||C"
    multi_key = f"{name}_multi"
    if not items:
        raw = (request.args.get(multi_key) or "").strip()
        if raw:
            items = [x.strip() for x in raw.split("||") if x.strip()]

    # remove duplicados (case-insensitive)
    seen = set()
    out = []
    for x in items:
        k = x.upper()
        if k not in seen:
            seen.add(k)
            out.append(x)
    return out


def _get_filters_from_request():
    return {
        "status": _n(request.args.get("status")),
        "origem": _n(request.args.get("origem")),
        "data_ini": _n(request.args.get("data_ini")),  # YYYY-MM-DD
        "data_fim": _n(request.args.get("data_fim")),  # YYYY-MM-DD
        "limit": int(request.args.get("limit") or 500),

        # MULTI
        "curso_list": _get_list("curso"),
        "polo_list": _get_list("polo"),
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
            return jsonify({"ok": False, "error": f"ENV obrigatórias faltando: {missing}", "source": _source_ref()}), 500

        try:
            if "file" not in request.files:
                return jsonify({"ok": False, "error": "Nenhum arquivo enviado (campo 'file').", "source": _source_ref()}), 400

            f = request.files["file"]
            if not f.filename:
                return jsonify({"ok": False, "error": "Nome de arquivo vazio.", "source": _source_ref()}), 400

            source = (request.form.get("source") or "UPLOAD_PAINEL").strip()
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

    # ------------------- api: export csv (safe) -------------------
    @app.get("/api/export")
    def api_export():
        ok, missing = _required_envs_ok()
        if not ok:
            return jsonify({"ok": False, "error": f"ENV obrigatórias faltando: {missing}", "source": _source_ref()}), 500

        try:
            filters = _get_filters_from_request()

            # limite específico p/ export (não mistura com limite da tela)
            export_limit = int(request.args.get("export_limit") or 200000)
            export_limit = max(1000, min(export_limit, 500000))  # trava de segurança
            filters["limit"] = export_limit

            rows = query_leads(filters)

            df = pd.DataFrame(rows)

            # CSV amigável pro Excel BR:
            # - sep=';' (não conflita com vírgula decimal)
            # - encoding utf-8-sig (BOM -> não quebra acentos)
            out = io.StringIO()
            df.to_csv(out, index=False, sep=";", encoding="utf-8-sig")
            raw = out.getvalue().encode("utf-8-sig")

            filename = f"leads_export_{pd.Timestamp.now().strftime('%Y-%m-%d')}.csv"
            return send_file(
                io.BytesIO(raw),
                mimetype="text/csv; charset=utf-8",
                as_attachment=True,
                download_name=filename,
            )

        except Exception as e:
            print("🚨 /api/export ERROR:", repr(e))
            return jsonify(_error_payload(e, "Falha ao exportar CSV.")), 500

    # ------------------- download modelo -------------------
    @app.get("/download/modelo")
    def download_modelo():
        """
        Modelo XLSX com as colunas esperadas no upload.
        """
        try:
            cols = [
                "origem","polo","tipo_negocio","curso","modalidade","nome","cpf","celular","email",
                "endereco","convenio","empresa_conveniada","voucher","campanha","consultor","status","obs",
                "peca_disparo","texto_disparo","consultor_disparo","tipo_disparo",
                "matriculado","inscrito",
                "data_envio_dt","data_inscricao_dt","data_disparo_dt","data_contato_dt",
                "data_matricula_d","data_nascimento_d"
            ]

            df = pd.DataFrame(columns=cols)

            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="modelo_upload")
            bio.seek(0)

            return send_file(
                bio,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name="modelo_importacao_leads.xlsx",
            )
        except Exception as e:
            print("🚨 /download/modelo ERROR:", repr(e))
            return jsonify(_error_payload(e, "Falha ao gerar modelo.")), 500

    # ============================================================
    # DEBUG
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
