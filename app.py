# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)

ENV (opcionais, defaults seguros):
- GCP_PROJECT_ID (default: painel-universidade)
- BQ_DATASET     (default: modelo_estrela)
- BQ_VIEW_LEADS  (default: vw_leads_painel_lite)
- BQ_LOCATION    (default: us-central1)
- BQ_OPTIONS_LIMIT (default: 200000)  # sem cap baixo no dropdown
"""

import os
import io
import traceback
import datetime as dt
from typing import Optional, Dict, Any, List

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

DEFAULT_PROJECT = "painel-universidade"
DEFAULT_DATASET = "modelo_estrela"
DEFAULT_VIEW_LEADS = "vw_leads_painel_lite"
DEFAULT_BQ_LOCATION = "us-central1"


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    v = v.strip() if isinstance(v, str) else v
    return v if v else default


def _source_ref():
    try:
        opt_lim = int(_env("BQ_OPTIONS_LIMIT", "200000"))
    except Exception:
        opt_lim = 200000

    project = _env("GCP_PROJECT_ID", DEFAULT_PROJECT)
    dataset = _env("BQ_DATASET", DEFAULT_DATASET)
    view = _env("BQ_VIEW_LEADS", DEFAULT_VIEW_LEADS)
    location = _env("BQ_LOCATION", DEFAULT_BQ_LOCATION)

    return {
        "project": project,
        "dataset": dataset,
        "view": view,
        "location": location,
        "upload_table": _env("BQ_UPLOAD_TABLE", "stg_leads_upload"),
        "pipeline_proc": _env("BQ_PIPELINE_PROC", "sp_v9_run_pipeline"),
        "options_limit": opt_lim,
    }


def _error_payload(e: Exception, public_msg: str):
    return {
        "ok": False,
        "error": public_msg,
        "details": str(e),
        "trace": traceback.format_exc(limit=8),
        "source": _source_ref(),
    }


def _to_date(v: Optional[str]) -> Optional[dt.date]:
    s = (v or "").strip()
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        return None


def _get_list(name: str) -> List[str]:
    """
    Aceita:
    - ?campo=A&campo=B
    - ?campo_multi=A||B||C
    """
    items = [x.strip() for x in request.args.getlist(name) if (x or "").strip()]

    if not items:
        raw = (request.args.get(f"{name}_multi") or "").strip()
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


def _get_filters_from_request(for_export: bool = False) -> Dict[str, Any]:
    if for_export:
        lim = int(request.args.get("export_limit") or 200000)
        lim = max(1000, min(lim, 500000))
    else:
        lim = int(request.args.get("limit") or 500)
        lim = max(50, min(lim, 5000))

    return {
        # compat single (se alguém usar)
        "status": (request.args.get("status") or "").strip() or None,
        "origem": (request.args.get("origem") or "").strip() or None,

        # ✅ multi
        "status_list": _get_list("status"),
        "origem_list": _get_list("origem"),
        "curso_list": _get_list("curso"),
        "polo_list": _get_list("polo"),

        "data_ini": _to_date(request.args.get("data_ini")),
        "data_fim": _to_date(request.args.get("data_fim")),
        "limit": lim,
    }


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 25 * 1024 * 1024  # 25MB

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/health")
    def health():
        return jsonify({"status": "ok", "source": _source_ref()})

    @app.get("/api/leads")
    def api_leads():
        filters = _get_filters_from_request(for_export=False)
        try:
            rows = query_leads(filters)
            return jsonify({"count": len(rows), "rows": rows, "source": _source_ref()})
        except Exception as e:
            print("🚨 /api/leads ERROR:", repr(e))
            return jsonify(_error_payload(e, "Falha ao consultar o BigQuery (leads).")), 500

    @app.get("/api/kpis")
    def api_kpis():
        filters = _get_filters_from_request(for_export=False)
        try:
            data = query_kpis(filters)
            return jsonify({**data, "source": _source_ref()})
        except Exception as e:
            print("🚨 /api/kpis ERROR:", repr(e))
            return jsonify(_error_payload(e, "Falha ao consultar o BigQuery (KPIs).")), 500

    @app.get("/api/options")
    def api_options():
        try:
            data = query_options()
            return jsonify({**data, "source": _source_ref()})
        except Exception as e:
            print("🚨 /api/options ERROR:", repr(e))
            return jsonify(_error_payload(e, "Falha ao consultar o BigQuery (options).")), 500

    @app.post("/api/upload")
    def api_upload():
        try:
            if "file" not in request.files:
                return jsonify({"ok": False, "error": "Nenhum arquivo enviado (campo 'file')."}), 400

            f = request.files["file"]
            if not f or not f.filename:
                return jsonify({"ok": False, "error": "Nome de arquivo vazio."}), 400

            source = (request.form.get("source") or "UPLOAD_PAINEL").strip() or "UPLOAD_PAINEL"
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

    @app.get("/api/export")
    def api_export():
        try:
            filters = _get_filters_from_request(for_export=True)
            rows = query_leads(filters)
            df = pd.DataFrame(rows)

            out = io.StringIO()
            df.to_csv(out, index=False, sep=";")
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

    @app.get("/download/modelo")
    def download_modelo():
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

    # DEBUG
    @app.get("/api/debug/source")
    def api_debug_source():
        return jsonify({"ok": True, "source": _source_ref()})

    @app.get("/api/debug/count")
    def api_debug_count():
        try:
            c = debug_count()
            return jsonify({"ok": True, "count": c, "source": _source_ref()})
        except Exception as e:
            return jsonify(_error_payload(e, "Falha ao contar registros na view.")), 500

    @app.get("/api/debug/sample")
    def api_debug_sample():
        try:
            rows = debug_sample(limit=5)
            return jsonify({"ok": True, "rows": rows, "source": _source_ref()})
        except Exception as e:
            return jsonify(_error_payload(e, "Falha ao coletar amostra da view.")), 500

    return app


app = create_app()

if __name__ == "__main__":
    port = int(_env("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
