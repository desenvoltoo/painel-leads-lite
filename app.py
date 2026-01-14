# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)

Rotas:
- /                     -> tela
- /health               -> status do app + envs
- /api/leads            -> lista leads (BigQuery) + suporta multi-filtros (arrays)
- /api/kpis             -> KPIs (BigQuery) + suporta multi-filtros (arrays)
- /api/options          -> distincts para datalist / selects
- /api/export           -> export CSV (UTF-8-SIG) com filtros aplicados
- /download/modelo      -> baixa modelo de importação (csv/xlsx)
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

Modelo (opcional):
- MODEL_TEMPLATE_PATH (default: static/modelos/modelo_importacao.xlsx)
"""

import os
import csv
import io
import traceback
from datetime import datetime

from flask import (
    Flask, render_template, request, jsonify,
    Response, send_file, send_from_directory
)

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
        "promote_proc": _env("BQ_PROMOTE_PROC", "sp_promote_stg_leads_upload"),
    }


def _error_payload(e: Exception, public_msg: str):
    return {
        "ok": False,
        "error": public_msg,
        "details": str(e),
        "trace": traceback.format_exc(limit=6),
        "source": _source_ref(),
    }


def _norm_str(v):
    v = (v or "").strip()
    return v if v else None


def _parse_multi_param(name: str):
    """
    Aceita:
    - ?polo=A&polo=B (getlist)
    - ?polo=A,B,C (csv)
    - ?polo=   (vazio)
    Retorna: list[str] ou []
    """
    vals = request.args.getlist(name)  # pega repetidos
    vals = [x for x in vals if _norm_str(x)]

    if len(vals) == 1 and vals[0] and "," in vals[0]:
        # caso venha "A,B,C" num único parâmetro
        parts = [p.strip() for p in vals[0].split(",")]
        vals = [p for p in parts if p]

    # remove duplicados mantendo ordem
    seen = set()
    out = []
    for x in vals:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _get_filters_from_request():
    """
    Mantém compatibilidade com o modelo antigo (string única),
    mas adiciona *_list para multi seleção.
    """
    status_list = _parse_multi_param("status")
    curso_list = _parse_multi_param("curso")
    polo_list = _parse_multi_param("polo")
    origem_list = _parse_multi_param("origem")

    data_ini = _norm_str(request.args.get("data_ini"))  # YYYY-MM-DD
    data_fim = _norm_str(request.args.get("data_fim"))  # YYYY-MM-DD

    limit_raw = request.args.get("limit")
    try:
        limit = int(limit_raw) if limit_raw else 500
    except:
        limit = 500

    # compat: se tiver lista, expõe também singular (primeiro item)
    return {
        "status": status_list[0] if status_list else _norm_str(request.args.get("status")),
        "curso": curso_list[0] if curso_list else _norm_str(request.args.get("curso")),
        "polo": polo_list[0] if polo_list else _norm_str(request.args.get("polo")),
        "origem": origem_list[0] if origem_list else _norm_str(request.args.get("origem")),
        "status_list": status_list,
        "curso_list": curso_list,
        "polo_list": polo_list,
        "origem_list": origem_list,
        "data_ini": data_ini,
        "data_fim": data_fim,
        "limit": limit,
    }


def _rows_to_csv_bytes(rows: list[dict], delimiter=";") -> bytes:
    """
    Exporta com UTF-8-SIG (BOM) para o Excel abrir acentos corretamente.
    """
    if not rows:
        # cabeçalho mínimo vazio
        content = ""
        return content.encode("utf-8-sig")

    # garante ordem de colunas estável: usa as chaves do primeiro row
    fieldnames = list(rows[0].keys())

    buf = io.StringIO(newline="")
    writer = csv.DictWriter(buf, fieldnames=fieldnames, delimiter=delimiter)
    writer.writeheader()
    for r in rows:
        # garante que toda linha tem todas colunas
        row = {k: r.get(k, "") for k in fieldnames}
        writer.writerow(row)

    return buf.getvalue().encode("utf-8-sig")


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
            return jsonify({"count": len(rows), "rows": rows, "source": _source_ref(), "filters": filters})
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
            data["filters"] = filters
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

    # ------------------- api: export -------------------
    @app.get("/api/export")
    def api_export():
        ok, missing = _required_envs_ok()
        if not ok:
            return jsonify({
                "ok": False,
                "error": f"ENV obrigatórias faltando: {missing}",
                "source": _source_ref(),
            }), 500

        filters = _get_filters_from_request()

        # para exportar, normalmente queremos mais linhas do que o limite de tela
        export_limit_raw = request.args.get("export_limit")
        try:
            export_limit = int(export_limit_raw) if export_limit_raw else 200000
        except:
            export_limit = 200000

        filters_export = dict(filters)
        filters_export["limit"] = export_limit

        try:
            rows = query_leads(filters_export)
            csv_bytes = _rows_to_csv_bytes(rows, delimiter=";")

            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"leads_export_{stamp}.csv"

            return Response(
                csv_bytes,
                mimetype="text/csv; charset=utf-8",
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"',
                    "Cache-Control": "no-store",
                },
            )
        except Exception as e:
            print("🚨 /api/export ERROR:", repr(e))
            return jsonify(_error_payload(e, "Falha ao exportar CSV.")), 500

    # ------------------- download: modelo importação -------------------
    @app.get("/download/modelo")
    def download_modelo():
        """
        Prioridade:
        1) Se existir arquivo em MODEL_TEMPLATE_PATH, serve ele.
        2) Caso não exista, gera um CSV simples com cabeçalhos (fallback).
        """
        try:
            model_path = _env("MODEL_TEMPLATE_PATH", "static/modelos/modelo_importacao.xlsx")
            # se for relativo, resolve pelo cwd
            abs_path = model_path if os.path.isabs(model_path) else os.path.join(os.getcwd(), model_path)

            if os.path.exists(abs_path) and os.path.isfile(abs_path):
                # decide download_name conforme extensão
                base = os.path.basename(abs_path)
                return send_file(abs_path, as_attachment=True, download_name=base)

            # fallback CSV (ajuste as colunas depois que formos para services.bigquery)
            headers = [
                "origem", "polo", "curso", "nome", "cpf", "celular", "email",
                "data_inscricao", "modalidade", "campanha", "consultor", "status", "obs"
            ]
            buf = io.StringIO(newline="")
            w = csv.writer(buf, delimiter=";")
            w.writerow(headers)
            # linha exemplo vazia
            w.writerow([""] * len(headers))
            payload = buf.getvalue().encode("utf-8-sig")

            return Response(
                payload,
                mimetype="text/csv; charset=utf-8",
                headers={"Content-Disposition": 'attachment; filename="modelo_importacao.csv"'},
            )

        except Exception as e:
            print("🚨 /download/modelo ERROR:", repr(e))
            return jsonify(_error_payload(e, "Falha ao baixar modelo.")), 500

    # ------------------- api: upload -------------------
    @app.post("/api/upload")
    def api_upload():
        ok, missing = _required_envs_ok()
        if not ok:
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
