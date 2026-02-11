# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)
Versão: 3.0 - Modelo Estrela Consolidado V14
"""

import os
import io
import csv
import logging
import traceback
import uuid
import pandas as pd
from typing import Optional
from flask import Flask, render_template, request, jsonify, Response, stream_with_context, send_file

from services.bigquery import (
    query_leads,
    query_leads_count,
    query_options,
    process_upload_dataframe_batched,
    export_staging_variable_rows,
    export_leads_rows,
    EXPORT_VARIABLE_COLUMNS,
    EXPORT_COLUMNS,
)

# ============================================================
# HELPERS
# ============================================================
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) else v if v else default

logging.basicConfig(level=_env("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

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

    # filtros
    filters = {
        "status": n(request.args.get("status")),
        "curso": n(request.args.get("curso")),
        "polo": n(request.args.get("polo")),
        "consultor": n(request.args.get("consultor")),
        "modalidade": n(request.args.get("modalidade")),
        "cpf": n(request.args.get("cpf")),
        "celular": n(request.args.get("celular")),
        "email": n(request.args.get("email")),
        "nome": n(request.args.get("nome")),
        "data_ini": n(request.args.get("data_ini")),
        "data_fim": n(request.args.get("data_fim")),
    }
    filters = {k: v for k, v in filters.items() if v}

    # meta (paginação/ordenação)
    meta = {
        "limit": int(request.args.get("limit") or 500),
        "offset": int(request.args.get("offset") or 0),
        "order_by": n(request.args.get("order_by")) or "data_inscricao_dt",
        "order_dir": n(request.args.get("order_dir")) or "DESC",
    }
    return filters, meta

def _error_payload(e: Exception, public_msg: str, upload_id: Optional[str] = None):
    payload = {
        "ok": False,
        "error": public_msg,
        "details": str(e),
        "trace": traceback.format_exc(limit=3)
    }
    if upload_id:
        payload["upload_id"] = upload_id
    return payload

def _read_csv_flexible(file_storage):
    """Lê CSV com tolerância a encoding/separador para evitar falhas de import."""
    raw = file_storage.read()
    if hasattr(file_storage, "stream"):
        file_storage.stream.seek(0)

    attempts = [
        {"sep": ",", "encoding": "utf-8"},
        {"sep": ";", "encoding": "utf-8"},
        {"sep": ",", "encoding": "latin1"},
        {"sep": ";", "encoding": "latin1"},
    ]

    last_error = None
    for cfg in attempts:
        try:
            df = pd.read_csv(io.BytesIO(raw), sep=cfg["sep"], encoding=cfg["encoding"])
            if df.shape[1] == 1 and cfg["sep"] == ",":
                # Evita falso-positivo quando arquivo na prática é separado por ';'
                last_error = ValueError("CSV parece usar ';' como separador")
                continue
            return df
        except Exception as e:
            last_error = e
            logger.warning("Falha ao ler CSV com sep=%s encoding=%s: %s", cfg["sep"], cfg["encoding"], e)

    raise ValueError(f"Falha ao ler CSV. Verifique encoding/separador. Erro: {last_error}")


def _repair_mojibake_text(value):
    """Corrige mojibake comum em textos antes de escrever no CSV."""
    if not isinstance(value, str):
        return value

    s = value.strip()
    if not s:
        return s
    if "Ã" not in s and "Â" not in s:
        return s

    try:
        fixed = s.encode("cp1252").decode("utf-8")
        if fixed.count("Ã") + fixed.count("Â") < s.count("Ã") + s.count("Â"):
            return fixed
    except Exception:
        pass
    return s


# ============================================================
# APP FACTORY
# ============================================================
def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024  # Limite de 30MB para uploads

    asset_version = _env("ASSET_VERSION", "20260210-visual16")
    ui_version = _env("UI_VERSION", f"v{asset_version}")


    @app.after_request
    def add_no_cache_headers(response):
        content_type = (response.headers.get("Content-Type") or "").lower()
        if "text/html" in content_type:
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

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
        OBS: Esta rota faz uma query completa de leads para calcular KPI.
        Em produção, o ideal é ter uma query agregada dedicada no BigQuery.
        """
        try:
            filters, meta = _get_filters_from_request()
            # Usa o mesmo limit da requisição, mas pra KPI você pode querer ignorar limit.
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

            return jsonify({
                "ok": True,
                "total": total,
                "top_status": top_status
            })
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao calcular KPIs.")), 500

    @app.get("/api/options")
    def api_options():
        try:
            data = query_options()
            return jsonify({"ok": True, "data": data})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao carregar opções dos filtros.")), 500

    @app.get("/api/export/variaveis")
    def api_export_variaveis():
        try:
            logger.info("Iniciando exportação de variáveis da staging")
            max_rows = int(request.args.get("max_rows") or 50000)

            def _iter_csv_lines():
                sio = io.StringIO()
                writer = csv.writer(sio, delimiter=';', quoting=csv.QUOTE_ALL)

                writer.writerow(EXPORT_VARIABLE_COLUMNS)
                yield sio.getvalue()
                sio.seek(0)
                sio.truncate(0)

                for row in export_staging_variable_rows(max_rows=max_rows):
                    writer.writerow([_repair_mojibake_text(row.get(col)) for col in EXPORT_VARIABLE_COLUMNS])
                    yield sio.getvalue()
                    sio.seek(0)
                    sio.truncate(0)

            ts = pd.Timestamp.utcnow().strftime('%Y%m%d-%H%M%S')
            filename = f"variaveis_staging_{ts}.csv"
            headers = {
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Type": "text/csv; charset=utf-8",
            }

            def _stream_with_bom_bytes():
                yield b"\xef\xbb\xbf"  # UTF-8 BOM
                for line in _iter_csv_lines():
                    yield line.encode("utf-8")

            logger.info("Export variáveis concluído com sucesso")
            return Response(stream_with_context(_stream_with_bom_bytes()), headers=headers)
        except Exception as e:
            logger.exception("Falha na exportação de variáveis")
            return jsonify(_error_payload(e, "Falha ao exportar variáveis da staging.")), 500

    @app.get("/api/export/csv")
    def api_export_filtrado_csv():
        try:
            filters, _meta = _get_filters_from_request()
            max_rows = int(request.args.get("max_rows") or 50000)
            logger.info("Iniciando exportação filtrada [max_rows=%s filters=%s]", max_rows, filters)

            def _iter_csv_lines():
                sio = io.StringIO()
                writer = csv.writer(sio, delimiter=';', quoting=csv.QUOTE_ALL)

                writer.writerow([h for _, h in EXPORT_COLUMNS])
                yield sio.getvalue()
                sio.seek(0)
                sio.truncate(0)

                for row in export_leads_rows(filters=filters, max_rows=max_rows):
                    writer.writerow([_repair_mojibake_text(row.get(k)) for k, _ in EXPORT_COLUMNS])
                    yield sio.getvalue()
                    sio.seek(0)
                    sio.truncate(0)

            ts = pd.Timestamp.utcnow().strftime('%Y%m%d-%H%M%S')
            filename = f"leads_filtrados_{ts}.csv"
            headers = {
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Type": "text/csv; charset=utf-8",
            }

            def _stream_with_bom_bytes():
                yield b"\xef\xbb\xbf"
                for line in _iter_csv_lines():
                    yield line.encode("utf-8")

            logger.info("Export filtrado concluído com sucesso")
            return Response(stream_with_context(_stream_with_bom_bytes()), headers=headers)
        except Exception as e:
            logger.exception("Falha na exportação filtrada")
            return jsonify(_error_payload(e, "Falha ao exportar leads filtrados.")), 500


    @app.get("/api/export/xlsx")
    @app.get("/api/export")
    def api_export_filtrado_xlsx():
        try:
            filters, _meta = _get_filters_from_request()
            max_rows = int(request.args.get("max_rows") or 50000)
            logger.info("Iniciando exportação XLSX filtrada [max_rows=%s filters=%s]", max_rows, filters)

            rows = []
            for row in export_leads_rows(filters=filters, max_rows=max_rows):
                rows.append({h: _repair_mojibake_text(row.get(k)) for k, h in EXPORT_COLUMNS})

            df = pd.DataFrame(rows, columns=[h for _, h in EXPORT_COLUMNS])
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="Leads")
            output.seek(0)

            ts = pd.Timestamp.utcnow().strftime('%Y%m%d-%H%M%S')
            filename = f"leads_filtrados_{ts}.xlsx"
            logger.info("Export XLSX filtrado concluído com sucesso")
            return send_file(
                output,
                as_attachment=True,
                download_name=filename,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as e:
            logger.exception("Falha na exportação XLSX filtrada")
            return jsonify(_error_payload(e, "Falha ao exportar leads filtrados em XLSX.")), 500

    @app.post("/api/upload")
    def api_upload():
        upload_id = str(uuid.uuid4())

        if "file" not in request.files:
            logger.error("Upload sem arquivo [upload_id=%s]", upload_id)
            return jsonify({"ok": False, "error": "Arquivo não encontrado.", "upload_id": upload_id}), 400

        f = request.files["file"]
        if not f.filename:
            logger.error("Upload com nome vazio [upload_id=%s]", upload_id)
            return jsonify({"ok": False, "error": "Nome de arquivo vazio.", "upload_id": upload_id}), 400

        try:
            filename = (f.filename or "").lower()
            logger.info(
                "Iniciando upload [upload_id=%s] filename=%s mimetype=%s",
                upload_id,
                f.filename,
                getattr(f, "mimetype", None),
            )

            if filename.endswith(".csv"):
                # lê CSV com fallback de encoding/separador e processa em lotes
                csv_df = _read_csv_flexible(f)
                logger.info(
                    "CSV lido [upload_id=%s] linhas=%s colunas=%s",
                    upload_id,
                    len(csv_df),
                    list(csv_df.columns),
                )
                total_rows = process_upload_dataframe_batched(csv_df)
                logger.info("Upload finalizado [upload_id=%s] total_rows=%s", upload_id, total_rows)
                return jsonify(
                    {
                        "ok": True,
                        "upload_id": upload_id,
                        "message": f"Processado com sucesso! (staging + procedure V14) - {total_rows} linhas em lotes.",
                    }
                ), 200
            elif filename.endswith(".xlsx") or filename.endswith(".xls"):
                try:
                    df = pd.read_excel(f)
                except ImportError as e:
                    logger.exception("Falha de dependência no Excel [upload_id=%s]", upload_id)
                    return jsonify(
                        {
                            "ok": False,
                            "upload_id": upload_id,
                            "error": "Leitura de Excel indisponível no ambiente. Instale 'openpyxl' para processar XLSX.",
                            "details": str(e),
                        }
                    ), 500

                logger.info(
                    "Excel lido [upload_id=%s] linhas=%s colunas=%s",
                    upload_id,
                    len(df),
                    list(df.columns),
                )
                total_rows = process_upload_dataframe_batched(df)
                logger.info("Upload finalizado [upload_id=%s] total_rows=%s", upload_id, total_rows)
                return jsonify(
                    {
                        "ok": True,
                        "upload_id": upload_id,
                        "message": f"Processado com sucesso! (staging + procedure V14) - {total_rows} linhas em lotes.",
                    }
                ), 200
            else:
                logger.error("Formato inválido [upload_id=%s] filename=%s", upload_id, f.filename)
                return jsonify({"ok": False, "upload_id": upload_id, "error": "Formato inválido. Envie CSV ou XLSX."}), 400
        except Exception as e:
            logger.exception("Falha na ingestão V14 durante upload [upload_id=%s]", upload_id)
            return jsonify(_error_payload(e, "Falha na ingestão V14.", upload_id=upload_id)), 500

    return app


if __name__ == "__main__":
    app = create_app()
    port = int(_env("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
