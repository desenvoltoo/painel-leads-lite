# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)
Versão: 4.1 - Upload Assíncrono (staging + dispara SP sem bloquear)
"""
 
import os
import traceback
import io
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
 
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, g, session
from werkzeug.security import check_password_hash
 
from services.bigquery import (
    query_leads,
    query_leads_count,
    query_options,
    process_upload_dataframe,   # agora retorna job_id (async)
    get_bq_job_status,          # novo
    export_leads_rows,
    df_to_xlsx,                 # salva cópia do upload
    rows_to_xlsx,               # gera export XLSX no servidor
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


def _env_int(name: str, default: int) -> int:
    raw = _env(name, str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default
 
def _get_filters_from_request() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    def n(v):
        v = (v or "").strip()
        return v if v else None
 
    filters = {
        "status": n(request.args.get("status")),  # pode ser status_inscricao também
        "curso": n(request.args.get("curso")),
        "polo": n(request.args.get("polo")),
        "origem": n(request.args.get("origem")),
        "consultor": n(request.args.get("consultor")),  # compat -> consultor_disparo
        "consultor_disparo": n(request.args.get("consultor_disparo")),
        "consultor_comercial": n(request.args.get("consultor_comercial")),
        "modalidade": n(request.args.get("modalidade")),
        "turno": n(request.args.get("turno")),
        "canal": n(request.args.get("canal")),
        "campanha": n(request.args.get("campanha")),
        "tipo_disparo": n(request.args.get("tipo_disparo")),
        "tipo_negocio": n(request.args.get("tipo_negocio")),
        "cpf": n(request.args.get("cpf")),
        "celular": n(request.args.get("celular")),
        "email": n(request.args.get("email")),
        "nome": n(request.args.get("nome")),
        "matriculado": n(request.args.get("matriculado")),
        "data_ini": n(request.args.get("data_ini")),
        "data_fim": n(request.args.get("data_fim")),
    }
    filters = {k: v for k, v in filters.items() if v is not None and str(v).strip() != ""}
 
    meta = {
        "limit": int(request.args.get("limit") or 500),
        "offset": int(request.args.get("offset") or 0),
        "order_by": n(request.args.get("order_by")) or "data_inscricao",
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
# Upload parsing: CSV/XLSX robusto (separador, encoding)
# FIX: força cpf/celular como str ANTES do pandas inferir float64,
#      evitando corrupção de números longos (ex: 55119443914040 -> 55119443914040**0**)
# ============================================================
 
# colunas que jamais devem ser lidas como número
_PHONEISH_UPLOAD_COLS = {"cpf", "celular"}
 
 
def _dtype_map_for_phoneish(columns) -> dict:
    """
    Recebe a lista de colunas do arquivo e devolve um dtype_map
    forçando str para qualquer coluna que seja cpf ou celular.
    """
    return {
        col: str
        for col in columns
        if str(col).strip().lower() in _PHONEISH_UPLOAD_COLS
    }
 
 
def _read_upload_to_df(file_storage) -> pd.DataFrame:
    filename = (file_storage.filename or "").lower().strip()
    raw = file_storage.read()
 
    # ------------------------------------------------------------------
    # XLSX
    # ------------------------------------------------------------------
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        # 1ª leitura: só cabeçalho para descobrir nomes das colunas
        preview = pd.read_excel(io.BytesIO(raw), nrows=0)
        dtype_map = _dtype_map_for_phoneish(preview.columns)
        # 2ª leitura: com dtype forçado nas colunas phoneish
        return pd.read_excel(io.BytesIO(raw), dtype=dtype_map)
 
    # ------------------------------------------------------------------
    # CSV — tenta separadores e encodings em cascata
    # ------------------------------------------------------------------
    def _read_csv(raw_bytes: bytes, sep: str, encoding: str) -> pd.DataFrame:
        # 1ª leitura: só cabeçalho
        preview = pd.read_csv(
            io.BytesIO(raw_bytes), sep=sep, encoding=encoding, nrows=0
        )
        dtype_map = _dtype_map_for_phoneish(preview.columns)
        # 2ª leitura: completa com dtype forçado
        return pd.read_csv(
            io.BytesIO(raw_bytes), sep=sep, encoding=encoding, dtype=dtype_map
        )
 
    for sep in (";", ","):
        for enc in ("utf-8", "latin-1"):
            try:
                return _read_csv(raw, sep, enc)
            except Exception:
                continue
 
    raise ValueError(
        "Não foi possível ler o arquivo. "
        "Verifique se é um CSV válido (separador ; ou ,) ou XLSX."
    )
 
 
# ============================================================
# APP FACTORY
# ============================================================
def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024
    app.secret_key = _env("FLASK_SECRET_KEY", "painel-leads-lite-dev-secret-change-me")

    asset_version = _env("ASSET_VERSION", "20260225-star-v1")
    ui_version = _env("UI_VERSION", f"v{asset_version}")
    session_ttl_seconds = _env_int("SESSION_TTL_SECONDS", 28800)
    app.permanent_session_lifetime = timedelta(seconds=session_ttl_seconds)
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = _env("COOKIE_SECURE", "false").lower() == "true"

    # Usuários iniciais com senha hasheada (senha padrão: 123456)
    users = {
        "matheus": "pbkdf2:sha256:1000000$Ij5ppE2yYdLvAKlF$0d441b0096771e07525df01b224faf57cabedc83b444375cad21e44f9d6b5282",
        "miguel": "pbkdf2:sha256:1000000$rxyvycWVM3tJCDF0$36a69c69fd09385c4e39fd2c67549f60c9dccc1a68f7a56d0f8e8f00716fc49d",
    }
 
    # pastas locais (mantém XLSX)
    UPLOAD_DIR = Path(_env("UPLOAD_DIR", "enviados"))
    EXPORT_DIR = Path(_env("EXPORT_DIR", "exportados"))
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    def _is_public_path(path: str) -> bool:
        if path.startswith("/static/"):
            return True
        return path in ("/login", "/logout", "/health", "/api/auth/login")

    def _current_user() -> Optional[str]:
        username = session.get("username")
        return str(username) if username else None

    def _destroy_session():
        session.clear()

    @app.before_request
    def _auth_guard():
        if _is_public_path(request.path):
            return None

        user = _current_user()
        g.current_user = user
        if user:
            return None

        if request.path.startswith("/api/"):
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Sessão expirada. Faça login novamente.",
                        "redirect_to": "/login",
                    }
                ),
                401,
            )
        return redirect(url_for("login"))

    @app.get("/")
    def index():
        return render_template(
            "index.html",
            asset_version=asset_version,
            ui_version=ui_version,
            current_user=getattr(g, "current_user", None),
        )

    @app.get("/login")
    def login():
        if _current_user():
            return redirect(url_for("index"))
        return render_template(
            "login.html",
            asset_version=asset_version,
            ui_version=ui_version,
            error=None,
        )

    @app.post("/api/auth/login")
    def api_auth_login():
        payload = request.get_json(silent=True) or {}
        username = str(payload.get("username") or "").strip().lower()
        password = str(payload.get("password") or "")

        user_hash = users.get(username)
        if not user_hash or not check_password_hash(user_hash, password):
            return jsonify({"ok": False, "error": "Usuário ou senha incorretos. Tente novamente."}), 401

        session.clear()
        session.permanent = True
        session["username"] = username
        return jsonify({"ok": True, "redirect_to": "/"})

    @app.post("/logout")
    def logout():
        _destroy_session()
        return redirect(url_for("login"))
 
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
        Mantém seu KPI atual (rápido): conta em memória nos rows retornados.
        """
        try:
            filters, meta = _get_filters_from_request()
            rows = query_leads(
                filters=filters,
                limit=min(int(meta["limit"]), 5000),
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
    # ✅ AGORA ASSÍNCRONO: retorna job_id e NÃO trava request
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
            if not (filename.endswith(".csv") or filename.endswith(".xlsx") or filename.endswith(".xls")):
                return jsonify({"ok": False, "error": "Formato inválido. Envie CSV ou XLSX."}), 400
 
            # lê df (cpf/celular já chegam como str, sem risco de float64)
            df = _read_upload_to_df(f)
 
            # ✅ salva cópia SEMPRE em XLSX
            saved_name = f"upload_{_stamp()}_{uuid.uuid4().hex[:6]}.xlsx"
            saved_path = str(UPLOAD_DIR / saved_name)
            df_to_xlsx(df, saved_path, sheet_name="Upload")
 
            # ✅ staging + dispara SP async (retorna job_id)
            job_id = process_upload_dataframe(df)
 
            # 202 Accepted (processando)
            return jsonify(
                {
                    "ok": True,
                    "message": "Upload recebido. Importação em processamento (assíncrono).",
                    "saved_xlsx": saved_name,
                    "job_id": job_id,
                }
            ), 202
 
        except Exception as e:
            return jsonify(_error_payload(e, "Falha na ingestão.")), 500
 
    # ============================================================
    # ✅ STATUS DO UPLOAD (job do BigQuery)
    # ============================================================
    @app.get("/api/upload/status")
    def api_upload_status():
        try:
            job_id = (request.args.get("job_id") or "").strip()
            if not job_id:
                return jsonify({"ok": False, "error": "job_id é obrigatório"}), 400
 
            data = get_bq_job_status(job_id)
            return jsonify({"ok": True, "data": data}), 200
        except Exception as e:
            return jsonify(_error_payload(e, "Falha ao consultar status do upload.")), 500
 
    return app
 
 
if __name__ == "__main__":
    app = create_app()
    port = int(_env("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
 
