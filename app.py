# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)
Versão: 4.1 - Upload Assíncrono (staging + dispara SP sem bloquear)
"""
 
import os
import traceback
import io
import uuid
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple
 
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_file, session, redirect, url_for, g
from werkzeug.security import generate_password_hash, check_password_hash
 
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


def _default_users_payload() -> Dict[str, str]:
    default_password = "123456"
    return {
        "matheus": generate_password_hash(default_password),
        "miguel": generate_password_hash(default_password),
    }
 
 
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
    app.secret_key = _env("FLASK_SECRET_KEY", "painel-leads-lite-dev-key")
 
    asset_version = _env("ASSET_VERSION", "20260225-star-v1")
    ui_version = _env("UI_VERSION", f"v{asset_version}")
 
    # pastas locais (mantém XLSX)
    UPLOAD_DIR = Path(_env("UPLOAD_DIR", "enviados"))
    EXPORT_DIR = Path(_env("EXPORT_DIR", "exportados"))
    AUTH_DIR = Path(_env("AUTH_DIR", "/tmp/auth"))
    USERS_FILE = AUTH_DIR / "users.json"
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    AUTH_DIR.mkdir(parents=True, exist_ok=True)

    def _load_users() -> Dict[str, str]:
        if not USERS_FILE.exists():
            payload = _default_users_payload()
            USERS_FILE.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return payload
        data = json.loads(USERS_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Formato inválido no arquivo de usuários.")
        return {str(k).strip().lower(): str(v) for k, v in data.items()}

    def _save_users(users: Dict[str, str]) -> None:
        USERS_FILE.write_text(
            json.dumps(users, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _looks_like_password_hash(value: str) -> bool:
        s = str(value or "")
        return s.startswith("scrypt:") or s.startswith("pbkdf2:")

    @app.before_request
    def _auth_guard():
        public_routes = {"/", "/login", "/api/login", "/health"}
        if request.path.startswith("/static/"):
            return None
        if request.path in public_routes:
            return None

        username = session.get("username")
        if username:
            g.current_user = username
            return None

        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Não autenticado."}), 401

        return None

    @app.get("/")
    def index():
        return render_template(
            "index.html",
            asset_version=asset_version,
            ui_version=ui_version,
            current_user=session.get("username"),
        )

    @app.get("/login")
    def login_page():
        if session.get("username"):
            return redirect(url_for("index"))
        return render_template(
            "login.html",
            asset_version=asset_version,
            ui_version=ui_version,
        )

    @app.post("/api/login")
    def api_login():
        body = request.get_json(silent=True) or {}
        username = str(body.get("username") or "").strip().lower()
        password = str(body.get("password") or "")

        if not username or not password:
            return jsonify({"ok": False, "error": "Usuário e senha são obrigatórios."}), 400

        users = _load_users()
        stored_password = users.get(username)
        if not stored_password:
            return jsonify({"ok": False, "error": "Usuário ou senha inválidos."}), 401

        # Compatibilidade/migração: aceita valor legado em texto puro e converte para hash.
        if _looks_like_password_hash(stored_password):
            is_valid = check_password_hash(stored_password, password)
        else:
            is_valid = stored_password == password
            if is_valid:
                users[username] = generate_password_hash(password)
                _save_users(users)

        if not is_valid:
            return jsonify({"ok": False, "error": "Usuário ou senha inválidos."}), 401

        session["username"] = username
        return jsonify({"ok": True, "message": "Login realizado com sucesso.", "user": username})

    @app.post("/api/logout")
    def api_logout():
        session.clear()
        return jsonify({"ok": True, "message": "Sessão encerrada."})

    @app.post("/api/change-password")
    def api_change_password():
        username = session.get("username")
        if not username:
            return jsonify({"ok": False, "error": "Não autenticado."}), 401

        body = request.get_json(silent=True) or {}
        current_password = str(body.get("current_password") or "")
        new_password = str(body.get("new_password") or "")
        confirm_password = str(body.get("confirm_password") or "")

        if not current_password or not new_password or not confirm_password:
            return jsonify({"ok": False, "error": "Preencha todos os campos de senha."}), 400
        if new_password != confirm_password:
            return jsonify({"ok": False, "error": "A confirmação da nova senha não confere."}), 400
        if len(new_password) < 6:
            return jsonify({"ok": False, "error": "A nova senha deve ter no mínimo 6 caracteres."}), 400

        users = _load_users()
        stored_password = users.get(username)
        if not stored_password:
            return jsonify({"ok": False, "error": "Senha atual inválida."}), 401

        if _looks_like_password_hash(stored_password):
            is_valid = check_password_hash(stored_password, current_password)
        else:
            is_valid = stored_password == current_password

        if not is_valid:
            return jsonify({"ok": False, "error": "Senha atual inválida."}), 401

        users[username] = generate_password_hash(new_password)
        _save_users(users)
        return jsonify({"ok": True, "message": "Senha alterada com sucesso."})
 
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
 
