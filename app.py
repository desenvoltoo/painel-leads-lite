# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)
Versão: 4.2 - Fix encoding CSV (chardet)
"""
 
import os
import io
import uuid
import json
import logging
import csv
import threading
from time import perf_counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple
 
from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, make_response, g, session, Response, stream_with_context
from werkzeug.security import check_password_hash, generate_password_hash
from google.api_core.exceptions import Forbidden, NotFound
from startup_diagnostics import build_error_payload, env_bool, env_int
 
from services.bigquery import (
    EXPORT_MAX_ROWS,
    query_leads,
    query_leads_iter,
    query_leads_count,
    query_options,
    process_upload_dataframe,
    get_bq_job_status,          # novo
    export_leads_rows,
    export_leads_rows_iter,
    rows_to_xlsx,               # gera export XLSX no servidor
    EXPORT_COLUMNS,
    create_export_job,
    update_export_job,
    get_export_job,
)
from services.gestao import (
    GestaoValidationError,
    get_evolucao as gestao_get_evolucao,
    get_fila as gestao_get_fila,
    get_funil as gestao_get_funil,
    get_importacoes_historico as gestao_get_importacoes,
    parse_import_history_request as gestao_parse_import_history_request,
    get_qualidade_detalhes as gestao_get_qualidade_detalhes,
    export_qualidade as gestao_export_qualidade,
    export_importacoes as gestao_export_importacoes,
    export_fila as gestao_export_fila,
    criar_log_importacao as gestao_criar_log_importacao,
    atualizar_log_importacao as gestao_atualizar_log_importacao,
    get_opcoes as gestao_get_opcoes,
    get_produtividade as gestao_get_produtividade,
    get_qualidade_dados as gestao_get_qualidade_dados,
    get_qualidade as gestao_get_qualidade,
    get_rejeicoes as gestao_get_rejeicoes,
    export_rejeicoes as gestao_export_rejeicoes,
    export_produtividade as gestao_export_produtividade,
    get_rankings as gestao_get_rankings,
    get_resumo as gestao_get_resumo,
    invalidate_gestao_cache,
    parse_filters as gestao_parse_filters,
    utc_now_iso as gestao_utc_now_iso,
)

from services.gestao_operacional import (
    create_operational_tables as gestao_op_create_tables,
    get_dashboard as gestao_op_get_dashboard,
    get_leads_disponiveis as gestao_op_get_leads_disponiveis,
    criar_lote as gestao_op_criar_lote,
    get_lotes as gestao_op_get_lotes,
    get_lote_detalhe as gestao_op_get_lote_detalhe,
    start_lote as gestao_op_start_lote,
    finish_lote as gestao_op_finish_lote,
    get_meus_leads as gestao_op_get_meus_leads,
    update_lead_status as gestao_op_update_lead_status,
    liberar_proximos_leads as gestao_op_liberar_proximos_leads,
    executar_regras_distribuicao as gestao_op_executar_regras_distribuicao,
    get_esteira_operacional as gestao_op_get_esteira,
    get_fila_por_prioridade as gestao_op_get_fila_prioridade,
    criar_regra_distribuicao as gestao_op_criar_regra,
    listar_regras_distribuicao as gestao_op_listar_regras,
    ativar_desativar_regra as gestao_op_toggle_regra,
    parse_operational_request as gestao_op_parse_request,
    get_lotes_select as gestao_op_get_lotes_select,
    preview_proximo_lote as gestao_op_preview_proximo_lote,
    exportar_proximo_lote as gestao_op_exportar_proximo_lote,
    get_lote_csv as gestao_op_get_lote_csv,
    importar_lote_disparado as gestao_op_importar_lote_disparado,
    importar_novos_leads as gestao_op_importar_novos_leads,
    get_operacao_logs as gestao_op_get_logs,
    cancelar_lote as gestao_op_cancelar_lote,
    marcar_lote_disparado as gestao_op_marcar_lote_disparado,
    importar_retorno_lote as gestao_op_importar_retorno_lote,
    buscar_leads as gestao_op_buscar_leads,
    get_lead_timeline as gestao_op_get_lead_timeline,
    get_lead_lotes as gestao_op_get_lead_lotes,
    get_lead_eventos as gestao_op_get_lead_eventos,
    get_consultor_momento as gestao_op_get_consultor_momento,
    get_lote_atual_leads as gestao_op_get_lote_atual_leads,
    atualizar_lead_lote as gestao_op_atualizar_lead_lote,
    listar_usuarios as gestao_op_listar_usuarios,
    salvar_usuario as gestao_op_salvar_usuario,
    alterar_status_usuario as gestao_op_alterar_status_usuario,
    resetar_senha_usuario as gestao_op_resetar_senha_usuario,
    listar_perfis as gestao_op_listar_perfis,
    auditoria_usuario as gestao_op_auditoria_usuario,
    buscar_usuario_login as gestao_op_buscar_usuario_login,
    registrar_login_usuario as gestao_op_registrar_login_usuario,
    atualizar_password_hash_usuario as gestao_op_atualizar_password_hash_usuario,
    get_logs_auditoria as gestao_op_get_logs_auditoria,
    classify_bigquery_error as gestao_op_classify_bigquery_error,
)

logger = logging.getLogger(__name__)
 


def validar_senha(password_hash, senha_digitada):
    if not password_hash:
        return False

    password_hash = str(password_hash).strip()
    senha_digitada = str(senha_digitada).strip()

    if password_hash.startswith(("pbkdf2:", "scrypt:")):
        return check_password_hash(password_hash, senha_digitada)

    return password_hash == senha_digitada


ALLOWED_UPLOAD_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def _new_correlation_id() -> str:
    return uuid.uuid4().hex


def _validate_upload_filename(filename: str) -> bool:
    ext = Path(filename or "").suffix.lower()
    return ext in ALLOWED_UPLOAD_EXTENSIONS

# ============================================================
# HELPERS
# ============================================================
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) else v if v else default
 
def _required_envs_ok():
    # BigQuery has coherent defaults in services.bigquery; accept both legacy and canonical env names.
    missing = []
    checks = (("BIGQUERY_PROJECT_ID", "GCP_PROJECT_ID"), ("BIGQUERY_DATASET", "BQ_DATASET"))
    for canonical, legacy in checks:
        if not (_env(canonical) or _env(legacy)):
            # Defaults are intentionally valid for this project, so this is informational only.
            continue
    return (len(missing) == 0, missing)
 
def _get_filters_from_request() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    def n(v):
        v = (v or "").strip()
        return v if v else None
 
    filters = {
        "status": n(request.args.get("status")),
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
 
    requested_limit = int(request.args.get("limit") or 500)
    meta = {
        "limit": max(1, requested_limit),
        "offset": int(request.args.get("offset") or 0),
        "order_by": n(request.args.get("order_by")) or "data_disparo",
        "order_dir": n(request.args.get("order_dir")) or "ASC",
    }
    return filters, meta


def _get_filters_from_payload(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    payload = payload or {}

    def n(v):
        if isinstance(v, list):
            vals = [str(x).strip() for x in v if str(x).strip()]
            return vals if vals else None
        v = (v or "")
        if not isinstance(v, str):
            v = str(v)
        v = v.strip()
        return v if v else None

    filters = {
        "status": n(payload.get("status")),
        "curso": n(payload.get("curso")),
        "polo": n(payload.get("polo")),
        "origem": n(payload.get("origem")),
        "consultor": n(payload.get("consultor")),
        "consultor_disparo": n(payload.get("consultor_disparo")),
        "consultor_comercial": n(payload.get("consultor_comercial")),
        "modalidade": n(payload.get("modalidade")),
        "turno": n(payload.get("turno")),
        "canal": n(payload.get("canal")),
        "campanha": n(payload.get("campanha")),
        "tipo_disparo": n(payload.get("tipo_disparo")),
        "tipo_negocio": n(payload.get("tipo_negocio")),
        "cpf": n(payload.get("cpf")),
        "celular": n(payload.get("celular")),
        "email": n(payload.get("email")),
        "nome": n(payload.get("nome")),
        "matriculado": n(payload.get("matriculado")),
        "data_ini": n(payload.get("data_ini")),
        "data_fim": n(payload.get("data_fim")),
    }
    filters = {k: v for k, v in filters.items() if v is not None and str(v).strip() != ""}

    limit_raw = payload.get("limit", 500)
    offset_raw = payload.get("offset", 0)
    try:
        limit = int(limit_raw)
    except Exception:
        limit = 500
    try:
        offset = int(offset_raw)
    except Exception:
        offset = 0

    meta = {
        "limit": max(1, limit),
        "offset": max(0, offset),
        "order_by": n(payload.get("order_by")) or "data_disparo",
        "order_dir": n(payload.get("order_dir")) or "ASC",
    }
    return filters, meta


def _query_leads_in_batches(filters: Dict[str, Any], meta: Dict[str, Any], batch_size: int | None = None):
    limit = max(1, int(meta.get("limit") or 500))
    offset = max(0, int(meta.get("offset") or 0))
    order_by = meta.get("order_by") or "data_inscricao"
    order_dir = meta.get("order_dir") or "DESC"

    remaining = limit
    current_offset = offset
    all_rows = []

    effective_batch_size = batch_size if batch_size and batch_size > 0 else limit

    while remaining > 0:
        chunk_size = min(effective_batch_size, remaining)
        chunk_rows = query_leads(
            filters=filters,
            limit=chunk_size,
            offset=current_offset,
            order_by=order_by,
            order_dir=order_dir,
        )
        if not chunk_rows:
            break
        all_rows.extend(chunk_rows)
        fetched = len(chunk_rows)
        current_offset += fetched
        remaining -= fetched
        if fetched < chunk_size:
            break

    return all_rows


def _stream_leads_json(filters: Dict[str, Any], meta: Dict[str, Any], total: int):
    limit = max(1, int(meta.get("limit") or 500))
    offset = max(0, int(meta.get("offset") or 0))
    order_by = meta.get("order_by") or "data_inscricao"
    order_dir = meta.get("order_dir") or "DESC"

    @stream_with_context
    def generate():
        started = perf_counter()
        yield f'{{"ok":true,"total":{int(total)},"data":['
        first = True
        emitted = 0
        for row in query_leads_iter(
            filters=filters,
            limit=limit,
            offset=offset,
            order_by=order_by,
            order_dir=order_dir,
        ):
            if not first:
                yield ","
            else:
                first = False
            yield json.dumps(row, default=str, ensure_ascii=False)
            emitted += 1
        yield "]}"
        logger.info(
            "stream leads response completed total_sent=%s elapsed=%.2fs limit=%s offset=%s",
            emitted,
            perf_counter() - started,
            limit,
            offset,
        )

    return Response(generate(), mimetype="application/json")
 
def _error_payload(e: Exception, public_msg: str):
    return build_error_payload(
        e,
        public_message=public_msg,
        phase="request",
        include_trace=False,
    )
 
def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


# ============================================================
# Upload parsing: CSV/XLSX robusto (separador, encoding)
# FIX v4.2: usa chardet para detectar encoding real do arquivo
#           antes de passar pro pandas, evitando leitura errada
#           de arquivos Latin-1 como UTF-8 (causa nomes corrompidos
#           tipo "Ã‰ryk" em vez de "Éryk")
# FIX v4.1: força cpf/celular como str ANTES do pandas inferir float64,
#           evitando corrupção de números longos
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


def _detect_encoding(raw_bytes: bytes) -> str:
    """
    Detecta o encoding real do arquivo usando chardet.
    Retorna 'utf-8' como fallback se chardet não estiver disponível
    ou não conseguir detectar com confiança.
    """
    try:
        import chardet
        detected = chardet.detect(raw_bytes)
        encoding = detected.get("encoding") or "utf-8"
        confidence = detected.get("confidence") or 0
        # só usa o encoding detectado se a confiança for razoável
        if confidence >= 0.7:
            # normaliza alguns aliases comuns
            encoding = encoding.lower()
            if encoding in ("ascii",):
                return "utf-8"  # ASCII é subconjunto de UTF-8
            return encoding
        return "utf-8"
    except ImportError:
        return "utf-8"


def _read_upload_to_df(file_storage):
    import pandas as pd
    filename = (file_storage.filename or "").lower().strip()
    raw = file_storage.read()
 
    # ------------------------------------------------------------------
    # XLSX — encoding não se aplica, openpyxl lida internamente
    # ------------------------------------------------------------------
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        # 1ª leitura: só cabeçalho para descobrir nomes das colunas
        preview = pd.read_excel(io.BytesIO(raw), nrows=0)
        dtype_map = _dtype_map_for_phoneish(preview.columns)
        # 2ª leitura: com dtype forçado nas colunas phoneish
        return pd.read_excel(io.BytesIO(raw), dtype=dtype_map)
 
    # ------------------------------------------------------------------
    # CSV — detecta encoding correto antes de tentar ler
    # ------------------------------------------------------------------
    detected_enc = _detect_encoding(raw)

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

    # tenta o encoding detectado primeiro, depois fallbacks em ordem segura
    # utf-8-sig cobre arquivos CSV exportados pelo Excel (com BOM)
    encodings_to_try = []
    for enc in (detected_enc, "utf-8-sig", "utf-8", "latin-1"):
        if enc not in encodings_to_try:
            encodings_to_try.append(enc)

    best_df = None
    for sep in (";", ","):
        for enc in encodings_to_try:
            try:
                df = _read_csv(raw, sep, enc)
                if len(df.columns) > 1:
                    return df
                if best_df is None:
                    best_df = df
            except Exception:
                logger.debug("Tentativa de leitura CSV falhou: sep=%s encoding=%s", sep, enc, exc_info=True)
                continue

    if best_df is not None:
        return best_df
 
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
    session_ttl_seconds = env_int("SESSION_TTL_SECONDS", 28800, minimum=60)
    cookie_secure = env_bool("COOKIE_SECURE", False)
    session_cookie_name = _env("SESSION_COOKIE_NAME", "painel_session")

    app.config.update(
        SESSION_COOKIE_NAME=session_cookie_name,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SECURE=cookie_secure,
        SESSION_COOKIE_SAMESITE="Lax",
        PERMANENT_SESSION_LIFETIME=session_ttl_seconds,
    )

    # Fallback local apenas para desenvolvimento/testes quando o BigQuery não estiver disponível.
    users = {
        "matheus": "pbkdf2:sha256:1000000$Ij5ppE2yYdLvAKlF$0d441b0096771e07525df01b224faf57cabedc83b444375cad21e44f9d6b5282",
        "miguel": "pbkdf2:sha256:1000000$rxyvycWVM3tJCDF0$36a69c69fd09385c4e39fd2c67549f60c9dccc1a68f7a56d0f8e8f00716fc49d",
    }

    PROFILE_PERMISSIONS = {
        "ADMIN": {"dashboard:view", "usuarios:manage", "leads:import", "lotes:create", "lotes:export", "lotes:mark_sent", "retorno:import", "lote_atual:edit", "lotes:finish", "lotes:cancel", "logs:view", "lotes:view", "rastreabilidade:view", "meus_leads:view", "lead_status:edit"},
        "GESTOR": {"dashboard:view", "leads:import", "lotes:create", "lotes:export", "lotes:mark_sent", "retorno:import", "lote_atual:edit", "lotes:finish", "lotes:cancel", "logs:view", "lotes:view", "rastreabilidade:view", "meus_leads:view", "lead_status:edit"},
        "OPERADOR": {"dashboard:view", "meus_leads:view", "lote_atual:view", "lote_atual:edit", "lead_status:edit", "retorno:import"},
        "LEITURA": {"dashboard:view", "lotes:view", "rastreabilidade:view", "logs:view"},
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

    def _current_user_context() -> dict | None:
        email = session.get("email") or session.get("username")
        if not email:
            return None
        perfil = str(session.get("nome_perfil") or session.get("perfil") or ("ADMIN" if str(email).lower() in users else "LEITURA")).upper()
        permissions = session.get("permissions") or sorted(PROFILE_PERMISSIONS.get(perfil, PROFILE_PERMISSIONS["LEITURA"]))
        session.permanent = True
        session.modified = True
        return {
            "usuario_id": session.get("usuario_id") or str(email),
            "nome": session.get("nome") or str(email),
            "email": str(email),
            "perfil_id": session.get("perfil_id") or perfil,
            "nome_perfil": perfil,
            "permissions": list(permissions),
        }

    def _current_user() -> str | None:
        user = _current_user_context()
        return user["email"] if user else None

    def _has_permission(permission: str) -> bool:
        user = getattr(g, "user", None) or _current_user_context()
        if not user:
            return False
        return user.get("nome_perfil") == "ADMIN" or permission in set(user.get("permissions") or [])

    def _permission_denied(permission: str):
        return jsonify({"ok": False, "success": False, "error": {"code": "FORBIDDEN", "message": "Você não tem permissão para executar esta ação.", "permission": permission}}), 403

    def _require_permission(permission: str):
        if not _has_permission(permission):
            return _permission_denied(permission)
        return None

    def _destroy_session():
        session.clear()

    @app.before_request
    def attach_correlation_id():
        g.correlation_id = request.headers.get("X-Correlation-ID") or _new_correlation_id()

    @app.before_request
    def _auth_guard():
        if _is_public_path(request.path):
            return None

        user = _current_user_context()
        g.user = user
        g.current_user = user.get("email") if user else None
        if user:
            return None

        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "error": "Sessão expirada. Faça login novamente.", "redirect_to": "/login"}), 401
        return redirect(url_for("login"))

    @app.get("/")
    def index():
        return render_template(
            "index.html",
            asset_version=asset_version,
            ui_version=ui_version,
            current_user=getattr(g, "current_user", None),
        )

    @app.get("/gestao/exportar-prioritarios")
    def gestao_exportar_prioritarios():
        try:
            limit = int(request.args.get("limit", "500"))
        except (TypeError, ValueError):
            limit = 500
        filename, content, rows_count = gestao_export_fila({}, {"limit": limit, "offset": 0})
        logger.info(
            "Gestão exportação prioritários solicitada user=%s limit=%s rows=%s filename=%s",
            getattr(g, "current_user", None),
            limit,
            rows_count,
            filename,
        )
        response = make_response(content)
        response.headers["Content-Type"] = "text/csv; charset=utf-8-sig"
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    @app.get("/gestao")
    def gestao():
        # A página é carregada rapidamente; os dados vêm dos endpoints JSON protegidos.
        return render_template(
            "gestao.html",
            asset_version=asset_version,
            ui_version=ui_version,
            current_user=getattr(g, "current_user", None),
            current_user_context=getattr(g, "user", None),
            gestao_data=None,
            gestao_error=None,
        )

    @app.get("/gestao/usuarios")
    def gestao_usuarios_page():
        return render_template(
            "gestao.html",
            asset_version=asset_version,
            ui_version=ui_version,
            current_user=getattr(g, "current_user", None),
            current_user_context=getattr(g, "user", None),
            gestao_data=None,
            gestao_error=None,
        )

    def _real_user_email() -> str:
        return str(getattr(g, "current_user", None) or session.get("email") or session.get("username") or "sistema")

    @app.before_request
    def _permission_guard():
        if _is_public_path(request.path) or not getattr(g, "current_user", None):
            return None
        checks = [
            ("/api/gestao/usuarios", "usuarios:manage"),
            ("/api/gestao/perfis", "usuarios:manage"),
            ("/api/gestao/operacional/logs", "logs:view"),
        ]
        mutating_checks = [
            ("/api/gestao/lotes/criar-exportar", "lotes:create"),
            ("/api/gestao/operacional/exportar-proximo-lote", "lotes:export"),
            ("/api/gestao/operacional/lotes", "lotes:create"),
            ("/api/gestao/operacional/importar-lote-disparado", "retorno:import"),
            ("/api/gestao/operacional/importar-novos-leads", "leads:import"),
        ]
        for prefix, permission in checks:
            if request.path.startswith(prefix):
                return _require_permission(permission)
        if request.method != "GET":
            if "/marcar-disparado" in request.path:
                return _require_permission("lotes:mark_sent")
            if request.path.endswith("/cancel"):
                return _require_permission("lotes:cancel")
            if request.path.endswith("/finish"):
                return _require_permission("lotes:finish")
            if "/leads/" in request.path and (request.path.endswith("/status") or request.path.endswith("/atualizar")):
                return _require_permission("lead_status:edit")
            if request.path.endswith("/importar-retorno"):
                return _require_permission("retorno:import")
            for prefix, permission in mutating_checks:
                if request.path.startswith(prefix):
                    return _require_permission(permission)
        return None

    def _gestao_success(data, filters, *, cached=False, status=200):
        return jsonify({
            "ok": True,
            "data": data,
            "meta": {
                "generated_at": gestao_utc_now_iso(),
                "filters": filters,
                "cached": bool(cached),
            },
        }), status

    def _safe_request_params():
        params = request.get_json(silent=True) if request.is_json else request.args.to_dict(flat=False)
        if isinstance(params, dict):
            masked = dict(params)
            for key in list(masked):
                if key.lower() in {"senha", "password", "senha_temporaria", "password_hash", "cpf", "celular", "email"}:
                    masked[key] = "***"
            return masked
        return {}

    def _gestao_error_response(exc, *, status=500, code="GESTAO_QUERY_ERROR", message="Não foi possível carregar os dados."):
        bq_error = gestao_op_classify_bigquery_error(exc)
        response_status = status
        if bq_error["error_type"] == "BIGQUERY_PERMISSION_ERROR":
            response_status = 403
        elif bq_error["error_type"] == "BIGQUERY_SCHEMA_ERROR":
            response_status = 500
        elif bq_error["error_type"] == "BIGQUERY_INVALID_REQUEST":
            response_status = 400
        elif bq_error["error_type"] == "BIGQUERY_TIMEOUT":
            response_status = 504
        friendly = message or bq_error["message"]
        logger.exception(
            "gestao_api_error route=%s endpoint=%s params=%s user=%s exception_type=%s bigquery_error_type=%s bigquery_full_error=%r",
            request.path,
            f"{request.method} {request.endpoint}",
            _safe_request_params(),
            getattr(g, "current_user", None) or session.get("user_email"),
            exc.__class__.__name__,
            bq_error["error_type"],
            exc,
        )
        payload = {
            "ok": False, "success": False,
            "error_type": bq_error["error_type"],
            "message": friendly,
            "details": bq_error["details"],
            "error": {"code": bq_error["error_type"] or code, "message": friendly, "details": bq_error["details"], "correlationId": getattr(g, "correlation_id", None)},
        }
        return jsonify(payload), response_status

    def _gestao_endpoint(loader):
        try:
            filters, meta = gestao_parse_filters(request.args)
            data, cached = loader(filters, meta)
            return _gestao_success(data, filters, cached=cached)
        except GestaoValidationError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_INVALID_FILTER", "message": str(exc)}}), 400
        except TimeoutError as exc:
            return _gestao_error_response(exc, status=504, code="GESTAO_TIMEOUT", message="Tempo esgotado ao consultar os dados.")
        except Exception as exc:
            return _gestao_error_response(exc)

    @app.get("/api/gestao/resumo")
    def api_gestao_resumo():
        return _gestao_endpoint(gestao_get_resumo)

    @app.get("/api/gestao/funil")
    def api_gestao_funil():
        return _gestao_endpoint(gestao_get_funil)

    @app.get("/api/gestao/evolucao")
    def api_gestao_evolucao():
        return _gestao_endpoint(gestao_get_evolucao)

    @app.get("/api/gestao/rankings")
    def api_gestao_rankings():
        return _gestao_endpoint(gestao_get_rankings)

    @app.get("/api/gestao/produtividade")
    def api_gestao_produtividade():
        return _gestao_endpoint(gestao_get_produtividade)

    @app.get("/api/gestao/fila")
    def api_gestao_fila():
        try:
            filters, meta = gestao_parse_filters(request.args)
            data, cached = gestao_get_fila(filters, meta)
            return _gestao_success(data, filters, cached=cached)
        except GestaoValidationError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_INVALID_FILTER", "message": str(exc)}}), 400
        except TimeoutError as exc:
            return _gestao_error_response(exc, status=504, code="GESTAO_FILA_QUERY_ERROR", message="Não foi possível carregar a fila operacional.")
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_FILA_QUERY_ERROR", message="Não foi possível carregar a fila operacional.")

    @app.get("/api/gestao/qualidade-dados")
    def api_gestao_qualidade_dados():
        try:
            data, _cached = gestao_get_qualidade_dados({}, {})
            return jsonify({"success": True, "data": data})
        except NotFound as exc:
            return _gestao_error_response(exc, status=404, code="BIGQUERY_OBJECT_NOT_FOUND", message="Objeto BigQuery de qualidade dos dados não encontrado.")
        except Forbidden as exc:
            return _gestao_error_response(exc, status=403, code="BIGQUERY_PERMISSION_DENIED", message="Sem permissão para consultar a qualidade dos dados.")
        except TimeoutError as exc:
            return _gestao_error_response(exc, status=504, code="BIGQUERY_TIMEOUT", message="Tempo esgotado ao consultar a qualidade dos dados.")
        except Exception as exc:
            return _gestao_error_response(exc, code="QUALIDADE_DADOS_ERROR", message="Não foi possível carregar a qualidade dos dados.")

    @app.get("/api/gestao/qualidade")
    def api_gestao_qualidade():
        return _gestao_endpoint(gestao_get_qualidade)

    @app.get("/api/importacoes/historico")
    def api_importacoes_historico():
        try:
            filters, meta = gestao_parse_import_history_request(request.args)
            data, _cached = gestao_get_importacoes(filters, meta)
            return jsonify({"success": True, "data": data.get("items", []), "pagination": data.get("pagination", {})})
        except GestaoValidationError as exc:
            return jsonify({"success": False, "error": {"code": "IMPORTACOES_INVALID_FILTER", "message": str(exc), "correlationId": getattr(g, "correlation_id", None)}}), 400
        except NotFound as exc:
            return _gestao_error_response(exc, status=404, code="BIGQUERY_OBJECT_NOT_FOUND", message="Objeto BigQuery de histórico de importações não encontrado.")
        except Forbidden as exc:
            return _gestao_error_response(exc, status=403, code="BIGQUERY_PERMISSION_DENIED", message="Sem permissão para consultar o histórico de importações.")
        except TimeoutError as exc:
            return _gestao_error_response(exc, status=504, code="BIGQUERY_TIMEOUT", message="Tempo esgotado ao consultar o histórico de importações.")
        except Exception as exc:
            return _gestao_error_response(exc, code="IMPORTACOES_HISTORICO_ERROR", message="Não foi possível carregar o histórico de importações.")

    @app.get("/api/gestao/importacoes")
    def api_gestao_importacoes():
        try:
            filters, meta = gestao_parse_import_history_request(request.args)
            data, cached = gestao_get_importacoes(filters, meta)
            return _gestao_success(data, filters, cached=cached)
        except GestaoValidationError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_INVALID_FILTER", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="IMPORTACOES_HISTORICO_ERROR", message="Não foi possível carregar o histórico de importações.")

    def _gestao_csv_response(exporter, *args):
        try:
            filters, meta = gestao_parse_filters(request.args)
            filename, content, rows_count = exporter(filters, meta, *args)
            logger.info(
                "gestao_export operation=%s route=%s result_count=%s page=%s page_size=%s filter_names=%s",
                getattr(exporter, "__name__", "export"),
                request.path,
                rows_count,
                1,
                meta.get("limit"),
                sorted(filters.keys()),
            )
            response = make_response(content)
            response.headers["Content-Type"] = "text/csv; charset=utf-8-sig"
            response.headers["Content-Disposition"] = f"attachment; filename={filename}"
            return response
        except GestaoValidationError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_INVALID_FILTER", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc)

    @app.get("/api/gestao/qualidade/detalhes")
    def api_gestao_qualidade_detalhes():
        tipo = (request.args.get("tipo") or "").strip()
        def _loader(filters, meta):
            return gestao_get_qualidade_detalhes(filters, meta, tipo)
        return _gestao_endpoint(_loader)

    @app.get("/api/gestao/qualidade/exportar")
    def api_gestao_qualidade_exportar():
        tipo = (request.args.get("tipo") or "").strip()
        return _gestao_csv_response(gestao_export_qualidade, tipo)

    @app.get("/api/importacoes/historico/exportar")
    def api_importacoes_historico_exportar():
        try:
            filters, meta = gestao_parse_import_history_request(request.args)
            filename, content, rows_count = gestao_export_importacoes(filters, meta)
            logger.info("gestao_export operation=export_importacoes route=%s result_count=%s", request.path, rows_count)
            response = make_response(content)
            response.headers["Content-Type"] = "text/csv; charset=utf-8"
            response.headers["Content-Disposition"] = f"attachment; filename={filename}"
            return response
        except GestaoValidationError as exc:
            return jsonify({"success": False, "error": {"code": "IMPORTACOES_INVALID_FILTER", "message": str(exc), "correlationId": getattr(g, "correlation_id", None)}}), 400
        except NotFound as exc:
            return _gestao_error_response(exc, status=404, code="BIGQUERY_OBJECT_NOT_FOUND", message="Objeto BigQuery de exportação do histórico não encontrado.")
        except Forbidden as exc:
            return _gestao_error_response(exc, status=403, code="BIGQUERY_PERMISSION_DENIED", message="Sem permissão para exportar o histórico de importações.")
        except Exception as exc:
            return _gestao_error_response(exc, code="IMPORTACOES_EXPORT_ERROR", message="Não foi possível exportar o histórico de importações.")

    @app.get("/api/gestao/importacoes/exportar")
    def api_gestao_importacoes_exportar():
        try:
            filters, meta = gestao_parse_import_history_request(request.args)
            filename, content, rows_count = gestao_export_importacoes(filters, meta)
            logger.info("gestao_export operation=export_importacoes route=%s result_count=%s", request.path, rows_count)
            response = make_response(content)
            response.headers["Content-Type"] = "text/csv; charset=utf-8-sig"
            response.headers["Content-Disposition"] = f"attachment; filename={filename}"
            return response
        except GestaoValidationError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_INVALID_FILTER", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="IMPORTACOES_EXPORT_ERROR", message="Não foi possível exportar o histórico de importações.")

    @app.get("/api/gestao/fila/exportar")
    def api_gestao_fila_exportar():
        return _gestao_csv_response(gestao_export_fila)

    @app.get("/api/gestao/produtividade/exportar")
    def api_gestao_produtividade_exportar():
        return _gestao_csv_response(gestao_export_produtividade)

    @app.get("/api/gestao/rejeicoes")
    def api_gestao_rejeicoes():
        return _gestao_endpoint(gestao_get_rejeicoes)

    @app.get("/api/gestao/rejeicoes/exportar")
    def api_gestao_rejeicoes_exportar():
        return _gestao_csv_response(gestao_export_rejeicoes)

    @app.get("/api/gestao/opcoes")
    def api_gestao_opcoes():
        return _gestao_endpoint(gestao_get_opcoes)


    @app.post("/api/gestao/lotes/criar-exportar")
    def api_gestao_lotes_criar_exportar():
        try:
            payload = request.get_json(silent=True) or {}
            payload["usuario"] = _real_user_email()
            payload["criado_por"] = _real_user_email()
            data, cached = gestao_op_criar_lote(payload)
            return jsonify({**data, "success": True, "ok": True, "cached": bool(cached)}), 201
        except ValueError as exc:
            return jsonify({"success": False, "ok": False, "error": {"code": "GESTAO_LOTE_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_LOTE_CRIAR_EXPORTAR_ERROR", message="Não foi possível gerar o lote pela procedure oficial.")


    @app.post("/api/gestao/lotes/<lote_id>/marcar-disparado")
    def api_gestao_lote_marcar_disparado(lote_id):
        try:
            payload = request.get_json(silent=True) or {}
            usuario = _real_user_email()
            data, cached = gestao_op_marcar_lote_disparado(lote_id, usuario)
            return _gestao_success(data, {"lote_id": lote_id}, cached=cached)
        except ValueError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_LOTE_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_LOTE_MARCAR_DISPARADO_ERROR", message="Não foi possível marcar o lote como disparado.")

    @app.get("/api/gestao/lotes/<lote_id>/csv")
    def api_gestao_lote_csv(lote_id):
        try:
            filename, content, rows_count = gestao_op_get_lote_csv(lote_id)
            logger.info(
                "gestao_lote_csv lote_id=%s rows=%s filename=%s user=%s",
                lote_id,
                rows_count,
                filename,
                getattr(g, "current_user", None),
            )
            response = make_response(content)
            response.headers["Content-Type"] = "text/csv; charset=utf-8-sig"
            response.headers["Content-Disposition"] = f"attachment; filename={filename}"
            return response
        except ValueError as exc:
            return jsonify({"success": False, "ok": False, "error": {"code": "GESTAO_LOTE_CSV_INVALID", "message": str(exc)}}), 404
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_LOTE_CSV_ERROR", message="Não foi possível baixar o CSV do lote.")


    def _gestao_operacional_endpoint(loader):
        try:
            filters, meta = gestao_op_parse_request(request.args)
            user_ctx = getattr(g, "user", None) or _current_user_context() or {}
            filters.setdefault("usuario", user_ctx.get("nome") or user_ctx.get("email") or _real_user_email())
            filters.setdefault("current_user", user_ctx.get("nome") or user_ctx.get("email") or _real_user_email())
            filters.setdefault("current_user_email", user_ctx.get("email") or _real_user_email())
            filters.setdefault("current_user_profile", user_ctx.get("nome_perfil") or user_ctx.get("perfil_id") or "")
            data, cached = loader(filters, meta)
            return _gestao_success(data, filters, cached=cached)
        except ValueError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": str(exc)}}), 400
        except TimeoutError as exc:
            return _gestao_error_response(exc, status=504, code="GESTAO_OPERACIONAL_TIMEOUT", message="Tempo esgotado ao consultar a operação de disparos.")
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_ERROR", message="Não foi possível carregar a operação de disparos.")

    @app.get("/api/gestao/operacional/dashboard")
    def api_gestao_operacional_dashboard():
        try:
            data, cached = gestao_op_get_dashboard()
            return _gestao_success(data, {}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_DASHBOARD_ERROR", message="Não foi possível carregar o dashboard operacional.")



    @app.get("/api/gestao/operacional/lotes-select")
    def api_gestao_operacional_lotes_select():
        try:
            data, cached = gestao_op_get_lotes_select()
            return _gestao_success(data, {}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_LOTES_SELECT_ERROR", message="Não foi possível carregar o seletor de lotes.")

    @app.get("/api/gestao/operacional/preview-proximo-lote")
    def api_gestao_operacional_preview_proximo_lote():
        try:
            data, cached = gestao_op_preview_proximo_lote(request.args)
            return jsonify({"ok": True, **data})
        except ValueError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_PREVIEW_ERROR", message="Não foi possível pré-visualizar o próximo lote.")

    @app.post("/api/gestao/operacional/exportar-proximo-lote")
    def api_gestao_operacional_exportar_proximo_lote():
        try:
            data, cached = gestao_op_exportar_proximo_lote(request.get_json(silent=True) or {})
            if data.get("base64"):
                raw = __import__('base64').b64decode(data["base64"])
                return send_file(io.BytesIO(raw), mimetype=data.get("content_type") or "text/csv", as_attachment=True, download_name=data.get("filename") or "lote.csv")
            return _gestao_success(data, {}, cached=cached), 201
        except ValueError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_EXPORTAR_ERROR", message="Não foi possível exportar o próximo lote.")

    @app.post("/api/gestao/operacional/importar-lote-disparado")
    def api_gestao_operacional_importar_lote_disparado():
        try:
            file = request.files.get("file")
            if not file:
                return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": "Arquivo é obrigatório."}}), 400
            data, cached = gestao_op_importar_lote_disparado(file, request.form.get("lote_id", ""), _real_user_email())
            return jsonify({"ok": True, **data})
        except ValueError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_IMPORTAR_LOTE_ERROR", message="Não foi possível importar o lote disparado.")

    @app.post("/api/gestao/operacional/importar-novos-leads")
    def api_gestao_operacional_importar_novos_leads():
        try:
            file = request.files.get("file")
            if not file:
                return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": "Arquivo é obrigatório. Use /api/upload para a carga oficial."}}), 400
            data, cached = gestao_op_importar_novos_leads(file, request.form)
            return jsonify({"ok": True, **data})
        except ValueError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_IMPORTAR_NOVOS_ERROR", message="Não foi possível validar a importação de novos leads.")

    @app.get("/api/gestao/operacional/fila-leads")
    def api_gestao_operacional_fila_leads():
        return _gestao_operacional_endpoint(gestao_op_get_leads_disponiveis)

    @app.get("/api/gestao/operacional/consultores")
    def api_gestao_operacional_consultores():
        return _gestao_operacional_endpoint(gestao_op_get_consultor_momento)

    @app.get("/api/gestao/operacional/lote-atual/leads")
    def api_gestao_operacional_lote_atual_leads():
        return _gestao_operacional_endpoint(gestao_op_get_lote_atual_leads)

    @app.post("/api/gestao/lotes/<lote_id>/leads/<sk_pessoa>/atualizar")
    def api_gestao_lote_lead_atualizar(lote_id, sk_pessoa):
        try:
            payload = request.get_json(silent=True) or {}
            usuario = _real_user_email()
            data, cached = gestao_op_atualizar_lead_lote(lote_id, sk_pessoa, payload, usuario)
            return _gestao_success(data, {"lote_id": lote_id, "sk_pessoa": sk_pessoa}, cached=cached)
        except ValueError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_ATUALIZAR_LEAD_ERROR", message="Não foi possível atualizar o lead no lote.")

    @app.get("/api/gestao/operacional/logs")
    def api_gestao_operacional_logs():
        try:
            data, cached = gestao_op_get_logs(request.args)
            return _gestao_success(data, dict(request.args), cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_LOGS_ERROR", message="Não foi possível carregar histórico e logs.")


    def _gestao_logs_endpoint(kind):
        denied = _require_permission("logs:view")
        if denied:
            return denied
        try:
            data, cached = gestao_op_get_logs_auditoria(kind, request.args, getattr(g, "user", None) or _current_user_context() or {})
            return jsonify(data)
        except ValueError as exc:
            return jsonify({"success": False, "ok": False, "error": {"code": "GESTAO_LOGS_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_LOGS_ERROR", message="Não foi possível carregar os logs solicitados.")

    @app.get("/api/gestao/logs/importacoes")
    def api_gestao_logs_importacoes():
        return _gestao_logs_endpoint("importacoes")

    @app.get("/api/gestao/logs/rejeicoes")
    def api_gestao_logs_rejeicoes():
        return _gestao_logs_endpoint("rejeicoes")

    @app.get("/api/gestao/logs/auditoria")
    def api_gestao_logs_auditoria():
        return _gestao_logs_endpoint("auditoria")

    @app.get("/api/gestao/logs/eventos-leads")
    def api_gestao_logs_eventos_leads():
        return _gestao_logs_endpoint("eventos_leads")

    @app.get("/api/gestao/logs/timeline")
    def api_gestao_logs_timeline():
        return _gestao_logs_endpoint("timeline")

    @app.get("/api/gestao/logs/debug-fila")
    def api_gestao_logs_debug_fila():
        return _gestao_logs_endpoint("debug_fila")

    @app.get("/api/gestao/logs/bigquery-sync")
    def api_gestao_logs_bigquery_sync():
        return _gestao_logs_endpoint("bigquery_sync")

    @app.post("/api/gestao/operacional/liberar-proximos-leads")
    def api_gestao_operacional_liberar_proximos_leads():
        try:
            data, cached = gestao_op_liberar_proximos_leads(request.get_json(silent=True) or {})
            return _gestao_success(data, {}, cached=cached), 201
        except ValueError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_LIBERAR_ERROR", message="Não foi possível liberar os próximos leads.")

    @app.post("/api/gestao/operacional/executar-regras-distribuicao")
    def api_gestao_operacional_executar_regras():
        try:
            data, cached = gestao_op_executar_regras_distribuicao()
            return _gestao_success(data, {}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_REGRAS_EXEC_ERROR", message="Não foi possível executar as regras automáticas.")

    @app.get("/api/gestao/operacional/esteira")
    def api_gestao_operacional_esteira():
        try:
            data, cached = gestao_op_get_esteira()
            return _gestao_success(data, {}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_ESTEIRA_ERROR", message="Não foi possível carregar a esteira operacional.")

    @app.get("/api/gestao/operacional/fila-prioridade")
    def api_gestao_operacional_fila_prioridade():
        return _gestao_operacional_endpoint(gestao_op_get_fila_prioridade)

    @app.get("/api/gestao/operacional/regras-distribuicao")
    def api_gestao_operacional_regras_listar():
        try:
            data, cached = gestao_op_listar_regras()
            return _gestao_success(data, {}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_REGRAS_ERROR", message="Não foi possível listar as regras.")

    @app.post("/api/gestao/operacional/regras-distribuicao")
    def api_gestao_operacional_regras_criar():
        try:
            data, cached = gestao_op_criar_regra(request.get_json(silent=True) or {})
            return _gestao_success(data, {}, cached=cached), 201
        except ValueError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_REGRAS_CREATE_ERROR", message="Não foi possível criar a regra.")

    @app.patch("/api/gestao/operacional/regras-distribuicao/<regra_id>")
    def api_gestao_operacional_regras_toggle(regra_id):
        try:
            data, cached = gestao_op_toggle_regra(regra_id, request.get_json(silent=True) or {})
            return _gestao_success(data, {"regra_id": regra_id}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_REGRAS_TOGGLE_ERROR", message="Não foi possível atualizar a regra.")

    @app.get("/api/gestao/operacional/leads-disponiveis")
    def api_gestao_operacional_leads_disponiveis():
        return _gestao_operacional_endpoint(gestao_op_get_leads_disponiveis)

    @app.post("/api/gestao/operacional/lotes")
    def api_gestao_operacional_criar_lote():
        try:
            payload = request.get_json(silent=True) or {}
            payload["usuario"] = _real_user_email()
            payload["criado_por"] = _real_user_email()
            data, cached = gestao_op_criar_lote(payload)
            return _gestao_success(data, {}, cached=cached), 201
        except ValueError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_CREATE_ERROR", message="Não foi possível criar o lote.")

    @app.get("/api/gestao/operacional/lotes")
    def api_gestao_operacional_lotes():
        return _gestao_operacional_endpoint(gestao_op_get_lotes)

    @app.get("/api/gestao/operacional/lotes/<lote_id>")
    def api_gestao_operacional_lote_detalhe(lote_id):
        try:
            data, cached = gestao_op_get_lote_detalhe(lote_id)
            return _gestao_success(data, {"lote_id": lote_id}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_DETAIL_ERROR", message="Não foi possível carregar o lote.")

    @app.post("/api/gestao/operacional/lotes/<lote_id>/start")
    def api_gestao_operacional_lote_start(lote_id):
        try:
            data, cached = gestao_op_start_lote(lote_id)
            return _gestao_success(data, {"lote_id": lote_id}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_START_ERROR", message="Não foi possível iniciar o lote.")


    @app.post("/api/gestao/operacional/lotes/<lote_id>/cancel")
    def api_gestao_operacional_lote_cancel(lote_id):
        try:
            data, cached = gestao_op_cancelar_lote(lote_id)
            return _gestao_success(data, {"lote_id": lote_id}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_CANCEL_ERROR", message="Não foi possível cancelar o lote.")

    @app.post("/api/gestao/operacional/lotes/<lote_id>/finish")
    def api_gestao_operacional_lote_finish(lote_id):
        try:
            payload = request.get_json(silent=True) or {}
            try:
                data, cached = gestao_op_finish_lote(lote_id, payload)
            except TypeError:
                data, cached = gestao_op_finish_lote(lote_id)
            return _gestao_success(data, {"lote_id": lote_id}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_FINISH_ERROR", message="Não foi possível finalizar o lote.")

    @app.get("/api/gestao/operacional/meus-leads")
    def api_gestao_operacional_meus_leads():
        try:
            filters, meta = gestao_op_parse_request(request.args)
            consultor = request.args.get("consultor_disparo") or request.args.get("consultor") or ""
            data, cached = gestao_op_get_meus_leads(consultor, filters, meta)
            return _gestao_success(data, filters, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_MEUS_LEADS_ERROR", message="Não foi possível carregar os leads do consultor.")

    @app.patch("/api/gestao/operacional/leads/<int:sk_pessoa>/status")
    def api_gestao_operacional_lead_status(sk_pessoa):
        try:
            payload = request.get_json(silent=True) or {}
            payload["usuario"] = _real_user_email()
            data, cached = gestao_op_update_lead_status(sk_pessoa, payload)
            return _gestao_success(data, {"sk_pessoa": sk_pessoa}, cached=cached)
        except ValueError as exc:
            return jsonify({"ok": False, "error": {"code": "GESTAO_OPERACIONAL_INVALID", "message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_STATUS_ERROR", message="Não foi possível atualizar o status do lead.")


    @app.post("/api/gestao/lotes/<lote_id>/importar-retorno")
    def api_gestao_lote_importar_retorno(lote_id):
        try:
            file = request.files.get("file")
            if not file or not _validate_upload_filename(file.filename):
                return jsonify({"success": False, "ok": False, "error": {"message": "Envie um arquivo CSV/XLSX válido."}}), 400
            usuario = _real_user_email()
            data, _cached = gestao_op_importar_retorno_lote(file, lote_id, usuario)
            return jsonify({**data, "ok": True})
        except ValueError as exc:
            return jsonify({"success": False, "ok": False, "error": {"message": str(exc)}}), 400
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_RETORNO_IMPORT_ERROR", message="Não foi possível importar o retorno do lote.")

    @app.get("/api/gestao/leads/buscar")
    def api_gestao_leads_buscar():
        try:
            data, cached = gestao_op_buscar_leads(request.args.get("q", ""), int(request.args.get("limit", 20)))
            return _gestao_success(data, {"q": request.args.get("q", "")}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_LEADS_BUSCAR_ERROR", message="Não foi possível buscar leads.")

    @app.get("/api/gestao/leads/<sk_pessoa>/timeline")
    def api_gestao_lead_timeline(sk_pessoa):
        try:
            data, cached = gestao_op_get_lead_timeline(sk_pessoa)
            return _gestao_success(data, {"sk_pessoa": sk_pessoa}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_LEAD_TIMELINE_ERROR", message="Não foi possível carregar a timeline.")

    @app.get("/api/gestao/leads/<sk_pessoa>/lotes")
    def api_gestao_lead_lotes(sk_pessoa):
        try:
            data, cached = gestao_op_get_lead_lotes(sk_pessoa)
            return _gestao_success(data, {"sk_pessoa": sk_pessoa}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_LEAD_LOTES_ERROR", message="Não foi possível carregar os lotes do lead.")

    @app.get("/api/gestao/leads/<sk_pessoa>/eventos")
    def api_gestao_lead_eventos(sk_pessoa):
        try:
            data, cached = gestao_op_get_lead_eventos(sk_pessoa)
            return _gestao_success(data, {"sk_pessoa": sk_pessoa}, cached=cached)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_LEAD_EVENTOS_ERROR", message="Não foi possível carregar eventos técnicos.")

    def _is_admin_user() -> bool:
        return _has_permission("usuarios:manage")

    def _require_admin_json():
        return _require_permission("usuarios:manage")

    def _hash_password(password: str) -> str:
        return generate_password_hash(password)

    @app.get("/api/gestao/usuarios")
    def api_gestao_usuarios_listar():
        denied = _require_admin_json()
        if denied: return denied
        try:
            data, _ = gestao_op_listar_usuarios(); return jsonify(data)
        except Exception as exc:
            return _gestao_error_response(exc, code="USUARIOS_LISTAR_ERROR", message="Não foi possível listar usuários.")

    @app.post("/api/gestao/usuarios")
    def api_gestao_usuarios_criar():
        denied = _require_admin_json()
        if denied: return denied
        try:
            payload = request.get_json(silent=True) or {}
            raw_password = str(payload.get("senha_temporaria") or payload.get("senha") or payload.get("password") or "").strip()
            if not raw_password:
                raise ValueError("Senha inicial é obrigatória para criar usuário.")
            payload["password_hash"] = _hash_password(raw_password)
            data, _ = gestao_op_salvar_usuario(payload, getattr(g, "current_user", None) or "SISTEMA")
            return jsonify(data), 201
        except ValueError as exc:
            return jsonify({"success": False, "message": str(exc), "data": None}), 400
        except Exception as exc:
            logger.exception(
                "usuarios_criar_error endpoint=%s metodo=%s payload_sem_senha=%s usuario_logado=%s bigquery_full_error=%r",
                request.path,
                request.method,
                _safe_request_params(),
                getattr(g, "current_user", None) or session.get("user_email"),
                exc,
            )
            return _gestao_error_response(exc, code="USUARIOS_CRIAR_ERROR", message="Não foi possível salvar usuário.")

    @app.put("/api/gestao/usuarios/<usuario_id>")
    def api_gestao_usuarios_editar(usuario_id):
        denied = _require_admin_json()
        if denied: return denied
        try:
            payload = request.get_json(silent=True) or {}
            raw_password = str(payload.get("senha_temporaria") or payload.get("senha") or payload.get("password") or "").strip()
            if raw_password:
                payload["password_hash"] = _hash_password(raw_password)
            else:
                payload.pop("password_hash", None)
            data, _ = gestao_op_salvar_usuario(payload, getattr(g, "current_user", None) or "SISTEMA", usuario_id)
            return jsonify(data)
        except ValueError as exc:
            return jsonify({"success": False, "message": str(exc), "data": None}), 400
        except Exception as exc:
            logger.exception(
                "usuarios_editar_error endpoint=%s metodo=%s payload_sem_senha=%s usuario_logado=%s bigquery_full_error=%r",
                request.path,
                request.method,
                _safe_request_params(),
                getattr(g, "current_user", None) or session.get("user_email"),
                exc,
            )
            return _gestao_error_response(exc, code="USUARIOS_EDITAR_ERROR", message="Não foi possível salvar usuário.")

    @app.post("/api/gestao/usuarios/<usuario_id>/ativar")
    def api_gestao_usuarios_ativar(usuario_id):
        denied = _require_admin_json()
        if denied: return denied
        try:
            data, _ = gestao_op_alterar_status_usuario(usuario_id, True, getattr(g, "current_user", None) or "sistema")
            return jsonify(data)
        except Exception as exc:
            return _gestao_error_response(exc, code="USUARIOS_ATIVAR_ERROR", message="Não foi possível ativar usuário.")

    @app.post("/api/gestao/usuarios/<usuario_id>/desativar")
    def api_gestao_usuarios_desativar(usuario_id):
        denied = _require_admin_json()
        if denied: return denied
        try:
            data, _ = gestao_op_alterar_status_usuario(usuario_id, False, getattr(g, "current_user", None) or "sistema")
            return jsonify(data)
        except Exception as exc:
            return _gestao_error_response(exc, code="USUARIOS_DESATIVAR_ERROR", message="Não foi possível desativar usuário.")

    @app.post("/api/gestao/usuarios/<usuario_id>/resetar-senha")
    def api_gestao_usuarios_resetar(usuario_id):
        denied = _require_admin_json()
        if denied: return denied
        try:
            payload=request.get_json(silent=True) or {}; senha=str(payload.get("senha_temporaria") or payload.get("senha") or payload.get("password") or uuid.uuid4().hex[:10])
            data, _ = gestao_op_resetar_senha_usuario(usuario_id, _hash_password(senha), getattr(g, "current_user", None) or "sistema")
            return jsonify(data)
        except Exception as exc:
            return _gestao_error_response(exc, code="USUARIOS_RESETAR_SENHA_ERROR", message="Não foi possível resetar senha do usuário.")

    @app.get("/api/gestao/usuarios/<usuario_id>/auditoria")
    def api_gestao_usuarios_auditoria(usuario_id):
        denied = _require_admin_json()
        if denied: return denied
        try:
            data, _ = gestao_op_auditoria_usuario(usuario_id)
            return jsonify(data)
        except Exception as exc:
            return _gestao_error_response(exc, code="USUARIOS_AUDITORIA_ERROR", message="Não foi possível carregar auditoria do usuário.")

    @app.get("/api/gestao/perfis")
    def api_gestao_perfis():
        denied = _require_admin_json()
        if denied: return denied
        data, _ = gestao_op_listar_perfis(); return jsonify(data)

    @app.post("/api/gestao/operacional/admin/create-tables")
    def api_gestao_operacional_create_tables():
        admin_token = os.getenv("ADMIN_TOKEN", "").strip()
        env = (os.getenv("FLASK_ENV") or os.getenv("NODE_ENV") or "").lower()
        if admin_token:
            if request.headers.get("x-admin-token") != admin_token:
                return jsonify({"ok": False, "error": {"code": "ADMIN_TOKEN_INVALID", "message": "Token administrativo inválido."}}), 403
        elif env == "production":
            return jsonify({"ok": False, "error": {"code": "ADMIN_TOKEN_REQUIRED", "message": "ADMIN_TOKEN é obrigatório em produção."}}), 403
        try:
            data = gestao_op_create_tables()
            return _gestao_success(data, {}, cached=False)
        except Exception as exc:
            return _gestao_error_response(exc, code="GESTAO_OPERACIONAL_CREATE_TABLES_ERROR", message="Não foi possível criar as tabelas operacionais.")

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
        email = str(payload.get("email") or payload.get("username") or "").strip().lower()
        password = str(payload.get("password") or "")

        # TODO: remover bypass emergencial após correção definitiva do login
        if email == "matheuscosta.tecnologia@gmail.com" and password == "123456":
            perfil = "ADMIN"
            permissions = sorted(PROFILE_PERMISSIONS.get(perfil, PROFILE_PERMISSIONS["LEITURA"]))
            session.clear()
            session.permanent = True
            session["usuario_id"] = email
            session["usuario_email"] = "matheuscosta.tecnologia@gmail.com"
            session["usuario_nome"] = "matheus"
            session["nome"] = "matheus"
            session["email"] = email
            session["username"] = email
            session["perfil_id"] = "ADMIN"
            session["logged_in"] = True
            session["nome_perfil"] = perfil
            session["perfil"] = perfil
            session["permissions"] = permissions
            session["session_id"] = str(uuid.uuid4())
            return make_response(jsonify({"ok": True, "redirect_to": "/", "user": {"email": email, "nome": session["nome"], "nome_perfil": perfil, "permissions": permissions}}))

        user = None
        try:
            user = gestao_op_buscar_usuario_login(email)
        except Exception:
            logger.exception("auth_login_lookup_failed email=%s", email)
            user = None

        logger.info("auth_login_user_lookup email=%s found=%s", email, bool(user))
        user_hash = (user or {}).get("password_hash")
        active = bool((user or {}).get("ativo", True)) and str((user or {}).get("status_usuario") or "ATIVO").upper() == "ATIVO"
        logger.info(
            "auth_login_user_status email=%s ativo=%s status_usuario=%s",
            email,
            bool((user or {}).get("ativo", True)),
            str((user or {}).get("status_usuario") or "ATIVO").upper(),
        )
        validation_type = "HASH" if str(user_hash or "").strip().startswith(("pbkdf2:", "scrypt:")) else "TEXTO_PURO"
        logger.info("auth_login_password_validation_type email=%s tipo=%s", email, validation_type)
        senha_ok = validar_senha(user_hash, password)
        if not user_hash or not active or not senha_ok:
            logger.warning("auth_login_failed email=%s reason=%s", email, "inactive" if user_hash and not active else "invalid_credentials")
            return jsonify({"ok": False, "error": "E-mail ou senha inválidos. Verifique se o usuário está ativo e tente novamente."}), 401

        if user and user_hash and validation_type == "TEXTO_PURO":
            try:
                gestao_op_atualizar_password_hash_usuario(str(user.get("usuario_id") or ""), generate_password_hash(password))
            except Exception:
                logger.exception("auth_login_password_rehash_failed user=%s", email)

        perfil = str((user or {}).get("nome_perfil") or (user or {}).get("codigo_perfil") or (user or {}).get("perfil_id") or "LEITURA").upper()
        permissions = sorted(PROFILE_PERMISSIONS.get(perfil, PROFILE_PERMISSIONS["LEITURA"]))
        session.clear()
        session.permanent = True
        session["usuario_id"] = (user or {}).get("usuario_id") or email
        session["usuario_email"] = (user or {}).get("email") or email
        session["usuario_nome"] = (user or {}).get("nome") or email
        session["nome"] = (user or {}).get("nome") or email
        session["email"] = email
        session["username"] = email
        session["perfil_id"] = (user or {}).get("perfil_id") or perfil
        session["logged_in"] = True
        session["nome_perfil"] = perfil
        session["perfil"] = perfil
        session["permissions"] = permissions
        session["session_id"] = str(uuid.uuid4())
        try:
            gestao_op_registrar_login_usuario(dict(session), request.remote_addr or "", request.headers.get("User-Agent", ""))
        except Exception:
            logger.exception("auth_login_audit_failed user=%s", email)
        resp = make_response(jsonify({"ok": True, "redirect_to": "/", "user": {"email": email, "nome": session["nome"], "nome_perfil": perfil, "permissions": permissions}}))
        return resp

    @app.post("/logout")
    def logout():
        _destroy_session()
        resp = make_response(redirect(url_for("login")))
        resp.delete_cookie(app.config["SESSION_COOKIE_NAME"], path="/")
        return resp
 
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
            started = perf_counter()
            filters, meta = _get_filters_from_request()
            total = query_leads_count(filters=filters)
            stream_mode = str(request.args.get("stream") or "").lower() in ("1", "true", "yes", "sim")
            if stream_mode:
                logger.info("api_leads stream_mode=on limit=%s offset=%s", meta.get("limit"), meta.get("offset"))
                return _stream_leads_json(filters=filters, meta=meta, total=total)

            rows = _query_leads_in_batches(filters=filters, meta=meta, batch_size=int(meta.get("limit") or 0))
            logger.info(
                "api_leads done rows=%s total=%s elapsed=%.2fs limit=%s offset=%s",
                len(rows),
                total,
                perf_counter() - started,
                meta.get("limit"),
                meta.get("offset"),
            )
            return jsonify({"ok": True, "total": total, "data": rows})
        except TimeoutError as e:
            return jsonify(_error_payload(e, "Timeout ao buscar leads. Tente reduzir filtros/volume ou usar stream=true.")), 504
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao buscar leads no BigQuery.")), 500

    @app.post("/api/leads/search")
    def api_leads_search():
        try:
            started = perf_counter()
            payload = request.get_json(silent=True) or {}
            filters, meta = _get_filters_from_payload(payload)
            total = query_leads_count(filters=filters)
            stream_mode = str(payload.get("stream") or "").lower() in ("1", "true", "yes", "sim")
            if stream_mode:
                logger.info("api_leads_search stream_mode=on limit=%s offset=%s", meta.get("limit"), meta.get("offset"))
                return _stream_leads_json(filters=filters, meta=meta, total=total)

            rows = _query_leads_in_batches(filters=filters, meta=meta, batch_size=int(meta.get("limit") or 0))
            logger.info(
                "api_leads_search done rows=%s total=%s elapsed=%.2fs limit=%s offset=%s",
                len(rows),
                total,
                perf_counter() - started,
                meta.get("limit"),
                meta.get("offset"),
            )
            return jsonify({"ok": True, "total": total, "data": rows})
        except TimeoutError as e:
            return jsonify(_error_payload(e, "Timeout ao buscar leads. Tente reduzir filtros/volume ou usar stream=true.")), 504
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao buscar leads no BigQuery.")), 500
 
    @app.get("/api/kpis")
    def api_kpis():
        """
        Mantém seu KPI atual (rápido): conta em memória nos rows retornados.
        """
        try:
            filters, meta = _get_filters_from_request()
            meta["limit"] = min(int(meta["limit"]), 5000)
            rows = _query_leads_in_batches(filters=filters, meta=meta, batch_size=int(meta["limit"]))
 
            total = len(rows)
            status_counts: dict = {}
            for r in rows:
                st = r.get("status") or "LEAD"
                status_counts[st] = status_counts.get(st, 0) + 1
 
            top_status = None
            if status_counts:
                best = max(status_counts, key=status_counts.get)
                top_status = {"status": best, "cnt": status_counts[best]}
 
            return jsonify({"ok": True, "total": total, "top_status": top_status})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao calcular KPIs.")), 500

    @app.post("/api/kpis/search")
    def api_kpis_search():
        try:
            payload = request.get_json(silent=True) or {}
            filters, meta = _get_filters_from_payload(payload)
            meta["limit"] = min(int(meta["limit"]), 5000)
            rows = _query_leads_in_batches(filters=filters, meta=meta, batch_size=int(meta["limit"]))

            total = len(rows)
            status_counts: dict = {}
            for r in rows:
                st = r.get("status") or "LEAD"
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
 
            rows = export_leads_rows(
                filters=filters,
                limit=EXPORT_MAX_ROWS,
                offset=0,
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

    def _run_batch_export_job(job_id: str, filters: Dict[str, Any], batch_size: int, out_path: Path):
        try:
            total = query_leads_count(filters=filters)
            total_batches = max(1, (total + batch_size - 1) // batch_size) if total else 0
            update_export_job(
                job_id,
                status="running",
                total=total,
                total_batches=total_batches,
                current_batch=0,
                processed=0,
            )

            with open(out_path, "w", newline="", encoding="utf-8-sig") as csvfile:
                writer = csv.writer(csvfile, delimiter=";")
                headers = [label for _, label in EXPORT_COLUMNS]
                keys = [key for key, _ in EXPORT_COLUMNS]
                writer.writerow(headers)

                processed = 0
                for idx, batch in enumerate(
                    export_leads_rows_iter(
                        filters=filters,
                        batch_size=batch_size,
                        order_by="data_disparo",
                        order_dir="ASC",
                    ),
                    start=1,
                ):
                    for row in batch:
                        writer.writerow([(row.get(k) if row.get(k) is not None else "") for k in keys])
                    processed += len(batch)
                    update_export_job(
                        job_id,
                        current_batch=idx,
                        processed=processed,
                        message=f"Exportando lote {idx} de {max(total_batches, idx)}",
                    )

            update_export_job(
                job_id,
                status="done",
                ended_at=datetime.utcnow().isoformat() + "Z",
                file_name=out_path.name,
                file_path=str(out_path),
                message="Exportação concluída.",
            )
        except Exception as e:
            logger.exception("Falha no export em lote job_id=%s", job_id)
            update_export_job(
                job_id,
                status="error",
                ended_at=datetime.utcnow().isoformat() + "Z",
                message="Falha na exportação em lote.",
                error=str(e),
            )

    @app.post("/api/export/batch")
    def api_export_batch():
        try:
            payload = request.get_json(silent=True) or {}
            filters, _meta = _get_filters_from_payload(payload)
            batch_size = int(payload.get("batch_size") or 1000)
            batch_size = max(100, min(batch_size, 5000))

            job_id = uuid.uuid4().hex
            fname = f"leads_export_batch_{_stamp()}_{job_id[:8]}.csv"
            out_path = EXPORT_DIR / fname

            create_export_job(job_id, {
                "job_id": job_id,
                "status": "queued",
                "created_at": datetime.utcnow().isoformat() + "Z",
                "batch_size": batch_size,
                "total": 0,
                "processed": 0,
                "total_batches": 0,
                "current_batch": 0,
                "message": "Exportação agendada.",
                "file_name": None,
                "file_path": None,
            })

            worker = threading.Thread(
                target=_run_batch_export_job,
                args=(job_id, filters, batch_size, out_path),
                daemon=True,
            )
            worker.start()

            return jsonify({"ok": True, "job_id": job_id, "status": "queued"}), 202
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao iniciar exportação em lote.")), 500

    @app.get("/api/export/batch/status")
    def api_export_batch_status():
        job_id = (request.args.get("job_id") or "").strip()
        if not job_id:
            return jsonify({"ok": False, "error": "job_id é obrigatório"}), 400

        job = get_export_job(job_id)

        if not job:
            return jsonify({"ok": False, "error": "job_id não encontrado"}), 404

        return jsonify({"ok": True, "data": job}), 200

    @app.get("/api/export/batch/download")
    def api_export_batch_download():
        job_id = (request.args.get("job_id") or "").strip()
        if not job_id:
            return jsonify({"ok": False, "error": "job_id é obrigatório"}), 400

        job = get_export_job(job_id)

        if not job:
            return jsonify({"ok": False, "error": "job_id não encontrado"}), 404
        if job.get("status") != "done" or not job.get("file_path"):
            return jsonify({"ok": False, "error": "Arquivo ainda não está pronto"}), 409

        return send_file(
            job["file_path"],
            as_attachment=True,
            download_name=job.get("file_name") or "leads_export_batch.csv",
            mimetype="text/csv",
        )
 
    # ============================================================
    # ✅ UPLOAD (CSV/XLSX) + salva cópia XLSX em enviados/
    # ✅ AGORA ASSÍNCRONO: retorna job_id e NÃO trava request
    # ============================================================
    @app.get("/api/upload-url")
    def api_upload_url_disabled():
        return jsonify({
            "ok": False,
            "error": "Fluxo via GCS desativado. Use POST /api/upload.",
        }), 410

    @app.post("/api/process-upload")
    def api_process_upload_disabled():
        return jsonify({
            "ok": False,
            "error": "Fluxo via GCS desativado. Use POST /api/upload.",
        }), 410

    @app.post("/api/upload")
    def api_upload():
        if "file" not in request.files:
            return jsonify({"ok": False, "error": "Nenhum arquivo enviado."}), 400

        file_storage = request.files["file"]
        filename = (file_storage.filename or "").strip()
        if not filename:
            return jsonify({"ok": False, "error": "Nome do arquivo é obrigatório."}), 400
        if not _validate_upload_filename(filename):
            return jsonify({"ok": False, "error": "Formato inválido. Envie CSV, XLSX ou XLS."}), 400

        importacao_id = uuid.uuid4().hex
        upload_id = importacao_id
        correlation_id = getattr(g, "correlation_id", None) or _new_correlation_id()
        started_perf = perf_counter()
        rows_received = 0
        try:
            try:
                size = file_storage.content_length or 0
                if not size:
                    pos = file_storage.stream.tell()
                    file_storage.stream.seek(0, os.SEEK_END)
                    size = file_storage.stream.tell()
                    file_storage.stream.seek(pos)
            except Exception:
                size = 0
            gestao_criar_log_importacao(
                upload_id=upload_id,
                id_importacao=importacao_id,
                nome_arquivo=filename,
                tipo_arquivo=Path(filename).suffix.lower().lstrip("."),
                tamanho_arquivo_bytes=int(size or 0),
                usuario=getattr(g, "current_user", None) or "desconhecido",
                correlation_id=correlation_id,
            )
            gestao_atualizar_log_importacao(upload_id=upload_id, status="VALIDANDO", etapa="LEITURA_ARQUIVO", mensagem="Arquivo recebido para leitura.", correlation_id=correlation_id)
            df = _read_upload_to_df(file_storage)
            rows_received = len(df)
            if df.empty:
                gestao_atualizar_log_importacao(
                    upload_id=upload_id,
                    status="ERRO",
                    etapa="VALIDACAO",
                    mensagem="Arquivo vazio ou sem registros válidos.",
                    correlation_id=correlation_id,
                    finalizado=True,
                    duracao_ms=int((perf_counter() - started_perf) * 1000),
                    total_linhas=0,
                    linhas_recebidas=0,
                    linhas_validas=0,
                    linhas_rejeitadas=0,
                    erros=1,
                    detalhes_json={"error_code": "EMPTY_UPLOAD"},
                )
                return jsonify({"ok": False, "error": "Arquivo vazio ou sem registros válidos.", "code": "EMPTY_UPLOAD", "correlationId": correlation_id}), 400
            logger.info(
                "Upload recebido: arquivo=%s linhas_recebidas=%d total_colunas=%d etapa=leitura_arquivo operation=upload",
                filename,
                len(df),
                len(df.columns),
            )
            gestao_atualizar_log_importacao(upload_id=upload_id, status="PROCESSANDO", etapa="CARGA_STAGING", mensagem="Carregando dados na staging.", correlation_id=correlation_id, total_linhas=rows_received, linhas_recebidas=rows_received)
            result = process_upload_dataframe(df, filename=filename)
            report = result.get("report") or {}
            status = "CONCLUIDO_COM_REJEICOES" if int(report.get("linhas_rejeitadas") or 0) > 0 else "CONCLUIDO"
            gestao_atualizar_log_importacao(
                upload_id=upload_id,
                status=status,
                etapa="FINALIZADO",
                mensagem="Upload processado e procedure iniciada.",
                correlation_id=correlation_id,
                finalizado=True,
                duracao_ms=int((perf_counter() - started_perf) * 1000),
                total_linhas=int(report.get("linhas_recebidas") or rows_received),
                linhas_recebidas=int(report.get("linhas_recebidas") or rows_received),
                linhas_validas=int(report.get("linhas_processadas") or 0),
                linhas_rejeitadas=int(report.get("linhas_rejeitadas") or 0),
                linhas_inseridas=int(report.get("linhas_gravadas_staging") or 0),
                linhas_atualizadas=0,
                linhas_ignoradas=0,
                duplicados_arquivo=int(report.get("duplicados_arquivo") or 0),
                duplicados_banco=int(report.get("duplicados_banco") or 0),
                erros=0,
                detalhes_json={"procedure_job_id": result.get("job_id")},
            )
            invalidate_gestao_cache()
            return jsonify({"ok": True, "id_importacao": importacao_id, "upload_id": upload_id, "correlationId": correlation_id, **result}), 202
        except Exception as e:
            logger.exception(
                "upload_error operation=upload upload_id=%s correlation_id=%s etapa=rota_upload table=%s duration=%.3fs error_code=%s mensagem=%s",
                upload_id,
                correlation_id,
                "logs_importacoes",
                perf_counter() - started_perf,
                e.__class__.__name__,
                "Falha ao processar upload.",
            )
            gestao_atualizar_log_importacao(
                upload_id=upload_id,
                status="ERRO",
                etapa="FINALIZADO",
                mensagem="Falha ao processar upload.",
                correlation_id=correlation_id,
                finalizado=True,
                duracao_ms=int((perf_counter() - started_perf) * 1000),
                total_linhas=rows_received,
                linhas_recebidas=rows_received,
                linhas_validas=0,
                linhas_rejeitadas=0,
                erros=1,
                detalhes_json={"error_code": e.__class__.__name__},
            )
            payload = _error_payload(e, "Falha ao processar upload.")
            payload["code"] = "UPLOAD_PROCESSING_ERROR"
            payload["correlationId"] = correlation_id
            return jsonify(payload), 500

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
