# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)
Versão: 4.2 - Fix encoding CSV (chardet)
"""
 
import os
import traceback
import io
import uuid
import mimetypes
import json
import logging
import csv
import threading
from time import perf_counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple
 
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, make_response, g, session, Response, stream_with_context
from werkzeug.security import check_password_hash
 
from services.bigquery import (
    query_leads,
    query_leads_iter,
    query_leads_count,
    query_options,
    process_gcs_upload,
    generate_gcs_signed_upload,
    get_bq_job_status,          # novo
    export_leads_rows,
    export_leads_rows_iter,
    rows_to_xlsx,               # gera export XLSX no servidor
    EXPORT_COLUMNS,
)

logger = logging.getLogger(__name__)
EXPORT_BATCH_JOBS: Dict[str, Dict[str, Any]] = {}
EXPORT_BATCH_JOBS_LOCK = threading.Lock()
 


ALLOWED_UPLOAD_EXTENSIONS = {".csv", ".xlsx", ".xls"}


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
    return {
        "ok": False,
        "error": public_msg,
        "details": str(e),
        "trace": traceback.format_exc(limit=3),
    }
 
def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _update_export_job(job_id: str, **kwargs):
    with EXPORT_BATCH_JOBS_LOCK:
        if job_id in EXPORT_BATCH_JOBS:
            EXPORT_BATCH_JOBS[job_id].update(kwargs)
 
 
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


def _read_upload_to_df(file_storage) -> pd.DataFrame:
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

    for sep in (";", ","):
        for enc in encodings_to_try:
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
    session_ttl_seconds = int(_env("SESSION_TTL_SECONDS", "28800"))
    cookie_secure = _env("COOKIE_SECURE", "false").lower() == "true"
    session_cookie_name = _env("SESSION_COOKIE_NAME", "painel_session")

    app.config.update(
        SESSION_COOKIE_NAME=session_cookie_name,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SECURE=cookie_secure,
        SESSION_COOKIE_SAMESITE="Lax",
        PERMANENT_SESSION_LIFETIME=session_ttl_seconds,
    )

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

    def _current_user() -> str | None:
        username = session.get("username")
        if not username:
            return None
        # renova TTL da sessão em uso normal do painel
        session.permanent = True
        session.modified = True
        return str(username)

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
        resp = make_response(jsonify({"ok": True, "redirect_to": "/"}))
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
                st = r.get("status_inscricao") or r.get("status") or "LEAD"
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

    def _run_batch_export_job(job_id: str, filters: Dict[str, Any], batch_size: int, out_path: Path):
        try:
            total = query_leads_count(filters=filters)
            total_batches = max(1, (total + batch_size - 1) // batch_size) if total else 0
            _update_export_job(
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
                    _update_export_job(
                        job_id,
                        current_batch=idx,
                        processed=processed,
                        message=f"Exportando lote {idx} de {max(total_batches, idx)}",
                    )

            _update_export_job(
                job_id,
                status="done",
                ended_at=datetime.utcnow().isoformat() + "Z",
                file_name=out_path.name,
                file_path=str(out_path),
                message="Exportação concluída.",
            )
        except Exception as e:
            logger.exception("Falha no export em lote job_id=%s", job_id)
            _update_export_job(
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

            with EXPORT_BATCH_JOBS_LOCK:
                EXPORT_BATCH_JOBS[job_id] = {
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
                }

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

        with EXPORT_BATCH_JOBS_LOCK:
            job = EXPORT_BATCH_JOBS.get(job_id)

        if not job:
            return jsonify({"ok": False, "error": "job_id não encontrado"}), 404

        return jsonify({"ok": True, "data": job}), 200

    @app.get("/api/export/batch/download")
    def api_export_batch_download():
        job_id = (request.args.get("job_id") or "").strip()
        if not job_id:
            return jsonify({"ok": False, "error": "job_id é obrigatório"}), 400

        with EXPORT_BATCH_JOBS_LOCK:
            job = EXPORT_BATCH_JOBS.get(job_id)

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
    def api_upload_url():
        filename = (request.args.get("filename") or "").strip()
        if not filename:
            return jsonify({"ok": False, "error": "filename é obrigatório."}), 400
        if not _validate_upload_filename(filename):
            return jsonify({"ok": False, "error": "Formato inválido. Envie CSV ou XLSX."}), 400

        try:
            source = (request.args.get("source") or "manual").strip() or "manual"
            payload = generate_gcs_signed_upload(filename=filename, source_tag=source)
            return jsonify({"ok": True, "data": payload}), 200
        except Exception as e:
            return jsonify(_error_payload(e, "Falha ao gerar URL de upload.")), 500

    @app.post("/api/process-upload")
    def api_process_upload():
        payload = request.get_json(silent=True) or {}
        object_name = (payload.get("object_name") or "").strip()
        if not object_name:
            return jsonify({"ok": False, "error": "object_name é obrigatório."}), 400

        try:
            result = process_gcs_upload(object_name)
            return jsonify({"ok": True, **result}), 202
        except Exception as e:
            return jsonify(_error_payload(e, "Falha na ingestão via GCS.")), 500

    @app.post("/api/upload")
    def api_upload():
        return jsonify({"ok": False, "error": "Endpoint legado. Use /api/upload-url + /api/process-upload."}), 410

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
