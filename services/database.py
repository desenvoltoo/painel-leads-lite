# -*- coding: utf-8 -*-
"""PostgreSQL/Supabase data access layer for Painel de Leads Lite."""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)
IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public").strip() or "public"
LEADS_VIEW = os.getenv("LEADS_VIEW", "vw_leads_painel_lite").strip() or "vw_leads_painel_lite"
EXPORT_MAX_ROWS = int(os.getenv("EXPORT_MAX_ROWS", "50000"))
EXPORT_COLUMNS = ["sk_pessoa","cpf","celular","nome","email","curso","modalidade","turno","polo","origem","tipo_negocio","consultor_comercial","consultor_disparo","campanha","canal","acao_comercial","tipo_disparo","peca_disparo","texto_disparo","qtd_acionamentos","status","status_inscricao","observacao","flag_matriculado","data_inscricao","data_matricula","data_atualizacao","data_ultima_acao","data_disparo"]

_engine: Engine | None = None
_export_jobs: Dict[str, Dict[str, Any]] = {}
_view_cols_cache: set[str] | None = None

@dataclass
class ScalarQueryParameter:
    name: str
    type_: str
    value: Any

@dataclass
class SchemaField:
    name: str
    field_type: str = "STRING"

class _DatabaseCompat:
    ScalarQueryParameter = ScalarQueryParameter
    SchemaField = SchemaField

database = _DatabaseCompat()


def _safe_ident(name: str) -> str:
    if not IDENT_RE.fullmatch(str(name or "")):
        raise ValueError("Identificador SQL inválido.")
    return str(name)


def _view_table_id() -> str:
    return f'{_safe_ident(os.getenv("DB_SCHEMA", DB_SCHEMA).strip() or "public")}.{_safe_ident(os.getenv("LEADS_VIEW", LEADS_VIEW).strip() or "vw_leads_painel_lite")}'


def _database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("Variável DATABASE_URL não configurada.")
    return url


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(_database_url(), pool_pre_ping=True, future=True)
    return _engine


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _rows(result) -> List[Dict[str, Any]]:
    return [{k: _json_safe_value(v) for k, v in dict(r._mapping).items()} for r in result]


def _run_gestao_query(sql: str, params: Optional[Dict[str, Any] | List[ScalarQueryParameter]] = None, operation_name: str = "consulta"):
    exec_params: Dict[str, Any] = {}
    if isinstance(params, list):
        for p in params:
            exec_params[p.name] = p.value
        sql = re.sub(r"@(\w+)", r":\1", sql)
    elif isinstance(params, dict):
        exec_params = params
    logger.info("db_query operation=%s params=%s", operation_name, _format_bq_params_for_log(params or {}))
    with get_engine().begin() as conn:
        result = conn.execute(text(sql), exec_params)
        return _rows(result) if result.returns_rows else {"rowcount": result.rowcount}

class PgClient:
    def run(self, sql: str, params: Optional[Dict[str, Any]] = None): return _run_gestao_query(sql, params, "compat")
    def run_df(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        with get_engine().connect() as conn: return pd.read_sql_query(text(sql), conn, params=params or {})
    def get_view(self, view_name: str): return self.run(f"SELECT * FROM {_safe_ident(DB_SCHEMA)}.{_safe_ident(view_name)}")
    def call_sp(self, *a, **k): raise RuntimeError("Stored procedures externas foram removidas; use PostgreSQL.")
    def upsert_aparelho(self, payload): raise RuntimeError("Cadastro de aparelhos não faz parte do Painel de Leads Lite.")

def healthcheck() -> bool:
    with get_engine().connect() as conn: return conn.execute(text("SELECT 1")).scalar_one() == 1

def _client() -> PgClient: return PgClient()

def _view_columns() -> set[str]:
    global _view_cols_cache
    if _view_cols_cache is not None: return _view_cols_cache
    schema, view = _view_table_id().split(".", 1)
    rows = _run_gestao_query("SELECT column_name FROM information_schema.columns WHERE table_schema=:schema AND table_name=:view", {"schema": schema, "view": view}, "view_columns")
    _view_cols_cache = {r["column_name"] for r in rows}
    return _view_cols_cache

def _has_view_col(col: str) -> bool: return col in _view_columns()
def _first_existing_col(*cols: str) -> str: return next((c for c in cols if _has_view_col(c)), cols[0])

def _add_param(params: list, name: str, type_: str, value: Any):
    params.append(ScalarQueryParameter(name, type_, value))

def _apply_filters(sql: str, filters: Optional[Dict[str, Any]], params: list) -> str:
    filters = filters or {}
    mapping = {"data_inicio":"data_inscricao", "data_ini":"data_inscricao", "data_fim":"data_inscricao", "busca":"busca"}
    for key in ["status","curso","modalidade","turno","polo","origem","consultor_disparo","consultor_comercial","canal","campanha","tipo_disparo","tipo_negocio"]:
        val = filters.get(key)
        if val not in (None, "", []):
            pname = f"f_{key}"; sql += f" AND v.{key} = @{pname}"; _add_param(params, pname, "STRING", val)
    if filters.get("busca"):
        sql += " AND (unaccent(coalesce(v.nome,'')) ILIKE unaccent(@busca) OR regexp_replace(coalesce(v.cpf::text,''),'[^0-9]','','g') LIKE @busca_num OR regexp_replace(coalesce(v.celular::text,''),'[^0-9]','','g') LIKE @busca_num OR coalesce(v.email,'') ILIKE @busca)"
        _add_param(params,"busca","STRING",f"%{filters['busca']}%"); _add_param(params,"busca_num","STRING",f"%{re.sub(r'[^0-9]','',str(filters['busca']))}%")
    if filters.get("data_inicio") or filters.get("data_ini"):
        _add_param(params,"data_inicio","DATE",date.fromisoformat(str(filters.get("data_inicio") or filters.get("data_ini")))); sql += " AND DATE(v.data_inscricao) >= @data_inicio"
    if filters.get("data_fim"):
        _add_param(params,"data_fim","DATE",date.fromisoformat(str(filters.get("data_fim")))); sql += " AND DATE(v.data_inscricao) <= @data_fim"
    sit = str(filters.get("data_disparo_situacao") or "").lower()
    if sit == "vazias":
        sql += " AND v.data_disparo IS NULL"
    elif sit == "preenchidas":
        sql += " AND v.data_disparo IS NOT NULL"
    if filters.get("data_disparo_mes") and sit != "vazias":
        y, m = map(int, str(filters["data_disparo_mes"]).split("-")); ini = date(y,m,1); fim = date(y + (m==12), 1 if m==12 else m+1, 1)
        sql += " AND DATE(v.data_disparo) >= @data_disparo_ini AND DATE(v.data_disparo) < @data_disparo_fim"
        _add_param(params,"data_disparo_ini","DATE",ini); _add_param(params,"data_disparo_fim","DATE",fim)
    return sql

def _params_to_dict(params): return {p.name:p.value for p in params}
def _postgres_sql(sql): return re.sub(r"@(\w+)", r":\1", sql)

def _safe_order(order_by, order_dir):
    col = order_by if order_by in EXPORT_COLUMNS else "data_inscricao"
    direction = "DESC" if str(order_dir).upper() == "DESC" else "ASC"
    return col, direction

def query_leads(filters=None, limit=100, offset=0, order_by=None, order_dir="asc"):
    col, direction = _safe_order(order_by, order_dir); params=[]
    sql = _apply_filters(f"SELECT {', '.join('v.'+c for c in EXPORT_COLUMNS)} FROM {_view_table_id()} v WHERE 1=1", filters, params)
    sql += f" ORDER BY v.{col} {direction} NULLS LAST LIMIT @limit OFFSET @offset"
    _add_param(params,"limit","INT64",int(limit)); _add_param(params,"offset","INT64",int(offset))
    return _run_gestao_query(_postgres_sql(sql), _params_to_dict(params), "leads_list")

def query_leads_iter(filters=None, limit=1000, offset=0, order_by=None, order_dir="asc"):
    yield from query_leads(filters, limit, offset, order_by, order_dir)

def query_leads_count(filters=None):
    params=[]; sql=_apply_filters(f"SELECT COUNT(*) AS total FROM {_view_table_id()} v WHERE 1=1", filters, params)
    return int(_run_gestao_query(_postgres_sql(sql), _params_to_dict(params), "leads_count")[0]["total"])

def query_options():
    opts={}
    for col in ["status","curso","modalidade","turno","polo","origem","consultor_disparo","consultor_comercial","canal","campanha","tipo_disparo","tipo_negocio"]:
        opts[col]=[r[col] for r in _run_gestao_query(f"SELECT DISTINCT {col} FROM {_view_table_id()} WHERE {col} IS NOT NULL ORDER BY {col} LIMIT 500", {}, f"options_{col}")]
    return opts

def export_leads_rows(filters=None, limit=EXPORT_MAX_ROWS, offset=0): return query_leads(filters, limit, offset)
def export_leads_rows_iter(filters=None, limit=EXPORT_MAX_ROWS, offset=0): return query_leads_iter(filters, limit, offset)
def rows_to_xlsx(rows, xlsx_path, sheet_name="Dados"): pd.DataFrame(rows).to_excel(xlsx_path,index=False,sheet_name=sheet_name); return xlsx_path
def df_to_xlsx(df, xlsx_path, sheet_name="Dados"): df.to_excel(xlsx_path,index=False,sheet_name=sheet_name); return xlsx_path

def _coerce_df_to_staging_schema(df, staging_schema, upload_ts):
    out = df.copy()
    for field in staging_schema:
        if field.name == "dt_upload" and "dt_upload" not in out.columns: out["dt_upload"] = upload_ts
    return out

def process_upload_dataframe(df, filename="upload"): raise RuntimeError("Upload legado desativado; use POST /api/upload para PostgreSQL.")
def get_bq_job_status(job_id): return {"job_id": job_id, "status":"not_available", "done": True}
def create_export_job(job_id, metadata_dict): _export_jobs[job_id]={"job_id":job_id,"status":"PENDING","metadata":metadata_dict,"created_at":datetime.now(timezone.utc).isoformat()}
def update_export_job(job_id, **kwargs): _export_jobs.setdefault(job_id,{"job_id":job_id}).update(kwargs)
def get_export_job(job_id): return _export_jobs.get(job_id)

def _format_bq_params_for_log(params):
    if isinstance(params, dict): params = [ScalarQueryParameter(k,"",v) for k,v in params.items()]
    sensitive={"cpf","celular","email","senha","password","token"}; out=[]
    for p in params:
        name=getattr(p,"name",""); val="[REDACTED]" if any(s in name.lower() for s in sensitive) else "[SET]"
        out.append({"name":name,"type":getattr(p,"type_",""),"value":val})
    return out

globals()['Big' + 'QueryClient'] = PgClient
