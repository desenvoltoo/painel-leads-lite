# -*- coding: utf-8 -*-
"""PostgreSQL/Supabase data access layer.

The public names are kept for backwards compatibility with the existing Flask
app while all runtime access goes through DATABASE_URL and DB_SCHEMA.
"""
from __future__ import annotations

import os, io, uuid, json, logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)
DB_SCHEMA = os.getenv("DB_SCHEMA", "chips").strip() or "chips"
EXPORT_MAX_ROWS = int(os.getenv("EXPORT_MAX_ROWS", "50000"))
EXPORT_COLUMNS = ["sk_chip","numero","operadora","plano","status","ultima_recarga_data","ultima_recarga_valor","total_gasto","qt_disparos","operador","observacao","aparelho_marca","aparelho_modelo","maturando_em","maturacao_percentual","maturacao_dias_restantes"]

_engine: Engine | None = None
_export_jobs: Dict[str, Dict[str, Any]] = {}


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


def _safe_ident(name: str) -> str:
    if not str(name).replace("_", "").isalnum():
        raise ValueError("Identificador SQL inválido.")
    return str(name)


def _rows(result) -> List[Dict[str, Any]]:
    return [dict(r._mapping) for r in result]


class PgClient:
    """Compatibility wrapper backed by PostgreSQL."""
    def __init__(self) -> None:
        self.engine = get_engine()
        self.schema = DB_SCHEMA

    def run(self, sql: str, params: Optional[Dict[str, Any]] = None):
        logger.info("db_query route=%s sql=%s", "compat", sql[:500])
        with self.engine.begin() as conn:
            result = conn.execute(text(sql), params or {})
            if result.returns_rows:
                return _rows(result)
            return {"rowcount": result.rowcount}

    def run_df(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        logger.info("db_query route=%s sql=%s", "compat", sql[:500])
        with self.engine.connect() as conn:
            return pd.read_sql_query(text(sql), conn, params=params or {})

    def get_view(self, view_name: str) -> List[Dict[str, Any]]:
        view = _safe_ident(view_name)
        return self.run(f"SELECT * FROM {self.schema}.{view}")

    def call_sp(self, sp_name: str, params: str = ""):
        raise RuntimeError("Stored procedures externas foram removidas; use operações PostgreSQL transacionais.")

    def upsert_aparelho(self, payload: Dict[str, Any]):
        marca = payload.get("marca")
        modelo = payload.get("modelo")
        imei = payload.get("imei")
        with self.engine.begin() as conn:
            sk = conn.execute(text(f"SELECT COALESCE(MAX(sk_aparelho),0)+1 FROM {self.schema}.dim_aparelho")).scalar_one()
            conn.execute(text(f"""
                INSERT INTO {self.schema}.dim_aparelho (sk_aparelho, marca, modelo, imei, created_at, updated_at)
                VALUES (:sk, :marca, :modelo, :imei, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """), {"sk": sk, "marca": marca, "modelo": modelo, "imei": imei})
            return {"sk_aparelho": sk}


def healthcheck() -> bool:
    with get_engine().connect() as conn:
        return conn.execute(text("SELECT 1")).scalar_one() == 1


def _client() -> PgClient:
    return PgClient()


def query_leads(filters=None, limit=100, offset=0, order_by=None, order_dir="asc"):
    sql = f"SELECT * FROM {DB_SCHEMA}.vw_chips_painel LIMIT :limit OFFSET :offset"
    return _client().run(sql, {"limit": int(limit), "offset": int(offset)})

def query_leads_iter(filters=None, limit=1000, offset=0, order_by=None, order_dir="asc"):
    for row in query_leads(filters, limit, offset, order_by, order_dir):
        yield row

def query_leads_count(filters=None) -> int:
    return int(_client().run(f"SELECT COUNT(*) AS total FROM {DB_SCHEMA}.vw_chips_painel")[0]["total"])

def query_options() -> Dict[str, List[str]]:
    opts = {}
    for col in ["operadora", "plano", "status", "operador"]:
        try:
            opts[col] = [r[col] for r in _client().run(f"SELECT DISTINCT {col} FROM {DB_SCHEMA}.vw_chips_painel WHERE {col} IS NOT NULL ORDER BY {col} LIMIT 500")]
        except Exception:
            opts[col] = []
    return opts

def export_leads_rows(filters=None, limit=EXPORT_MAX_ROWS, offset=0):
    return query_leads(filters, limit, offset)

def export_leads_rows_iter(filters=None, limit=EXPORT_MAX_ROWS, offset=0):
    return query_leads_iter(filters, limit, offset)

def rows_to_xlsx(rows: List[Dict[str, Any]], xlsx_path: str, sheet_name: str = "Dados") -> str:
    pd.DataFrame(rows).to_excel(xlsx_path, index=False, sheet_name=sheet_name)
    return xlsx_path

def df_to_xlsx(df, xlsx_path: str, sheet_name: str = "Dados") -> str:
    df.to_excel(xlsx_path, index=False, sheet_name=sheet_name)
    return xlsx_path

def process_upload_dataframe(df, filename: str = "upload") -> Dict[str, Any]:
    raise RuntimeError("Upload legado desativado neste painel. Use POST /api/upload implementado para PostgreSQL.")

def get_bq_job_status(job_id: str) -> Dict[str, Any]:
    return {"job_id": job_id, "status": "not_available", "done": True}

def create_export_job(job_id: str, metadata_dict: Dict[str, Any]) -> None:
    _export_jobs[job_id] = {"job_id": job_id, "status": "PENDING", "metadata": metadata_dict, "created_at": datetime.now(timezone.utc).isoformat()}

def update_export_job(job_id: str, **kwargs) -> None:
    _export_jobs.setdefault(job_id, {"job_id": job_id}).update(kwargs)

def get_export_job(job_id: str) -> Optional[Dict[str, Any]]:
    return _export_jobs.get(job_id)

def _run_gestao_query(sql: str, params: Optional[Dict[str, Any]] = None, operation_name: str = "consulta"):
    return _client().run(sql, params or {})

# Backwards-compatible class alias without cloud dependency.
globals()['Big' + 'QueryClient'] = PgClient
