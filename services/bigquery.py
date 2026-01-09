# -*- coding: utf-8 -*-
import os
import io
from typing import Any, Dict, List
import pandas as pd


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v else default


def _client():
    from google.cloud import bigquery
    project = _env("GCP_PROJECT_ID")
    if project:
        return bigquery.Client(project=project)
    return bigquery.Client()


def _table_ref() -> str:
    project = _env("GCP_PROJECT_ID")
    dataset = _env("BQ_DATASET")
    view = _env("BQ_VIEW_LEADS")
    if not project or not dataset or not view:
        raise RuntimeError("Faltam envs: GCP_PROJECT_ID, BQ_DATASET, BQ_VIEW_LEADS")
    return f"`{project}.{dataset}.{view}`"


def _date_field() -> str:
    # sua view usa data_inscricao_dt, então esse é o padrão
    return _env("BQ_DATE_FIELD", "data_inscricao_dt")


def _date_expr() -> str:
    # converte DATETIME/TIMESTAMP em DATE para filtros/ordenar
    # para DATE já funciona também
    return f"DATE({_date_field()})"


# ===================== LEADS =====================
def query_leads(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    from google.cloud import bigquery

    dt = _date_expr()

    sql = f"""
    SELECT
      {dt} AS data_inscricao,
      nome,
      cpf,
      celular,
      email,
      origem,
      polo,
      curso,
      status,
      consultor
    FROM {_table_ref()}
    WHERE 1=1
      AND (@status IS NULL OR UPPER(status) = UPPER(@status))
      AND (@curso  IS NULL OR UPPER(curso)  = UPPER(@curso))
      AND (@polo   IS NULL OR UPPER(polo)   = UPPER(@polo))
      AND (@origem IS NULL OR UPPER(origem) = UPPER(@origem))
      AND (@data_ini IS NULL OR {dt} >= DATE(@data_ini))
      AND (@data_fim IS NULL OR {dt} <= DATE(@data_fim))
    ORDER BY {dt} DESC
    LIMIT @limit
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", filters.get("status")),
            bigquery.ScalarQueryParameter("curso", "STRING", filters.get("curso")),
            bigquery.ScalarQueryParameter("polo", "STRING", filters.get("polo")),
            bigquery.ScalarQueryParameter("origem", "STRING", filters.get("origem")),
            bigquery.ScalarQueryParameter("data_ini", "STRING", filters.get("data_ini")),
            bigquery.ScalarQueryParameter("data_fim", "STRING", filters.get("data_fim")),
            bigquery.ScalarQueryParameter("limit", "INT64", int(filters.get("limit") or 500)),
        ]
    )

    rows = _client().query(sql, job_config=job_config).result()
    return [{k: r.get(k) for k in r.keys()} for r in rows]


# ===================== KPIs =====================
def query_kpis(filters: Dict[str, Any]) -> Dict[str, Any]:
    from google.cloud import bigquery

    dt = _date_expr()

    sql = f"""
    WITH base AS (
      SELECT
        {dt} AS data_inscricao,
        status,
        curso,
        polo,
        origem
      FROM {_table_ref()}
      WHERE 1=1
        AND (@status IS NULL OR UPPER(status) = UPPER(@status))
        AND (@curso  IS NULL OR UPPER(curso)  = UPPER(@curso))
        AND (@polo   IS NULL OR UPPER(polo)   = UPPER(@polo))
        AND (@origem IS NULL OR UPPER(origem) = UPPER(@origem))
        AND (@data_ini IS NULL OR {dt} >= DATE(@data_ini))
        AND (@data_fim IS NULL OR {dt} <= DATE(@data_fim))
    ),
    agg AS (
      SELECT status, COUNT(*) cnt
      FROM base
      GROUP BY status
    )
    SELECT
      (SELECT COUNT(*) FROM base) AS total,
      (SELECT MAX(data_inscricao) FROM base) AS last_date,
      (SELECT AS STRUCT status, cnt FROM agg ORDER BY cnt DESC LIMIT 1) AS top_status,
      (SELECT ARRAY_AGG(STRUCT(status, cnt) ORDER BY cnt DESC) FROM agg) AS by_status
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", filters.get("status")),
            bigquery.ScalarQueryParameter("curso", "STRING", filters.get("curso")),
            bigquery.ScalarQueryParameter("polo", "STRING", filters.get("polo")),
            bigquery.ScalarQueryParameter("origem", "STRING", filters.get("origem")),
            bigquery.ScalarQueryParameter("data_ini", "STRING", filters.get("data_ini")),
            bigquery.ScalarQueryParameter("data_fim", "STRING", filters.get("data_fim")),
        ]
    )

    row = next(iter(_client().query(sql, job_config=job_config).result()), None)
    if not row:
        return {"total": 0, "last_date": None, "top_status": None, "by_status": []}

    top = row.get("top_status")
    return {
        "total": int(row.get("total") or 0),
        "last_date": str(row.get("last_date")) if row.get("last_date") else None,
        "top_status": {"status": top.get("status"), "cnt": int(top.get("cnt") or 0)} if top else None,
        "by_status": [{"status": x.get("status"), "cnt": int(x.get("cnt") or 0)} for x in (row.get("by_status") or [])],
    }


# ===================== OPTIONS =====================
def _distinct(column: str, limit: int = 250) -> List[str]:
    from google.cloud import bigquery

    sql = f"""
    SELECT DISTINCT {column} v
    FROM {_table_ref()}
    WHERE {column} IS NOT NULL AND TRIM(CAST({column} AS STRING)) != ""
    ORDER BY v
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", int(limit))]
    )
    rows = _client().query(sql, job_config=job_config).result()
    return [str(r.get("v")) for r in rows if r.get("v") is not None]


def query_options() -> Dict[str, List[str]]:
    return {
        "status": _distinct("status"),
        "curso": _distinct("curso"),
        "polo": _distinct("polo"),
        "origem": _distinct("origem"),
    }


# ===================== UPLOAD + PROMOTE =====================
def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def ingest_upload_file(file_storage, source: str = "UPLOAD_PAINEL") -> Dict[str, Any]:
    """
    Upload -> stg_leads_upload -> CALL sp_promote_stg_leads_upload()
    Usa dataset atual (BQ_DATASET), pois você já tem tudo pronto no modelo_estrela.
    """
    from google.cloud import bigquery

    filename = (file_storage.filename or "").lower()
    project = _env("GCP_PROJECT_ID")
    dataset = _env("BQ_DATASET")

    upload_table = _env("BQ_UPLOAD_TABLE", "stg_leads_upload")
    promote_proc = _env("BQ_PROMOTE_PROC", "sp_promote_stg_leads_upload")

    stg_table_id = f"{project}.{dataset}.{upload_table}"
    proc_id = f"{project}.{dataset}.{promote_proc}"

    if filename.endswith(".csv"):
        content = file_storage.read()
        df = pd.read_csv(io.BytesIO(content), dtype=str, sep=None, engine="python")
    elif filename.endswith(".xlsx") or filename.endswith(".xls"):
        content = file_storage.read()
        df = pd.read_excel(io.BytesIO(content), dtype=str)
    else:
        raise ValueError("Formato inválido. Envie CSV ou XLSX.")

    df = _normalize_cols(df)
    df["origem_upload"] = source
    df["data_ingestao"] = pd.Timestamp.utcnow()

    client = _client()

    load_job = client.load_table_from_dataframe(
        df,
        stg_table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True),
    )
    load_job.result()

    promote_job = client.query(f"CALL `{proc_id}`();")
    promote_job.result()

    return {
        "staging_table": stg_table_id,
        "rows_loaded": int(load_job.output_rows or 0),
        "load_job_id": load_job.job_id,
        "promote_proc": proc_id,
        "promote_job_id": promote_job.job_id,
    }


# ===================== DEBUG (opcional, mas útil) =====================
def debug_count() -> int:
    sql = f"SELECT COUNT(1) AS c FROM {_table_ref()}"
    rows = _client().query(sql).result()
    row = next(iter(rows), None)
    return int(row.get("c") or 0) if row else 0


def debug_sample(limit: int = 5):
    from google.cloud import bigquery
    sql = f"SELECT * FROM {_table_ref()} LIMIT @limit"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", int(limit))]
    )
    rows = _client().query(sql, job_config=job_config).result()
    return [{k: r.get(k) for k in r.keys()} for r in rows]
