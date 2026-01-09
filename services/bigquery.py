# -*- coding: utf-8 -*-
import os
import io
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd


# ============================================================
# ENV / CLIENT
# ============================================================
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v else default


def _client():
    from google.cloud import bigquery
    project = _env("GCP_PROJECT_ID")
    return bigquery.Client(project=project) if project else bigquery.Client()


def _table_ref() -> str:
    project = _env("GCP_PROJECT_ID")
    dataset = _env("BQ_DATASET")
    view = _env("BQ_VIEW_LEADS")
    if not project or not dataset or not view:
        raise RuntimeError("Faltam envs: GCP_PROJECT_ID, BQ_DATASET, BQ_VIEW_LEADS")
    return f"`{project}.{dataset}.{view}`"


def _date_field() -> str:
    # Para vw_painel_leads / vw_leads_painel_lite normalmente é DATE
    return _env("BQ_DATE_FIELD", "data_inscricao")


def _date_expr() -> str:
    return f"DATE({_date_field()})"


# ============================================================
# NORMALIZAÇÃO
# ============================================================
def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _clean_str(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in ("nan", "nat", "none", "<na>"):
        return ""
    return s


def _to_bool(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("true", "t", "1", "sim", "s", "yes", "y"):
        return True
    if s in ("false", "f", "0", "nao", "não", "n", "no"):
        return False
    if s == "" or s.lower() in ("nan", "nat", "none", "<na>"):
        return None
    return None


def _normalize_datetime_cols(df: pd.DataFrame) -> pd.DataFrame:
    # DATETIME (vira string padrão)
    dt_cols = ("data_envio_dt", "data_inscricao_dt", "data_disparo_dt", "data_contato_dt")
    for col in dt_cols:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce")
            df[col] = parsed.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")

    # DATE (vira string padrão)
    d_cols = ("data_matricula_d", "data_nascimento_d")
    for col in d_cols:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce")
            df[col] = parsed.dt.strftime("%Y-%m-%d").fillna("")

    return df


# ============================================================
# LEADS
# ============================================================
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


# ============================================================
# KPIs
# ============================================================
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
      SELECT status, COUNT(*) AS cnt
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


# ============================================================
# OPTIONS
# ============================================================
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
    return [str(r.get("v")) for r in rows if r.get("v")]


def query_options() -> Dict[str, List[str]]:
    return {
        "status": _distinct("status"),
        "curso": _distinct("curso"),
        "polo": _distinct("polo"),
        "origem": _distinct("origem"),
    }


# ============================================================
# UPLOAD (CSV BYTES, SCHEMA FIXO) + PROMOTE
# ============================================================
def ingest_upload_file(file_storage, source: str = "UPLOAD_PAINEL") -> Dict[str, Any]:
    from google.cloud import bigquery

    filename = (file_storage.filename or "").lower()
    project = _env("GCP_PROJECT_ID")
    dataset = _env("BQ_DATASET")

    upload_table = _env("BQ_UPLOAD_TABLE", "stg_leads_upload")
    promote_proc = _env("BQ_PROMOTE_PROC", "sp_promote_stg_leads_upload")

    stg_table_id = f"{project}.{dataset}.{upload_table}"
    proc_id = f"{project}.{dataset}.{promote_proc}"

    # ---------- READ ----------
    if filename.endswith(".csv"):
        df = pd.read_csv(file_storage, dtype=str, sep=None, engine="python")
    elif filename.endswith(".xlsx") or filename.endswith(".xls"):
        df = pd.read_excel(file_storage, dtype=str)
    else:
        raise ValueError("Formato inválido. Envie CSV ou XLSX.")

    # ---------- NORMALIZE ----------
    df = _normalize_cols(df)

    # booleanos (mantém compatível com BOOL no schema)
    for col in ("matriculado", "inscrito", "ativo"):
        if col in df.columns:
            df[col] = df[col].apply(_to_bool)

    # datas -> string padrão (para não quebrar e bater no schema)
    df = _normalize_datetime_cols(df)

    # origem_upload + data_ingestao (TIMESTAMP)
    df["origem_upload"] = source
    df["data_ingestao"] = datetime.utcnow().isoformat()

    # limpeza final (strings)
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].apply(_clean_str)

    # ---------- DF -> CSV BYTES ----------
    csv_bytes = df.to_csv(index=False, encoding="utf-8").encode("utf-8")

    # ---------- SCHEMA FIXO (evita autodetect conflitante) ----------
    schema = [
        bigquery.SchemaField("origem", "STRING"),
        bigquery.SchemaField("polo", "STRING"),
        bigquery.SchemaField("tipo_negocio", "STRING"),
        bigquery.SchemaField("curso", "STRING"),
        bigquery.SchemaField("modalidade", "STRING"),
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("cpf", "STRING"),
        bigquery.SchemaField("celular", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("endereco", "STRING"),
        bigquery.SchemaField("convenio", "STRING"),
        bigquery.SchemaField("empresa_conveniada", "STRING"),
        bigquery.SchemaField("voucher", "STRING"),
        bigquery.SchemaField("campanha", "STRING"),
        bigquery.SchemaField("consultor", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("obs", "STRING"),
        bigquery.SchemaField("peca_disparo", "STRING"),
        bigquery.SchemaField("texto_disparo", "STRING"),
        bigquery.SchemaField("consultor_disparo", "STRING"),
        bigquery.SchemaField("tipo_disparo", "STRING"),
        bigquery.SchemaField("matriculado", "BOOL"),
        bigquery.SchemaField("inscrito", "BOOL"),
        bigquery.SchemaField("data_envio_dt", "DATETIME"),
        bigquery.SchemaField("data_inscricao_dt", "DATETIME"),
        bigquery.SchemaField("data_disparo_dt", "DATETIME"),
        bigquery.SchemaField_toggle("""data_contato_dt""", "DATETIME") if False else bigquery.SchemaField("data_contato_dt", "DATETIME"),
        bigquery.SchemaField("data_matricula_d", "DATE"),
        bigquery.SchemaField("data_nascimento_d", "DATE"),
        bigquery.SchemaField("origem_upload", "STRING"),
        bigquery.SchemaField("data_ingestao", "TIMESTAMP"),
    ]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        schema=schema,
        autodetect=False,
    )

    client = _client()

    load_job = client.load_table_from_file(
        io.BytesIO(csv_bytes),
        stg_table_id,
        job_config=job_config,
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

# ============================================================
# DEBUG (opcional)
# ============================================================
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
