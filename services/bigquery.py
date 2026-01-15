# -*- coding: utf-8 -*-
import os
from typing import Any, Dict, List

import pandas as pd
from google.cloud import bigquery


# ============================================================
# ENV / CLIENT (DEFAULTS SEGUROS)
# ============================================================
DEFAULT_PROJECT = "painel-universidade"
DEFAULT_DATASET = "modelo_estrela"
DEFAULT_VIEW_LEADS = "vw_leads_painel_lite"
# Dataset regional (confirmado: us-central1). Se no futuro seu dataset virar multi-região,
# ajuste via ENV BQ_LOCATION ("US" ou "EU").
DEFAULT_BQ_LOCATION = "us-central1"


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    v = v.strip() if isinstance(v, str) else v
    return v if v else default


def _require_env(names: List[str]) -> Dict[str, str]:
    values = {n: _env(n, "") for n in names}
    missing = [n for n, v in values.items() if not v]
    if missing:
        snapshot = {n: (values.get(n) or None) for n in names}
        raise RuntimeError(f"ENV obrigatórias faltando: {missing} | snapshot={snapshot}")
    return values


def _bq_location() -> str:
    """
    IMPORTANTE:
    - Dataset multi-região: use "US" (ou "EU")
    - Dataset regional: "us-central1" (exemplo)
    Ajuste via env BQ_LOCATION.
    """
    return _env("BQ_LOCATION", DEFAULT_BQ_LOCATION)


def _client() -> bigquery.Client:
    # Default seguro evita cair em project errado quando ENV não está setada.
    project = _env("GCP_PROJECT_ID", DEFAULT_PROJECT)
    return bigquery.Client(project=project) if project else bigquery.Client()


def _table_ref() -> str:
    """
    View de leitura do painel.
    Defaults seguros evitam "tela vazia" por falta de ENV no Cloud Run.
    """
    project = _env("GCP_PROJECT_ID", DEFAULT_PROJECT)
    dataset = _env("BQ_DATASET", DEFAULT_DATASET)
    view = _env("BQ_VIEW_LEADS", DEFAULT_VIEW_LEADS)

    # se por algum motivo project vier vazio, força erro explícito
    if not project:
        _require_env(["GCP_PROJECT_ID"])

    return f"`{project}.{dataset}.{view}`"


def _date_field() -> str:
    # Na view lite normalmente é data_inscricao (DATE)
    return _env("BQ_DATE_FIELD", "data_inscricao")


def _date_expr() -> str:
    """Expressão de data mais resiliente.

    Objetivo: evitar "tela vazia" quando a view expõe data em formatos diferentes.

    - DATE/DATETIME/TIMESTAMP: SAFE_CAST -> DATE
    - STRING: tenta ISO (YYYY-MM-DD) e BR (DD/MM/YYYY)
    """
    f = _date_field()
    s = f"CAST({f} AS STRING)"
    return (
        f"COALESCE("
        f"  SAFE_CAST({f} AS DATE),"
        f"  SAFE.PARSE_DATE('%Y-%m-%d', {s}),"
        f"  SAFE.PARSE_DATE('%d/%m/%Y', {s})"
        f")"
    )


def _norm_list_upper(values: List[str]) -> List[str]:
    out = []
    seen = set()
    for x in values or []:
        s = str(x or "").strip()
        if not s:
            continue
        u = s.upper()
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


# ============================================================
# NORMALIZAÇÃO UPLOAD (mantida)
# ============================================================
def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
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
    return None


def _normalize_datetime_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    dt_cols = ("data_envio_dt", "data_inscricao_dt", "data_disparo_dt", "data_contato_dt")
    for col in dt_cols:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
            df[col] = parsed.dt.tz_convert(None)

    d_cols = ("data_matricula_d", "data_nascimento_d")
    for col in d_cols:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce")
            df[col] = parsed.dt.date

    return df


# ============================================================
# LEADS (MULTI FILTER) — COM DEFAULTS SEGUROS
# ============================================================
def query_leads(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    dt = _date_expr()

    # Compatibilidade: aceita filtros single (curso/polo) e multi (curso_list/polo_list)
    curso_list = filters.get("curso_list") or []
    polo_list = filters.get("polo_list") or []
    curso_single = (filters.get("curso") or "").strip()
    polo_single = (filters.get("polo") or "").strip()
    if curso_single and not curso_list:
        curso_list = [curso_single]
    if polo_single and not polo_list:
        polo_list = [polo_single]

    curso_list_up = _norm_list_upper(curso_list)
    polo_list_up = _norm_list_upper(polo_list)

    sql = f"""
    SELECT
      {dt} AS data_inscricao,
      nome, cpf, celular, email,
      origem, polo, curso, status, consultor
    FROM {_table_ref()}
    WHERE 1=1
      AND (@status IS NULL OR UPPER(TRIM(status)) = UPPER(TRIM(@status)))
      AND (@origem IS NULL OR UPPER(TRIM(origem)) = UPPER(TRIM(@origem)))

      AND (
        ARRAY_LENGTH(@curso_list) = 0
        OR UPPER(TRIM(curso)) IN UNNEST(@curso_list)
      )
      AND (
        ARRAY_LENGTH(@polo_list) = 0
        OR UPPER(TRIM(polo)) IN UNNEST(@polo_list)
      )

      AND (@data_ini IS NULL OR {dt} >= @data_ini)
      AND (@data_fim IS NULL OR {dt} <= @data_fim)
    ORDER BY {dt} DESC
    LIMIT @limit
    """

    data_ini = filters.get("data_ini") or None
    data_fim = filters.get("data_fim") or None

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", filters.get("status") or None),
            bigquery.ScalarQueryParameter("origem", "STRING", filters.get("origem") or None),
            bigquery.ArrayQueryParameter("curso_list", "STRING", curso_list_up),
            bigquery.ArrayQueryParameter("polo_list", "STRING", polo_list_up),
            bigquery.ScalarQueryParameter("data_ini", "DATE", data_ini),
            bigquery.ScalarQueryParameter("data_fim", "DATE", data_fim),
            bigquery.ScalarQueryParameter("limit", "INT64", int(filters.get("limit") or 500)),
        ]
    )

    rows = _client().query(sql, job_config=job_config, location=_bq_location()).result()
    return [{k: r.get(k) for k in r.keys()} for r in rows]


def query_kpis(filters: Dict[str, Any]) -> Dict[str, Any]:
    dt = _date_expr()

    # Compatibilidade: aceita filtros single (curso/polo) e multi (curso_list/polo_list)
    curso_list = filters.get("curso_list") or []
    polo_list = filters.get("polo_list") or []
    curso_single = (filters.get("curso") or "").strip()
    polo_single = (filters.get("polo") or "").strip()
    if curso_single and not curso_list:
        curso_list = [curso_single]
    if polo_single and not polo_list:
        polo_list = [polo_single]

    curso_list_up = _norm_list_upper(curso_list)
    polo_list_up = _norm_list_upper(polo_list)

    sql = f"""
    WITH base AS (
      SELECT
        {dt} AS data_inscricao,
        status, curso, polo, origem
      FROM {_table_ref()}
      WHERE 1=1
        AND (@status IS NULL OR UPPER(TRIM(status)) = UPPER(TRIM(@status)))
        AND (@origem IS NULL OR UPPER(TRIM(origem)) = UPPER(TRIM(@origem)))
        AND (ARRAY_LENGTH(@curso_list) = 0 OR UPPER(TRIM(curso)) IN UNNEST(@curso_list))
        AND (ARRAY_LENGTH(@polo_list)  = 0 OR UPPER(TRIM(polo))  IN UNNEST(@polo_list))
        AND (@data_ini IS NULL OR {dt} >= @data_ini)
        AND (@data_fim IS NULL OR {dt} <= @data_fim)
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

    data_ini = filters.get("data_ini") or None
    data_fim = filters.get("data_fim") or None

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", filters.get("status") or None),
            bigquery.ScalarQueryParameter("origem", "STRING", filters.get("origem") or None),
            bigquery.ArrayQueryParameter("curso_list", "STRING", curso_list_up),
            bigquery.ArrayQueryParameter("polo_list", "STRING", polo_list_up),
            bigquery.ScalarQueryParameter("data_ini", "DATE", data_ini),
            bigquery.ScalarQueryParameter("data_fim", "DATE", data_fim),
        ]
    )

    row = next(iter(_client().query(sql, job_config=job_config, location=_bq_location()).result()), None)
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
def _distinct(column: str, limit: int) -> List[str]:
    sql = f"""
    SELECT DISTINCT TRIM(CAST({column} AS STRING)) v
    FROM {_table_ref()}
    WHERE {column} IS NOT NULL AND TRIM(CAST({column} AS STRING)) != ""
    ORDER BY v
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", int(limit))]
    )
    rows = _client().query(sql, job_config=job_config, location=_bq_location()).result()
    return [str(r.get("v")) for r in rows if r.get("v")]


def query_options() -> Dict[str, List[str]]:
    limit = int(_env("BQ_OPTIONS_LIMIT", "5000"))
    limit = max(1000, min(limit, 200000))
    return {
        "status": _distinct("status", limit),
        "curso": _distinct("curso", limit),
        "polo": _distinct("polo", limit),
        "origem": _distinct("origem", limit),
    }


# ============================================================
# UPLOAD + PIPELINE (mantido)
# ============================================================
def ingest_upload_file(file_storage, source: str = "UPLOAD_PAINEL") -> Dict[str, Any]:
    project = _env("GCP_PROJECT_ID", DEFAULT_PROJECT)
    dataset = _env("BQ_DATASET", DEFAULT_DATASET)
    location = _bq_location()

    upload_table = _env("BQ_UPLOAD_TABLE", "stg_leads_upload")
    pipeline_proc = _env("BQ_PIPELINE_PROC", "sp_v9_run_pipeline")

    # Aqui defaults já cobrem, mas se alguém zerar envs, explodimos com mensagem boa
    if not project or not dataset:
        _require_env(["GCP_PROJECT_ID", "BQ_DATASET"])

    stg_table_id = f"{project}.{dataset}.{upload_table}"
    proc_id = f"{project}.{dataset}.{pipeline_proc}"

    filename = (getattr(file_storage, "filename", "") or "").strip()
    filename_lower = filename.lower()

    client = _client()

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
        bigquery.SchemaField("data_contato_dt", "DATETIME"),
        bigquery.SchemaField("data_matricula_d", "DATE"),
        bigquery.SchemaField("data_nascimento_d", "DATE"),
        bigquery.SchemaField("origem_upload", "STRING"),
        bigquery.SchemaField("data_ingestao", "TIMESTAMP"),
    ]

    schema_cols = [f.name for f in schema]
    dt_cols = {"data_envio_dt", "data_inscricao_dt", "data_disparo_dt", "data_contato_dt"}
    d_cols = {"data_matricula_d", "data_nascimento_d"}
    bool_cols = {"matriculado", "inscrito"}

    def _ensure_schema_cols(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in schema_cols:
            if col not in df.columns:
                if col in dt_cols:
                    df[col] = pd.NaT
                elif col in d_cols:
                    df[col] = pd.NaT
                elif col in bool_cols:
                    df[col] = None
                else:
                    df[col] = ""
        return df[schema_cols]

    def _apply_transformations(df: pd.DataFrame) -> pd.DataFrame:
        df = _normalize_cols(df)

        for col in ("matriculado", "inscrito"):
            if col in df.columns:
                df[col] = df[col].apply(_to_bool)

        df = _normalize_datetime_cols(df)

        df["origem_upload"] = source
        df["data_ingestao"] = pd.Timestamp.utcnow()

        for c in df.columns:
            if c in dt_cols or c in d_cols or c in bool_cols:
                continue
            if df[c].dtype == "object":
                df[c] = df[c].apply(_clean_str)

        return _ensure_schema_cols(df)

    def _load_df(df: pd.DataFrame, write_disposition: str) -> bigquery.LoadJob:
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            schema=schema,
            autodetect=False,
            ignore_unknown_values=True,
        )
        job = client.load_table_from_dataframe(
            df,
            stg_table_id,
            job_config=job_config,
            location=location,
        )
        job.result()
        return job

    rows_loaded_total = 0
    last_load_job_id = None

    if filename_lower.endswith(".csv"):
        chunksize = int(_env("UPLOAD_CHUNKSIZE", "20000"))
        try:
            file_storage.stream.seek(0)
        except Exception:
            pass

        first = True
        for chunk in pd.read_csv(
            file_storage.stream,
            dtype=str,
            sep=None,
            engine="python",
            chunksize=chunksize,
            keep_default_na=False,
        ):
            chunk = _apply_transformations(chunk)
            wd = bigquery.WriteDisposition.WRITE_TRUNCATE if first else bigquery.WriteDisposition.WRITE_APPEND
            job = _load_df(chunk, wd)

            rows_loaded_total += int(job.output_rows or 0)
            last_load_job_id = job.job_id
            first = False

        if first:
            raise ValueError("CSV vazio ou sem linhas válidas para importar.")

    elif filename_lower.endswith(".xlsx") or filename_lower.endswith(".xls"):
        try:
            file_storage.stream.seek(0)
        except Exception:
            pass

        df = pd.read_excel(file_storage.stream, dtype=str, keep_default_na=False)
        df = _apply_transformations(df)

        job = _load_df(df, bigquery.WriteDisposition.WRITE_TRUNCATE)
        rows_loaded_total = int(job.output_rows or 0)
        last_load_job_id = job.job_id

    else:
        raise ValueError("Formato inválido. Envie CSV ou XLSX.")

    pipeline_sql = f"CALL `{proc_id}`(@fn);"
    pipeline_job = client.query(
        pipeline_sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("fn", "STRING", filename)]
        ),
        location=location,
    )
    pipeline_job.result()

    return {
        "staging_table": stg_table_id,
        "rows_loaded": int(rows_loaded_total),
        "load_job_id": last_load_job_id,
        "pipeline_proc": proc_id,
        "pipeline_job_id": pipeline_job.job_id,
        "filename": filename,
        "location": location,
    }


# ============================================================
# DEBUG
# ============================================================
def debug_count() -> int:
    sql = f"SELECT COUNT(1) c FROM {_table_ref()}"
    row = next(iter(_client().query(sql, location=_bq_location()).result()), None)
    return int(row.get("c") or 0) if row else 0


def debug_sample(limit: int = 5):
    sql = f"SELECT * FROM {_table_ref()} LIMIT @limit"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    rows = _client().query(sql, job_config=job_config, location=_bq_location()).result()
    return [{k: r.get(k) for k in r.keys()} for r in rows]
