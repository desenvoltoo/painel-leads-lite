# -*- coding: utf-8 -*-
import os
from typing import Any, Dict, List
import pandas as pd
from google.cloud import bigquery

# ============================================================
# ENV / CLIENT (À PROVA DE CLOUD RUN)
# ============================================================
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    v = v.strip() if isinstance(v, str) else v
    return v if v else default

def _require_env(names: List[str]) -> Dict[str, str]:
    values = {n: _env(n, "") for n in names}
    missing = [n for n, v in values.items() if not v]
    if missing:
        raise RuntimeError(f"ENV obrigatórias faltando: {missing}")
    return values

def _bq_location() -> str:
    return _env("BQ_LOCATION", "us-central1")

def _client() -> bigquery.Client:
    project = _env("GCP_PROJECT_ID", "painel-universidade")
    return bigquery.Client(project=project) if project else bigquery.Client()

def _table_ref() -> str:
    project = _env("GCP_PROJECT_ID", "painel-universidade")
    dataset = _env("BQ_DATASET", "modelo_estrela")
    view = _env("BQ_VIEW_LEADS", "vw_leads_painel_lite") # Usaremos a View Lite que reflete a Fato
    return f"`{project}.{dataset}.{view}`"

def _date_expr() -> str:
    # No novo modelo, a View Lite já expõe 'data_inscricao' formatada
    return "data_inscricao"

# ============================================================
# Helpers: multi-filtro
# ============================================================
def _split_multi(v: Any) -> List[str]:
    if v is None: return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x or "").strip()]
    s = str(v).strip()
    if not s: return []
    return [p.strip() for p in s.split("||") if p.strip()]

def _upper_list(v: Any) -> List[str]:
    arr = _split_multi(v)
    return [x.upper() for x in arr]

# ============================================================
# NORMALIZAÇÃO (upload)
# ============================================================
def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def _to_bool(v):
    if v is None: return None
    s = str(v).strip().lower()
    if s in ("true", "t", "1", "sim", "s", "yes", "y"): return True
    if s in ("false", "f", "0", "nao", "não", "n", "no"): return False
    return None

# ============================================================
# CONSULTAS (LEADS & KPIs)
# ============================================================
def query_leads(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    dt = _date_expr()
    sql = f"""
    SELECT
      {dt}, nome, cpf, celular, email,
      origem, polo, curso, status, consultor
    FROM {_table_ref()}
    WHERE 1=1
      AND (ARRAY_LENGTH(@status_list)=0 OR UPPER(CAST(status AS STRING)) IN UNNEST(@status_list))
      AND (ARRAY_LENGTH(@curso_list)=0  OR UPPER(CAST(curso  AS STRING)) IN UNNEST(@curso_list))
      AND (ARRAY_LENGTH(@polo_list)=0   OR UPPER(CAST(polo   AS STRING)) IN UNNEST(@polo_list))
      AND (ARRAY_LENGTH(@origem_list)=0 OR UPPER(CAST(origem AS STRING)) IN UNNEST(@origem_list))
      AND (@data_ini IS NULL OR {dt} >= @data_ini)
      AND (@data_fim IS NULL OR {dt} <= @data_fim)
    ORDER BY {dt} DESC, data_ingestao DESC
    LIMIT @limit
    """
    
    params = [
        bigquery.ArrayQueryParameter("status_list", "STRING", _upper_list(filters.get("status"))),
        bigquery.ArrayQueryParameter("curso_list", "STRING", _upper_list(filters.get("curso"))),
        bigquery.ArrayQueryParameter("polo_list", "STRING", _upper_list(filters.get("polo"))),
        bigquery.ArrayQueryParameter("origem_list", "STRING", _upper_list(filters.get("origem"))),
        bigquery.ScalarQueryParameter("data_ini", "DATE", filters.get("data_ini") or None),
        bigquery.ScalarQueryParameter("data_fim", "DATE", filters.get("data_fim") or None),
        bigquery.ScalarQueryParameter("limit", "INT64", int(filters.get("limit") or 500)),
    ]

    rows = _client().query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=_bq_location()).result()
    return [dict(row) for row in rows]

def query_kpis(filters: Dict[str, Any]) -> Dict[str, Any]:
    dt = _date_expr()
    sql = f"""
    WITH base AS (
      SELECT {dt} AS data_inscricao, status 
      FROM {_table_ref()}
      WHERE 1=1
        AND (ARRAY_LENGTH(@status_list)=0 OR UPPER(CAST(status AS STRING)) IN UNNEST(@status_list))
        AND (ARRAY_LENGTH(@curso_list)=0  OR UPPER(CAST(curso  AS STRING)) IN UNNEST(@curso_list))
        AND (ARRAY_LENGTH(@polo_list)=0   OR UPPER(CAST(polo   AS STRING)) IN UNNEST(@polo_list))
        AND (ARRAY_LENGTH(@origem_list)=0 OR UPPER(CAST(origem AS STRING)) IN UNNEST(@origem_list))
        AND (@data_ini IS NULL OR {dt} >= @data_ini)
        AND (@data_fim IS NULL OR {dt} <= @data_fim)
    ),
    agg AS ( SELECT status, COUNT(*) cnt FROM base GROUP BY status )
    SELECT
      (SELECT COUNT(*) FROM base) AS total,
      (SELECT MAX(data_inscricao) FROM base) AS last_date,
      (SELECT AS STRUCT status, cnt FROM agg ORDER BY cnt DESC LIMIT 1) AS top_status
    """
    
    params = [
        bigquery.ArrayQueryParameter("status_list", "STRING", _upper_list(filters.get("status"))),
        bigquery.ArrayQueryParameter("curso_list", "STRING", _upper_list(filters.get("curso"))),
        bigquery.ArrayQueryParameter("polo_list", "STRING", _upper_list(filters.get("polo"))),
        bigquery.ArrayQueryParameter("origem_list", "STRING", _upper_list(filters.get("origem"))),
        bigquery.ScalarQueryParameter("data_ini", "DATE", filters.get("data_ini") or None),
        bigquery.ScalarQueryParameter("data_fim", "DATE", filters.get("data_fim") or None),
    ]

    res = next(iter(_client().query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=_bq_location()).result()), {})
    top = res.get("top_status")
    return {
        "total": int(res.get("total") or 0),
        "last_date": str(res.get("last_date")) if res.get("last_date") else None,
        "top_status": top.get("status") if top else "N/A"
    }

def query_options() -> Dict[str, List[str]]:
    client = _client()
    cols = ["status", "curso", "polo", "origem"]
    results = {}
    for c in cols:
        sql = f"SELECT DISTINCT {c} FROM {_table_ref()} WHERE {c} IS NOT NULL ORDER BY {c} LIMIT 1000"
        rows = client.query(sql, location=_bq_location()).result()
        results[c] = [str(row[0]) for row in rows]
    return results

# ============================================================
# UPLOAD + PIPELINE OTIMIZADA
# ============================================================
def ingest_upload_file(file_storage, source: str = "UPLOAD_PAINEL") -> Dict[str, Any]:
    # Configurações de destino
    project = _env("GCP_PROJECT_ID", "painel-universidade")
    dataset = _env("BQ_DATASET", "modelo_estrela")
    stg_table = _env("BQ_UPLOAD_TABLE", "stg_leads_upload")
    proc_name = _env("BQ_PROMOTE_PROC", "sp_v9_run_pipeline") # Procedure mestra
    
    client = _client()
    filename = (getattr(file_storage, "filename", "arquivo_desconhecido")).strip()

    # 1. Carregar DataFrame
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(file_storage.stream, dtype=str, sep=None, engine="python")
    else:
        df = pd.read_excel(file_storage.stream, dtype=str)

    # 2. Normalização básica para bater com o Schema da Staging
    df = _normalize_cols(df)
    df["origem_upload"] = source
    df["data_ingestao"] = pd.Timestamp.utcnow()
    
    # 3. Enviar para BigQuery Staging (Truncate para garantir limpeza)
    job = client.load_table_from_dataframe(
        df, f"{project}.{dataset}.{stg_table}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()

    # 4. Chamar a Procedure de Tratamento (AMARRAÇÃO DAS SKs)
    proc_id = f"{project}.{dataset}.{proc_name}"
    client.query(
        f"CALL `{proc_id}`(@fn);",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("fn", "STRING", filename)]
        ),
        location=_bq_location()
    ).result()

    return {"status": "success", "filename": filename, "rows": len(df)}

# ============================================================
# DEBUG
# ============================================================
def debug_count() -> int:
    sql = f"SELECT COUNT(1) c FROM {_table_ref()}"
    row = next(iter(_client().query(sql, location=_bq_location()).result()), None)
    return int(row.get("c") or 0) if row else 0

def debug_sample(limit: int = 5):
    sql = f"SELECT * FROM {_table_ref()} LIMIT {limit}"
    rows = _client().query(sql, location=_bq_location()).result()
    return [dict(row) for row in rows]
