# -*- coding: utf-8 -*-
import os
from typing import Any, Dict, List
import pandas as pd
from google.cloud import bigquery

# ============================================================
# ENV / CLIENT
# ============================================================
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    v = v.strip() if isinstance(v, str) else v
    return v if v else default

def _bq_location() -> str:
    return _env("BQ_LOCATION", "us-central1")

def _client() -> bigquery.Client:
    project = _env("GCP_PROJECT_ID", "painel-universidade")
    return bigquery.Client(project=project) if project else bigquery.Client()

def _to_list(v: Any) -> List[str]:
    if v is None: return []
    if isinstance(v, list):
        return [str(x).strip().upper() for x in v if str(x or "").strip()]
    s = str(v).strip()
    if not s: return []
    return [p.strip().upper() for p in s.split("||") if p.strip()]

# ============================================================
# CONSULTAS (LEADS) - CONSOLIDADO V14
# ============================================================
def query_leads(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    project = _env("GCP_PROJECT_ID", "painel-universidade")
    dataset = _env("BQ_DATASET", "modelo_estrela")
    
    status_list = _to_list(filters.get("status"))
    curso_list = _to_list(filters.get("curso"))
    polo_list = _to_list(filters.get("polo"))

    # SQL utilizando os JOINs com as novas dimensões
    sql = f"""
    SELECT 
        f.data_inscricao_dt as data,
        p.nome,
        p.cpf,
        p.celular,
        c.nome_curso as curso,
        pol.polo_original as polo,
        cons.consultor_original as consultor,
        st.status_original as status,
        f.canal,
        f.matriculado,
        f.observacao
    FROM `{project}.{dataset}.f_lead` f
    LEFT JOIN `{project}.{dataset}.dim_pessoa` p ON f.sk_pessoa = p.sk_pessoa
    LEFT JOIN `{project}.{dataset}.dim_curso` c ON f.sk_curso = c.sk_curso
    LEFT JOIN `{project}.{dataset}.dim_consultor` cons ON f.sk_consultor = cons.sk_consultor
    LEFT JOIN `{project}.{dataset}.dim_polo` pol ON f.sk_polo = pol.sk_polo
    LEFT JOIN `{project}.{dataset}.dim_status` st ON f.sk_status = st.sk_status
    WHERE 1=1
      AND (ARRAY_LENGTH(@status_list)=0 OR UPPER(st.status_original) IN UNNEST(@status_list))
      AND (ARRAY_LENGTH(@curso_list)=0  OR UPPER(c.nome_curso) IN UNNEST(@curso_list))
      AND (ARRAY_LENGTH(@polo_list)=0   OR UPPER(pol.polo_original) IN UNNEST(@polo_list))
      AND (@data_ini IS NULL OR f.data_inscricao_dt >= @data_ini)
      AND (@data_fim IS NULL OR f.data_inscricao_dt <= @data_fim)
    ORDER BY f.data_inscricao_dt DESC
    LIMIT @limit
    """
    
    params = [
        bigquery.ArrayQueryParameter("status_list", "STRING", status_list),
        bigquery.ArrayQueryParameter("curso_list", "STRING", curso_list),
        bigquery.ArrayQueryParameter("polo_list", "STRING", polo_list),
        bigquery.ScalarQueryParameter("data_ini", "DATETIME", filters.get("data_ini") or None),
        bigquery.ScalarQueryParameter("data_fim", "DATETIME", filters.get("data_fim") or None),
        bigquery.ScalarQueryParameter("limit", "INT64", int(filters.get("limit") or 500)),
    ]

    client = _client()
    query_job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=_bq_location())
    return [dict(row) for row in query_job.result()]

# ============================================================
# INGESTÃO - CHAMANDO A SP_V14_CARGA_CONSOLIDADA
# ============================================================
def ingest_upload_file(file_storage, source: str = "UPLOAD_PAINEL") -> Dict[str, Any]:
    project = _env("GCP_PROJECT_ID", "painel-universidade")
    dataset = _env("BQ_DATASET", "modelo_estrela")
    stg_table = "stg_leads_site"
    
    # Procedure Oficial Consolidada
    proc_name = "sp_v14_carga_consolidada"
    
    client = _client()
    filename = (getattr(file_storage, "filename", "arquivo_desconhecido")).strip()

    if filename.lower().endswith(".csv"):
        df = pd.read_csv(file_storage.stream, dtype=str, sep=None, engine="python")
    else:
        df = pd.read_excel(file_storage.stream, dtype=str)

    # Envia para a staging
    job = client.load_table_from_dataframe(
        df, f"{project}.{dataset}.{stg_table}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()

    # Executa a limpeza e distribuição para as dimensões
    proc_id = f"{project}.{dataset}.{proc_name}"
    client.query(f"CALL `{proc_id}`();", location=_bq_location()).result()

    return {"status": "success", "filename": filename, "rows_loaded": len(df)}

def query_options() -> Dict[str, List[str]]:
    client = _client()
    project = _env("GCP_PROJECT_ID", "painel-universidade")
    dataset = _env("BQ_DATASET", "modelo_estrela")
    
    # Mapeamento para os filtros do App buscarem nas tabelas de dimensão
    opts = {
        "status": f"SELECT DISTINCT status_original FROM `{project}.{dataset}.dim_status` ORDER BY 1",
        "curso": f"SELECT DISTINCT nome_curso FROM `{project}.{dataset}.dim_curso` ORDER BY 1",
        "polo": f"SELECT DISTINCT polo_original FROM `{project}.{dataset}.dim_polo` ORDER BY 1"
    }
    
    results = {}
    for key, sql in opts.items():
        rows = client.query(sql, location=_bq_location()).result()
        results[key] = [str(row[0]) for row in rows]
    return results
