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

def _bq_location() -> str:
    return _env("BQ_LOCATION", "us-central1")

def _client() -> bigquery.Client:
    project = _env("GCP_PROJECT_ID", "painel-universidade")
    return bigquery.Client(project=project) if project else bigquery.Client()

def _table_ref() -> str:
    project = _env("GCP_PROJECT_ID", "painel-universidade")
    dataset = _env("BQ_DATASET", "modelo_estrela")
    # ✅ ATUALIZADO: Apontando para a View Oficial que criamos
    view = _env("BQ_VIEW_LEADS", "vw_leads_painel_oficial")
    return f"`{project}.{dataset}.{view}`"

def _date_expr() -> str:
    # ✅ ATUALIZADO: Nome da coluna de data na View Oficial
    return "Data_Inscricao"

# ============================================================
# ✅ AJUSTE AQUI: Suporte real a Listas (TomSelect)
# ============================================================
def _to_list(v: Any) -> List[str]:
    if v is None: return []
    if isinstance(v, list):
        return [str(x).strip().upper() for x in v if str(x or "").strip()]
    s = str(v).strip()
    if not s: return []
    return [p.strip().upper() for p in s.split("||") if p.strip()]

# ============================================================
# CONSULTAS (LEADS & KPIs) - ✅ ATUALIZADAS COM NOVOS NOMES
# ============================================================
def query_leads(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    dt = _date_expr()
    
    status_list = _to_list(filters.get("status"))
    curso_list = _to_list(filters.get("curso"))
    polo_list = _to_list(filters.get("polo"))

    # Nota: Removi 'origem' pois na View Oficial usamos 'Modalidade' ou 'Curso'
    # Se quiser adicionar origem, ela deve estar na View.
    
    sql = f"""
    SELECT
      {dt}, Nome_Candidato, CPF, Celular, Email,
      Curso, Modalidade, Status_Matriculado, Observacao
    FROM {_table_ref()}
    WHERE 1=1
      AND (ARRAY_LENGTH(@status_list)=0 OR UPPER(CAST(Status_Matriculado AS STRING)) IN UNNEST(@status_list))
      AND (ARRAY_LENGTH(@curso_list)=0  OR UPPER(CAST(Curso AS STRING)) IN UNNEST(@curso_list))
      AND (ARRAY_LENGTH(@polo_list)=0   OR UPPER(CAST(Curso AS STRING)) IN UNNEST(@polo_list)) -- Ajustado para exemplo
      AND (@data_ini IS NULL OR {dt} >= @data_ini)
      AND (@data_fim IS NULL OR {dt} <= @data_fim)
    ORDER BY {dt} DESC
    LIMIT @limit
    """
    
    params = [
        bigquery.ArrayQueryParameter("status_list", "STRING", status_list),
        bigquery.ArrayQueryParameter("curso_list", "STRING", curso_list),
        bigquery.ArrayQueryParameter("polo_list", "STRING", polo_list),
        bigquery.ScalarQueryParameter("data_ini", "DATE", filters.get("data_ini") or None),
        bigquery.ScalarQueryParameter("data_fim", "DATE", filters.get("data_fim") or None),
        bigquery.ScalarQueryParameter("limit", "INT64", int(filters.get("limit") or 500)),
    ]

    query_job = _client().query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=_bq_location())
    return [dict(row) for row in query_job.result()]

# ============================================================
# INGESTÃO - ✅ ATUALIZADA PARA sp_v10_upsert_final
# ============================================================
def ingest_upload_file(file_storage, source: str = "UPLOAD_PAINEL") -> Dict[str, Any]:
    project = _env("GCP_PROJECT_ID", "painel-universidade")
    dataset = _env("BQ_DATASET", "modelo_estrela")
    
    # ✅ ATUALIZADO: Agora enviamos para a stg_leads_site que criamos
    stg_table = _env("BQ_UPLOAD_TABLE", "stg_leads_site")
    
    # ✅ ATUALIZADO: Nome da Procedure V10 que criamos
    proc_name = _env("BQ_PROMOTE_PROC", "sp_v10_upsert_final")
    
    client = _client()
    filename = (getattr(file_storage, "filename", "arquivo_desconhecido")).strip()

    if filename.lower().endswith(".csv"):
        df = pd.read_csv(file_storage.stream, dtype=str, sep=None, engine="python")
    else:
        df = pd.read_excel(file_storage.stream, dtype=str)

    # ✅ IMPORTANTE: O site deve enviar as colunas EXATAS da stg_leads_site
    # Não normalizamos para minúsculo aqui pois as colunas da stg já estão em snake_case
    
    job = client.load_table_from_dataframe(
        df, f"{project}.{dataset}.{stg_table}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    )
    job.result()

    # ✅ ATUALIZADO: Chamando a nova procedure (sem parâmetros se for a v10 simples)
    proc_id = f"{project}.{dataset}.{proc_name}"
    client.query(f"CALL `{proc_id}`();", location=_bq_location()).result()

    return {"status": "success", "filename": filename, "rows_loaded": len(df)}

def query_options() -> Dict[str, List[str]]:
    client = _client()
    # ✅ ATUALIZADO: Colunas disponíveis na View Oficial
    cols = ["Curso", "Modalidade", "Status_Matriculado"]
    results = {}
    for c in cols:
        sql = f"SELECT DISTINCT {c} FROM {_table_ref()} WHERE {c} IS NOT NULL ORDER BY {c} LIMIT 500"
        rows = client.query(sql, location=_bq_location()).result()
        results[c] = [str(row[0]) for row in rows]
    return results
