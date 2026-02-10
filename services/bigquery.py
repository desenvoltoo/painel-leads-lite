# services/bigquery.py
# =========================
# V14 — Staging (WRITE_TRUNCATE) -> CALL SP -> Query f_lead + LEFT JOIN dims
# + query_options() direto nas dimensões (sem NotFound)
# + FILTROS MULTI (IN UNNEST) para: status/curso/polo/consultor
# =========================

import os
from typing import Any, Dict, List, Optional

from google.cloud import bigquery


# =========================
# ENV (Cloud Run)
# =========================
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
BQ_DATASET = os.getenv("BQ_DATASET", "modelo_estrela")
BQ_LOCATION = os.getenv("BQ_LOCATION", "us-central1")

BQ_STAGING_TABLE = os.getenv("BQ_STAGING_TABLE", "stg_leads_site")
BQ_FACT_TABLE = os.getenv("BQ_FACT_TABLE", "f_lead")
BQ_PROCEDURE = os.getenv("BQ_PROCEDURE", "sp_v14_carga_consolidada")

DEFAULT_LIMIT = int(os.getenv("BQ_DEFAULT_LIMIT", "200"))
MAX_LIMIT = int(os.getenv("BQ_MAX_LIMIT", "2000"))

_bq_client: Optional[bigquery.Client] = None


def get_bq_client() -> bigquery.Client:
    """Client singleton (bom pra Cloud Run)."""
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
    return _bq_client


def _tbl(table_name: str) -> str:
    """Fully qualified table name com crases."""
    return f"`{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}`"


def _as_list(v: Any) -> List[str]:
    """
    Normaliza filtros multi vindos do app:
    - None -> []
    - str -> tenta split por '||' (o JS manda "A || B"), e também aceita ','.
    - list/tuple -> lista de strings
    """
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    if "||" in s:
        parts = [p.strip() for p in s.split("||")]
        return [p for p in parts if p]
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        return [p for p in parts if p]
    return [s]


# =========================
# 1) LOAD -> STAGING (WRITE_TRUNCATE)
# =========================
def load_to_staging(df) -> None:
    """
    Carrega DataFrame para a staging (limpa a cada upload).
    - Tabela via ENV: BQ_STAGING_TABLE
    - write_disposition: WRITE_TRUNCATE
    """
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


# =========================
# 2) CALL STORED PROCEDURE (V14)
# =========================
def run_procedure() -> None:
    """
    Dispara: CALL `painel-universidade.modelo_estrela.sp_v14_carga_consolidada`();
    Procedure via ENV: BQ_PROCEDURE
    """
    client = get_bq_client()
    sql = f"CALL `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PROCEDURE}`();"
    job = client.query(sql)
    job.result()


# =========================
# 3) QUERY PRINCIPAL (PAINEL) — MULTI FILTERS
# =========================
def query_leads(
    filters: Optional[Dict[str, Any]] = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    order_by: str = "data_inscricao_dt",
    order_dir: str = "DESC",
) -> List[Dict[str, Any]]:
    """
    Retorna leads com NOMES (via LEFT JOIN dims), sem exibir IDs numéricos.

    filters (opcionais):
      - status, curso, polo, consultor  -> aceita string "A || B" ou lista ["A","B"]
      - cpf, celular, email, nome (busca parcial)
      - data_ini, data_fim (filtra por data_inscricao_dt)
    """
    client = get_bq_client()
    filters = filters or {}

    # limites e paginação
    limit = max(1, min(int(limit), MAX_LIMIT))
    offset = max(0, int(offset))
    order_dir = "ASC" if str(order_dir).upper() == "ASC" else "DESC"

    # allowlist de ordenação (segurança)
    allowed_order = {
        "data_inscricao_dt": "f.data_inscricao_dt",
        "data_envio_dt": "f.data_envio_dt",
        "data_disparo_dt": "f.data_disparo_dt",
        "status": "ds.status",
        "curso": "dc.curso",
        "polo": "dp.polo",
        "consultor": "dco.consultor",
        "nome": "dpe.nome",
    }
    order_expr = allowed_order.get(order_by, "f.data_inscricao_dt")

    sql = f"""
    SELECT
      -- Pessoa
      dpe.nome            AS nome,
      dpe.cpf             AS cpf,
      dpe.celular         AS celular,
      dpe.email           AS email,

      -- Dimensões
      dc.curso            AS curso,
      dp.polo             AS polo,
      dco.consultor       AS consultor,
      ds.status           AS status,

      -- Fato (campos operacionais)
      f.data_inscricao_dt AS data_inscricao_dt,
      f.modalidade        AS modalidade,
      f.campanha          AS campanha,
      f.tipo_negocio      AS tipo_negocio,
      f.origem            AS origem,
      f.peca_disparo      AS peca_disparo,
      f.texto_disparo     AS texto_disparo,
      f.tipo_disparo      AS tipo_disparo,
      f.data_disparo_dt   AS data_disparo_dt,
      f.data_envio_dt     AS data_envio_dt,
      f.obs               AS obs
    FROM {_tbl(BQ_FACT_TABLE)} f
    LEFT JOIN {_tbl("dim_pessoa")}     dpe ON dpe.sk_pessoa     = f.sk_pessoa
    LEFT JOIN {_tbl("dim_curso")}      dc  ON dc.sk_curso       = f.sk_curso
    LEFT JOIN {_tbl("dim_polo")}       dp  ON dp.sk_polo        = f.sk_polo
    LEFT JOIN {_tbl("dim_consultor")}  dco ON dco.sk_consultor  = f.sk_consultor
    LEFT JOIN {_tbl("dim_status")}     ds  ON ds.sk_status      = f.sk_status
    WHERE 1=1
    """

    params: List[bigquery.QueryParameter] = []

    # ---- MULTI FILTERS (ARRAY<STRING>) ----
    cursos = _as_list(filters.get("curso"))
    polos = _as_list(filters.get("polo"))
    status_list = _as_list(filters.get("status"))
    consultores = _as_list(filters.get("consultor"))

    if cursos:
        sql += " AND dc.curso IN UNNEST(@cursos)"
        params.append(bigquery.ArrayQueryParameter("cursos", "STRING", cursos))

    if polos:
        sql += " AND dp.polo IN UNNEST(@polos)"
        params.append(bigquery.ArrayQueryParameter("polos", "STRING", polos))

    if status_list:
        sql += " AND ds.status IN UNNEST(@status_list)"
        params.append(bigquery.ArrayQueryParameter("status_list", "STRING", status_list))

    if consultores:
        sql += " AND dco.consultor IN UNNEST(@consultores)"
        params.append(bigquery.ArrayQueryParameter("consultores", "STRING", consultores))

    # ---- SINGLE filters ----
    if filters.get("cpf"):
        sql += " AND dpe.cpf = @cpf"
        params.append(bigquery.ScalarQueryParameter("cpf", "STRING", str(filters["cpf"]).strip()))

    if filters.get("celular"):
        sql += " AND dpe.celular = @celular"
        params.append(bigquery.ScalarQueryParameter("celular", "STRING", str(filters["celular"]).strip()))

    if filters.get("email"):
        sql += " AND LOWER(dpe.email) = LOWER(@email)"
        params.append(bigquery.ScalarQueryParameter("email", "STRING", str(filters["email"]).strip()))

    if filters.get("nome"):
        sql += " AND LOWER(dpe.nome) LIKE LOWER(@nome_like)"
        params.append(bigquery.ScalarQueryParameter("nome_like", "STRING", f"%{str(filters['nome']).strip()}%"))

    # ---- date range ----
    if filters.get("data_ini"):
        sql += " AND f.data_inscricao_dt >= @data_ini"
        params.append(bigquery.ScalarQueryParameter("data_ini", "DATE", filters["data_ini"]))

    if filters.get("data_fim"):
        sql += " AND f.data_inscricao_dt <= @data_fim"
        params.append(bigquery.ScalarQueryParameter("data_fim", "DATE", filters["data_fim"]))

    sql += f" ORDER BY {order_expr} {order_dir} LIMIT @limit OFFSET @offset"

    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = client.query(sql, job_config=job_config).result()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(dict(r.items()))
    return out


def query_leads_count(filters: Optional[Dict[str, Any]] = None) -> int:
    """
    Retorna total de registros para paginação (mesmos filtros do query_leads).
    Respeita filtros MULTI via IN UNNEST.
    """
    client = get_bq_client()
    filters = filters or {}

    sql = f"""
    SELECT COUNT(1) AS total
    FROM {_tbl(BQ_FACT_TABLE)} f
    LEFT JOIN {_tbl("dim_pessoa")}     dpe ON dpe.sk_pessoa     = f.sk_pessoa
    LEFT JOIN {_tbl("dim_curso")}      dc  ON dc.sk_curso       = f.sk_curso
    LEFT JOIN {_tbl("dim_polo")}       dp  ON dp.sk_polo        = f.sk_polo
    LEFT JOIN {_tbl("dim_consultor")}  dco ON dco.sk_consultor  = f.sk_consultor
    LEFT JOIN {_tbl("dim_status")}     ds  ON ds.sk_status      = f.sk_status
    WHERE 1=1
    """

    params: List[bigquery.QueryParameter] = []

    cursos = _as_list(filters.get("curso"))
    polos = _as_list(filters.get("polo"))
    status_list = _as_list(filters.get("status"))
    consultores = _as_list(filters.get("consultor"))

    if cursos:
        sql += " AND dc.curso IN UNNEST(@cursos)"
        params.append(bigquery.ArrayQueryParameter("cursos", "STRING", cursos))

    if polos:
        sql += " AND dp.polo IN UNNEST(@polos)"
        params.append(bigquery.ArrayQueryParameter("polos", "STRING", polos))

    if status_list:
        sql += " AND ds.status IN UNNEST(@status_list)"
        params.append(bigquery.ArrayQueryParameter("status_list", "STRING", status_list))

    if consultores:
        sql += " AND dco.consultor IN UNNEST(@consultores)"
        params.append(bigquery.ArrayQueryParameter("consultores", "STRING", consultores))

    if filters.get("cpf"):
        sql += " AND dpe.cpf = @cpf"
        params.append(bigquery.ScalarQueryParameter("cpf", "STRING", str(filters["cpf"]).strip()))

    if filters.get("celular"):
        sql += " AND dpe.celular = @celular"
        params.append(bigquery.ScalarQueryParameter("celular", "STRING", str(filters["celular"]).strip()))

    if filters.get("email"):
        sql += " AND LOWER(dpe.email) = LOWER(@email)"
        params.append(bigquery.ScalarQueryParameter("email", "STRING", str(filters["email"]).strip()))

    if filters.get("nome"):
        sql += " AND LOWER(dpe.nome) LIKE LOWER(@nome_like)"
        params.append(bigquery.ScalarQueryParameter("nome_like", "STRING", f"%{str(filters['nome']).strip()}%"))

    if filters.get("data_ini"):
        sql += " AND f.data_inscricao_dt >= @data_ini"
        params.append(bigquery.ScalarQueryParameter("data_ini", "DATE", filters["data_ini"]))

    if filters.get("data_fim"):
        sql += " AND f.data_inscricao_dt <= @data_fim"
        params.append(bigquery.ScalarQueryParameter("data_fim", "DATE", filters["data_fim"]))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(sql, job_config=job_config).result())
    return int(rows[0]["total"]) if rows else 0


# =========================
# 4) OPTIONS (FILTROS) DIRETO NAS DIMs
# =========================
def _distinct_dim_values(table: str, col: str, alias: str) -> List[str]:
    """Pega SELECT DISTINCT de uma coluna de uma dimensão."""
    client = get_bq_client()
    sql = f"""
    SELECT DISTINCT {col} AS {alias}
    FROM {_tbl(table)}
    WHERE {col} IS NOT NULL AND TRIM(CAST({col} AS STRING)) != ''
    ORDER BY {alias}
    """
    rows = client.query(sql).result()
    return [str(r[alias]) for r in rows]


def query_options() -> Dict[str, List[str]]:
    """
    Retorna opções para filtros diretamente das dimensões.
    - status: dim_status.status
    - cursos: dim_curso.curso
    - polos: dim_polo.polo
    - consultores: dim_consultor.consultor
    """
    return {
        "status": _distinct_dim_values("dim_status", "status", "status"),
        "cursos": _distinct_dim_values("dim_curso", "curso", "curso"),
        "polos": _distinct_dim_values("dim_polo", "polo", "polo"),
        "consultores": _distinct_dim_values("dim_consultor", "consultor", "consultor"),
    }


# =========================
# HELPER: upload end-to-end
# =========================
def process_upload_dataframe(df) -> None:
    """
    Pipeline completo do V14:
      1) load_to_staging(df) com TRUNCATE
      2) run_procedure() para consolidar em dims + fato
    """
    load_to_staging(df)
    run_procedure()
