# services/bigquery.py
# =========================
# V14 — Staging (WRITE_TRUNCATE) -> CALL SP -> Query f_lead + LEFT JOIN dims
# + query_options() direto nas dimensões
# + FILTROS MULTI (IN UNNEST) para: status/curso/polo/consultor
# + EXPORT ROWS (server-side) para endpoint /api/export
# =========================

import os
from typing import Any, Dict, List, Optional, Iterable

from google.cloud import bigquery


# =========================
# ENV (Cloud Run)
# =========================
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
BQ_DATASET = os.getenv("BQ_DATASET", "modelo_estrela")

# IMPORTANTE:
# - BigQuery usa location do DATASET (normalmente "US" ou "EU").
# - "us-central1" é região do Cloud Run, NÃO do BigQuery.
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

BQ_STAGING_TABLE = os.getenv("BQ_STAGING_TABLE", "stg_leads_site")
BQ_FACT_TABLE = os.getenv("BQ_FACT_TABLE", "f_lead")
BQ_PROCEDURE = os.getenv("BQ_PROCEDURE", "sp_v14_carga_consolidada")

DEFAULT_LIMIT = int(os.getenv("BQ_DEFAULT_LIMIT", "200"))
MAX_LIMIT = int(os.getenv("BQ_MAX_LIMIT", "2000"))

# Export safety
EXPORT_MAX_ROWS = int(os.getenv("BQ_EXPORT_MAX_ROWS", "50000"))  # trava de segurança
EXPORT_PAGE_SIZE = int(os.getenv("BQ_EXPORT_PAGE_SIZE", "5000"))

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
    Aceita:
      - lista/tupla
      - string "A || B || C"
      - string "A,B,C"
      - string única
    Retorna lista de strings não vazias.
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
    Dispara a procedure V14.
    """
    client = get_bq_client()
    sql = f"CALL `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PROCEDURE}`();"
    client.query(sql).result()


# =========================
# SQL BASE (JOINs)
# =========================
def _base_select_sql() -> str:
    return f"""
    FROM {_tbl(BQ_FACT_TABLE)} f
    LEFT JOIN {_tbl("dim_pessoa")}     dpe ON dpe.sk_pessoa     = f.sk_pessoa
    LEFT JOIN {_tbl("dim_curso")}      dc  ON dc.sk_curso       = f.sk_curso
    LEFT JOIN {_tbl("dim_polo")}       dp  ON dp.sk_polo        = f.sk_polo
    LEFT JOIN {_tbl("dim_consultor")}  dco ON dco.sk_consultor  = f.sk_consultor
    LEFT JOIN {_tbl("dim_status")}     ds  ON ds.sk_status      = f.sk_status
    WHERE 1=1
    """


def _apply_filters(sql: str, filters: Dict[str, Any], params: List[bigquery.QueryParameter]) -> str:
    """
    Aplica filtros no SQL + adiciona QueryParameters.

    Multi-select: status/curso/polo/consultor via IN UNNEST(@array)
    Dimensões reais (modelo_estrela):
      - dim_curso.nome_curso
      - dim_polo.polo_original
      - dim_consultor.consultor_original
      - dim_status.status_original
    """
    # MULTI (arrays)
    cursos = _as_list(filters.get("curso"))
    polos = _as_list(filters.get("polo"))
    status_list = _as_list(filters.get("status"))
    consultores = _as_list(filters.get("consultor"))

    if cursos:
        sql += " AND dc.nome_curso IN UNNEST(@cursos)"
        params.append(bigquery.ArrayQueryParameter("cursos", "STRING", cursos))

    if polos:
        sql += " AND dp.polo_original IN UNNEST(@polos)"
        params.append(bigquery.ArrayQueryParameter("polos", "STRING", polos))

    if status_list:
        sql += " AND ds.status_original IN UNNEST(@status_list)"
        params.append(bigquery.ArrayQueryParameter("status_list", "STRING", status_list))

    if consultores:
        sql += " AND dco.consultor_original IN UNNEST(@consultores)"
        params.append(bigquery.ArrayQueryParameter("consultores", "STRING", consultores))

    # SINGLE
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

    # DATE RANGE (data_inscricao_dt é DATETIME no f_lead)
    # Front-end manda YYYY-MM-DD; aqui usamos DATE(f.data_inscricao_dt) pra comparar com DATE.
    if filters.get("data_ini"):
        sql += " AND DATE(f.data_inscricao_dt) >= @data_ini"
        params.append(bigquery.ScalarQueryParameter("data_ini", "DATE", filters["data_ini"]))

    if filters.get("data_fim"):
        sql += " AND DATE(f.data_inscricao_dt) <= @data_fim"
        params.append(bigquery.ScalarQueryParameter("data_fim", "DATE", filters["data_fim"]))

    return sql


# =========================
# 3) QUERY PRINCIPAL (PAINEL)
# =========================
def query_leads(
    filters: Optional[Dict[str, Any]] = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    order_by: str = "data_inscricao_dt",
    order_dir: str = "DESC",
) -> List[Dict[str, Any]]:
    """
    Retorna leads com NOMES (via LEFT JOIN dims).
    Campos retornados são os que o front espera:
      data_inscricao_dt, nome, cpf, celular, email, origem, polo, curso, status, consultor, campanha
    """
    client = get_bq_client()
    filters = filters or {}

    limit = max(1, min(int(limit), MAX_LIMIT))
    offset = max(0, int(offset))
    order_dir = "ASC" if str(order_dir).upper() == "ASC" else "DESC"

    allowed_order = {
        "data_inscricao_dt": "f.data_inscricao_dt",
        "status": "ds.status_original",
        "curso": "dc.nome_curso",
        "polo": "dp.polo_original",
        "consultor": "dco.consultor_original",
        "nome": "dpe.nome",
        "cpf": "dpe.cpf",
    }
    order_expr = allowed_order.get(order_by, "f.data_inscricao_dt")

    sql = """
    SELECT
      f.data_inscricao_dt        AS data_inscricao_dt,
      dpe.nome                  AS nome,
      dpe.cpf                   AS cpf,
      dpe.celular               AS celular,
      dpe.email                 AS email,

      -- compat com o front: "origem"
      f.canal                   AS origem,

      dp.polo_original          AS polo,
      dc.nome_curso             AS curso,
      dco.consultor_original    AS consultor,
      ds.status_original        AS status,

      -- extras (não atrapalham; podem ser usados depois)
      f.campanha                AS campanha
    """ + _base_select_sql()

    params: List[bigquery.QueryParameter] = []
    sql = _apply_filters(sql, filters, params)

    sql += f"\n ORDER BY {order_expr} {order_dir} \n LIMIT @limit OFFSET @offset"
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = client.query(sql, job_config=job_config).result()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(dict(r))
    return out


def query_leads_count(filters: Optional[Dict[str, Any]] = None) -> int:
    """
    Retorna total de registros para paginação (mesmos filtros do query_leads).
    """
    client = get_bq_client()
    filters = filters or {}

    sql = "SELECT COUNT(1) AS total " + _base_select_sql()
    params: List[bigquery.QueryParameter] = []
    sql = _apply_filters(sql, filters, params)

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(sql, job_config=job_config).result())
    if not rows:
        return 0
    return int(rows[0]["total"])


# =========================
# 4) OPTIONS (FILTROS)
# =========================
def _distinct_dim_values(table: str, col: str, alias: str) -> List[str]:
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
    Opções para filtros diretamente das dimensões (nomes reais do modelo_estrela).
    """
    return {
        "status": _distinct_dim_values("dim_status", "status_original", "status"),
        "cursos": _distinct_dim_values("dim_curso", "nome_curso", "curso"),
        "polos": _distinct_dim_values("dim_polo", "polo_original", "polo"),
        "consultores": _distinct_dim_values("dim_consultor", "consultor_original", "consultor"),
    }


# =========================
# 5) EXPORT (server-side)
# =========================
EXPORT_COLUMNS = [
    ("data_inscricao_dt", "Data Inscrição"),
    ("nome", "Candidato"),
    ("cpf", "CPF"),
    ("celular", "Celular"),
    ("email", "Email"),
    ("origem", "Origem"),
    ("polo", "Polo"),
    ("curso", "Curso"),
    ("status", "Status"),
    ("consultor", "Consultor"),
    ("campanha", "Campanha"),
]


def export_leads_rows(
    filters: Optional[Dict[str, Any]] = None,
    max_rows: int = EXPORT_MAX_ROWS
) -> Iterable[Dict[str, Any]]:
    """
    Itera linhas para export com mesma lógica de filtros.
    Trava max_rows pra não estourar custo/memória.
    """
    client = get_bq_client()
    filters = filters or {}

    max_rows = max(1, min(int(max_rows), EXPORT_MAX_ROWS))

    sql = """
    SELECT
      f.data_inscricao_dt        AS data_inscricao_dt,
      dpe.nome                  AS nome,
      dpe.cpf                   AS cpf,
      dpe.celular               AS celular,
      dpe.email                 AS email,
      f.canal                   AS origem,
      dp.polo_original          AS polo,
      dc.nome_curso             AS curso,
      ds.status_original        AS status,
      dco.consultor_original    AS consultor,
      f.campanha                AS campanha
    """ + _base_select_sql()

    params: List[bigquery.QueryParameter] = []
    sql = _apply_filters(sql, filters, params)

    sql += "\n ORDER BY f.data_inscricao_dt DESC"

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    job = client.query(sql, job_config=job_config)

    yielded = 0
    for page in job.result(page_size=EXPORT_PAGE_SIZE).pages:
        for row in page:
            yield dict(row)
            yielded += 1
            if yielded >= max_rows:
                return


# =========================
# 6) PIPELINE UPLOAD
# =========================
def process_upload_dataframe(df) -> None:
    """
    Pipeline completo do V14:
      1) load_to_staging(df) com TRUNCATE
      2) run_procedure() para consolidar em dims + fato
    """
    load_to_staging(df)
    run_procedure()
