# services/bigquery.py
# =========================
# V14 — Staging (WRITE_TRUNCATE) -> CALL SP -> Query f_lead + LEFT JOIN dims
# + query_options() direto nas dimensões
# + FILTROS MULTI (IN UNNEST) para: status/curso/polo/consultor
# + EXPORT CSV (server-side)
# =========================

import os
from typing import Any, Dict, List, Optional, Iterable

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

# Export safety
EXPORT_MAX_ROWS = int(os.getenv("BQ_EXPORT_MAX_ROWS", "50000"))  # trava de segurança
EXPORT_PAGE_SIZE = int(os.getenv("BQ_EXPORT_PAGE_SIZE", "5000"))

_bq_client: Optional[bigquery.Client] = None


def get_bq_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
    return _bq_client


def _tbl(table_name: str) -> str:
    return f"`{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}`"


def _as_list(v: Any) -> List[str]:
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
    # MULTI
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

    # DATE RANGE
    if filters.get("data_ini"):
        sql += " AND f.data_inscricao_dt >= @data_ini"
        params.append(bigquery.ScalarQueryParameter("data_ini", "DATE", filters["data_ini"]))
    if filters.get("data_fim"):
        sql += " AND f.data_inscricao_dt <= @data_fim"
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
    client = get_bq_client()
    filters = filters or {}

    limit = max(1, min(int(limit), MAX_LIMIT))
    offset = max(0, int(offset))
    order_dir = "ASC" if str(order_dir).upper() == "ASC" else "DESC"

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

    sql = """
    SELECT
      dpe.nome            AS nome,
      dpe.cpf             AS cpf,
      dpe.celular         AS celular,
      dpe.email           AS email,
      dc.curso            AS curso,
      dp.polo             AS polo,
      dco.consultor       AS consultor,
      ds.status           AS status,
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
    """ + _base_select_sql()

    params: List[bigquery.QueryParameter] = []
    sql = _apply_filters(sql, filters, params)

    sql += f" ORDER BY {order_expr} {order_dir} LIMIT @limit OFFSET @offset"
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = client.query(sql, job_config=job_config).result()

    return [dict(r.items()) for r in rows]


def query_leads_count(filters: Optional[Dict[str, Any]] = None) -> int:
    client = get_bq_client()
    filters = filters or {}

    sql = "SELECT COUNT(1) AS total " + _base_select_sql()
    params: List[bigquery.QueryParameter] = []
    sql = _apply_filters(sql, filters, params)

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(sql, job_config=job_config).result())
    return int(rows[0]["total"]) if rows else 0


# =========================
# 4) OPTIONS
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
    return {
        "status": _distinct_dim_values("dim_status", "status", "status"),
        "cursos": _distinct_dim_values("dim_curso", "curso", "curso"),
        "polos": _distinct_dim_values("dim_polo", "polo", "polo"),
        "consultores": _distinct_dim_values("dim_consultor", "consultor", "consultor"),
    }


# =========================
# 5) EXPORT CSV (server-side)
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
    ("modalidade", "Modalidade"),
    ("tipo_negocio", "Tipo Negócio"),
    ("data_envio_dt", "Data Envio"),
    ("data_disparo_dt", "Data Disparo"),
    ("tipo_disparo", "Tipo Disparo"),
    ("peca_disparo", "Peça Disparo"),
    ("texto_disparo", "Texto Disparo"),
    ("obs", "Obs"),
]


def export_leads_rows(filters: Optional[Dict[str, Any]] = None, max_rows: int = EXPORT_MAX_ROWS) -> Iterable[Dict[str, Any]]:
    """
    Itera linhas para export com mesma lógica de filtros.
    Tem trava de segurança max_rows pra não estourar memória/custo.
    """
    client = get_bq_client()
    filters = filters or {}

    max_rows = max(1, min(int(max_rows), EXPORT_MAX_ROWS))

    select_list = ",\n      ".join([f"{'f.' if c in ('origem','campanha','modalidade','tipo_negocio','peca_disparo','texto_disparo','tipo_disparo','obs','data_inscricao_dt','data_envio_dt','data_disparo_dt') else ''}{''}" for c, _ in []])  # dummy

    # Monta SELECT explicitamente (evita SELECT *)
    sql = """
    SELECT
      f.data_inscricao_dt AS data_inscricao_dt,
      dpe.nome            AS nome,
      dpe.cpf             AS cpf,
      dpe.celular         AS celular,
      dpe.email           AS email,
      f.origem            AS origem,
      dp.polo             AS polo,
      dc.curso            AS curso,
      ds.status           AS status,
      dco.consultor       AS consultor,
      f.campanha          AS campanha,
      f.modalidade        AS modalidade,
      f.tipo_negocio      AS tipo_negocio,
      f.data_envio_dt     AS data_envio_dt,
      f.data_disparo_dt   AS data_disparo_dt,
      f.tipo_disparo      AS tipo_disparo,
      f.peca_disparo      AS peca_disparo,
      f.texto_disparo     AS texto_disparo,
      f.obs               AS obs
    """ + _base_select_sql()

    params: List[bigquery.QueryParameter] = []
    sql = _apply_filters(sql, filters, params)

    # ordenação fixa pro export (estável)
    sql += " ORDER BY f.data_inscricao_dt DESC LIMIT @max_rows"
    params.append(bigquery.ScalarQueryParameter("max_rows", "INT64", max_rows))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = client.query(sql, job_config=job_config).result(page_size=EXPORT_PAGE_SIZE)

    for r in rows:
        yield dict(r.items())


def process_upload_dataframe(df) -> None:
    load_to_staging(df)
    run_procedure()
