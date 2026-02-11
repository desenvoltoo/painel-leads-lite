# services/bigquery.py
# =========================
# V14 — Staging (WRITE_TRUNCATE) -> CALL SP -> Query f_lead + LEFT JOIN dims
# + query_options() direto nas dimensões
# + FILTROS MULTI (IN UNNEST) para: status/curso/polo/consultor
# + EXPORT ROWS (server-side) para endpoint /api/export
# =========================

from __future__ import annotations  # ✅ evita avaliar type hints no import (Python 3.11)

import os
from typing import Any, Dict, List, Optional, Iterable

import pandas as pd
from google.cloud import bigquery


# =========================
# ENV (Cloud Run)
# =========================
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
BQ_DATASET = os.getenv("BQ_DATASET", "modelo_estrela")

# IMPORTANTE:
# - BigQuery usa location do DATASET (normalmente "US" ou "EU").
# - "us-central1" é região do Cloud Run, NÃO do BigQuery.
_RAW_BQ_LOCATION = os.getenv("BQ_LOCATION", "uscentral-1")

# Normaliza valores comuns de configuração para evitar erro de mapping.
_LOCATION_MAP = {
    "uscentral-1": "us-central1",
    "USCENTRAL-1": "us-central1",
}
BQ_LOCATION = _LOCATION_MAP.get(_RAW_BQ_LOCATION, _RAW_BQ_LOCATION)

BQ_STAGING_TABLE = os.getenv("BQ_STAGING_TABLE", "stg_leads_site")
BQ_FACT_TABLE = os.getenv("BQ_FACT_TABLE", "f_lead")
BQ_PROCEDURE = os.getenv("BQ_PROCEDURE", "sp_v14_carga_consolidada")

DEFAULT_LIMIT = int(os.getenv("BQ_DEFAULT_LIMIT", "200"))
MAX_LIMIT = int(os.getenv("BQ_MAX_LIMIT", "2000"))

# Export safety
EXPORT_MAX_ROWS = int(os.getenv("BQ_EXPORT_MAX_ROWS", "50000"))  # trava de segurança
EXPORT_PAGE_SIZE = int(os.getenv("BQ_EXPORT_PAGE_SIZE", "5000"))
CSV_UPLOAD_CHUNK_SIZE = int(os.getenv("CSV_UPLOAD_CHUNK_SIZE", "5000"))
DATAFRAME_UPLOAD_BATCH_SIZE = int(os.getenv("DATAFRAME_UPLOAD_BATCH_SIZE", "5000"))

EXPECTED_STAGING_COLUMNS = [
    "status_inscricao",
    "data_inscricao",
    "nome",
    "cpf",
    "celular",
    "curso",
    "unidade",
    "modalidade",
    "turno",
    "situacao_negociacao",
    "proprietario",
    "acao_comercial",
    "canal",
    "status_matriculado",
    "peca_disparo",
    "texto_disparo",
    "consultor_disparo",
    "data_envio",
    "campanha",
    "observacao",
    "data_contato",
    "data_matricula",
]

FLOAT_STAGING_COLUMNS = {
    "texto_disparo",
    "consultor_disparo",
    "data_envio",
    "campanha",
    "observacao",
    "data_contato",
    "data_matricula",
}

_bq_client: Optional[bigquery.Client] = None
_column_exists_cache: Dict[tuple[str, str], bool] = {}
_table_columns_cache: Dict[str, set[str]] = {}


def get_bq_client() -> bigquery.Client:
    """Client singleton (bom pra Cloud Run)."""
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
    return _bq_client


def _tbl(table_name: str) -> str:
    """Fully qualified table name com crases."""
    return f"`{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}`"


def _column_exists(table_name: str, column_name: str) -> bool:
    """Verifica existência de coluna no dataset para evitar SQL inválida."""
    key = (table_name, column_name)
    if key in _column_exists_cache:
        return _column_exists_cache[key]

    sql = f"""
    SELECT COUNT(1) AS cnt
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table_name
      AND column_name = @column_name
    """
    params = [
        bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
        bigquery.ScalarQueryParameter("column_name", "STRING", column_name),
    ]
    try:
        client = get_bq_client()
        rows = list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())
        exists = bool(rows and int(rows[0]["cnt"]) > 0)
    except Exception:
        # Evita quebrar API caso INFORMATION_SCHEMA não esteja acessível no ambiente.
        exists = False

    _column_exists_cache[key] = exists
    return exists


def _get_table_columns(table_name: str) -> set[str]:
    """Lista colunas da tabela via INFORMATION_SCHEMA (com cache)."""
    if table_name in _table_columns_cache:
        return _table_columns_cache[table_name]

    sql = f"""
    SELECT column_name
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table_name
    """
    params = [bigquery.ScalarQueryParameter("table_name", "STRING", table_name)]

    try:
        client = get_bq_client()
        rows = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        cols = {str(r["column_name"]).lower() for r in rows}
    except Exception:
        cols = set()

    _table_columns_cache[table_name] = cols
    return cols


def _get_modalidade_expr() -> Optional[str]:
    """Resolve a coluna de modalidade priorizando dim_curso."""
    for col in ("modalidade", "modalidade_curso", "tp_modalidade"):
        if _column_exists("dim_curso", col):
            return f"dc.{col}"
    for col in ("modalidade", "modalidade_curso", "tp_modalidade"):
        if _column_exists(BQ_FACT_TABLE, col):
            return f"f.{col}"
    return None


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


def _fix_mojibake(value: Any) -> Any:
    """Corrige casos comuns de mojibake UTF-8 lido como Latin-1 (ex.: JOSÃ‰)."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    # Heurística: tenta corrigir apenas quando há sinais típicos de mojibake.
    if "Ã" not in s and "Â" not in s:
        return s

    try:
        fixed = s.encode("cp1252").decode("utf-8")
        if fixed.count("Ã") + fixed.count("Â") < s.count("Ã") + s.count("Â"):
            return fixed
    except Exception:
        pass

    return s


# =========================
# 1) LOAD -> STAGING (WRITE_TRUNCATE)
# =========================
def normalize_upload_dataframe(df):
    """
    Normaliza o dataframe de upload para o schema esperado da staging.
    - renomeia colunas para snake_case simples
    - garante presença de todas as colunas esperadas
    - converte colunas não numéricas para STRING e colunas mapeadas para FLOAT
    - mantém apenas as colunas esperadas na ordem correta
    """
    norm_cols = {
        str(c).strip().lower().replace(" ", "_").replace("-", "_"): c
        for c in df.columns
    }

    out = df.copy()
    rename_map = {orig: norm for norm, orig in norm_cols.items()}
    out = out.rename(columns=rename_map)

    for col in EXPECTED_STAGING_COLUMNS:
        if col not in out.columns:
            out[col] = None

    out = out[EXPECTED_STAGING_COLUMNS]

    for col in EXPECTED_STAGING_COLUMNS:
        if col in FLOAT_STAGING_COLUMNS:
            out[col] = pd.to_numeric(out[col], errors="coerce")
        else:
            out[col] = out[col].apply(_fix_mojibake)

    return out


def load_to_staging(df, write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE) -> None:
    """
    Carrega DataFrame para a staging.
    """
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE}"

    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def load_to_staging_in_batches(frames: Iterable[pd.DataFrame]) -> int:
    """
    Carrega em lotes para reduzir pico de memória no upload CSV.
    Primeiro lote faz TRUNCATE, demais fazem APPEND.
    Retorna total de linhas carregadas.
    """
    total_rows = 0
    batch_idx = 0

    for frame in frames:
        if frame is None or frame.empty:
            continue

        normalized = normalize_upload_dataframe(frame)
        write_mode = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if batch_idx == 0
            else bigquery.WriteDisposition.WRITE_APPEND
        )
        load_to_staging(normalized, write_disposition=write_mode)

        total_rows += len(normalized)
        batch_idx += 1

    if batch_idx == 0:
        raise ValueError("Arquivo CSV sem linhas válidas para processamento.")

    return total_rows


def iter_dataframe_batches(df: pd.DataFrame, batch_size: int = DATAFRAME_UPLOAD_BATCH_SIZE) -> Iterable[pd.DataFrame]:
    """Divide DataFrame em lotes para reduzir memória durante carga."""
    batch_size = max(1000, int(batch_size or DATAFRAME_UPLOAD_BATCH_SIZE))
    total = len(df)
    for start in range(0, total, batch_size):
        end = start + batch_size
        yield df.iloc[start:end].copy()


def process_upload_csv_stream(file_obj, chunksize: int = CSV_UPLOAD_CHUNK_SIZE) -> int:
    """
    Processa CSV em streaming/lotes e retorna quantidade de linhas carregadas.
    """
    chunksize = max(1000, int(chunksize or CSV_UPLOAD_CHUNK_SIZE))
    chunks = pd.read_csv(file_obj, chunksize=chunksize)
    total_rows = load_to_staging_in_batches(chunks)
    run_procedure()
    return total_rows


def process_upload_dataframe_batched(df: pd.DataFrame, batch_size: int = DATAFRAME_UPLOAD_BATCH_SIZE) -> int:
    """Processa DataFrame em lotes e retorna quantidade total de linhas carregadas."""
    total_rows = load_to_staging_in_batches(iter_dataframe_batches(df, batch_size=batch_size))
    run_procedure()
    return total_rows


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


def _apply_filters(sql: str, filters: Dict[str, Any], params: List[Any]) -> str:
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
    modalidades = _as_list(filters.get("modalidade"))

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

    modalidade_expr = _get_modalidade_expr()
    if modalidades and modalidade_expr:
        sql += f" AND {modalidade_expr} IN UNNEST(@modalidades)"
        params.append(bigquery.ArrayQueryParameter("modalidades", "STRING", modalidades))

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
      f.canal                   AS origem,
      dp.polo_original          AS polo,
      dc.nome_curso             AS curso,
      dco.consultor_original    AS consultor,
      ds.status_original        AS status,
      f.campanha                AS campanha
    """ + _base_select_sql()

    params: List[Any] = []
    sql = _apply_filters(sql, filters, params)

    sql += f"\n ORDER BY {order_expr} {order_dir} \n LIMIT @limit OFFSET @offset"
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = client.query(sql, job_config=job_config).result()
    return [dict(r) for r in rows]


def query_leads_count(filters: Optional[Dict[str, Any]] = None) -> int:
    client = get_bq_client()
    filters = filters or {}

    sql = "SELECT COUNT(1) AS total " + _base_select_sql()
    params: List[Any] = []
    sql = _apply_filters(sql, filters, params)

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(sql, job_config=job_config).result())
    return int(rows[0]["total"]) if rows else 0


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
    modalidades: List[str] = []
    modalidade_expr = _get_modalidade_expr()
    try:
        if modalidade_expr and modalidade_expr.startswith("dc."):
            col = modalidade_expr.replace("dc.", "")
            modalidades = _distinct_dim_values("dim_curso", col, "modalidade")
        elif modalidade_expr and modalidade_expr.startswith("f."):
            col = modalidade_expr.replace("f.", "")
            modalidades = _distinct_dim_values(BQ_FACT_TABLE, col, "modalidade")
    except Exception:
        modalidades = []

    return {
        "status": _distinct_dim_values("dim_status", "status_original", "status"),
        "cursos": _distinct_dim_values("dim_curso", "nome_curso", "curso"),
        "polos": _distinct_dim_values("dim_polo", "polo_original", "polo"),
        "consultores": _distinct_dim_values("dim_consultor", "consultor_original", "consultor"),
        "modalidades": modalidades,
    }


# =========================
# 5) EXPORT (server-side)
# =========================
EXPORT_VARIABLE_COLUMNS = [
    "status_inscricao",
    "data_inscricao",
    "nome",
    "cpf",
    "celular",
    "curso",
    "unidade",
    "modalidade",
    "turno",
    "situacao_negociacao",
    "proprietario",
    "acao_comercial",
    "canal",
    "status_matriculado",
    "peca_disparo",
    "texto_disparo",
    "consultor_disparo",
    "data_envio",
    "campanha",
    "observacao",
    "data_contato",
    "data_matricula",
]

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


def export_staging_variable_rows(max_rows: int = EXPORT_MAX_ROWS) -> Iterable[Dict[str, Any]]:
    """Exporta variáveis da staging com ordem fixa, preenchendo colunas ausentes com NULL."""
    client = get_bq_client()
    max_rows = max(1, min(int(max_rows), EXPORT_MAX_ROWS))

    existing_cols = _get_table_columns(BQ_STAGING_TABLE)
    select_exprs = []
    for col in EXPORT_VARIABLE_COLUMNS:
        if col.lower() in existing_cols:
            select_exprs.append(f"`{col}` AS `{col}`")
        else:
            select_exprs.append(f"CAST(NULL AS STRING) AS `{col}`")

    select_cols = ",\n      ".join(select_exprs)
    sql = f"""
    SELECT
      {select_cols}
    FROM {_tbl(BQ_STAGING_TABLE)}
    """

    job = client.query(sql)

    yielded = 0
    for page in job.result(page_size=EXPORT_PAGE_SIZE).pages:
        for row in page:
            d = dict(row)
            yield {col: d.get(col) for col in EXPORT_VARIABLE_COLUMNS}
            yielded += 1
            if yielded >= max_rows:
                return


def export_leads_rows(
    filters: Optional[Dict[str, Any]] = None,
    max_rows: int = EXPORT_MAX_ROWS
) -> Iterable[Dict[str, Any]]:
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

    params: List[Any] = []
    sql = _apply_filters(sql, filters, params)
    sql += "\n ORDER BY f.data_inscricao_dt DESC"

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    job = client.query(sql, job_config=job_config)

    yielded = 0
    for page in job.result(page_size=EXPORT_PAGE_SIZE).pages:
        for row in page:
            raw = dict(row)
            normalized = {
                k: (_fix_mojibake(v) if isinstance(v, str) else v)
                for k, v in raw.items()
            }
            yield normalized
            yielded += 1
            if yielded >= max_rows:
                return


# =========================
# 6) PIPELINE UPLOAD
# =========================
def process_upload_dataframe(df) -> None:
    """Mantido por compatibilidade; usa caminho batched para evitar estouro."""
    process_upload_dataframe_batched(df)
