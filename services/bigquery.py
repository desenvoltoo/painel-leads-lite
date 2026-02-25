# services/bigquery.py
# =========================
# V14 — Staging (WRITE_TRUNCATE) -> CALL SP -> Query via VIEW vw_leads_painel_lite
# + FILTROS MULTI (IN UNNEST) para: status/curso/polo/consultor/modalidade/canal/campanha
# + EXPORT ROWS (server-side) para endpoint /api/export/xlsx
# =========================

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from google.cloud import bigquery


# =========================
# ENV (Cloud Run)
# =========================
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
BQ_DATASET = os.getenv("BQ_DATASET", "modelo_estrela")

# ✅ Seu dataset é us-central1 (confirmado por você)
BQ_LOCATION = os.getenv("BQ_LOCATION", "us-central1")

BQ_STAGING_TABLE = os.getenv("BQ_STAGING_TABLE", "stg_leads_site")
BQ_PROCEDURE = os.getenv("BQ_PROCEDURE", "sp_v14_carga_consolidada")

# ✅ View oficial para leitura/export
BQ_VIEW_LEADS = os.getenv("BQ_VIEW_LEADS", "vw_leads_painel_lite")

DEFAULT_LIMIT = int(os.getenv("BQ_DEFAULT_LIMIT", "200"))
MAX_LIMIT = int(os.getenv("BQ_MAX_LIMIT", "2000"))

# Export safety
EXPORT_MAX_ROWS = int(os.getenv("BQ_EXPORT_MAX_ROWS", "50000"))
EXPORT_PAGE_SIZE = int(os.getenv("BQ_EXPORT_PAGE_SIZE", "5000"))

_bq_client: Optional[bigquery.Client] = None


def get_bq_client() -> bigquery.Client:
    """Client singleton (bom pra Cloud Run)."""
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
    return _bq_client


def _tbl(table_name: str) -> str:
    """Fully qualified table/view name com crases."""
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
# SQL BASE (VIEW)
# =========================
def _base_select_sql() -> str:
    # A view já vem pronta (sem SK, com nomes finais)
    return f"""
    FROM {_tbl(BQ_VIEW_LEADS)} v
    WHERE 1=1
    """


def _apply_filters(sql: str, filters: Dict[str, Any], params: List[Any]) -> str:
    """
    Aplica filtros no SQL + adiciona QueryParameters.

    Colunas disponíveis na view vw_leads_painel_lite:
      nome, cpf, celular, email,
      curso, modalidade, unidade, polo,
      status, matriculado, situacao_negociacao, turno, acao_comercial, proprietario, canal,
      campanha, tipo_disparo, peca_disparo, texto_disparo, consultor_disparo,
      data_inscricao, data_matricula, data_contato, data_atualizacao,
      observacao
    """
    # MULTI (arrays)
    cursos = _as_list(filters.get("curso"))
    polos = _as_list(filters.get("polo"))
    status_list = _as_list(filters.get("status"))
    consultores = _as_list(filters.get("consultor_disparo")) or _as_list(filters.get("consultor"))
    modalidades = _as_list(filters.get("modalidade"))
    canais = _as_list(filters.get("canal"))
    campanhas = _as_list(filters.get("campanha"))

    if cursos:
        sql += " AND v.curso IN UNNEST(@cursos)"
        params.append(bigquery.ArrayQueryParameter("cursos", "STRING", cursos))

    if polos:
        sql += " AND v.polo IN UNNEST(@polos)"
        params.append(bigquery.ArrayQueryParameter("polos", "STRING", polos))

    if status_list:
        sql += " AND v.status IN UNNEST(@status_list)"
        params.append(bigquery.ArrayQueryParameter("status_list", "STRING", status_list))

    if consultores:
        sql += " AND v.consultor_disparo IN UNNEST(@consultores)"
        params.append(bigquery.ArrayQueryParameter("consultores", "STRING", consultores))

    if modalidades:
        sql += " AND v.modalidade IN UNNEST(@modalidades)"
        params.append(bigquery.ArrayQueryParameter("modalidades", "STRING", modalidades))

    if canais:
        sql += " AND v.canal IN UNNEST(@canais)"
        params.append(bigquery.ArrayQueryParameter("canais", "STRING", canais))

    if campanhas:
        sql += " AND v.campanha IN UNNEST(@campanhas)"
        params.append(bigquery.ArrayQueryParameter("campanhas", "STRING", campanhas))

    # SINGLE
    if filters.get("cpf"):
        sql += " AND v.cpf = @cpf"
        params.append(bigquery.ScalarQueryParameter("cpf", "STRING", str(filters["cpf"]).strip()))

    if filters.get("celular"):
        sql += " AND v.celular = @celular"
        params.append(bigquery.ScalarQueryParameter("celular", "STRING", str(filters["celular"]).strip()))

    if filters.get("email"):
        sql += " AND LOWER(v.email) = LOWER(@email)"
        params.append(bigquery.ScalarQueryParameter("email", "STRING", str(filters["email"]).strip()))

    if filters.get("nome"):
        sql += " AND LOWER(v.nome) LIKE LOWER(@nome_like)"
        params.append(bigquery.ScalarQueryParameter("nome_like", "STRING", f"%{str(filters['nome']).strip()}%"))

    # BOOL (matriculado)
    if filters.get("matriculado") in (True, False, "true", "false", "1", "0", 1, 0):
        val = filters.get("matriculado")
        b = True if str(val).lower() in ("true", "1") else False if str(val).lower() in ("false", "0") else bool(val)
        sql += " AND v.matriculado = @matriculado"
        params.append(bigquery.ScalarQueryParameter("matriculado", "BOOL", b))

    # DATE RANGE (data_inscricao é DATETIME na view)
    if filters.get("data_ini"):
        sql += " AND DATE(v.data_inscricao) >= @data_ini"
        params.append(bigquery.ScalarQueryParameter("data_ini", "DATE", filters["data_ini"]))

    if filters.get("data_fim"):
        sql += " AND DATE(v.data_inscricao) <= @data_fim"
        params.append(bigquery.ScalarQueryParameter("data_fim", "DATE", filters["data_fim"]))

    return sql


# =========================
# 3) QUERY PRINCIPAL (PAINEL) — agora via VIEW
# =========================
def query_leads(
    filters: Optional[Dict[str, Any]] = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    order_by: str = "data_inscricao",
    order_dir: str = "DESC",
) -> List[Dict[str, Any]]:
    client = get_bq_client()
    filters = filters or {}

    limit = max(1, min(int(limit), MAX_LIMIT))
    offset = max(0, int(offset))
    order_dir = "ASC" if str(order_dir).upper() == "ASC" else "DESC"

    allowed_order = {
        "data_inscricao": "v.data_inscricao",
        "status": "v.status",
        "curso": "v.curso",
        "modalidade": "v.modalidade",
        "polo": "v.polo",
        "nome": "v.nome",
        "cpf": "v.cpf",
        "canal": "v.canal",
        "campanha": "v.campanha",
    }
    order_expr = allowed_order.get(order_by, "v.data_inscricao")

    sql = """
    SELECT
      v.data_inscricao     AS data_inscricao,
      v.nome              AS nome,
      v.cpf               AS cpf,
      v.celular           AS celular,
      v.email             AS email,
      v.curso             AS curso,
      v.modalidade        AS modalidade,
      v.unidade           AS unidade,
      v.polo              AS polo,
      v.status            AS status,
      v.matriculado       AS matriculado,
      v.canal             AS canal,
      v.campanha          AS campanha
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
def _distinct_values_from_view(col: str, alias: str) -> List[str]:
    client = get_bq_client()
    sql = f"""
    SELECT DISTINCT {col} AS {alias}
    FROM {_tbl(BQ_VIEW_LEADS)}
    WHERE {col} IS NOT NULL AND TRIM(CAST({col} AS STRING)) != ''
    ORDER BY {alias}
    """
    rows = client.query(sql).result()
    return [str(r[alias]) for r in rows]


def query_options() -> Dict[str, List[str]]:
    # ✅ opções agora refletem o que realmente existe na VIEW (melhor UX e zero mismatch)
    return {
        "status": _distinct_values_from_view("status", "status"),
        "cursos": _distinct_values_from_view("curso", "curso"),
        "modalidades": _distinct_values_from_view("modalidade", "modalidade"),
        "polos": _distinct_values_from_view("polo", "polo"),
        "canais": _distinct_values_from_view("canal", "canal"),
        "campanhas": _distinct_values_from_view("campanha", "campanha"),
        "consultores_disparo": _distinct_values_from_view("consultor_disparo", "consultor_disparo"),
        "tipos_disparo": _distinct_values_from_view("tipo_disparo", "tipo_disparo"),
    }


# =========================
# 5) EXPORT (server-side) — agora exporta a VIEW completa (sem SK)
# =========================
EXPORT_COLUMNS = [
    ("data_inscricao", "Data Inscrição"),
    ("nome", "Candidato"),
    ("cpf", "CPF"),
    ("celular", "Celular"),
    ("email", "Email"),
    ("curso", "Curso"),
    ("modalidade", "Modalidade"),
    ("unidade", "Unidade"),
    ("polo", "Polo"),
    ("status", "Status"),
    ("matriculado", "Matriculado"),
    ("situacao_negociacao", "Situação Negociação"),
    ("turno", "Turno"),
    ("acao_comercial", "Ação Comercial"),
    ("proprietario", "Proprietário"),
    ("canal", "Canal"),
    ("campanha", "Campanha"),
    ("tipo_disparo", "Tipo Disparo"),
    ("peca_disparo", "Peça Disparo"),
    ("texto_disparo", "Texto Disparo"),
    ("consultor_disparo", "Consultor Disparo"),
    ("data_matricula", "Data Matrícula"),
    ("data_contato", "Data Contato"),
    ("data_atualizacao", "Atualizado em"),
    ("observacao", "Observação"),
]


def export_leads_rows(
    filters: Optional[Dict[str, Any]] = None,
    limit: int = EXPORT_MAX_ROWS,
    offset: int = 0,
    order_by: str = "data_inscricao",
    order_dir: str = "DESC",
) -> List[Dict[str, Any]]:
    client = get_bq_client()
    filters = filters or {}

    limit = max(1, min(int(limit), EXPORT_MAX_ROWS))
    offset = max(0, int(offset))
    order_dir = "ASC" if str(order_dir).upper() == "ASC" else "DESC"

    allowed_order = {
        "data_inscricao": "v.data_inscricao",
        "status": "v.status",
        "curso": "v.curso",
        "modalidade": "v.modalidade",
        "polo": "v.polo",
        "nome": "v.nome",
        "cpf": "v.cpf",
        "canal": "v.canal",
        "campanha": "v.campanha",
    }
    order_expr = allowed_order.get(order_by, "v.data_inscricao")

    sql = """
    SELECT
      v.nome,
      v.cpf,
      v.celular,
      v.email,
      v.curso,
      v.modalidade,
      v.unidade,
      v.polo,
      v.status,
      v.matriculado,
      v.situacao_negociacao,
      v.turno,
      v.acao_comercial,
      v.proprietario,
      v.canal,
      v.campanha,
      v.tipo_disparo,
      v.peca_disparo,
      v.texto_disparo,
      v.consultor_disparo,
      v.data_inscricao,
      v.data_matricula,
      v.data_contato,
      v.data_atualizacao,
      v.observacao
    """ + _base_select_sql()

    params: List[Any] = []
    sql = _apply_filters(sql, filters, params)

    sql += f"\n ORDER BY {order_expr} {order_dir} \n LIMIT @limit OFFSET @offset"
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))

    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = client.query(sql, job_config=job_config).result()
    return [dict(r) for r in rows]


# =========================
# 6) PIPELINE UPLOAD
# =========================
def process_upload_dataframe(df) -> None:
    load_to_staging(df)
    run_procedure()
