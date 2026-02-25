# services/bigquery.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery

# XLSX
from openpyxl import Workbook
from openpyxl.utils import get_column_letter


# ============================================================
# CONFIG (ENV + travas)
# ============================================================
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
BQ_DATASET = os.getenv("BQ_DATASET", "modelo_estrela")

# Mantém seu staging atual (tabela no MESMO dataset)
BQ_STAGING_TABLE = os.getenv("BQ_STAGING_TABLE", "stg_leads_site")

# Procedure do seu star (no MESMO dataset)
BQ_PROCEDURE = os.getenv("BQ_PROCEDURE", "sp_import_star_from_site")

# ✅ TRAVADO: painel lê SOMENTE essa view
BQ_VIEW_LEADS = "vw_leads_painel_lite"

DEFAULT_LIMIT = int(os.getenv("BQ_DEFAULT_LIMIT", "200"))
MAX_LIMIT = int(os.getenv("BQ_MAX_LIMIT", "2000"))
EXPORT_MAX_ROWS = int(os.getenv("BQ_EXPORT_MAX_ROWS", "50000"))

_bq_client: Optional[bigquery.Client] = None


def get_bq_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    return _bq_client


def _tbl(name: str) -> str:
    return f"`{GCP_PROJECT_ID}.{BQ_DATASET}.{name}`"


def _as_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    if "||" in s:
        return [p.strip() for p in s.split("||") if p.strip()]
    if "," in s:
        return [p.strip() for p in s.split(",") if p.strip()]
    return [s]


# ============================================================
# STAGING + PROCEDURE (upload)
# ============================================================
def load_to_staging(df) -> None:
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE}"
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    )
    job.result()


def run_procedure() -> None:
    client = get_bq_client()
    sql = f"CALL `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PROCEDURE}`();"
    client.query(sql).result()


def process_upload_dataframe(df) -> None:
    load_to_staging(df)
    run_procedure()


# ============================================================
# QUERY HELPERS (sempre a VIEW)
# ============================================================
def _base_select_sql() -> str:
    return f"FROM {_tbl(BQ_VIEW_LEADS)} v WHERE 1=1"


def _apply_filters(sql: str, filters: Dict[str, Any], params: List[Any]) -> str:
    cursos = _as_list(filters.get("curso"))
    polos = _as_list(filters.get("polo"))
    modalidades = _as_list(filters.get("modalidade"))
    turnos = _as_list(filters.get("turno"))
    canais = _as_list(filters.get("canal"))
    campanhas = _as_list(filters.get("campanha"))
    origens = _as_list(filters.get("origem"))
    tipos_negocio = _as_list(filters.get("tipo_negocio"))
    tipos_disparo = _as_list(filters.get("tipo_disparo"))

    status_list = _as_list(filters.get("status")) or _as_list(filters.get("status_inscricao"))
    consultores_disp = _as_list(filters.get("consultor_disparo")) or _as_list(filters.get("consultor"))
    consultores_com = _as_list(filters.get("consultor_comercial"))

    if cursos:
        sql += " AND v.curso IN UNNEST(@cursos)"
        params.append(bigquery.ArrayQueryParameter("cursos", "STRING", cursos))

    if polos:
        sql += " AND v.polo IN UNNEST(@polos)"
        params.append(bigquery.ArrayQueryParameter("polos", "STRING", polos))

    if modalidades:
        sql += " AND v.modalidade IN UNNEST(@modalidades)"
        params.append(bigquery.ArrayQueryParameter("modalidades", "STRING", modalidades))

    if turnos:
        sql += " AND v.turno IN UNNEST(@turnos)"
        params.append(bigquery.ArrayQueryParameter("turnos", "STRING", turnos))

    if canais:
        sql += " AND v.canal IN UNNEST(@canais)"
        params.append(bigquery.ArrayQueryParameter("canais", "STRING", canais))

    if campanhas:
        sql += " AND v.campanha IN UNNEST(@campanhas)"
        params.append(bigquery.ArrayQueryParameter("campanhas", "STRING", campanhas))

    if origens:
        sql += " AND v.origem IN UNNEST(@origens)"
        params.append(bigquery.ArrayQueryParameter("origens", "STRING", origens))

    if tipos_negocio:
        sql += " AND v.tipo_negocio IN UNNEST(@tipos_negocio)"
        params.append(bigquery.ArrayQueryParameter("tipos_negocio", "STRING", tipos_negocio))

    if status_list:
        sql += " AND (v.status_inscricao IN UNNEST(@status_list) OR v.status IN UNNEST(@status_list))"
        params.append(bigquery.ArrayQueryParameter("status_list", "STRING", status_list))

    if consultores_disp:
        sql += " AND v.consultor_disparo IN UNNEST(@consultores_disp)"
        params.append(bigquery.ArrayQueryParameter("consultores_disp", "STRING", consultores_disp))

    if consultores_com:
        sql += " AND v.consultor_comercial IN UNNEST(@consultores_com)"
        params.append(bigquery.ArrayQueryParameter("consultores_com", "STRING", consultores_com))

    if tipos_disparo:
        sql += " AND v.tipo_disparo IN UNNEST(@tipos_disparo)"
        params.append(bigquery.ArrayQueryParameter("tipos_disparo", "STRING", tipos_disparo))

    # busca direta
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
        params.append(
            bigquery.ScalarQueryParameter(
                "nome_like",
                "STRING",
                f"%{str(filters['nome']).strip()}%",
            )
        )

    # ✅ matriculado: só flag_matriculado
    if filters.get("matriculado") is not None and str(filters.get("matriculado")).strip() != "":
        val = str(filters.get("matriculado")).lower().strip()
        b = (
            True
            if val in ("true", "1", "sim", "yes")
            else False
            if val in ("false", "0", "nao", "não", "no")
            else None
        )
        if b is not None:
            sql += " AND IFNULL(v.flag_matriculado, FALSE) = @matriculado"
            params.append(bigquery.ScalarQueryParameter("matriculado", "BOOL", b))

    # datas
    if filters.get("data_ini"):
        sql += " AND v.data_inscricao >= @data_ini"
        params.append(bigquery.ScalarQueryParameter("data_ini", "DATE", filters["data_ini"]))

    if filters.get("data_fim"):
        sql += " AND v.data_inscricao <= @data_fim"
        params.append(bigquery.ScalarQueryParameter("data_fim", "DATE", filters["data_fim"]))

    return sql


# ============================================================
# LISTAGEM
# ============================================================
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

    if order_by == "data_inscricao_dt":
        order_by = "data_inscricao"

    allowed_order = {
        "data_inscricao": "v.data_inscricao",
        "status": "v.status_inscricao",
        "curso": "v.curso",
        "modalidade": "v.modalidade",
        "polo": "v.polo",
        "nome": "v.nome",
        "cpf": "v.cpf",
        "canal": "v.canal",
        "campanha": "v.campanha",
        "consultor_disparo": "v.consultor_disparo",
    }
    order_expr = allowed_order.get(order_by, "v.data_inscricao")

    sql = """
    SELECT
      v.data_inscricao,
      v.nome, v.cpf, v.celular, v.email,
      v.curso, v.modalidade, v.turno,
      v.polo,
      v.origem,
      v.status_inscricao, v.status,
      v.flag_matriculado,
      v.consultor_comercial, v.consultor_disparo,
      v.canal, v.campanha
    """ + _base_select_sql()

    params: List[Any] = []
    sql = _apply_filters(sql, filters, params)
    sql += f"\n ORDER BY {order_expr} {order_dir} \n LIMIT @limit OFFSET @offset"

    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))

    rows = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return [dict(r) for r in rows]


def query_leads_count(filters: Optional[Dict[str, Any]] = None) -> int:
    client = get_bq_client()
    filters = filters or {}

    sql = "SELECT COUNT(1) AS total " + _base_select_sql()
    params: List[Any] = []
    sql = _apply_filters(sql, filters, params)

    rows = list(client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result())
    return int(rows[0]["total"]) if rows else 0


# ============================================================
# OPTIONS
# ============================================================
def _distinct_values_from_view(col: str, alias: str) -> List[str]:
    client = get_bq_client()
    sql = f"""
    SELECT DISTINCT {col} AS {alias}
    FROM {_tbl(BQ_VIEW_LEADS)}
    WHERE {col} IS NOT NULL AND TRIM(CAST({col} AS STRING)) != ''
    ORDER BY {alias}
    """
    return [str(r[alias]) for r in client.query(sql).result()]


def query_options() -> Dict[str, List[str]]:
    return {
        "status": _distinct_values_from_view("status_inscricao", "status"),
        "cursos": _distinct_values_from_view("curso", "curso"),
        "modalidades": _distinct_values_from_view("modalidade", "modalidade"),
        "turnos": _distinct_values_from_view("turno", "turno"),
        "polos": _distinct_values_from_view("polo", "polo"),
        "origens": _distinct_values_from_view("origem", "origem"),
        "canais": _distinct_values_from_view("canal", "canal"),
        "campanhas": _distinct_values_from_view("campanha", "campanha"),
        "consultores_disparo": _distinct_values_from_view("consultor_disparo", "consultor_disparo"),
        "consultores_comercial": _distinct_values_from_view("consultor_comercial", "consultor_comercial"),
        "tipos_disparo": _distinct_values_from_view("tipo_disparo", "tipo_disparo"),
        "tipos_negocio": _distinct_values_from_view("tipo_negocio", "tipo_negocio"),
    }


# ============================================================
# EXPORT (XLSX)
# ============================================================
EXPORT_COLUMNS: List[Tuple[str, str]] = [
    ("data_inscricao", "Data Inscrição"),
    ("nome", "Candidato"),
    ("cpf", "CPF"),
    ("celular", "Celular"),
    ("email", "Email"),
    ("curso", "Curso"),
    ("modalidade", "Modalidade"),
    ("turno", "Turno"),
    ("polo", "Polo"),
    ("origem", "Origem"),
    ("status_inscricao", "Status Inscrição"),
    ("status", "Status"),
    ("flag_matriculado", "Matriculado"),
    ("tipo_negocio", "Tipo Negócio"),
    ("consultor_comercial", "Consultor Comercial"),
    ("consultor_disparo", "Consultor Disparo"),
    ("canal", "Canal"),
    ("campanha", "Campanha"),
    ("acao_comercial", "Ação Comercial"),
    ("tipo_disparo", "Tipo Disparo"),
    ("peca_disparo", "Peça Disparo"),
    ("texto_disparo", "Texto Disparo"),
    ("qtd_acionamentos", "Qtd Acionamentos"),
    ("data_matricula", "Data Matrícula"),
    ("data_ultima_acao", "Data Última Ação"),
    ("data_disparo", "Data Disparo"),
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

    if order_by == "data_inscricao_dt":
        order_by = "data_inscricao"

    allowed_order = {
        "data_inscricao": "v.data_inscricao",
        "status": "v.status_inscricao",
        "curso": "v.curso",
        "modalidade": "v.modalidade",
        "polo": "v.polo",
        "nome": "v.nome",
        "cpf": "v.cpf",
        "canal": "v.canal",
        "campanha": "v.campanha",
    }
    order_expr = allowed_order.get(order_by, "v.data_inscricao")

    select_cols = ",\n      ".join([f"v.{c}" for c, _ in EXPORT_COLUMNS])

    sql = f"""
    SELECT
      {select_cols}
    """ + _base_select_sql()

    params: List[Any] = []
    sql = _apply_filters(sql, filters, params)

    sql += f"\n ORDER BY {order_expr} {order_dir} \n LIMIT @limit OFFSET @offset"
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))

    rows = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return [dict(r) for r in rows]


def rows_to_xlsx(rows: List[Dict[str, Any]], xlsx_path: str, sheet_name: str = "Leads") -> str:
    """
    Gera XLSX no disco (Excel não aceita datetime com timezone).
    """
    from datetime import datetime, date

    def _excel_safe(v):
        # Remove tzinfo de datetimes (Excel/OpenPyXL não suporta timezone)
        if isinstance(v, datetime):
            return v.replace(tzinfo=None) if v.tzinfo is not None else v
        # date ok
        if isinstance(v, date):
            return v
        return v

    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]

    headers = [label for _, label in EXPORT_COLUMNS]
    keys = [key for key, _ in EXPORT_COLUMNS]
    ws.append(headers)

    for r in rows:
        ws.append([_excel_safe(r.get(k)) for k in keys])

    for col_idx, header in enumerate(headers, start=1):
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = max(12, min(42, len(str(header)) + 6))

    Path(xlsx_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(xlsx_path)
    return xlsx_path


def df_to_xlsx(df, xlsx_path: str, sheet_name: str = "Upload") -> str:
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]

    ws.append(list(df.columns))
    for row in df.itertuples(index=False, name=None):
        ws.append(list(row))

    Path(xlsx_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(xlsx_path)
    return xlsx_path
