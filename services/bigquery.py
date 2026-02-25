# services/bigquery.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from google.cloud import bigquery

# XLSX
from openpyxl import Workbook
from openpyxl.utils import get_column_letter


# ENV
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
BQ_DATASET = os.getenv("BQ_DATASET", "modelo_estrela")
BQ_LOCATION = os.getenv("BQ_LOCATION", "us-central1")  # ✅ confirmado por você

# Mantém seu staging atual
BQ_STAGING_TABLE = os.getenv("BQ_STAGING_TABLE", "stg_leads_site")

# ✅ Nova SP do novo star
BQ_PROCEDURE = os.getenv("BQ_PROCEDURE", "sp_import_star_from_site")

# ✅ View fixa exigida pelo painel (sempre lê exatamente esta view)
BQ_VIEW_LEADS = "vw_leads_painel_lite"

DEFAULT_LIMIT = int(os.getenv("BQ_DEFAULT_LIMIT", "200"))
MAX_LIMIT = int(os.getenv("BQ_MAX_LIMIT", "2000"))
EXPORT_MAX_ROWS = int(os.getenv("BQ_EXPORT_MAX_ROWS", "50000"))

_bq_client: Optional[bigquery.Client] = None
_view_columns_cache: Optional[Set[str]] = None


def get_bq_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
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


# ---------------------------
# STAGING + PROCEDURE
# ---------------------------
def load_to_staging(df) -> None:
    """
    Carrega o dataframe na staging (WRITE_TRUNCATE).
    Mantém o que você já fazia.
    """
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


# ---------------------------
# QUERY HELPERS
# ---------------------------
def _base_select_sql() -> str:
    return f"FROM {_tbl(BQ_VIEW_LEADS)} v WHERE 1=1"


def _get_view_columns() -> Set[str]:
    """
    Lê o schema da view uma vez e guarda em cache local do processo.
    """
    global _view_columns_cache
    if _view_columns_cache is None:
        client = get_bq_client()
        table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_VIEW_LEADS}"
        table = client.get_table(table_ref)
        _view_columns_cache = {f.name for f in table.schema}
    return _view_columns_cache


def _has_col(col: str) -> bool:
    return col in _get_view_columns()


def _col_expr(col: str, alias: Optional[str] = None) -> str:
    """
    Retorna expressão segura para SELECT:
    - se a coluna existir: v.col
    - se não existir: NULL AS alias
    """
    target = alias or col
    if _has_col(col):
        if alias and alias != col:
            return f"v.{col} AS {alias}"
        return f"v.{col}"
    return f"NULL AS {target}"


def _matriculado_expr() -> str:
    """
    Compatibilidade de schema: algumas versões da view expõem
    `flag_matriculado`, outras `matriculado_flag` (ou ambas).
    """
    has_flag = _has_col("flag_matriculado")
    has_alt = _has_col("matriculado_flag")
    if has_flag and has_alt:
        return "COALESCE(v.flag_matriculado, v.matriculado_flag)"
    if has_flag:
        return "v.flag_matriculado"
    if has_alt:
        return "v.matriculado_flag"
    return "NULL"


def _apply_filters(sql: str, filters: Dict[str, Any], params: List[Any]) -> str:
    """
    Filtros alinhados à vw_leads_painel_lite (novo star).

    Colunas principais disponíveis na view:
    - pessoa: nome, cpf, celular, email
    - curso: curso, modalidade, turno
    - polo: polo
    - origem: origem
    - consultor: consultor_comercial, consultor_disparo
    - status: status, status_inscricao, observacao, matriculado_flag/flag_matriculado
    - campanha: campanha, canal, acao_comercial
    - disparo: tipo_disparo, peca_disparo, texto_disparo, qtd_acionamentos
    - tipo_negocio: tipo_negocio
    - datas: data_inscricao (DATE), data_matricula (DATE), data_contato, data_ultima_acao, data_disparo, data_atualizacao
    """
    cursos = _as_list(filters.get("curso"))
    polos = _as_list(filters.get("polo"))
    modalidades = _as_list(filters.get("modalidade"))
    turnos = _as_list(filters.get("turno"))
    canais = _as_list(filters.get("canal"))
    campanhas = _as_list(filters.get("campanha"))
    origens = _as_list(filters.get("origem"))
    tipos_negocio = _as_list(filters.get("tipo_negocio"))

    # status: painel antigo pode mandar "status" e esperar status_inscricao
    status_list = _as_list(filters.get("status")) or _as_list(filters.get("status_inscricao"))

    consultores_disp = _as_list(filters.get("consultor_disparo")) or _as_list(filters.get("consultor"))
    consultores_com = _as_list(filters.get("consultor_comercial"))

    tipos_disparo = _as_list(filters.get("tipo_disparo"))

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
        # tenta bater em status_inscricao primeiro, mas também permite status
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
        params.append(bigquery.ScalarQueryParameter("nome_like", "STRING", f"%{str(filters['nome']).strip()}%"))

    # matriculado (no lite eu expus matriculado_flag + flag_matriculado)
    if filters.get("matriculado") is not None and str(filters.get("matriculado")).strip() != "":
        val = str(filters.get("matriculado")).lower().strip()
        b = True if val in ("true", "1", "sim", "yes") else False if val in ("false", "0", "nao", "não", "no") else None
        if b is not None:
            sql += f" AND IFNULL({_matriculado_expr()}, FALSE) = @matriculado"
            params.append(bigquery.ScalarQueryParameter("matriculado", "BOOL", b))

    # data_inscricao já é DATE na view
    if filters.get("data_ini"):
        sql += " AND v.data_inscricao >= @data_ini"
        params.append(bigquery.ScalarQueryParameter("data_ini", "DATE", filters["data_ini"]))
    if filters.get("data_fim"):
        sql += " AND v.data_inscricao <= @data_fim"
        params.append(bigquery.ScalarQueryParameter("data_fim", "DATE", filters["data_fim"]))

    return sql


# ---------------------------
# LISTAGEM (Tabela do painel)
# ---------------------------
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

    # compat antiga
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

    select_exprs = [
        _col_expr("sk_pessoa"),
        _col_expr("data_inscricao"),
        _col_expr("data_matricula"),
        _col_expr("data_contato"),
        _col_expr("data_ultima_acao"),
        _col_expr("data_disparo"),
        _col_expr("data_atualizacao"),
        _col_expr("ano_mes_inscricao"),
        _col_expr("nome"), _col_expr("cpf"), _col_expr("celular"), _col_expr("email"),
        _col_expr("curso"), _col_expr("modalidade"), _col_expr("turno"),
        _col_expr("polo"),
        _col_expr("origem"),
        _col_expr("status_inscricao"), _col_expr("status"),
        _col_expr("matriculado_flag"),
        _col_expr("flag_matriculado"),
        f"{_matriculado_expr()} AS matriculado",
        _col_expr("observacao"),
        _col_expr("tipo_negocio"),
        _col_expr("consultor_comercial"), _col_expr("consultor_disparo"),
        _col_expr("canal"), _col_expr("campanha"), _col_expr("acao_comercial"),
        _col_expr("tipo_disparo"), _col_expr("peca_disparo"), _col_expr("texto_disparo"),
        _col_expr("qtd_acionamentos"),
    ]

    sql = "SELECT\n      " + ",\n      ".join(select_exprs) + "\n" + _base_select_sql()

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


# ---------------------------
# OPTIONS (dropdowns/autocomplete)
# ---------------------------
def _distinct_values_from_view(col: str, alias: str) -> List[str]:
    if not _has_col(col):
        return []

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


# ---------------------------
# EXPORT (XLSX)
# ---------------------------
EXPORT_COLUMNS: List[Tuple[str, str]] = [
    ("sk_pessoa", "SK Pessoa"),
    ("data_inscricao", "Data Inscrição"),
    ("ano_mes_inscricao", "Ano/Mês Inscrição"),
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
    ("matriculado_flag", "Matriculado Flag"),
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
    ("data_contato", "Data Contato"),
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

    select_cols = ",\n      ".join([_col_expr(c) for c, _ in EXPORT_COLUMNS])

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
    Gera XLSX no disco (mantém 100% XLSX).
    """
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]

    headers = [label for _, label in EXPORT_COLUMNS]
    keys = [key for key, _ in EXPORT_COLUMNS]

    ws.append(headers)

    for r in rows:
        ws.append([r.get(k) for k in keys])

    # ajuste de largura simples
    for col_idx, header in enumerate(headers, start=1):
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = max(12, min(42, len(str(header)) + 6))

    Path(xlsx_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(xlsx_path)
    return xlsx_path


def df_to_xlsx(df, xlsx_path: str, sheet_name: str = "Upload") -> str:
    """
    Salva cópia do upload em XLSX.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]

    ws.append(list(df.columns))
    for row in df.itertuples(index=False, name=None):
        ws.append(list(row))

    Path(xlsx_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(xlsx_path)
    return xlsx_path
