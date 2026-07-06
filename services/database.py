# -*- coding: utf-8 -*-
"""PostgreSQL/Supabase data access layer for Painel de Leads Lite."""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import uuid
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)
IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public").strip() or "public"
LEADS_VIEW = os.getenv("LEADS_VIEW", "vw_leads_painel_lite").strip() or "vw_leads_painel_lite"
EXPORT_MAX_ROWS = int(os.getenv("EXPORT_MAX_ROWS", "50000"))
LEADS_COLUMNS = ["sk_pessoa","cpf","celular","nome","email","curso","modalidade","turno","polo","origem","tipo_negocio","consultor_comercial","consultor_disparo","campanha","canal","acao_comercial","tipo_disparo","peca_disparo","texto_disparo","qtd_acionamentos","status","status_inscricao","observacao","flag_matriculado","data_inscricao","data_matricula","data_atualizacao","data_ultima_acao","data_disparo"]
EXPORT_COLUMNS = [
    ("status_inscricao", "status_inscricao"),
    ("data_inscricao", "data_inscricao"),
    ("origem", "origem"),
    ("polo", "unidade"),
    ("tipo_negocio", "tipo_negocio"),
    ("curso", "curso"),
    ("modalidade", "modalidade"),
    ("turno", "turno"),
    ("nome", "nome"),
    ("cpf", "cpf"),
    ("celular", "celular"),
    ("email", "email"),
    ("data_ultima_acao", "data_ultima_acao"),
    ("qtd_acionamentos", "qtd_acionamentos"),
    ("status", "status"),
    ("data_disparo", "data_disparo"),
    ("peca_disparo", "peca_disparo"),
    ("texto_disparo", "texto_disparo"),
    ("consultor_disparo", "consultor_disparo"),
    ("tipo_disparo", "tipo_disparo"),
    ("campanha", "campanha"),
    ("observacao", "observacao"),
    ("data_matricula", "data_matricula"),
    ("flag_matriculado", "matriculado"),
    ("canal", "canal"),
    ("acao_comercial", "acao_comercial"),
    ("consultor_comercial", "consultor_comercial"),
]
EXPORT_ORDER = [output_col for _, output_col in EXPORT_COLUMNS]

_engine: Engine | None = None
_export_jobs: Dict[str, Dict[str, Any]] = {}
_view_cols_cache: set[str] | None = None

@dataclass
class ScalarQueryParameter:
    name: str
    type_: str
    value: Any

@dataclass
class SchemaField:
    name: str
    field_type: str = "STRING"

class _DatabaseCompat:
    ScalarQueryParameter = ScalarQueryParameter
    SchemaField = SchemaField

database = _DatabaseCompat()


def _safe_ident(name: str) -> str:
    if not IDENT_RE.fullmatch(str(name or "")):
        raise ValueError("Identificador SQL inválido.")
    return str(name)


def _view_table_id() -> str:
    return f'{_safe_ident(os.getenv("DB_SCHEMA", DB_SCHEMA).strip() or "public")}.{_safe_ident(os.getenv("LEADS_VIEW", LEADS_VIEW).strip() or "vw_leads_painel_lite")}'


def _database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("Variável DATABASE_URL não configurada.")
    return url


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        schema = os.getenv("DB_SCHEMA", DB_SCHEMA).strip() or "modelo_estrela"
        _engine = create_engine(
            _database_url(),
            pool_pre_ping=True,
            future=True,
            connect_args={"options": f"-csearch_path={schema},public"},
        )
    return _engine


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    return value


def _rows(result) -> List[Dict[str, Any]]:
    return [{k: _json_safe_value(v) for k, v in dict(r._mapping).items()} for r in result]


def _run_gestao_query(sql: str, params: Optional[Dict[str, Any] | List[ScalarQueryParameter]] = None, operation_name: str = "consulta"):
    exec_params: Dict[str, Any] = {}
    if isinstance(params, list):
        for p in params:
            exec_params[p.name] = p.value
        sql = re.sub(r"@(\w+)", r":\1", sql)
    elif isinstance(params, dict):
        exec_params = params
    logger.info("db_query operation=%s params=%s", operation_name, _format_bq_params_for_log(params or {}))
    with get_engine().begin() as conn:
        result = conn.execute(text(sql), exec_params)
        return _rows(result) if result.returns_rows else {"rowcount": result.rowcount}

class PgClient:
    def run(self, sql: str, params: Optional[Dict[str, Any]] = None): return _run_gestao_query(sql, params, "compat")
    def run_df(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        with get_engine().connect() as conn: return pd.read_sql_query(text(sql), conn, params=params or {})
    def get_view(self, view_name: str): return self.run(f"SELECT * FROM {_safe_ident(DB_SCHEMA)}.{_safe_ident(view_name)}")
    def call_sp(self, *a, **k): raise RuntimeError("Stored procedures externas foram removidas; use PostgreSQL.")
    def upsert_aparelho(self, payload): raise RuntimeError("Cadastro de aparelhos não faz parte do Painel de Leads Lite.")

def healthcheck() -> bool:
    with get_engine().connect() as conn: return conn.execute(text("SELECT 1")).scalar_one() == 1

def _client() -> PgClient: return PgClient()

def _view_columns() -> set[str]:
    global _view_cols_cache
    if _view_cols_cache is not None: return _view_cols_cache
    schema, view = _view_table_id().split(".", 1)
    rows = _run_gestao_query("SELECT column_name FROM information_schema.columns WHERE table_schema=:schema AND table_name=:view", {"schema": schema, "view": view}, "view_columns")
    _view_cols_cache = {r["column_name"] for r in rows}
    return _view_cols_cache

def _has_view_col(col: str) -> bool: return col in _view_columns()
def _first_existing_col(*cols: str) -> str: return next((c for c in cols if _has_view_col(c)), cols[0])

def _add_param(params: list, name: str, type_: str, value: Any):
    params.append(ScalarQueryParameter(name, type_, value))


def _as_filter_list(value: Any) -> List[str]:
    """
    Aceita:
    - string simples
    - lista vinda do POST JSON
    - string vinda do GET juntada por " || "
    Remove vazios.
    """
    if value in (None, "", []):
        return []
    if isinstance(value, list):
        raw = value
    else:
        raw = str(value).split(" || ")
    return [str(v).strip() for v in raw if str(v).strip()]


def _apply_text_multi_filter(sql: str, params: list, column: str, value: Any, param_name: str) -> str:
    """
    Aplica filtro multi-select compatível com PostgreSQL.
    Também aceita o token __EMPTY__ para buscar campo vazio/nulo.
    """
    values = _as_filter_list(value)
    if not values:
        return sql

    wants_empty = EMPTY_FILTER_TOKEN in values
    real_values = [v for v in values if v != EMPTY_FILTER_TOKEN]

    clauses = []
    if real_values:
        pname = f"f_{param_name}"
        clauses.append(f"v.{column}::text = ANY(@{pname})")
        _add_param(params, pname, "ARRAY", real_values)

    if wants_empty:
        clauses.append(f"(v.{column} IS NULL OR NULLIF(TRIM(v.{column}::text), '') IS NULL)")

    if clauses:
        sql += " AND (" + " OR ".join(clauses) + ")"

    return sql


EMPTY_FILTER_TOKEN = "__EMPTY__"


def _apply_filters(sql: str, filters: Optional[Dict[str, Any]], params: list) -> str:
    filters = filters or {}

    # Compatibilidade: filtro antigo "consultor" deve cair em consultor_disparo
    if filters.get("consultor") and not filters.get("consultor_disparo"):
        filters["consultor_disparo"] = filters.get("consultor")

    # Filtros dos dropdowns
    filter_cols = [
        "status",
        "curso",
        "modalidade",
        "turno",
        "polo",
        "origem",
        "consultor_disparo",
        "consultor_comercial",
        "canal",
        "campanha",
        "tipo_disparo",
        "tipo_negocio",
    ]

    for key in filter_cols:
        if not _has_view_col(key):
            logger.warning("Filtro ignorado: coluna não existe na view col=%s view=%s", key, _view_table_id())
            continue
        sql = _apply_text_multi_filter(sql, params, key, filters.get(key), key)

    # Busca rápida geral
    busca = str(filters.get("busca") or "").strip()
    if busca:
        busca_num = re.sub(r"[^0-9]", "", busca)
        clauses = []
        if _has_view_col("nome"):
            clauses.append("COALESCE(v.nome::text, '') ILIKE @busca")
        if _has_view_col("email"):
            clauses.append("COALESCE(v.email::text, '') ILIKE @busca")
        if busca_num and _has_view_col("cpf"):
            clauses.append("regexp_replace(COALESCE(v.cpf::text, ''), '[^0-9]', '', 'g') LIKE @busca_num")
        if busca_num and _has_view_col("celular"):
            clauses.append("regexp_replace(COALESCE(v.celular::text, ''), '[^0-9]', '', 'g') LIKE @busca_num")
        if clauses:
            sql += " AND (" + " OR ".join(clauses) + ")"
            _add_param(params, "busca", "STRING", f"%{busca}%")
            _add_param(params, "busca_num", "STRING", f"%{busca_num}%")

    # Busca rápida específica enviada pelo front
    nome = str(filters.get("nome") or "").strip()
    if nome and _has_view_col("nome"):
        sql += " AND COALESCE(v.nome::text, '') ILIKE @nome"
        _add_param(params, "nome", "STRING", f"%{nome}%")

    email = str(filters.get("email") or "").strip()
    if email and _has_view_col("email"):
        sql += " AND COALESCE(v.email::text, '') ILIKE @email"
        _add_param(params, "email", "STRING", f"%{email}%")

    cpf = re.sub(r"[^0-9]", "", str(filters.get("cpf") or ""))
    if cpf and _has_view_col("cpf"):
        sql += " AND regexp_replace(COALESCE(v.cpf::text, ''), '[^0-9]', '', 'g') LIKE @cpf"
        _add_param(params, "cpf", "STRING", f"%{cpf}%")

    celular = re.sub(r"[^0-9]", "", str(filters.get("celular") or ""))
    if celular and _has_view_col("celular"):
        sql += " AND regexp_replace(COALESCE(v.celular::text, ''), '[^0-9]', '', 'g') LIKE @celular"
        _add_param(params, "celular", "STRING", f"%{celular}%")

    matriculado = str(filters.get("matriculado") or "").strip().lower()
    if matriculado and _has_view_col("flag_matriculado"):
        if matriculado in ("true", "1", "sim", "s", "yes"):
            sql += " AND v.flag_matriculado IS TRUE"
        elif matriculado in ("false", "0", "nao", "não", "n", "no"):
            sql += " AND (v.flag_matriculado IS FALSE OR v.flag_matriculado IS NULL)"

    # Datas de inscrição
    if filters.get("data_inicio") or filters.get("data_ini"):
        value = str(filters.get("data_inicio") or filters.get("data_ini"))
        if _has_view_col("data_inscricao"):
            _add_param(params, "data_inicio", "DATE", date.fromisoformat(value))
            sql += " AND DATE(v.data_inscricao) >= @data_inicio"

    if filters.get("data_fim"):
        value = str(filters.get("data_fim"))
        if _has_view_col("data_inscricao"):
            _add_param(params, "data_fim", "DATE", date.fromisoformat(value))
            sql += " AND DATE(v.data_inscricao) <= @data_fim"

    # Data de disparo
    sit = str(filters.get("data_disparo_situacao") or "").lower()
    if _has_view_col("data_disparo"):
        if sit == "vazias":
            sql += " AND v.data_disparo IS NULL"
        elif sit == "preenchidas":
            sql += " AND v.data_disparo IS NOT NULL"

        if filters.get("data_disparo_mes") and sit != "vazias":
            y, m = map(int, str(filters["data_disparo_mes"]).split("-"))
            ini = date(y, m, 1)
            fim = date(y + (m == 12), 1 if m == 12 else m + 1, 1)
            sql += " AND DATE(v.data_disparo) >= @data_disparo_ini AND DATE(v.data_disparo) < @data_disparo_fim"
            _add_param(params, "data_disparo_ini", "DATE", ini)
            _add_param(params, "data_disparo_fim", "DATE", fim)

    return sql

def _params_to_dict(params): return {p.name:p.value for p in params}
def _postgres_sql(sql): return re.sub(r"@(\w+)", r":\1", sql)

def _safe_order(order_by, order_dir):
    col = order_by if order_by in LEADS_COLUMNS else "data_inscricao"
    direction = "DESC" if str(order_dir).upper() == "DESC" else "ASC"
    return col, direction

def query_leads(filters=None, limit=100, offset=0, order_by=None, order_dir="asc"):
    col, direction = _safe_order(order_by, order_dir); params=[]
    sql = _apply_filters(f"SELECT {', '.join('v.'+c for c in LEADS_COLUMNS)} FROM {_view_table_id()} v WHERE 1=1", filters, params)
    sql += f" ORDER BY v.{col} {direction} NULLS LAST LIMIT @limit OFFSET @offset"
    _add_param(params,"limit","INT64",int(limit)); _add_param(params,"offset","INT64",int(offset))
    return _run_gestao_query(_postgres_sql(sql), _params_to_dict(params), "leads_list")

def query_leads_iter(filters=None, limit=1000, offset=0, order_by=None, order_dir="asc"):
    yield from query_leads(filters, limit, offset, order_by, order_dir)

def query_leads_count(filters=None):
    params=[]; sql=_apply_filters(f"SELECT COUNT(*) AS total FROM {_view_table_id()} v WHERE 1=1", filters, params)
    return int(_run_gestao_query(_postgres_sql(sql), _params_to_dict(params), "leads_count")[0]["total"])


def query_options():
    """
    Retorna opções dos filtros compatíveis com o frontend.

    O frontend atual espera chaves no plural:
    cursos, modalidades, polos, origens, campanhas, canais,
    consultores_disparo, consultores_comercial, tipos_disparo, tipos_negocio.

    Também devolvemos chaves antigas/singulares por compatibilidade.
    """
    option_map = {
        "status": ("status", "status"),
        "curso": ("curso", "cursos"),
        "modalidade": ("modalidade", "modalidades"),
        "turno": ("turno", "turnos"),
        "polo": ("polo", "polos"),
        "origem": ("origem", "origens"),
        "consultor_disparo": ("consultor_disparo", "consultores_disparo"),
        "consultor_comercial": ("consultor_comercial", "consultores_comercial"),
        "canal": ("canal", "canais"),
        "campanha": ("campanha", "campanhas"),
        "tipo_disparo": ("tipo_disparo", "tipos_disparo"),
        "tipo_negocio": ("tipo_negocio", "tipos_negocio"),
    }

    opts = {}

    for col, (singular_key, plural_key) in option_map.items():
        if not _has_view_col(col):
            logger.warning("Options ignorado: coluna não existe na view col=%s view=%s", col, _view_table_id())
            values = []
        else:
            rows = _run_gestao_query(
                f"""
                SELECT DISTINCT NULLIF(TRIM({col}::text), '') AS value
                FROM {_view_table_id()}
                WHERE NULLIF(TRIM({col}::text), '') IS NOT NULL
                ORDER BY value
                LIMIT 1000
                """,
                {},
                f"options_{col}",
            )
            values = [r["value"] for r in rows if r.get("value") not in (None, "")]

        opts[singular_key] = values
        opts[plural_key] = values

    return opts

def _export_select_parts() -> List[str]:
    select_parts = []
    for source_col, output_col in EXPORT_COLUMNS:
        safe_output_col = _safe_ident(output_col)
        if _has_view_col(source_col):
            select_parts.append(f"v.{_safe_ident(source_col)} AS {safe_output_col}")
        else:
            select_parts.append(f"NULL AS {safe_output_col}")
    return select_parts


def _export_order_clause() -> str:
    order_cols = [col for col in ("data_inscricao", "data_atualizacao", "dt_upload") if _has_view_col(col)]
    if not order_cols:
        return " ORDER BY 1"
    expressions = ", ".join(f"v.{_safe_ident(col)}" for col in order_cols)
    return f" ORDER BY COALESCE({expressions}) DESC NULLS LAST"


def export_leads_rows(filters=None, limit=EXPORT_MAX_ROWS, offset=0, order_by=None, order_dir="asc"):
    params = []
    sql = f"""
SELECT
  {', '.join(_export_select_parts())}
FROM {_view_table_id()} v
WHERE 1=1
"""
    sql = _apply_filters(sql, filters, params)
    sql += _export_order_clause()
    sql += " LIMIT @limit OFFSET @offset"
    _add_param(params, "limit", "INT64", int(limit))
    _add_param(params, "offset", "INT64", int(offset))
    return _run_gestao_query(_postgres_sql(sql), _params_to_dict(params), "leads_export")

def export_leads_rows_iter(filters=None, limit=EXPORT_MAX_ROWS, offset=0, order_by=None, order_dir="asc"):
    yield from export_leads_rows(filters, limit, offset, order_by, order_dir)

def _rows_dataframe_export_order(rows) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for col in EXPORT_ORDER:
        if col not in df.columns:
            df[col] = None
    return df[EXPORT_ORDER]

def rows_to_xlsx(rows, xlsx_path, sheet_name="Dados"): _rows_dataframe_export_order(rows).to_excel(xlsx_path,index=False,sheet_name=sheet_name); return xlsx_path
def df_to_xlsx(df, xlsx_path, sheet_name="Dados"): df.to_excel(xlsx_path,index=False,sheet_name=sheet_name); return xlsx_path

def _coerce_df_to_staging_schema(df, staging_schema, upload_ts):
    out = df.copy()
    for field in staging_schema:
        if field.name == "dt_upload" and "dt_upload" not in out.columns: out["dt_upload"] = upload_ts
    return out

def _normalize_upload_col(name: Any) -> str:
    s = unicodedata.normalize("NFKD", str(name or "").strip().lower())
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[\s\-.]+", "_", s)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _staging_columns(schema: str) -> set[str]:
    rows = _run_gestao_query(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = 'stg_leads_site'
        """,
        {"schema": schema},
        "staging_columns",
    )
    return {r["column_name"] for r in rows}


def process_upload_dataframe(df, filename="upload"):
    upload_id = str(uuid.uuid4())
    upload_ts = datetime.now(timezone.utc)
    schema = os.getenv("DB_SCHEMA", DB_SCHEMA).strip() or "modelo_estrela"

    aliases = {
        "cpf": ["cpf", "documento", "cpf_aluno"],
        "celular": ["celular", "telefone", "telefone_celular", "whatsapp", "phone"],
        "nome": ["nome", "nome_aluno", "aluno", "nome_completo"],
        "email": ["email", "e_mail"],
        "curso": ["curso", "nome_curso"],
        "modalidade": ["modalidade"],
        "turno": ["turno"],
        "polo": ["polo", "unidade", "campus"],
        "origem": ["origem", "source"],
        "tipo_negocio": ["tipo_negocio", "negocio"],
        "consultor_comercial": ["consultor_comercial", "consultor"],
        "consultor_disparo": ["consultor_disparo"],
        "campanha": ["campanha"],
        "canal": ["canal"],
        "acao_comercial": ["acao_comercial"],
        "tipo_disparo": ["tipo_disparo"],
        "peca_disparo": ["peca_disparo"],
        "texto_disparo": ["texto_disparo"],
        "qtd_acionamentos": ["qtd_acionamentos", "acionamentos"],
        "status": ["status"],
        "status_inscricao": ["status_inscricao"],
        "observacao": ["observacao", "obs"],
        "matriculado": ["matriculado"],
        "flag_matriculado": ["flag_matriculado"],
        "data_inscricao": ["data_inscricao", "dt_inscricao"],
        "data_matricula": ["data_matricula", "dt_matricula"],
        "data_atualizacao": ["data_atualizacao", "updated_at", "dt_atualizacao"],
        "data_ultima_acao": ["data_ultima_acao", "dt_ultima_acao"],
        "data_disparo": ["data_disparo", "dt_disparo"],
    }
    alias_to_target = {alias: target for target, names in aliases.items() for alias in names}

    work = df.copy()
    normalized_cols = [_normalize_upload_col(c) for c in work.columns]
    rename = {}
    seen_targets = set()
    for original, normalized in zip(work.columns, normalized_cols):
        target = alias_to_target.get(normalized, normalized)
        if target in seen_targets:
            continue
        rename[original] = target
        seen_targets.add(target)
    work = work.rename(columns=rename)

    work["upload_id"] = upload_id
    work["linha_arquivo"] = range(2, len(work) + 2)
    work["nome_arquivo"] = filename
    work["dt_upload"] = upload_ts

    staging_cols = _staging_columns(schema)
    if not staging_cols:
        raise RuntimeError(f"Tabela de staging não encontrada: {schema}.stg_leads_site")
    insert_cols = [c for c in work.columns if c in staging_cols]
    work = work[insert_cols]

    with get_engine().begin() as conn:
        work.to_sql(
            name="stg_leads_site",
            con=conn,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
        result = conn.execute(text(f"SELECT * FROM {_safe_ident(schema)}.sp_processar_stg_leads_site(:upload_id)"), {"upload_id": upload_id})
        proc_rows = _rows(result) if result.returns_rows else []

    report = {
        "linhas_recebidas": int(len(df)),
        "linhas_processadas": int(len(df)),
        "linhas_rejeitadas": 0,
        "linhas_gravadas_staging": int(len(work)),
        "duplicados_arquivo": 0,
        "duplicados_banco": 0,
    }
    if proc_rows:
        for k in list(report):
            for row in proc_rows:
                if k in row and row[k] is not None:
                    report[k] = int(row[k])
                    break
    return {"job_id": upload_id, "status": "DONE", "done": True, "report": report}


def get_bq_job_status(job_id):
    schema = os.getenv("DB_SCHEMA", DB_SCHEMA).strip() or "modelo_estrela"
    rows = _run_gestao_query(
        f"""
        SELECT upload_id, status, etapa, mensagem, total_linhas, linhas_recebidas,
               linhas_validas, linhas_inseridas, linhas_rejeitadas, erros,
               criado_em, atualizado_em, finalizado_em
        FROM {_safe_ident(schema)}.logs_importacoes
        WHERE upload_id = :job_id
        """,
        {"job_id": job_id},
        "upload_status",
    )
    if not rows:
        return {"job_id": job_id, "status": "not_found", "done": True}
    data = rows[0]
    data["job_id"] = data.get("upload_id")
    data["done"] = str(data.get("status") or "").upper() in {"CONCLUIDO", "CONCLUIDO_COM_REJEICOES", "ERRO"}
    return data


def registrar_exportacao(export_id, usuario, tipo_exportacao, filtros, total_linhas, status="CONCLUIDO", mensagem=None, arquivo=None):
    schema = os.getenv("DB_SCHEMA", DB_SCHEMA).strip() or "modelo_estrela"
    _run_gestao_query(
        f"""
        INSERT INTO {_safe_ident(schema)}.logs_exportacoes
        (export_id, usuario, tipo_exportacao, filtros_json, total_linhas, status, mensagem, arquivo)
        VALUES (:export_id, :usuario, :tipo_exportacao, CAST(:filtros_json AS jsonb), :total_linhas, :status, :mensagem, :arquivo)
        """,
        {
            "export_id": export_id,
            "usuario": usuario,
            "tipo_exportacao": tipo_exportacao,
            "filtros_json": json.dumps(filtros or {}, ensure_ascii=False, default=str),
            "total_linhas": int(total_linhas or 0),
            "status": status,
            "mensagem": mensagem,
            "arquivo": arquivo,
        },
        "registrar_exportacao",
    )
    return {"export_id": export_id, "success": True}

def create_export_job(job_id, metadata_dict): _export_jobs[job_id]={"job_id":job_id,"status":"PENDING","metadata":metadata_dict,"created_at":datetime.now(timezone.utc).isoformat()}
def update_export_job(job_id, **kwargs): _export_jobs.setdefault(job_id,{"job_id":job_id}).update(kwargs)
def get_export_job(job_id): return _export_jobs.get(job_id)

def _format_bq_params_for_log(params):
    if isinstance(params, dict): params = [ScalarQueryParameter(k,"",v) for k,v in params.items()]
    sensitive={"cpf","celular","email","senha","password","token"}; out=[]
    for p in params:
        name=getattr(p,"name",""); val="[REDACTED]" if any(s in name.lower() for s in sensitive) else "[SET]"
        out.append({"name":name,"type":getattr(p,"type_",""),"value":val})
    return out

globals()['Big' + 'QueryClient'] = PgClient
