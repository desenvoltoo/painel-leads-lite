# services/bigquery.py
from __future__ import annotations

import os
import logging
from time import perf_counter
from concurrent.futures import TimeoutError as FuturesTimeoutError
from decimal import Decimal, InvalidOperation
from pathlib import Path
from io import BytesIO, StringIO
from datetime import timedelta
import uuid
from typing import Any, Dict, Iterator, List, Optional, Tuple

from google.cloud import bigquery, storage
from google.api_core.exceptions import GoogleAPICallError, BadRequest, Forbidden, NotFound

# XLSX
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

logger = logging.getLogger(__name__)

# ============================================================
# CONFIG (ENV + travas)
# ============================================================
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "painel-universidade")
BQ_DATASET = os.getenv("BQ_DATASET", "modelo_estrela")

BQ_STAGING_TABLE = os.getenv("BQ_STAGING_TABLE", "stg_leads_site")
BQ_PROCEDURE = os.getenv("BQ_PROCEDURE", "sp_import_star_from_site")

# Painel lê somente essa view
BQ_VIEW_LEADS = "vw_leads_painel_lite"

DEFAULT_LIMIT = int(os.getenv("BQ_DEFAULT_LIMIT", "200"))
MAX_LIMIT = int(os.getenv("BQ_MAX_LIMIT", "200000"))  # <= 0 para sem limite
EXPORT_MAX_ROWS = int(os.getenv("BQ_EXPORT_MAX_ROWS", "50000"))
QUERY_TIMEOUT_SECONDS = int(os.getenv("BQ_QUERY_TIMEOUT_SECONDS", "180"))
EMPTY_FILTER_TOKEN = "__EMPTY__"

# colunas que não devem sofrer comportamento numérico
PHONEISH_COLUMNS = {"cpf", "celular"}

_bq_client: Optional[bigquery.Client] = None
_storage_client: Optional[storage.Client] = None


def get_bq_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    return _bq_client


def _tbl(name: str) -> str:
    return f"`{GCP_PROJECT_ID}.{BQ_DATASET}.{name}`"



GCS_UPLOAD_BUCKET = os.getenv("GCS_UPLOAD_BUCKET", "")
GCS_UPLOAD_PREFIX = os.getenv("GCS_UPLOAD_PREFIX", "uploads")


def get_storage_client() -> storage.Client:
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client(project=GCP_PROJECT_ID)
    return _storage_client


def _get_upload_bucket():
    if not GCS_UPLOAD_BUCKET:
        raise RuntimeError("Defina GCS_UPLOAD_BUCKET no ambiente.")
    return get_storage_client().bucket(GCS_UPLOAD_BUCKET)


def generate_gcs_signed_upload(filename: str, source_tag: str = "manual") -> Dict[str, str]:
    bucket = _get_upload_bucket()
    safe_name = Path(filename).name
    safe_source = (source_tag or "manual").replace("/", "_").replace("\\", "_")
    object_name = f"{GCS_UPLOAD_PREFIX.strip('/')}/{safe_source}/{uuid.uuid4().hex}_{safe_name}"
    blob = bucket.blob(object_name)
    try:
        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=15),
            method="PUT",
            content_type="application/octet-stream",
        )
    except Exception as e:
        raise RuntimeError(
            "Não foi possível assinar URL do GCS. "
            "Verifique GCS_UPLOAD_BUCKET e permissões da Service Account para assinar URLs."
        ) from e
    return {"upload_url": signed_url, "object_name": object_name, "bucket": bucket.name}



def _detect_csv_encoding(raw_bytes: bytes) -> str:
    try:
        import chardet

        detected = chardet.detect(raw_bytes)
        encoding = (detected.get("encoding") or "utf-8").lower()
        confidence = detected.get("confidence") or 0
        if confidence >= 0.7:
            return "utf-8" if encoding == "ascii" else encoding
    except ImportError:
        pass
    return "utf-8"


def _csv_blob_to_dataframe(blob) -> Any:
    import pandas as pd

    payload = blob.download_as_bytes()
    detected_enc = _detect_csv_encoding(payload)
    encodings_to_try: List[str] = []
    for enc in (detected_enc, "utf-8-sig", "utf-8", "latin-1"):
        if enc not in encodings_to_try:
            encodings_to_try.append(enc)

    last_error: Optional[Exception] = None
    best_df = None
    for sep in (";", ","):
        for enc in encodings_to_try:
            try:
                df = pd.read_csv(BytesIO(payload), sep=sep, encoding=enc, dtype=str)
                if len(df.columns) > 1:
                    return df
                if best_df is None:
                    best_df = df
            except Exception as exc:
                last_error = exc

    if best_df is not None:
        return best_df

    raise RuntimeError("Não foi possível ler o CSV enviado via GCS.") from last_error

def _xlsx_blob_to_dataframe(blob) -> Any:
    from openpyxl import load_workbook
    import pandas as pd

    payload = blob.download_as_bytes()
    wb = load_workbook(BytesIO(payload), data_only=True, read_only=True)
    ws = wb.active
    rows = ws.iter_rows(values_only=True)
    try:
        first_row = next(rows)
    except StopIteration:
        raise RuntimeError("Arquivo XLSX está vazio.")
    headers = [str(c).strip() if c is not None else "" for c in first_row]
    if not any(headers):
        raise RuntimeError("Cabeçalho do XLSX não encontrado.")
    data = []
    for r in rows:
        data.append({headers[i]: r[i] if i < len(r) else None for i in range(len(headers))})
    return pd.DataFrame(data)


def _load_dataframe_to_staging_via_gcs(df) -> None:
    client = get_bq_client()
    bucket = _get_upload_bucket()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE}"
    df2 = _coerce_df_to_staging_schema(df)

    temp_name = f"{GCS_UPLOAD_PREFIX.strip('/')}/tmp/staging_{uuid.uuid4().hex}.csv"
    tmp_blob = bucket.blob(temp_name)
    csv_buf = StringIO()
    df2.to_csv(csv_buf, index=False)
    tmp_blob.upload_from_string(csv_buf.getvalue(), content_type="text/csv")

    uri = f"gs://{bucket.name}/{temp_name}"
    job = client.load_table_from_uri(uri, table_id, job_config=bigquery.LoadJobConfig(schema=STAGING_SCHEMA, source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE))
    job.result()
    tmp_blob.delete()


def process_gcs_upload(object_name: str) -> Dict[str, Any]:
    bucket = _get_upload_bucket()
    blob = bucket.blob(object_name)
    if not blob.exists():
        raise RuntimeError(f"Arquivo não encontrado no GCS: {object_name}")

    suffix = Path(object_name).suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        df = _xlsx_blob_to_dataframe(blob)
    else:
        df = _csv_blob_to_dataframe(blob)

    _load_dataframe_to_staging_via_gcs(df)
    job_id = run_procedure_async()
    blob.delete()
    return {"message": "Upload processado com sucesso.", "job_id": job_id}

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


def _data_inscricao_order_clause(order_dir: str) -> str:
    """
    Ordena mantendo linhas sem data_inscricao no final.
    - primeiro: quem tem data (NULL por último)
    - depois: data mais recente/antiga conforme order_dir
    - por fim: desempate determinístico por data_atualizacao
    """
    return f"""
    CASE WHEN v.data_inscricao IS NULL THEN 1 ELSE 0 END ASC,
    v.data_inscricao {order_dir},
    v.data_atualizacao DESC
    """


def _data_disparo_priority_order_clause() -> str:
    """
    Regra de ordenação exigida pelo painel/export:
    1) data_disparo vazia (NULL ou string vazia) primeiro
    2) depois registros com data_disparo preenchida, da mais antiga para a mais recente
    3) desempate determinístico por data_inscricao e data_atualizacao
    """
    return """
    CASE WHEN v.data_disparo IS NULL OR TRIM(CAST(v.data_disparo AS STRING)) = '' THEN 0 ELSE 1 END ASC,
    SAFE_CAST(v.data_disparo AS DATE) ASC NULLS LAST,
    v.data_inscricao ASC NULLS LAST,
    v.data_atualizacao DESC
    """


# ============================================================
# NORMALIZADORES
# ============================================================
def _normalize_decimal_string_to_int_string(s: str) -> str:
    """
    Converte representações numéricas integrais para string inteira.

    Exemplos:
      "11974817404.0" -> "11974817404"
      "5.511944391404e13" -> "55119443914040"
      "  12345  " -> "12345"

    Se não for número integral, devolve o valor original.
    """
    s = str(s).strip()
    if not s:
        return s

    try:
        d = Decimal(s)
        if d == d.to_integral_value():
            return str(d.quantize(Decimal("1")))
        return s
    except (InvalidOperation, ValueError):
        return s


def _normalize_phoneish_value(x: Any) -> Optional[str]:
    """
    Normaliza CPF/celular preservando como texto e removendo '.0' indevido.

    Regras:
    - None / NaN / vazio -> None
    - float integral -> string inteira
    - string numérica integral com .0 / notação científica -> string inteira
    - demais strings -> trim simples
    """
    try:
        import pandas as pd
    except Exception:
        pd = None

    if x is None:
        return None

    if pd is not None:
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass

    # números Python
    if isinstance(x, int):
        return str(x)

    if isinstance(x, float):
        if pd is not None:
            try:
                if pd.isna(x):
                    return None
            except Exception:
                pass
        # se for 11974817404.0 -> 11974817404
        if x.is_integer():
            return str(int(x))
        s = str(x).strip()
        return s or None

    s = str(x).strip()
    if not s or s.lower() == "nan":
        return None

    # corrige:
    # "11974817404.0" -> "11974817404"
    # "5.511944391404e13" -> "55119443914040"
    s2 = _normalize_decimal_string_to_int_string(s)

    return s2 or None


def _normalize_generic_string(x: Any) -> Optional[str]:
    """
    Converte qualquer valor para STRING segura sem estourar upload.
    """
    try:
        import pandas as pd
    except Exception:
        pd = None

    if x is None:
        return None

    if pd is not None:
        try:
            if pd.isna(x):
                return None
        except Exception:
            pass

    s = str(x).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def _xlsx_safe_cell_value(key: str, value: Any):
    """
    Mantém datas compatíveis com Excel e força CPF/celular como texto.
    """
    from datetime import datetime, date

    if key in PHONEISH_COLUMNS:
        return _normalize_phoneish_value(value)

    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, date):
        return value

    return value


# ============================================================
# STAGING SCHEMA (BLINDADO)
# ============================================================
# Tudo STRING para não quebrar em CSV/XLSX e deixar a SP parsear
STAGING_SCHEMA: List[bigquery.SchemaField] = [
    bigquery.SchemaField("status_inscricao", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("data_inscricao", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("origem", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("unidade", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipo_negocio", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("curso", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("modalidade", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("turno", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("nome", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cpf", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("celular", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("data_ultima_acao", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("qtd_acionamentos", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("data_disparo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("peca_disparo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("texto_disparo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("consultor_disparo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipo_disparo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("campanha", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("observacao", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("data_matricula", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("matriculado", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("canal", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("acao_comercial", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("consultor_comercial", "STRING", mode="NULLABLE"),
]


def _coerce_df_to_staging_schema(df):
    """
    Ajusta o DataFrame para bater com o schema da staging.
    Blindado:
    - manda tudo como STRING;
    - protege CPF/celular de .0 / float / notação científica;
    - mantém ordem do schema.
    """
    import pandas as pd

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    expected_cols = [f.name for f in STAGING_SCHEMA]

    # cria colunas faltantes
    for c in expected_cols:
        if c not in df.columns:
            df[c] = None

    # remove extras e mantém ordem
    df = df[expected_cols]

    for col in expected_cols:
        if col in PHONEISH_COLUMNS:
            df[col] = df[col].map(_normalize_phoneish_value)
        else:
            df[col] = df[col].map(_normalize_generic_string)

    # reforça dtype object/string-like
    for col in expected_cols:
        df[col] = df[col].astype("object")

    return df


# ============================================================
# STAGING + PROCEDURE (upload)  AGORA ASSÍNCRONO
# ============================================================
def load_to_staging(df) -> None:
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE}"

    try:
        df2 = _coerce_df_to_staging_schema(df)
        job = client.load_table_from_dataframe(
            df2,
            table_id,
            job_config=bigquery.LoadJobConfig(
                schema=STAGING_SCHEMA,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        )
        job.result()
    except (BadRequest, Forbidden, NotFound, GoogleAPICallError) as e:
        logger.exception("BQ load_to_staging falhou: %s", str(e))
        raise RuntimeError(f"Erro ao carregar staging ({table_id}): {e}") from e


def run_procedure_async() -> str:
    """
    Dispara a procedure e retorna o job_id sem bloquear a request.
    """
    client = get_bq_client()
    sql = f"CALL `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PROCEDURE}`();"

    try:
        job = client.query(sql)
        return job.job_id
    except (BadRequest, Forbidden, NotFound, GoogleAPICallError) as e:
        logger.exception("BQ run_procedure_async falhou: %s", str(e))
        raise RuntimeError(f"Erro ao disparar procedure ({BQ_PROCEDURE}): {e}") from e


def process_upload_dataframe(df) -> str:
    """
    1) carrega staging (truncate)
    2) dispara SP async
    3) retorna job_id
    """
    load_to_staging(df)
    return run_procedure_async()


def get_bq_job_status(job_id: str) -> Dict[str, Any]:
    client = get_bq_client()
    job = client.get_job(job_id)

    payload: Dict[str, Any] = {
        "job_id": job.job_id,
        "state": job.state,
        "created": job.created.isoformat() if job.created else None,
        "started": job.started.isoformat() if job.started else None,
        "ended": job.ended.isoformat() if job.ended else None,
    }

    if job.error_result:
        payload["ok"] = False
        payload["error"] = job.error_result
        payload["errors"] = job.errors
    else:
        payload["ok"] = True if job.state == "DONE" else None

    return payload


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

    def _add_string_filter(column: str, values: List[str], param_name: str):
        nonlocal sql
        if not values:
            return

        include_empty = EMPTY_FILTER_TOKEN in values
        normal_values = [v for v in values if v != EMPTY_FILTER_TOKEN]

        clauses: List[str] = []
        if normal_values:
            clauses.append(f"{column} IN UNNEST(@{param_name})")
            params.append(bigquery.ArrayQueryParameter(param_name, "STRING", normal_values))

        if include_empty:
            clauses.append(f"({column} IS NULL OR TRIM(CAST({column} AS STRING)) = '')")

        if clauses:
            sql += " AND (" + " OR ".join(clauses) + ")"

    _add_string_filter("v.curso", cursos, "cursos")
    _add_string_filter("v.polo", polos, "polos")
    _add_string_filter("v.modalidade", modalidades, "modalidades")
    _add_string_filter("v.turno", turnos, "turnos")
    _add_string_filter("v.canal", canais, "canais")
    _add_string_filter("v.campanha", campanhas, "campanhas")
    _add_string_filter("v.origem", origens, "origens")
    _add_string_filter("v.tipo_negocio", tipos_negocio, "tipos_negocio")
    _add_string_filter("v.consultor_disparo", consultores_disp, "consultores_disp")
    _add_string_filter("v.consultor_comercial", consultores_com, "consultores_com")
    _add_string_filter("v.tipo_disparo", tipos_disparo, "tipos_disparo")

    if status_list:
        include_empty = EMPTY_FILTER_TOKEN in status_list
        normal_status = [v for v in status_list if v != EMPTY_FILTER_TOKEN]
        clauses: List[str] = []

        if normal_status:
            clauses.append("(v.status_inscricao IN UNNEST(@status_list) OR v.status IN UNNEST(@status_list))")
            params.append(bigquery.ArrayQueryParameter("status_list", "STRING", normal_status))

        if include_empty:
            clauses.append("(v.status IS NULL OR TRIM(CAST(v.status AS STRING)) = '')")

        if clauses:
            sql += " AND (" + " OR ".join(clauses) + ")"

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

    if filters.get("matriculado") is not None and str(filters.get("matriculado")).strip() != "":
        val = str(filters.get("matriculado")).lower().strip()
        b = True if val in ("true", "1", "sim", "yes") else False if val in ("false", "0", "nao", "não", "no") else None
        if b is not None:
            sql += " AND IFNULL(v.flag_matriculado, FALSE) = @matriculado"
            params.append(bigquery.ScalarQueryParameter("matriculado", "BOOL", b))

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
    rows_iter = query_leads_iter(
        filters=filters,
        limit=limit,
        offset=offset,
        order_by=order_by,
        order_dir=order_dir,
    )
    return list(rows_iter)


def _build_leads_query(
    filters: Optional[Dict[str, Any]] = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    order_by: str = "data_inscricao",
    order_dir: str = "DESC",
) -> Tuple[str, List[Any], int]:
    filters = filters or {}
    requested_limit = max(1, int(limit))
    effective_limit = requested_limit
    if MAX_LIMIT > 0 and requested_limit > MAX_LIMIT:
        logger.warning(
            "query_leads com limit acima do recomendado requested=%s recommended_max=%s (sem clamp aplicado)",
            requested_limit,
            MAX_LIMIT,
        )

    offset = max(0, int(offset))
    order_dir = "ASC" if str(order_dir).upper() == "ASC" else "DESC"

    if order_by == "data_inscricao_dt":
        order_by = "data_inscricao"

    allowed_order = {
        "data_inscricao": "v.data_inscricao",
        "data_inscricao_dt": "v.data_inscricao",
        "data_disparo": "v.data_disparo",
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
    if order_by == "data_disparo":
        order_clause = _data_disparo_priority_order_clause()
    elif order_by in ("data_inscricao", "data_inscricao_dt"):
        order_clause = _data_inscricao_order_clause(order_dir)
    else:
        order_clause = f"{order_expr} {order_dir}"
    sql += f"\n ORDER BY {order_clause} \n LIMIT @limit OFFSET @offset"

    params.append(bigquery.ScalarQueryParameter("limit", "INT64", effective_limit))
    params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))
    return sql, params, effective_limit


def query_leads_iter(
    filters: Optional[Dict[str, Any]] = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    order_by: str = "data_inscricao",
    order_dir: str = "DESC",
) -> Iterator[Dict[str, Any]]:
    client = get_bq_client()
    sql, params, effective_limit = _build_leads_query(
        filters=filters,
        limit=limit,
        offset=offset,
        order_by=order_by,
        order_dir=order_dir,
    )
    t0 = perf_counter()
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    try:
        rows = job.result(timeout=QUERY_TIMEOUT_SECONDS)
    except FuturesTimeoutError as e:
        logger.exception(
            "BQ query_leads timeout job_id=%s limit=%s timeout=%ss",
            getattr(job, "job_id", None),
            effective_limit,
            QUERY_TIMEOUT_SECONDS,
        )
        job.cancel()
        raise TimeoutError(
            f"Consulta excedeu timeout de {QUERY_TIMEOUT_SECONDS}s para limit={effective_limit}"
        ) from e
    logger.info(
        "BQ query_leads iniciado job_id=%s limit=%s elapsed=%.2fs",
        getattr(job, "job_id", None),
        effective_limit,
        perf_counter() - t0,
    )
    for r in rows:
        yield dict(r)


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
        "status": _distinct_values_from_view("status", "status"),
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


def export_leads_rows(
    filters: Optional[Dict[str, Any]] = None,
    limit: int = EXPORT_MAX_ROWS,
    offset: int = 0,
    order_by: str = "data_disparo",
    order_dir: str = "ASC",
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
        "data_inscricao_dt": "v.data_inscricao",
        "data_disparo": "v.data_disparo",
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

    if order_by == "data_disparo":
        order_clause = _data_disparo_priority_order_clause()
    elif order_by in ("data_inscricao", "data_inscricao_dt"):
        order_clause = _data_inscricao_order_clause(order_dir)
    else:
        order_clause = f"{order_expr} {order_dir}"
    sql += f"\n ORDER BY {order_clause} \n LIMIT @limit OFFSET @offset"
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))

    rows = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    return [dict(r) for r in rows]


def export_leads_rows_iter(
    filters: Optional[Dict[str, Any]] = None,
    batch_size: int = 1000,
    order_by: str = "data_disparo",
    order_dir: str = "ASC",
) -> Iterator[List[Dict[str, Any]]]:
    """
    Itera exportação paginada para evitar alto consumo de memória.
    """
    offset = 0
    size = max(1, int(batch_size))

    while True:
        rows = export_leads_rows(
            filters=filters,
            limit=size,
            offset=offset,
            order_by=order_by,
            order_dir=order_dir,
        )
        if not rows:
            break
        yield rows
        fetched = len(rows)
        offset += fetched
        if fetched < size:
            break


def rows_to_xlsx(rows: List[Dict[str, Any]], xlsx_path: str, sheet_name: str = "Leads") -> str:
    """
    Gera XLSX no disco.
    - Excel não aceita datetime com timezone
    - CPF/celular saem como TEXTO para evitar conversão numérica
    """
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]

    headers = [label for _, label in EXPORT_COLUMNS]
    keys = [key for key, _ in EXPORT_COLUMNS]
    ws.append(headers)

    for r in rows:
        ws.append([_xlsx_safe_cell_value(k, r.get(k)) for k in keys])

    for col_idx, header in enumerate(headers, start=1):
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = max(12, min(42, len(str(header)) + 6))

    Path(xlsx_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(xlsx_path)
    return xlsx_path


def df_to_xlsx(df, xlsx_path: str, sheet_name: str = "Upload") -> str:
    """
    Salva uma cópia do upload em XLSX.
    Mantém CPF/celular como texto quando possível.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]

    headers = [str(c) for c in df.columns]
    ws.append(headers)

    phoneish_indexes = {idx for idx, col in enumerate(headers) if str(col).strip() in PHONEISH_COLUMNS}

    for row in df.itertuples(index=False, name=None):
        out = []
        for idx, value in enumerate(row):
            col_name = headers[idx]
            if idx in phoneish_indexes or col_name in PHONEISH_COLUMNS:
                out.append(_normalize_phoneish_value(value))
            else:
                out.append(value)
        ws.append(out)

    for col_idx, header in enumerate(headers, start=1):
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = max(12, min(42, len(str(header)) + 6))

    Path(xlsx_path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(xlsx_path)
    return xlsx_path
