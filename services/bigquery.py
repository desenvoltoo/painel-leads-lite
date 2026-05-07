# services/bigquery.py
from __future__ import annotations

import os
import logging
import threading
import time
import uuid
from concurrent.futures import TimeoutError as FuturesTimeoutError
from decimal import Decimal, InvalidOperation
from datetime import timedelta
from io import BytesIO, StringIO
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, Iterator, List, Optional, Tuple

from google.cloud import bigquery, storage
from google.api_core.exceptions import (
    GoogleAPICallError,
    BadRequest,
    Forbidden,
    NotFound,
    ServiceUnavailable,
    TooManyRequests,
)

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
MAX_LIMIT = int(os.getenv("BQ_MAX_LIMIT", "2000"))
EXPORT_MAX_ROWS = int(os.getenv("BQ_EXPORT_MAX_ROWS", "50000"))
QUERY_TIMEOUT_SECONDS = int(os.getenv("BQ_QUERY_TIMEOUT_SECONDS", "180"))

# GCS upload
GCS_UPLOAD_BUCKET = os.getenv("GCS_UPLOAD_BUCKET", "")
GCS_UPLOAD_PREFIX = os.getenv("GCS_UPLOAD_PREFIX", "uploads")
GCS_SIGNED_URL_EXPIRY_MINUTES = int(os.getenv("GCS_SIGNED_URL_EXPIRY_MINUTES", "30"))
MAX_FILE_SIZE_BYTES = int(os.getenv("GCS_MAX_FILE_SIZE_BYTES", str(50 * 1024 * 1024)))
ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

# Retry
RETRY_MAX_ATTEMPTS = int(os.getenv("BQ_RETRY_MAX_ATTEMPTS", "3"))
RETRY_BASE_DELAY_S = float(os.getenv("BQ_RETRY_BASE_DELAY_S", "2.0"))
RETRY_MAX_DELAY_S = float(os.getenv("BQ_RETRY_MAX_DELAY_S", "30.0"))

# colunas que não devem sofrer comportamento numérico
PHONEISH_COLUMNS = {"cpf", "celular"}

# ============================================================
# CLIENTES THREAD-SAFE
# Cada thread mantém sua própria instância, evitando condições
# de corrida em ambientes multi-thread (FastAPI / Gunicorn).
# ============================================================
_thread_local = threading.local()


def get_bq_client() -> bigquery.Client:
    if not getattr(_thread_local, "bq_client", None):
        _thread_local.bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    return _thread_local.bq_client


def get_storage_client() -> storage.Client:
    if not getattr(_thread_local, "storage_client", None):
        _thread_local.storage_client = storage.Client(project=GCP_PROJECT_ID)
    return _thread_local.storage_client


def _get_upload_bucket() -> storage.Bucket:
    if not GCS_UPLOAD_BUCKET:
        raise RuntimeError("Defina GCS_UPLOAD_BUCKET no ambiente.")
    return get_storage_client().bucket(GCS_UPLOAD_BUCKET)


# ============================================================
# RETRY COM BACKOFF EXPONENCIAL
# Erros fatais (BadRequest, Forbidden, NotFound) não são retriados.
# ============================================================
_RETRYABLE_EXCEPTIONS = (ServiceUnavailable, TooManyRequests, GoogleAPICallError)


def _with_retry(fn, *args, operation_name: str = "operação", **kwargs) -> Any:
    delay = RETRY_BASE_DELAY_S
    last_exc: Optional[Exception] = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            return fn(*args, **kwargs)
        except (BadRequest, Forbidden, NotFound) as exc:
            logger.error("Erro fatal em '%s' (tentativa %d/%d): %s", operation_name, attempt, RETRY_MAX_ATTEMPTS, exc)
            raise RuntimeError(f"Erro fatal em {operation_name}: {exc}") from exc
        except _RETRYABLE_EXCEPTIONS as exc:
            last_exc = exc
            if attempt == RETRY_MAX_ATTEMPTS:
                break
            logger.warning("Erro transitório em '%s' (tentativa %d/%d), aguardando %.1fs: %s", operation_name, attempt, RETRY_MAX_ATTEMPTS, delay, exc)
            time.sleep(delay)
            delay = min(delay * 2, RETRY_MAX_DELAY_S)
        except Exception as exc:
            logger.exception("Erro inesperado em '%s': %s", operation_name, exc)
            raise
    raise RuntimeError(f"'{operation_name}' falhou após {RETRY_MAX_ATTEMPTS} tentativas.") from last_exc


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
    df2 = _coerce_df_to_staging_schema(df)

    def _do_load():
        job = client.load_table_from_dataframe(
            df2,
            table_id,
            job_config=bigquery.LoadJobConfig(
                schema=STAGING_SCHEMA,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        )
        job.result()
        logger.info("BQ load_table_from_dataframe concluído: job_id=%s", job.job_id)

    _with_retry(_do_load, operation_name=f"load_to_staging → {table_id}")


def run_procedure_async() -> str:
    """Dispara a procedure e retorna o job_id sem bloquear a request."""
    client = get_bq_client()
    sql = f"CALL `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_PROCEDURE}`();"

    def _do_call():
        job = client.query(sql)
        logger.info("Procedure disparada: job_id=%s procedure=%s", job.job_id, BQ_PROCEDURE)
        return job.job_id

    return _with_retry(_do_call, operation_name=f"run_procedure_async ({BQ_PROCEDURE})")


def process_upload_dataframe(df) -> str:
    """
    1) carrega staging (truncate)
    2) dispara SP async
    3) retorna job_id
    """
    load_to_staging(df)
    return run_procedure_async()


# ============================================================
# SIGNED URL + PROCESS GCS UPLOAD
# ============================================================
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
    data = [{headers[i]: r[i] if i < len(r) else None for i in range(len(headers))} for r in rows]
    return pd.DataFrame(data)


def _validate_blob(blob: storage.Blob, object_name: str) -> None:
    """Valida extensão e tamanho antes de qualquer processamento pesado."""
    blob.reload()
    suffix = Path(object_name).suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise ValueError(f"Extensão '{suffix}' não suportada. Use: {', '.join(sorted(ALLOWED_EXTENSIONS))}.")
    size = blob.size or 0
    if size == 0:
        raise ValueError("O arquivo enviado está vazio.")
    if size > MAX_FILE_SIZE_BYTES:
        mb = MAX_FILE_SIZE_BYTES // (1024 * 1024)
        raise ValueError(f"O arquivo excede o limite de {mb} MB ({size / (1024 * 1024):.1f} MB recebidos).")
    logger.info("Arquivo validado: object=%s ext=%s size_bytes=%d", object_name, suffix, size)


def _upload_df_to_gcs_temp(df, bucket: storage.Bucket) -> str:
    temp_name = f"{GCS_UPLOAD_PREFIX.strip('/')}/tmp/staging_{uuid.uuid4().hex}.csv"
    blob = bucket.blob(temp_name)
    csv_bytes = df.to_csv(index=False).encode("utf-8")

    def _do_upload():
        blob.upload_from_string(csv_bytes, content_type="text/csv")

    _with_retry(_do_upload, operation_name="upload CSV temporário para GCS")
    logger.info("CSV temporário enviado: gs://%s/%s (%d bytes)", bucket.name, temp_name, len(csv_bytes))
    return temp_name


def _load_gcs_csv_to_staging(bucket: storage.Bucket, temp_name: str) -> None:
    client = get_bq_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE}"
    uri = f"gs://{bucket.name}/{temp_name}"

    def _do_load():
        job = client.load_table_from_uri(
            uri, table_id,
            job_config=bigquery.LoadJobConfig(
                schema=STAGING_SCHEMA,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        )
        job.result()
        logger.info("BQ Load Job concluído: job_id=%s tabela=%s", job.job_id, table_id)

    _with_retry(_do_load, operation_name=f"BQ load_table_from_uri → {table_id}")


def _load_dataframe_to_staging_via_gcs(df) -> None:
    """Coerção → upload CSV temp no GCS → BQ load job → limpeza garantida."""
    bucket = _get_upload_bucket()
    df2 = _coerce_df_to_staging_schema(df)
    temp_name = _upload_df_to_gcs_temp(df2, bucket)
    try:
        _load_gcs_csv_to_staging(bucket, temp_name)
    finally:
        try:
            bucket.blob(temp_name).delete()
            logger.debug("Blob temporário removido: %s", temp_name)
        except Exception as exc:
            logger.warning("Falha ao remover blob temporário '%s': %s", temp_name, exc)


def generate_gcs_signed_upload(filename: str, source_tag: str = "manual") -> Dict[str, str]:
    """
    Gera signed URL PUT para upload direto ao GCS pelo cliente.
    Expiração controlada por GCS_SIGNED_URL_EXPIRY_MINUTES (padrão 30).
    """
    bucket = _get_upload_bucket()
    safe_name = Path(filename).name
    safe_source = (source_tag or "manual").replace("/", "_").replace("\\", "_")
    object_name = f"{GCS_UPLOAD_PREFIX.strip('/')}/{safe_source}/{uuid.uuid4().hex}_{safe_name}"
    blob = bucket.blob(object_name)

    try:
        signed_url = blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=GCS_SIGNED_URL_EXPIRY_MINUTES),
            method="PUT",
            content_type="application/octet-stream",
        )
    except Exception as exc:
        raise RuntimeError(
            "Não foi possível assinar URL do GCS. "
            "Verifique GCS_UPLOAD_BUCKET e permissões da Service Account para assinar URLs."
        ) from exc

    logger.info("Signed URL gerado: object=%s expiry_min=%d", object_name, GCS_SIGNED_URL_EXPIRY_MINUTES)
    return {
        "upload_url": signed_url,
        "object_name": object_name,
        "bucket": bucket.name,
        "expires_in_minutes": GCS_SIGNED_URL_EXPIRY_MINUTES,
    }


def process_gcs_upload(object_name: str) -> Dict[str, Any]:
    """
    Processa arquivo já presente no GCS:
      1. Valida extensão e tamanho (falha rápido)
      2. Lê CSV ou XLSX como DataFrame
      3. Carrega na staging via GCS (com retry e limpeza garantida)
      4. Dispara a procedure de forma assíncrona
      5. Remove o blob original (melhor esforço)
    """
    bucket = _get_upload_bucket()
    blob = bucket.blob(object_name)

    if not blob.exists():
        raise FileNotFoundError(f"Arquivo não encontrado no GCS: {object_name}")

    _validate_blob(blob, object_name)

    suffix = Path(object_name).suffix.lower()
    try:
        df = _xlsx_blob_to_dataframe(blob) if suffix in {".xlsx", ".xls"} else _csv_blob_to_dataframe(blob)
    except Exception as exc:
        logger.exception("Falha ao ler arquivo '%s': %s", object_name, exc)
        raise RuntimeError(f"Não foi possível ler o arquivo: {exc}") from exc

    logger.info("Arquivo lido: object=%s linhas=%d colunas=%d", object_name, len(df), len(df.columns))

    _load_dataframe_to_staging_via_gcs(df)
    job_id = run_procedure_async()

    try:
        blob.delete()
        logger.info("Blob original removido: %s", object_name)
    except Exception as exc:
        logger.warning("Não foi possível remover blob original '%s': %s", object_name, exc)

    return {"message": "Upload processado com sucesso.", "job_id": job_id, "rows_read": len(df)}


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
    return list(query_leads_iter(filters=filters, limit=limit, offset=offset, order_by=order_by, order_dir=order_dir))


def query_leads_iter(
    filters: Optional[Dict[str, Any]] = None,
    limit: int = DEFAULT_LIMIT,
    offset: int = 0,
    order_by: str = "data_inscricao",
    order_dir: str = "DESC",
) -> Iterator[Dict[str, Any]]:
    client = get_bq_client()
    filters = filters or {}

    limit = max(1, min(int(limit), MAX_LIMIT))
    offset = max(0, int(offset))
    order_dir = "ASC" if str(order_dir).upper() == "ASC" else "DESC"

    if order_by == "data_inscricao_dt":
        order_by = "data_inscricao"

    allowed_order = {
        "data_inscricao": "v.data_inscricao",
        "data_inscricao_dt": "v.data_inscricao",
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
    if order_by in ("data_inscricao", "data_inscricao_dt"):
        order_clause = _data_inscricao_order_clause(order_dir)
    else:
        order_clause = f"{order_expr} {order_dir}"
    sql += f"\n ORDER BY {order_clause} \n LIMIT @limit OFFSET @offset"

    params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
    params.append(bigquery.ScalarQueryParameter("offset", "INT64", offset))

    t0 = perf_counter()
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    try:
        rows = job.result(timeout=QUERY_TIMEOUT_SECONDS)
    except FuturesTimeoutError as exc:
        logger.exception("BQ query_leads_iter timeout job_id=%s", getattr(job, "job_id", None))
        job.cancel()
        raise TimeoutError(f"Consulta excedeu timeout de {QUERY_TIMEOUT_SECONDS}s") from exc
    logger.info("BQ query_leads_iter concluído job_id=%s elapsed=%.2fs", getattr(job, "job_id", None), perf_counter() - t0)
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
        "data_inscricao_dt": "v.data_inscricao",
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

    if order_by in ("data_inscricao", "data_inscricao_dt"):
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
    order_by: str = "data_inscricao",
    order_dir: str = "DESC",
) -> Iterator[List[Dict[str, Any]]]:
    """Itera exportação paginada para evitar alto consumo de memória."""
    offset = 0
    size = max(1, int(batch_size))
    while True:
        rows = export_leads_rows(filters=filters, limit=size, offset=offset, order_by=order_by, order_dir=order_dir)
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
