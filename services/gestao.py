from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import math
import os
import re
import threading
from collections import OrderedDict
from datetime import date, datetime, timezone, timedelta
from time import perf_counter
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from cachetools import TTLCache
from google.cloud import bigquery

from services import bigquery as bq

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID") or os.getenv("GCP_PROJECT_ID") or bq.GCP_PROJECT_ID or "painel-universidade"
DATASET = os.getenv("BIGQUERY_DATASET") or os.getenv("BQ_DATASET") or bq.BQ_DATASET or "modelo_estrela"
VIEW_LEADS = "vw_leads_painel_lite"
FACT_LEAD = "f_lead"
DIM_PESSOA = "dim_pessoa"
DIM_STATUS = "dim_status"
DIM_CURSO = "dim_curso"
DIM_POLO = "dim_polo"
LOGS_IMPORTACOES = "logs_importacoes"
LOGS_REJEICOES = "logs_rejeicoes_import"

MAX_PAGE_SIZE = int(os.getenv("GESTAO_MAX_PAGE_SIZE", "500"))
DEFAULT_PAGE_SIZE = int(os.getenv("GESTAO_DEFAULT_PAGE_SIZE", "25"))
CACHE_TTL_SECONDS = int(os.getenv("GESTAO_CACHE_TTL_SECONDS", str(getattr(bq, "GESTAO_CACHE_TTL_SECONDS", 60))))
CACHE_MAXSIZE = int(os.getenv("GESTAO_CACHE_MAXSIZE", "128"))
MIN_RANKING_LEADS = int(os.getenv("GESTAO_RANKING_MIN_LEADS", os.getenv("GESTAO_MIN_RANKING_LEADS", "20")))
EXPORT_LIMIT = int(os.getenv("GESTAO_EXPORT_LIMIT", "50000"))

FILTER_FIELDS = OrderedDict([
    ("data_inicio", "data_inicio"), ("data_fim", "data_fim"),
    ("curso", "curso"), ("modalidade", "modalidade"), ("turno", "turno"), ("polo", "polo"),
    ("origem", "origem"), ("tipo_negocio", "tipo_negocio"),
    ("consultor_comercial", "consultor_comercial"), ("consultor_disparo", "consultor_disparo"),
    ("campanha", "campanha"), ("canal", "canal"), ("acao_comercial", "acao_comercial"),
    ("status", "status"), ("status_inscricao", "status_inscricao"), ("matriculado", "matriculado"),
    ("busca", "busca"),
])
OPTION_FIELDS = ["curso", "modalidade", "turno", "polo", "origem", "tipo_negocio", "consultor_comercial", "consultor_disparo", "campanha", "canal", "acao_comercial", "status", "status_inscricao"]
PERSONAL_FILTERS = {"busca", "cpf", "celular", "email", "nome"}
EMPTY_TEXTS = {"", "NULL", "N/A", "NA", "SEM STATUS", "SEM INFORMACAO", "SEM INFORMAÇÃO", "-"}
CLOSED_STATUSES = {"MAT", "CANCELADO", "CANCELADA", "DESCARTADO", "DESCARTADA", "ENCERRADO", "ENCERRADA"}

ORDERABLE_FILA = {"grupo_prioridade": "grupo_prioridade", "data_inscricao": "data_inscricao", "data_atualizacao": "data_atualizacao", "nome": "nome"}
ORDERABLE_IMPORTACOES = {"criado_em": "criado_em", "nome_arquivo": "nome_arquivo", "usuario": "usuario", "status": "status", "total_linhas": "total_linhas"}
ORDERABLE_PRODUTIVIDADE = {"consultor": "consultor", "total_leads": "total_leads", "sem_status": "sem_status", "status_ec": "status_ec", "inscritos": "inscritos", "matriculados": "matriculados", "taxa_conversao": "taxa_conversao", "qtd_acionamentos": "qtd_acionamentos", "ultima_atividade": "ultima_atividade", "dias_sem_atividade": "dias_sem_atividade"}
QUALITY_TYPES = OrderedDict([
    ("sem_celular", "Sem celular"), ("celular_invalido", "Celular inválido"), ("telefone_invalido", "Celular inválido"),
    ("sem_email", "Sem e-mail"), ("sem_cpf", "Sem CPF"), ("cpf_incompleto", "CPF incompleto"), ("cpf_invalido", "CPF inválido"),
    ("sem_origem", "Sem origem"), ("sem_curso", "Sem curso"), ("sem_consultor", "Sem consultor"), ("sem_status", "Sem status"),
    ("sem_data_inscricao", "Sem data de inscrição"), ("sem_data_atualizacao", "Sem data de atualização"),
    ("duplicado_cpf", "CPF duplicado"), ("duplicado_celular", "Celular duplicado"), ("orfao_pessoa", "Lead sem pessoa"),
    ("orfao_status", "Lead sem dimensão status"), ("orfao_curso", "Lead sem dimensão curso"), ("orfao_polo", "Lead sem dimensão polo"),
    ("rejeitado", "Registro rejeitado"),
])

_CACHE: TTLCache = TTLCache(maxsize=CACHE_MAXSIZE, ttl=CACHE_TTL_SECONDS)
_CACHE_LOCK = threading.Lock()
_KEY_LOCKS: Dict[str, threading.Lock] = {}

class GestaoValidationError(ValueError):
    pass

# ---------- utilidades ----------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ref(table: str) -> str:
    return f"`{PROJECT_ID}.{DATASET}.{table}`"

def _date_or_none(value: Any, field: str) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        datetime.strptime(text, "%Y-%m-%d")
        return text
    except ValueError as exc:
        raise GestaoValidationError(f"{field} inválida. Use AAAA-MM-DD.") from exc

def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw = value
    elif hasattr(value, "getlist"):
        raw = value.getlist(value)  # not used
    else:
        raw = str(value).split(",")
    return [str(v).strip() for v in raw if str(v).strip()]

def parse_filters(source: Mapping[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    getlist = getattr(source, "getlist", None)
    def values_for(key: str) -> List[str]:
        aliases = [key]
        if key == "data_inicio": aliases += ["data_ini", "dataInicio"]
        if key == "data_fim": aliases += ["dataFim"]
        vals: List[str] = []
        for a in aliases:
            if getlist:
                vals.extend([str(v).strip() for v in source.getlist(a) if str(v).strip()])
            elif a in source:
                v = source.get(a)
                if isinstance(v, (list, tuple)):
                    vals.extend([str(x).strip() for x in v if str(x).strip()])
                elif str(v or "").strip():
                    vals.extend([str(v).strip()])
        return vals
    filters: Dict[str, Any] = {}
    for key in FILTER_FIELDS:
        vals = values_for(key)
        if not vals:
            continue
        if key in {"data_inicio", "data_fim"}:
            filters[key] = _date_or_none(vals[0], key)
        elif key == "matriculado":
            v = vals[0].lower()
            if v in {"1", "true", "sim", "s", "yes"}: filters[key] = True
            elif v in {"0", "false", "nao", "não", "n", "no"}: filters[key] = False
            else: raise GestaoValidationError("matriculado deve ser sim ou não.")
        elif key == "busca":
            filters[key] = vals[0][:120]
        else:
            filters[key] = vals[:50]
    if filters.get("data_inicio") and filters.get("data_fim") and filters["data_inicio"] > filters["data_fim"]:
        raise GestaoValidationError("data_inicio não pode ser maior que data_fim.")
    # compatibilidade legado
    if "data_ini" in source and "data_inicio" not in filters:
        val = _date_or_none(source.get("data_ini"), "data_ini")
        if val: filters["data_inicio"] = val
    try: limit = int(source.get("page_size") or source.get("pageSize") or source.get("limit") or DEFAULT_PAGE_SIZE)
    except Exception: limit = DEFAULT_PAGE_SIZE
    try: offset = int(source.get("offset") or 0)
    except Exception: offset = 0
    try: page = int(source.get("page") or (offset // max(limit, 1) + 1))
    except Exception: page = 1
    limit = max(1, min(limit, MAX_PAGE_SIZE)); offset = max(0, offset); page = max(1, page)
    if "page" in source and "offset" not in source:
        offset = (page - 1) * limit
    order_dir = str(source.get("order_dir") or source.get("orderDir") or "DESC").upper()
    if order_dir not in {"ASC", "DESC"}: order_dir = "DESC"
    meta = {"limit": limit, "offset": offset, "page": page, "page_size": limit, "order_by": str(source.get("order_by") or source.get("orderBy") or "data_inscricao"), "order_dir": order_dir, "force_refresh": str(source.get("force_refresh") or source.get("refresh") or "").lower() in {"1","true","sim"}}
    return filters, meta

def _has_personal_filters(filters: Mapping[str, Any]) -> bool:
    return any(k in filters and filters.get(k) not in (None, "", []) for k in PERSONAL_FILTERS)

def _cache_key(endpoint: str, filters: Mapping[str, Any], meta: Mapping[str, Any]) -> str:
    payload = json.dumps({"endpoint": endpoint, "filters": filters, "meta": meta, "project": PROJECT_ID, "dataset": DATASET}, sort_keys=True, ensure_ascii=False, default=str)
    return "gestao:" + hashlib.sha256(payload.encode()).hexdigest()

def invalidate_gestao_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()
    try:
        bq._gestao_cache.clear()
    except Exception:
        pass

def _with_cache(endpoint: str, filters: Mapping[str, Any], meta: Mapping[str, Any], force_refresh: bool, loader):
    if force_refresh or _has_personal_filters(filters):
        return loader(), False
    key = _cache_key(endpoint, filters, meta)
    with _CACHE_LOCK:
        if key in _CACHE:
            return _CACHE[key], True
        lock = _KEY_LOCKS.setdefault(key, threading.Lock())
    with lock:
        with _CACHE_LOCK:
            if key in _CACHE:
                return _CACHE[key], True
        data = loader()
        with _CACHE_LOCK:
            _CACHE[key] = data
            _KEY_LOCKS.pop(key, None)
        return data, False

def _run(sql: str, params: Optional[List[Any]] = None, operation: str = "gestao") -> List[Dict[str, Any]]:
    started = perf_counter()
    rows = bq._rows_to_json_safe(bq._run_gestao_query(sql, params=params or [], operation_name=operation))
    logger.info("gestao_query operation=%s duration_ms=%s result_count=%s", operation, int((perf_counter()-started)*1000), len(rows))
    return rows

def _single(sql: str, params: Optional[List[Any]] = None, operation: str = "gestao") -> Dict[str, Any]:
    rows = _run(sql, params, operation)
    return rows[0] if rows else {}

def _norm(text: Any) -> str:
    return str(text or "").strip().upper()

def is_status_empty(value: Any) -> bool:
    return _norm(value) in EMPTY_TEXTS

def is_status_ec(value: Any) -> bool:
    return _norm(value) == "EC"

def is_matriculado_row(row: Mapping[str, Any]) -> bool:
    if row.get("flag_matriculado") is True:
        return True
    if str(row.get("matriculado") or "").strip().lower() in {"sim", "s", "true", "1", "mat", "matriculado"}:
        return True
    if _norm(row.get("status")) == "MAT":
        return True
    if _norm(row.get("status_inscricao")) in {"MATRICULADO", "MATRÍCULADO", "MATRICULADOS"}:
        return True
    return bool(row.get("data_matricula"))

def _digits(value: Any) -> str:
    return re.sub(r"\D+", "", str(value or ""))

def is_valid_phone(value: Any) -> bool:
    d = _digits(value)
    return len(d) in (10, 11) and len(set(d)) > 1

def is_valid_cpf(value: Any) -> bool:
    cpf = _digits(value)
    if len(cpf) != 11 or len(set(cpf)) == 1:
        return False
    def calc(nums: str, factor: int) -> int:
        s = sum(int(n) * (factor - i) for i, n in enumerate(nums))
        r = (s * 10) % 11
        return 0 if r == 10 else r
    return calc(cpf[:9], 10) == int(cpf[9]) and calc(cpf[:10], 11) == int(cpf[10])

def mask_cpf(value: Any) -> str:
    d = _digits(value)
    return f"***.***.***-{d[-4:]}" if d else ""

def mask_phone(value: Any) -> str:
    d = _digits(value)
    return f"*******{d[-4:]}" if d else ""

def mask_email(value: Any) -> str:
    text = str(value or "").strip()
    if "@" not in text:
        return ""
    local, domain = text.split("@", 1)
    return f"{local[:2] if len(local)>2 else local[:1]}***@{domain}" if len(local) >= 2 else f"***@{domain}"

def mask_rejection_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    out = {k: v for k, v in dict(row).items() if k != "payload"}
    for k in ("cpf", "cpf_raw"):
        if k in out: out[k] = mask_cpf(out[k])
    for k in ("celular", "celular_raw", "telefone"):
        if k in out: out[k] = mask_phone(out[k])
    for k in ("email", "email_raw"):
        if k in out: out[k] = mask_email(out[k])
    if "nome_raw" in out:
        out["nome"] = out.pop("nome_raw")
    return out

def _sanitize_message(message: Any) -> Optional[str]:
    if message is None: return None
    text = str(message)
    text = re.sub(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b", "[cpf-mascarado]", text)
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "[email-mascarado]", text)
    text = re.sub(r"\b\d{10,11}\b", "[telefone-mascarado]", text)
    text = re.sub(r"(?i)(token|senha|password|secret)=\S+", r"\1=[redacted]", text)
    return text

def parse_lead_date(value: Any) -> Optional[date]:
    if value in (None, ""): return None
    if isinstance(value, datetime): return value.date()
    if isinstance(value, date): return value
    text = str(value).strip()
    if re.fullmatch(r"\d+(\.\d+)?", text):
        try:
            return date(1899, 12, 30) + timedelta(days=int(float(text)))
        except Exception:
            return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
        try: return datetime.strptime(text[:19] if fmt.endswith("%S") else text[:10], fmt).date()
        except ValueError: pass
    return None

def should_accept_upload_version(source_dt: datetime, current_dt: datetime) -> bool:
    return source_dt >= current_dt

def score_rule_documentation() -> List[Dict[str, str]]:
    return [
        {"regra": "Matriculados saem da fila operacional."},
        {"regra": "Leads sem status e com celular válido são prioridade alta."},
        {"regra": "dt_upload/data_atualizacao não substitui data_inscricao na ordenação da fila."},
    ]

# ---------- SQL central ----------
def _matriculado_sql(alias="v") -> str:
    return f"""(
      COALESCE(SAFE_CAST({alias}.flag_matriculado AS BOOL), FALSE)
      OR UPPER(TRIM(COALESCE(CAST({alias}.status AS STRING), ''))) = 'MAT'
      OR UPPER(TRIM(COALESCE(CAST({alias}.status_inscricao AS STRING), ''))) IN ('MATRICULADO','MATRÍCULADO','MATRICULADOS')
      OR {alias}.data_matricula IS NOT NULL
    )"""

def _inscrito_sql(alias="v") -> str:
    return f"({_matriculado_sql(alias)} OR UPPER(TRIM(COALESCE(CAST({alias}.status_inscricao AS STRING), ''))) = 'INSCRITO')"

def _sem_status_sql(alias="v") -> str:
    return f"NULLIF(TRIM(COALESCE(CAST({alias}.status AS STRING), '')), '') IS NULL"

def _where_filters(filters: Mapping[str, Any], params: List[Any], alias: str = "v", *, date_field: str = "data_inscricao", prefix: str = "") -> str:
    where = ["1=1"]
    if filters.get("data_inicio"):
        where.append(f"{alias}.{date_field} >= @{prefix}data_inicio")
        params.append(bigquery.ScalarQueryParameter(f"{prefix}data_inicio", "DATE", filters["data_inicio"]))
    if filters.get("data_fim"):
        where.append(f"{alias}.{date_field} <= @{prefix}data_fim")
        params.append(bigquery.ScalarQueryParameter(f"{prefix}data_fim", "DATE", filters["data_fim"]))
    for key in ["curso","modalidade","turno","polo","origem","tipo_negocio","consultor_comercial","consultor_disparo","campanha","canal","acao_comercial","status","status_inscricao"]:
        vals = filters.get(key)
        if vals:
            pname = f"{prefix}{key}"
            where.append(f"{alias}.{key} IN UNNEST(@{pname})")
            params.append(bigquery.ArrayQueryParameter(pname, "STRING", vals if isinstance(vals, list) else [vals]))
    if "matriculado" in filters:
        where.append(_matriculado_sql(alias) if filters["matriculado"] else f"NOT {_matriculado_sql(alias)}")
    if filters.get("busca"):
        pname = f"{prefix}busca"
        where.append(f"""(
          NORMALIZE_AND_CASEFOLD(COALESCE(CAST({alias}.nome AS STRING), '')) LIKE CONCAT('%', NORMALIZE_AND_CASEFOLD(@{pname}), '%')
          OR REGEXP_REPLACE(COALESCE(CAST({alias}.cpf AS STRING), ''), r'[^0-9]', '') LIKE CONCAT('%', REGEXP_REPLACE(@{pname}, r'[^0-9]', ''), '%')
          OR REGEXP_REPLACE(COALESCE(CAST({alias}.celular AS STRING), ''), r'[^0-9]', '') LIKE CONCAT('%', REGEXP_REPLACE(@{pname}, r'[^0-9]', ''), '%')
          OR NORMALIZE_AND_CASEFOLD(COALESCE(CAST({alias}.email AS STRING), '')) LIKE CONCAT('%', NORMALIZE_AND_CASEFOLD(@{pname}), '%')
        )""")
        params.append(bigquery.ScalarQueryParameter(pname, "STRING", filters["busca"]))
    return " AND ".join(where)

def _pagination(total: int, meta: Mapping[str, Any]) -> Dict[str, int]:
    page_size = int(meta.get("limit") or meta.get("page_size") or DEFAULT_PAGE_SIZE)
    offset = int(meta.get("offset") or 0)
    page = int(meta.get("page") or (offset // max(page_size, 1) + 1))
    return {"page": page, "page_size": page_size, "total": int(total or 0), "total_pages": math.ceil((total or 0) / page_size) if total else 0}

def _csv_bytes(headers: List[Tuple[str, str]], rows: Iterable[Mapping[str, Any]]) -> Tuple[bytes, int]:
    out = io.StringIO()
    writer = csv.writer(out, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    writer.writerow([label for _, label in headers])
    count = 0
    for row in rows:
        writer.writerow([row.get(key, "") if row.get(key) is not None else "" for key, _ in headers])
        count += 1
    return ("\ufeff" + out.getvalue()).encode("utf-8"), count

# ---------- módulos ----------
def get_resumo(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        params: List[Any] = []
        where = _where_filters(filters, params)
        matriculado = _matriculado_sql("v"); inscrito = _inscrito_sql("v"); sem_status = _sem_status_sql("v")
        sql = f"""
        SELECT COUNT(*) total_leads,
          COUNTIF(v.data_inscricao IS NOT NULL) novos_no_periodo,
          COUNTIF({sem_status} AND v.data_ultima_acao IS NULL AND v.data_disparo IS NULL AND COALESCE(v.qtd_acionamentos,0)=0) nunca_trabalhados,
          COUNTIF({inscrito} OR NOT {sem_status} OR v.data_ultima_acao IS NOT NULL OR v.data_disparo IS NOT NULL OR COALESCE(v.qtd_acionamentos,0)>0) trabalhados,
          COUNTIF({inscrito}) inscritos,
          COUNTIF({matriculado}) matriculados,
          SAFE_DIVIDE(COUNTIF({matriculado}), NULLIF(COUNT(*),0))*100 taxa_conversao,
          COUNTIF({sem_status}) sem_status,
          COUNTIF(NULLIF(TRIM(COALESCE(v.consultor_comercial,'')), '') IS NULL) sem_consultor,
          COUNTIF(NULLIF(TRIM(COALESCE(v.origem,'')), '') IS NULL) sem_origem,
          COUNTIF(NULLIF(TRIM(COALESCE(v.curso,'')), '') IS NULL) sem_curso,
          COUNTIF(v.data_atualizacao >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) atualizados_recentes
        FROM {_ref(VIEW_LEADS)} v WHERE {where}
        """
        row = _single(sql, params, "gestao_resumo")
        return {k: (round(v, 2) if isinstance(v, float) else int(v or 0) if k != "taxa_conversao" else round(float(v or 0),2)) for k,v in row.items()}
    return _with_cache("resumo", filters, {}, bool(meta.get("force_refresh")), load)

def build_funil_etapas(total_valido: Any, trabalhados: Any, inscritos: Any, matriculados: Any) -> List[Dict[str, Any]]:
    vals = [max(0, int(x or 0)) for x in (total_valido, trabalhados, inscritos, matriculados)]
    vals[1] = min(vals[1], vals[0]); vals[2] = min(vals[2], vals[1]); vals[3] = min(vals[3], vals[2])
    labels = ["Total válido", "Trabalhados", "Inscritos", "Matriculados"]
    out = []
    for i, (label, vol) in enumerate(zip(labels, vals)):
        prev = vals[i-1] if i else None
        conv = None if prev is None else (round((vol / prev) * 100, 1) if prev else 0)
        out.append({"etapa": label, "label": label, "volume": vol, "conversao_etapa_anterior": conv, "percentual_anterior": conv, "perda_etapa_anterior": None if prev is None else max(prev-vol,0)})
    return out

def get_funil(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        params: List[Any] = []
        where = _where_filters(filters, params)
        matriculado = _matriculado_sql("v"); inscrito = _inscrito_sql("v")
        trabalhado = f"({inscrito} OR NOT {_sem_status_sql('v')} OR v.data_ultima_acao IS NOT NULL OR v.data_disparo IS NOT NULL OR COALESCE(v.qtd_acionamentos,0)>0)"
        celular_valido = "NULLIF(REGEXP_REPLACE(COALESCE(CAST(v.celular AS STRING), ''), r'[^0-9]', ''), '') IS NOT NULL"
        sql = f"""SELECT COUNTIF({celular_valido}) total_valido, COUNTIF({celular_valido} AND {trabalhado}) trabalhados, COUNTIF({celular_valido} AND {inscrito}) inscritos, COUNTIF({celular_valido} AND {matriculado}) matriculados FROM {_ref(VIEW_LEADS)} v WHERE {where}"""
        r = _single(sql, params, "gestao_funil")
        etapas = build_funil_etapas(r.get("total_valido"), r.get("trabalhados"), r.get("inscritos"), r.get("matriculados"))
        return {"etapas": etapas, "total_valido": etapas[0]["volume"], "trabalhados": etapas[1]["volume"], "inscritos": etapas[2]["volume"], "matriculados": etapas[3]["volume"]}
    return _with_cache("funil", filters, {}, bool(meta.get("force_refresh")), load)

def get_evolucao(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    gran = str(meta.get("granularidade") or filters.get("granularidade") or "dia").lower()
    gran = str(meta.get("granularity") or gran)
    trunc = {"dia": "DAY", "semana": "WEEK(MONDAY)", "mês": "MONTH", "mes": "MONTH"}.get(gran)
    if not trunc: raise GestaoValidationError("Granularidade inválida.")
    def load():
        params: List[Any] = []
        where = _where_filters(filters, params)
        sql = f"""
        WITH series AS (
          SELECT DATE_TRUNC(v.data_inscricao, {trunc}) data, 'leads' serie, COUNT(*) valor FROM {_ref(VIEW_LEADS)} v WHERE {where} AND v.data_inscricao IS NOT NULL GROUP BY data
          UNION ALL SELECT DATE_TRUNC(v.data_matricula, {trunc}), 'matriculas', COUNT(*) FROM {_ref(VIEW_LEADS)} v WHERE {where} AND v.data_matricula IS NOT NULL GROUP BY 1
          UNION ALL SELECT DATE_TRUNC(DATE(v.data_atualizacao), {trunc}), 'atualizacoes', COUNT(*) FROM {_ref(VIEW_LEADS)} v WHERE {where} AND v.data_atualizacao IS NOT NULL GROUP BY 1
          UNION ALL SELECT DATE_TRUNC(DATE(v.data_ultima_acao), {trunc}), 'acoes', COUNT(*) FROM {_ref(VIEW_LEADS)} v WHERE {where} AND v.data_ultima_acao IS NOT NULL GROUP BY 1
          UNION ALL SELECT DATE_TRUNC(DATE(v.data_disparo), {trunc}), 'disparos', COUNT(*) FROM {_ref(VIEW_LEADS)} v WHERE {where} AND v.data_disparo IS NOT NULL GROUP BY 1
        ) SELECT * FROM series ORDER BY data, serie
        """
        return {"granularidade": gran, "series": _run(sql, params, "gestao_evolucao")}
    return _with_cache("evolucao", filters, {"granularidade": gran}, bool(meta.get("force_refresh")), load)

def _ranking_sql(field: str, filters: Mapping[str, Any], limit: int, min_leads: int, op: str) -> List[Dict[str, Any]]:
    params: List[Any] = []
    where = _where_filters(filters, params)
    matriculado = _matriculado_sql("v")
    params.extend([bigquery.ScalarQueryParameter("min_leads", "INT64", min_leads), bigquery.ScalarQueryParameter("limit", "INT64", limit)])
    sql = f"""SELECT COALESCE(NULLIF(TRIM(CAST(v.{field} AS STRING)), ''), 'Sem informação') nome, COUNT(*) total_leads, COUNTIF({matriculado}) matriculados, SAFE_DIVIDE(COUNTIF({matriculado}), NULLIF(COUNT(*),0))*100 taxa_conversao FROM {_ref(VIEW_LEADS)} v WHERE {where} GROUP BY nome HAVING total_leads >= @min_leads ORDER BY total_leads DESC, matriculados DESC, taxa_conversao DESC LIMIT @limit"""
    return _run(sql, params, op)

def get_rankings(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        limit = min(int(meta.get("limit") or 10), 50)
        min_leads = int(os.getenv("GESTAO_RANKING_MIN_LEADS", str(MIN_RANKING_LEADS)))
        return {
            "consultores_volume": _ranking_sql("consultor_comercial", filters, limit, 1, "gestao_ranking_consultores_volume"),
            "consultores_matricula": _ranking_sql("consultor_comercial", filters, limit, 1, "gestao_ranking_consultores_matricula"),
            "consultores_conversao": _ranking_sql("consultor_comercial", filters, limit, min_leads, "gestao_ranking_consultores_conversao"),
            "origens_volume": _ranking_sql("origem", filters, limit, 1, "gestao_ranking_origens_volume"),
            "origens_conversao": _ranking_sql("origem", filters, limit, min_leads, "gestao_ranking_origens_conversao"),
            "cursos_volume": _ranking_sql("curso", filters, limit, 1, "gestao_ranking_cursos_volume"),
            "cursos_conversao": _ranking_sql("curso", filters, limit, min_leads, "gestao_ranking_cursos_conversao"),
            "campanhas_volume": _ranking_sql("campanha", filters, limit, 1, "gestao_ranking_campanhas_volume"),
            "campanhas_conversao": _ranking_sql("campanha", filters, limit, min_leads, "gestao_ranking_campanhas_conversao"),
        }
    return _with_cache("rankings", filters, {"limit": meta.get("limit")}, bool(meta.get("force_refresh")), load)

def get_produtividade(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        params: List[Any] = []
        where = _where_filters(filters, params)
        limit = int(meta.get("limit") or DEFAULT_PAGE_SIZE); offset = int(meta.get("offset") or 0)
        order_by = ORDERABLE_PRODUTIVIDADE.get(str(meta.get("order_by") or "total_leads"), "total_leads")
        order_dir = meta.get("order_dir", "DESC")
        params.extend([bigquery.ScalarQueryParameter("limit", "INT64", limit), bigquery.ScalarQueryParameter("offset", "INT64", offset)])
        matriculado = _matriculado_sql("v"); inscrito = _inscrito_sql("v"); sem_status = _sem_status_sql("v")
        sql = f"""
        WITH agg AS (
          SELECT COALESCE(NULLIF(TRIM(v.consultor_comercial), ''), 'Sem consultor') consultor, COUNT(*) total_leads,
            COUNTIF({sem_status}) sem_status, COUNTIF(UPPER(TRIM(COALESCE(v.status,'')))='EC') status_ec,
            COUNTIF({inscrito}) inscritos, COUNTIF({matriculado}) matriculados,
            SAFE_DIVIDE(COUNTIF({matriculado}), NULLIF(COUNT(*),0))*100 taxa_conversao,
            SUM(COALESCE(v.qtd_acionamentos,0)) qtd_acionamentos,
            MAX(COALESCE(v.data_ultima_acao, v.data_disparo, v.data_atualizacao)) ultima_atividade,
            IF(MAX(COALESCE(v.data_ultima_acao, v.data_disparo, v.data_atualizacao)) IS NULL, NULL, DATE_DIFF(CURRENT_DATE(), DATE(MAX(COALESCE(v.data_ultima_acao, v.data_disparo, v.data_atualizacao))), DAY)) dias_sem_atividade
          FROM {_ref(VIEW_LEADS)} v WHERE {where} GROUP BY consultor
        ), numbered AS (SELECT agg.*, COUNT(*) OVER() total_registros FROM agg)
        SELECT * FROM numbered ORDER BY {order_by} {order_dir} LIMIT @limit OFFSET @offset
        """
        rows = _run(sql, params, "gestao_produtividade")
        total = rows[0].get("total_registros", 0) if rows else 0
        for r in rows: r.pop("total_registros", None)
        return {"items": rows, "rows": rows, "pagination": _pagination(total, meta)}
    return _with_cache("produtividade", filters, {k: meta.get(k) for k in ("limit","offset","order_by","order_dir")}, bool(meta.get("force_refresh")), load)

# fila
def _fila_cte(filters: Mapping[str, Any], params: List[Any]) -> str:
    where = _where_filters(filters, params, "v")
    matriculado = _matriculado_sql("v")
    return f"""
    WITH base AS (
      SELECT v.*,
        NULLIF(REGEXP_REPLACE(COALESCE(CAST(v.celular AS STRING), ''), r'[^0-9]', ''), '') celular_limpo,
        UPPER(TRIM(COALESCE(v.status, ''))) status_normalizado,
        UPPER(TRIM(COALESCE(v.status_inscricao, ''))) status_inscricao_normalizado,
        {matriculado} esta_matriculado
      FROM {_ref(VIEW_LEADS)} v WHERE {where}
    ), priorizada AS (
      SELECT *, CASE
        WHEN esta_matriculado THEN 99
        WHEN celular_limpo IS NULL THEN 99
        WHEN NULLIF(status_normalizado, '') IS NULL THEN 1
        WHEN status_normalizado = 'EC' THEN 2
        WHEN status_normalizado NOT IN ('MAT','CANCELADO','CANCELADA','DESCARTADO','DESCARTADA','ENCERRADO','ENCERRADA') THEN 3
        ELSE 99 END grupo_prioridade
      FROM base
    )
    """

def get_fila(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    limit = int(meta.get("limit") or DEFAULT_PAGE_SIZE); offset = int(meta.get("offset") or 0)
    params: List[Any] = []
    cte = _fila_cte(filters, params)
    params.extend([bigquery.ScalarQueryParameter("limit", "INT64", limit), bigquery.ScalarQueryParameter("offset", "INT64", offset)])
    sql = cte + """
    SELECT sk_pessoa, nome, celular, curso, modalidade, turno, polo, origem, campanha, consultor_comercial, status, status_inscricao, data_inscricao, data_ultima_acao, data_atualizacao, qtd_acionamentos, grupo_prioridade,
      CASE grupo_prioridade WHEN 1 THEN 'ALTA' WHEN 2 THEN 'MÉDIA' WHEN 3 THEN 'NORMAL' END prioridade,
      CASE grupo_prioridade WHEN 1 THEN 'Lead recente sem status' WHEN 2 THEN 'Lead com status EC' WHEN 3 THEN 'Lead elegível para acompanhamento' END motivo_prioridade,
      COUNT(*) OVER() total_registros
    FROM priorizada WHERE grupo_prioridade IN (1,2,3)
    ORDER BY grupo_prioridade ASC, data_inscricao DESC NULLS LAST, data_atualizacao DESC NULLS LAST, sk_pessoa
    LIMIT @limit OFFSET @offset
    """
    rows = _run(sql, params, "query_gestao_fila_operacional")
    total = rows[0].get("total_registros", 0) if rows else 0
    for r in rows:
        r.pop("total_registros", None)
        r["celular"] = mask_phone(r.get("celular"))
    return {"items": rows, "pagination": _pagination(total, {**meta, "limit": limit, "offset": offset})}, False

def prioritize_fila_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for row in rows:
        r = dict(row)
        status = _norm(r.get("status"))
        if is_matriculado_row(r) or not is_valid_phone(r.get("celular")):
            continue
        if is_status_empty(r.get("status")): grupo = 1
        elif status == "EC": grupo = 2
        elif status not in CLOSED_STATUSES: grupo = 3
        else: continue
        r["grupo_prioridade"] = grupo
        r["prioridade"] = {1:"ALTA",2:"MÉDIA",3:"NORMAL"}[grupo]
        r["motivo_prioridade"] = {1:"Lead recente sem status",2:"Lead com status EC",3:"Lead elegível para acompanhamento"}[grupo]
        out.append(r)
    out.sort(key=lambda r: (r["grupo_prioridade"], -(parse_lead_date(r.get("data_inscricao")) or date.min).toordinal(), str(r.get("nome") or "")))
    return out

def export_fila(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[str, bytes, int]:
    params: List[Any] = []
    cte = _fila_cte(filters, params)
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", min(int(meta.get("limit") or EXPORT_LIMIT), EXPORT_LIMIT)))
    sql = cte + """
    SELECT nome, celular, curso, modalidade, turno, polo, origem, campanha, consultor_comercial, status, status_inscricao, data_inscricao, data_ultima_acao, data_atualizacao, qtd_acionamentos, grupo_prioridade,
      CASE grupo_prioridade WHEN 1 THEN 'ALTA' WHEN 2 THEN 'MÉDIA' WHEN 3 THEN 'NORMAL' END prioridade,
      CASE grupo_prioridade WHEN 1 THEN 'Lead recente sem status' WHEN 2 THEN 'Lead com status EC' WHEN 3 THEN 'Lead elegível para acompanhamento' END motivo_prioridade
    FROM priorizada WHERE grupo_prioridade IN (1,2,3)
    ORDER BY grupo_prioridade ASC, data_inscricao DESC NULLS LAST, data_atualizacao DESC NULLS LAST, sk_pessoa
    LIMIT @limit
    """
    rows = _run(sql, params, "gestao_fila_exportar")
    safe = []
    for r in rows:
        rr = dict(r); rr["celular"] = mask_phone(rr.get("celular")); safe.append(rr)
    headers = [("nome","Nome"),("celular","Celular"),("curso","Curso"),("modalidade","Modalidade"),("polo","Polo"),("origem","Origem"),("campanha","Campanha"),("consultor_comercial","Consultor"),("status","Status"),("status_inscricao","Status inscrição"),("data_inscricao","Data inscrição"),("prioridade","Prioridade"),("motivo_prioridade","Motivo")]
    content, count = _csv_bytes(headers, safe)
    return f"fila_operacional_{datetime.utcnow():%Y%m%d_%H%M%S}.csv", content, count

# qualidade
def _quality_summary_sql(filters: Mapping[str, Any], params: List[Any]) -> str:
    where = _where_filters(filters, params)
    cpf_digits = "REGEXP_REPLACE(COALESCE(CAST(v.cpf AS STRING), ''), r'[^0-9]', '')"
    cel_digits = "REGEXP_REPLACE(COALESCE(CAST(v.celular AS STRING), ''), r'[^0-9]', '')"
    return f"""
    WITH base AS (SELECT v.*, {cpf_digits} cpf_limpo, {cel_digits} celular_limpo FROM {_ref(VIEW_LEADS)} v WHERE {where}),
    dup_cpf AS (SELECT SUM(qtd - 1) duplicados_cpf FROM (SELECT REGEXP_REPLACE(COALESCE(CAST(cpf AS STRING), ''), r'[^0-9]', '') chave, COUNT(*) qtd FROM {_ref(DIM_PESSOA)} GROUP BY chave HAVING chave != '' AND qtd > 1)),
    dup_cel AS (SELECT SUM(qtd - 1) duplicados_celular FROM (SELECT REGEXP_REPLACE(COALESCE(CAST(celular AS STRING), ''), r'[^0-9]', '') chave, COUNT(*) qtd FROM {_ref(DIM_PESSOA)} GROUP BY chave HAVING chave != '' AND qtd > 1)),
    orfaos AS (
      SELECT
        COUNTIF(p.sk_pessoa IS NULL) orfao_pessoa,
        COUNTIF(f.sk_status IS NOT NULL AND s.sk_status IS NULL) orfao_status,
        COUNTIF(f.sk_curso IS NOT NULL AND c.sk_curso IS NULL) orfao_curso,
        COUNTIF(f.sk_polo IS NOT NULL AND po.sk_polo IS NULL) orfao_polo
      FROM {_ref(FACT_LEAD)} f
      LEFT JOIN {_ref(DIM_PESSOA)} p USING(sk_pessoa)
      LEFT JOIN {_ref(DIM_STATUS)} s USING(sk_status)
      LEFT JOIN {_ref(DIM_CURSO)} c USING(sk_curso)
      LEFT JOIN {_ref(DIM_POLO)} po USING(sk_polo)
    ), rejeicoes AS (SELECT COUNT(*) total_rejeitados, COUNTIF(ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)) rejeitados_ultimos_7_dias FROM {_ref(LOGS_REJEICOES)})
    SELECT COUNT(*) total_leads,
      COUNTIF(NULLIF(celular_limpo,'') IS NULL) sem_celular,
      COUNTIF(NULLIF(celular_limpo,'') IS NOT NULL AND (LENGTH(celular_limpo) NOT IN (10,11) OR REGEXP_CONTAINS(celular_limpo, r'^(\\d)\\1+$'))) celular_invalido,
      COUNTIF(NULLIF(TRIM(COALESCE(email,'')), '') IS NULL) sem_email,
      COUNTIF(NULLIF(cpf_limpo,'') IS NULL) sem_cpf,
      COUNTIF(NULLIF(cpf_limpo,'') IS NOT NULL AND LENGTH(cpf_limpo) < 11) cpf_incompleto,
      COUNTIF(NULLIF(cpf_limpo,'') IS NOT NULL AND (LENGTH(cpf_limpo) != 11 OR REGEXP_CONTAINS(cpf_limpo, r'^(\\d)\\1+$'))) cpf_invalido,
      COUNTIF(NULLIF(TRIM(COALESCE(origem,'')), '') IS NULL) sem_origem,
      COUNTIF(NULLIF(TRIM(COALESCE(curso,'')), '') IS NULL) sem_curso,
      COUNTIF(NULLIF(TRIM(COALESCE(consultor_comercial,'')), '') IS NULL) sem_consultor,
      COUNTIF({_sem_status_sql('base')}) sem_status,
      COUNTIF(data_inscricao IS NULL) sem_data_inscricao,
      COUNTIF(data_atualizacao IS NULL) sem_data_atualizacao,
      COALESCE((SELECT duplicados_cpf FROM dup_cpf),0) duplicados_cpf,
      COALESCE((SELECT duplicados_celular FROM dup_cel),0) duplicados_celular,
      (SELECT AS STRUCT * FROM orfaos) orfaos,
      (SELECT total_rejeitados FROM rejeicoes) total_rejeitados,
      (SELECT rejeitados_ultimos_7_dias FROM rejeicoes) rejeitados_ultimos_7_dias
    FROM base
    """

def map_qualidade_row(row: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    r = dict(row or {})
    return {"totalRegistros": int(r.get("total_registros") or r.get("total_leads") or 0), "totalLeads": int(r.get("total_leads") or 0), "duplicidadesCpf": int(r.get("duplicidades_cpf") or 0), "duplicidadesCelular": int(r.get("duplicidades_celular") or 0), "duplicidadesEmail": int(r.get("duplicidades_email") or 0), "duplicidadesTotais": int(r.get("duplicidades_cpf") or 0)+int(r.get("duplicidades_celular") or 0)+int(r.get("duplicidades_email") or 0), "percentualDuplicidade": float(r.get("percentual_duplicidade") or 0), "ultimaAtualizacao": r.get("ultima_atualizacao")}

def get_qualidade_dados(filters: Mapping[str, Any] | None = None, meta: Mapping[str, Any] | None = None) -> Tuple[Dict[str, Any], bool]:
    data, cached = get_qualidade(filters or {}, meta or {})
    return map_qualidade_row({"total_leads": data.get("indicadores", {}).get("total_leads", 0), "duplicidades_cpf": data.get("indicadores", {}).get("duplicados_cpf",0), "duplicidades_celular": data.get("indicadores", {}).get("duplicados_celular",0)}), cached

def get_qualidade(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        params: List[Any] = []
        row = _single(_quality_summary_sql(filters, params), params, "gestao_qualidade")
        motivos = _run(f"SELECT motivo, COUNT(*) total FROM {_ref(LOGS_REJEICOES)} GROUP BY motivo ORDER BY total DESC LIMIT 20", [], "gestao_qualidade_rejeicoes_motivos")
        if isinstance(row.get("orfaos"), dict):
            orfaos = row.pop("orfaos")
            row.update(orfaos)
        row["rejeitados_por_motivo"] = motivos
        return {"indicadores": row, "quality_types": QUALITY_TYPES}
    return _with_cache("qualidade", filters, {}, bool(meta.get("force_refresh")), load)

get_qualidade_detalhes_types = QUALITY_TYPES

def _quality_details_sql(tipo: str, filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[str, List[Any]]:
    if tipo not in QUALITY_TYPES:
        raise GestaoValidationError("Tipo de indicador inválido.")
    params: List[Any] = []
    where = [_where_filters(filters, params)]
    cpf_digits = "REGEXP_REPLACE(COALESCE(CAST(v.cpf AS STRING), ''), r'[^0-9]', '')"
    cel_digits = "REGEXP_REPLACE(COALESCE(CAST(v.celular AS STRING), ''), r'[^0-9]', '')"
    clauses = {
        "sem_celular": f"NULLIF({cel_digits}, '') IS NULL", "celular_invalido": f"NULLIF({cel_digits}, '') IS NOT NULL AND LENGTH({cel_digits}) NOT IN (10,11)", "telefone_invalido": f"NULLIF({cel_digits}, '') IS NOT NULL AND LENGTH({cel_digits}) NOT IN (10,11)",
        "sem_email": "NULLIF(TRIM(COALESCE(v.email,'')), '') IS NULL", "sem_cpf": f"NULLIF({cpf_digits}, '') IS NULL", "cpf_incompleto": f"NULLIF({cpf_digits}, '') IS NOT NULL AND LENGTH({cpf_digits}) < 11", "cpf_invalido": f"NULLIF({cpf_digits}, '') IS NOT NULL AND LENGTH({cpf_digits}) != 11",
        "sem_origem": "NULLIF(TRIM(COALESCE(v.origem,'')), '') IS NULL", "sem_curso": "NULLIF(TRIM(COALESCE(v.curso,'')), '') IS NULL", "sem_consultor": "NULLIF(TRIM(COALESCE(v.consultor_comercial,'')), '') IS NULL", "sem_status": _sem_status_sql("v"),
        "sem_data_inscricao": "v.data_inscricao IS NULL", "sem_data_atualizacao": "v.data_atualizacao IS NULL",
    }
    if tipo == "duplicado_cpf":
        where.append(f"{cpf_digits} IN (SELECT chave FROM (SELECT REGEXP_REPLACE(COALESCE(CAST(cpf AS STRING), ''), r'[^0-9]', '') chave, COUNT(*) qtd FROM {_ref(DIM_PESSOA)} GROUP BY chave HAVING chave != '' AND qtd > 1) dup_cpf)")
    elif tipo == "duplicado_celular":
        where.append(f"{cel_digits} IN (SELECT chave FROM (SELECT REGEXP_REPLACE(COALESCE(CAST(celular AS STRING), ''), r'[^0-9]', '') chave, COUNT(*) qtd FROM {_ref(DIM_PESSOA)} GROUP BY chave HAVING chave != '' AND qtd > 1) dup_celular)")
    elif tipo == "rejeitado":
        params.extend([bigquery.ScalarQueryParameter("limit", "INT64", int(meta.get("limit") or DEFAULT_PAGE_SIZE)), bigquery.ScalarQueryParameter("offset", "INT64", int(meta.get("offset") or 0))])
        return f"SELECT ts, motivo, cpf_raw, celular_raw, nome_raw, email_raw, COUNT(*) OVER() total_registros FROM {_ref(LOGS_REJEICOES)} ORDER BY ts DESC LIMIT @limit OFFSET @offset", params
    elif tipo.startswith("orfao_"):
        col = {"orfao_pessoa":"p.sk_pessoa IS NULL", "orfao_status":"f.sk_status IS NOT NULL AND s.sk_status IS NULL", "orfao_curso":"f.sk_curso IS NOT NULL AND c.sk_curso IS NULL", "orfao_polo":"f.sk_polo IS NOT NULL AND po.sk_polo IS NULL"}[tipo]
        params.extend([bigquery.ScalarQueryParameter("limit", "INT64", int(meta.get("limit") or DEFAULT_PAGE_SIZE)), bigquery.ScalarQueryParameter("offset", "INT64", int(meta.get("offset") or 0))])
        return f"SELECT f.sk_pessoa, f.sk_curso, f.sk_polo, f.sk_status, f.data_inscricao, '{tipo}' motivo, COUNT(*) OVER() total_registros FROM {_ref(FACT_LEAD)} f LEFT JOIN {_ref(DIM_PESSOA)} p USING(sk_pessoa) LEFT JOIN {_ref(DIM_STATUS)} s USING(sk_status) LEFT JOIN {_ref(DIM_CURSO)} c USING(sk_curso) LEFT JOIN {_ref(DIM_POLO)} po USING(sk_polo) WHERE {col} LIMIT @limit OFFSET @offset", params
    else:
        where.append(clauses[tipo])
    params.extend([bigquery.ScalarQueryParameter("limit", "INT64", int(meta.get("limit") or DEFAULT_PAGE_SIZE)), bigquery.ScalarQueryParameter("offset", "INT64", int(meta.get("offset") or 0))])
    sql = f"SELECT '{QUALITY_TYPES[tipo]}' motivo, sk_pessoa, nome, cpf, celular, email, curso, consultor_comercial consultor, origem, status, data_inscricao, data_atualizacao, COUNT(*) OVER() total_registros FROM {_ref(VIEW_LEADS)} v WHERE {' AND '.join(where)} ORDER BY data_inscricao DESC NULLS LAST LIMIT @limit OFFSET @offset"
    return sql, params

def get_qualidade_detalhes(filters: Mapping[str, Any], meta: Mapping[str, Any], tipo: str) -> Tuple[Dict[str, Any], bool]:
    sql, params = _quality_details_sql(tipo, filters, meta)
    rows = _run(sql, params, "gestao_qualidade_detalhes")
    total = rows[0].get("total_registros", 0) if rows else 0
    safe = []
    for r in rows:
        r.pop("total_registros", None)
        if tipo == "rejeitado":
            rr = mask_rejection_row(r); rr = {"ts": rr.get("ts"), "motivo": rr.get("motivo"), "cpf_mascarado": rr.get("cpf_raw"), "celular_mascarado": rr.get("celular_raw"), "nome": rr.get("nome"), "email_mascarado": rr.get("email_raw")}
        else:
            rr = mask_rejection_row(r)
        safe.append(rr)
    return {"items": safe, "pagination": _pagination(total, meta)}, False

def export_qualidade(filters: Mapping[str, Any], meta: Mapping[str, Any], tipo: str) -> Tuple[str, bytes, int]:
    data, _ = get_qualidade_detalhes(filters, {**meta, "limit": min(int(meta.get("limit") or EXPORT_LIMIT), EXPORT_LIMIT), "offset": 0}, tipo)
    rows = data["items"]
    headers = [("motivo","Motivo"),("cpf","Identificador mascarado"),("cpf_mascarado","CPF mascarado"),("celular","Celular mascarado"),("celular_mascarado","Celular mascarado"),("nome","Nome"),("email","E-mail mascarado"),("email_mascarado","E-mail mascarado"),("curso","Curso"),("consultor","Consultor"),("origem","Origem"),("status","Status"),("data_inscricao","Data inscrição")]
    content, count = _csv_bytes(headers, rows)
    return f"qualidade_{tipo}_{datetime.utcnow():%Y%m%d_%H%M%S}.csv", content, count

# importações e rejeições
def parse_import_history_request(source: Mapping[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    filters, meta = parse_filters({"data_inicio": source.get("dataInicio") or source.get("data_inicio") or source.get("data_ini"), "data_fim": source.get("dataFim") or source.get("data_fim"), "page": source.get("page") or 1, "pageSize": source.get("pageSize") or source.get("page_size") or source.get("limit") or 20, "order_by": source.get("order_by") or "criado_em", "order_dir": source.get("order_dir") or "DESC"})
    out = {}
    if filters.get("data_inicio"): out["dataInicio"] = filters["data_inicio"]
    if filters.get("data_fim"): out["dataFim"] = filters["data_fim"]
    if str(source.get("status") or "").strip(): out["status"] = str(source.get("status")).strip()[:80]
    if str(source.get("nomeArquivo") or source.get("nome_arquivo") or "").strip(): out["nomeArquivo"] = str(source.get("nomeArquivo") or source.get("nome_arquivo")).strip()[:200]
    meta["pageSize"] = min(int(meta["limit"]), 100); meta["page"] = int(meta.get("page") or 1); meta["offset"] = (meta["page"]-1)*meta["pageSize"]
    return out, meta

def _import_where(filters: Mapping[str, Any], params: List[Any]) -> str:
    where = ["1=1"]
    if filters.get("dataInicio"):
        where.append("DATE(i.criado_em) >= @dataInicio"); params.append(bigquery.ScalarQueryParameter("dataInicio", "DATE", filters["dataInicio"]))
    if filters.get("dataFim"):
        where.append("DATE(i.criado_em) <= @dataFim"); params.append(bigquery.ScalarQueryParameter("dataFim", "DATE", filters["dataFim"]))
    if filters.get("status"):
        where.append("UPPER(TRIM(i.status)) = UPPER(@status)"); params.append(bigquery.ScalarQueryParameter("status", "STRING", filters["status"]))
    if filters.get("nomeArquivo"):
        where.append("UPPER(i.nome_arquivo) LIKE CONCAT('%', UPPER(@nomeArquivo), '%')"); params.append(bigquery.ScalarQueryParameter("nomeArquivo", "STRING", filters["nomeArquivo"]))
    return " AND ".join(where)

def _sanitize_import_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    allowed = ["upload_id","id_importacao","nome_arquivo","tipo_arquivo","tamanho_arquivo_bytes","usuario","status","etapa","mensagem","total_linhas","linhas_recebidas","linhas_validas","linhas_inseridas","linhas_atualizadas","linhas_ignoradas","linhas_rejeitadas","duplicados_arquivo","duplicados_banco","erros","criado_em","iniciado_em","atualizado_em","finalizado_em","duracao_ms"]
    return {k: row.get(k) if row.get(k) is not None else (0 if k in {"tamanho_arquivo_bytes","total_linhas","linhas_recebidas","linhas_validas","linhas_inseridas","linhas_atualizadas","linhas_ignoradas","linhas_rejeitadas","duplicados_arquivo","duplicados_banco","erros","duracao_ms"} else "") for k in allowed}

def get_importacoes(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    params: List[Any] = []; where = _import_where(filters, params)
    count_sql = f"SELECT COUNT(*) total FROM {_ref(LOGS_IMPORTACOES)} i WHERE {where}"
    total = int((_single(count_sql, list(params), "gestao_importacoes_count").get("total") or 0))
    order_by = ORDERABLE_IMPORTACOES.get(str(meta.get("order_by") or "criado_em"), "criado_em"); order_dir = meta.get("order_dir", "DESC")
    params.extend([bigquery.ScalarQueryParameter("limit", "INT64", int(meta.get("pageSize") or meta.get("limit") or DEFAULT_PAGE_SIZE)), bigquery.ScalarQueryParameter("offset", "INT64", int(meta.get("offset") or 0))])
    sql = f"SELECT upload_id,id_importacao,nome_arquivo,tipo_arquivo,tamanho_arquivo_bytes,usuario,status,etapa,mensagem,total_linhas,linhas_recebidas,linhas_validas,linhas_inseridas,linhas_atualizadas,linhas_ignoradas,linhas_rejeitadas,duplicados_arquivo,duplicados_banco,erros,criado_em,iniciado_em,atualizado_em,finalizado_em,duracao_ms FROM {_ref(LOGS_IMPORTACOES)} i WHERE {where} ORDER BY {order_by} {order_dir} LIMIT @limit OFFSET @offset"
    items = [_sanitize_import_row(r) for r in _run(sql, params, "gestao_importacoes")]
    page_size = int(meta.get("pageSize") or meta.get("limit") or DEFAULT_PAGE_SIZE); page = int(meta.get("page") or 1)
    return {"items": items, "pagination": {"page": page, "pageSize": page_size, "total": total, "totalPages": math.ceil(total/page_size) if total else 0, "page_size": page_size, "total_pages": math.ceil(total/page_size) if total else 0}}, False

get_importacoes_historico = get_importacoes

def export_importacoes(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[str, bytes, int]:
    params: List[Any] = []; where = _import_where(filters, params)
    sql = f"SELECT upload_id,id_importacao,nome_arquivo,tipo_arquivo,tamanho_arquivo_bytes,usuario,status,etapa,mensagem,total_linhas,linhas_recebidas,linhas_validas,linhas_inseridas,linhas_atualizadas,linhas_ignoradas,linhas_rejeitadas,duplicados_arquivo,duplicados_banco,erros,criado_em,iniciado_em,atualizado_em,finalizado_em,duracao_ms FROM {_ref(LOGS_IMPORTACOES)} i WHERE {where} ORDER BY criado_em DESC LIMIT {EXPORT_LIMIT}"
    rows = [_sanitize_import_row(r) for r in _run(sql, params, "gestao_importacoes_exportar")]
    headers = [("upload_id","Upload ID"),("id_importacao","ID importação"),("nome_arquivo","Arquivo"),("tipo_arquivo","Tipo"),("usuario","Usuário"),("status","Status"),("etapa","Etapa"),("mensagem","Mensagem"),("total_linhas","Total linhas"),("linhas_recebidas","Recebidas"),("linhas_validas","Válidas"),("linhas_inseridas","Inseridas"),("linhas_atualizadas","Atualizadas"),("linhas_ignoradas","Ignoradas"),("linhas_rejeitadas","Rejeitadas"),("duplicados_arquivo","Duplicados arquivo"),("duplicados_banco","Duplicados banco"),("erros","Erros"),("criado_em","Criado em"),("duracao_ms","Duração ms")]
    content, count = _csv_bytes(headers, rows)
    return f"historico_importacoes_{datetime.utcnow():%Y%m%d_%H%M%S}.csv", content, count

def get_rejeicoes(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    params: List[Any] = []
    where = ["1=1"]
    if filters.get("data_inicio"):
        where.append("DATE(r.ts) >= @data_inicio"); params.append(bigquery.ScalarQueryParameter("data_inicio", "DATE", filters["data_inicio"]))
    if filters.get("data_fim"):
        where.append("DATE(r.ts) <= @data_fim"); params.append(bigquery.ScalarQueryParameter("data_fim", "DATE", filters["data_fim"]))
    params.extend([bigquery.ScalarQueryParameter("limit", "INT64", int(meta.get("limit") or DEFAULT_PAGE_SIZE)), bigquery.ScalarQueryParameter("offset", "INT64", int(meta.get("offset") or 0))])
    sql = f"SELECT ts,motivo,cpf_raw,celular_raw,nome_raw,email_raw,COUNT(*) OVER() total_registros FROM {_ref(LOGS_REJEICOES)} r WHERE {' AND '.join(where)} ORDER BY ts DESC LIMIT @limit OFFSET @offset"
    rows = _run(sql, params, "gestao_rejeicoes")
    total = rows[0].get("total_registros", 0) if rows else 0
    items = []
    for r in rows:
        m = mask_rejection_row(r); items.append({"ts": m.get("ts"), "motivo": m.get("motivo"), "cpf_mascarado": m.get("cpf_raw"), "celular_mascarado": m.get("celular_raw"), "nome": m.get("nome"), "email_mascarado": m.get("email_raw")})
    return {"items": items, "pagination": _pagination(total, meta)}, False

def export_rejeicoes(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[str, bytes, int]:
    data, _ = get_rejeicoes(filters, {**meta, "limit": EXPORT_LIMIT, "offset": 0})
    headers = [("ts","Data"),("motivo","Motivo"),("cpf_mascarado","CPF"),("celular_mascarado","Celular"),("nome","Nome"),("email_mascarado","E-mail")]
    content, count = _csv_bytes(headers, data["items"])
    return f"rejeicoes_importacao_{datetime.utcnow():%Y%m%d_%H%M%S}.csv", content, count

def export_produtividade(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[str, bytes, int]:
    data, _ = get_produtividade(filters, {**meta, "limit": EXPORT_LIMIT, "offset": 0})
    headers = [("consultor","Consultor"),("total_leads","Total leads"),("sem_status","Sem status"),("status_ec","Status EC"),("inscritos","Inscritos"),("matriculados","Matriculados"),("taxa_conversao","Taxa conversão"),("qtd_acionamentos","Acionamentos"),("ultima_atividade","Última atividade"),("dias_sem_atividade","Dias sem atividade")]
    content, count = _csv_bytes(headers, data["items"])
    return f"produtividade_{datetime.utcnow():%Y%m%d_%H%M%S}.csv", content, count

def get_opcoes(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        out = {}
        for field in OPTION_FIELDS:
            sql = f"SELECT DISTINCT {field} valor FROM {_ref(VIEW_LEADS)} WHERE NULLIF(TRIM(CAST({field} AS STRING)), '') IS NOT NULL ORDER BY valor LIMIT 500"
            out[field] = [r["valor"] for r in _run(sql, [], f"gestao_opcoes_{field}")]
        return out
    return _with_cache("opcoes", {}, {}, bool(meta.get("force_refresh")), load)

# logs de upload
def criar_log_importacao(**kwargs) -> None:
    fields = ["upload_id","id_importacao","nome_arquivo","tipo_arquivo","tamanho_arquivo_bytes","usuario","status","etapa","mensagem","correlation_id","criado_em","atualizado_em"]
    data = {**kwargs, "status": kwargs.get("status") or "RECEBIDO", "etapa": kwargs.get("etapa") or "RECEBIDO", "mensagem": _sanitize_message(kwargs.get("mensagem") or "Upload recebido."), "criado_em": datetime.now(timezone.utc), "atualizado_em": datetime.now(timezone.utc)}
    params = []
    for f in fields:
        typ = "INT64" if f == "tamanho_arquivo_bytes" else "TIMESTAMP" if f.endswith("_em") else "STRING"
        params.append(bigquery.ScalarQueryParameter(f, typ, data.get(f)))
    sql = f"INSERT INTO {_ref(LOGS_IMPORTACOES)} ({', '.join(fields)}) VALUES ({', '.join('@'+f for f in fields)})"
    bq._run_gestao_query(sql, params=params, operation_name="import_log_create")

def atualizar_log_importacao(upload_id: str, **kwargs) -> None:
    allowed = {"status":"STRING","etapa":"STRING","mensagem":"STRING","total_linhas":"INT64","linhas_recebidas":"INT64","linhas_validas":"INT64","linhas_inseridas":"INT64","linhas_atualizadas":"INT64","linhas_ignoradas":"INT64","linhas_rejeitadas":"INT64","duplicados_arquivo":"INT64","duplicados_banco":"INT64","erros":"INT64","detalhes_json":"STRING","correlation_id":"STRING","duracao_ms":"INT64"}
    sets = ["atualizado_em = CURRENT_TIMESTAMP()"]
    params = [bigquery.ScalarQueryParameter("upload_id", "STRING", upload_id)]
    for k, typ in allowed.items():
        if k in kwargs and kwargs[k] is not None:
            value = json.dumps(kwargs[k], ensure_ascii=False, default=str) if k == "detalhes_json" and not isinstance(kwargs[k], str) else kwargs[k]
            if k == "mensagem": value = _sanitize_message(value)
            sets.append(f"{k} = @{k}"); params.append(bigquery.ScalarQueryParameter(k, typ, value))
    if kwargs.get("finalizado"):
        sets.append("finalizado_em = CURRENT_TIMESTAMP()")
    if kwargs.get("iniciado"):
        sets.append("iniciado_em = CURRENT_TIMESTAMP()")
    sql = f"UPDATE {_ref(LOGS_IMPORTACOES)} SET {', '.join(sets)} WHERE upload_id = @upload_id"
    bq._run_gestao_query(sql, params=params, operation_name="import_log_update")
