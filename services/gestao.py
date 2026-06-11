from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import logging
import os
import re
import threading
from collections import OrderedDict
from datetime import date, datetime, timezone, timedelta
from time import perf_counter
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from cachetools import TTLCache
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from services import bigquery as bq

logger = logging.getLogger(__name__)

MAX_PAGE_SIZE = int(os.getenv("GESTAO_MAX_PAGE_SIZE", "500"))
DEFAULT_PAGE_SIZE = int(os.getenv("GESTAO_DEFAULT_PAGE_SIZE", "50"))
CACHE_TTL_SECONDS = int(os.getenv("GESTAO_CACHE_TTL_SECONDS", str(bq.GESTAO_CACHE_TTL_SECONDS)))
CACHE_MAXSIZE = int(os.getenv("GESTAO_CACHE_MAXSIZE", "64"))
MIN_RANKING_LEADS = int(os.getenv("GESTAO_MIN_RANKING_LEADS", "10"))

_PERSONAL_FILTERS = {"busca", "cpf", "celular", "email", "nome"}
_CACHE = TTLCache(maxsize=CACHE_MAXSIZE, ttl=CACHE_TTL_SECONDS)
_CACHE_LOCK = threading.Lock()
_KEY_LOCKS: Dict[str, threading.Lock] = {}

FILTER_SPECS = OrderedDict([
    ("consultor_comercial", ("consultor_comercial", "string")),
    ("consultor_disparo", ("consultor_disparo", "string")),
    ("curso", ("curso", "string")),
    ("polo", ("polo", "string")),
    ("unidade", ("unidade", "string")),
    ("modalidade", ("modalidade", "string")),
    ("turno", ("turno", "string")),
    ("origem", ("origem", "string")),
    ("campanha", ("campanha", "string")),
    ("canal", ("canal", "string")),
    ("tipo_negocio", ("tipo_negocio", "string")),
    ("tipo_disparo", ("tipo_disparo", "string")),
    ("status", ("status", "string")),
])

OPTION_FIELDS = [
    "consultor_comercial", "consultor_disparo", "curso", "polo", "unidade", "modalidade",
    "turno", "origem", "campanha", "canal", "tipo_negocio", "tipo_disparo", "status",
]

ORDERABLE_PRODUTIVIDADE = {
    "consultor": "consultor_comercial",
    "consultor_comercial": "consultor_comercial",
    "total_leads": "total_leads",
    "leads_novos": "leads_novos",
    "leads_sem_status": "leads_sem_status",
    "leads_em_carteira": "leads_em_carteira",
    "matriculados": "matriculados",
    "taxa_conversao_pct": "taxa_conversao_pct",
    "quantidade_acionamentos": "quantidade_acionamentos",
    "ultima_atividade": "ultima_atividade",
    "dias_sem_atividade": "dias_sem_atividade",
    "score_medio_carteira": "score_medio_carteira",
}

ORDERABLE_FILA = {
    "grupo_prioridade": "grupo_prioridade",
    "data_inscricao": "data_inscricao",
    "dt_upload": "dt_upload",
    "prioridade": "prioridade",
    "nome": "nome",
}

ORDERABLE_IMPORTACOES = {
    "criado_em": "criado_em",
    "dt_upload": "criado_em",
    "nome_arquivo": "nome_arquivo",
    "usuario": "usuario",
    "status": "status",
    "total_linhas": "total_linhas",
}

EMPTY_TEXTS = {"", "NULL", "N/A", "NA", "SEM INFORMACAO", "SEM INFORMAÇÃO", "-"}
QUALITY_TYPES = OrderedDict([
    ("sem_celular", "Sem celular"),
    ("sem_email", "Sem e-mail"),
    ("sem_cpf", "Sem CPF"),
    ("sem_origem", "Sem origem"),
    ("sem_curso", "Sem curso"),
    ("sem_consultor", "Sem consultor"),
    ("sem_status", "Sem status"),
    ("telefone_invalido", "Telefone inválido"),
    ("cpf_invalido", "CPF inválido ou incompleto"),
    ("duplicado_celular", "Duplicado por celular"),
    ("duplicado_cpf", "Duplicado por CPF"),
    ("rejeitado", "Registro rejeitado no upload"),
    ("sem_dt_upload", "Sem dt_upload"),
    ("data_invalida", "Data inválida ou não interpretada"),
])

class GestaoValidationError(ValueError):
    pass


IMPORT_HISTORY_VIEW = "vw_historico_importacoes"
IMPORT_HISTORY_EXPORT_VIEW = "vw_export_historico_importacoes"
QUALITY_VIEW = "vw_qualidade_dados"
IMPORT_LOG_TABLE = "logs_importacoes"
REJECTIONS_SUMMARY_VIEW = "vw_resumo_rejeicoes_import"

QUALITY_RESPONSE_KEYS = {
    "total_registros": "totalRegistros",
    "total_leads": "totalLeads",
    "registros_com_cpf_valido": "registrosComCpfValido",
    "registros_sem_cpf_valido": "registrosSemCpfValido",
    "registros_com_celular_valido": "registrosComCelularValido",
    "registros_sem_celular_valido": "registrosSemCelularValido",
    "registros_com_email_valido": "registrosComEmailValido",
    "registros_sem_email_valido": "registrosSemEmailValido",
    "registros_sem_chave_valida": "registrosSemChaveValida",
    "duplicidades_cpf": "duplicidadesCpf",
    "duplicidades_celular": "duplicidadesCelular",
    "duplicidades_email": "duplicidadesEmail",
    "duplicidades_totais": "duplicidadesTotais",
    "percentual_duplicidade": "percentualDuplicidade",
    "leads_sem_pessoa": "leadsSemPessoa",
    "leads_sem_data_inscricao": "leadsSemDataInscricao",
    "leads_sem_status": "leadsSemStatus",
    "leads_sem_disparo": "leadsSemDisparo",
    "nunca_trabalhados": "nuncaTrabalhados",
    "ultima_atualizacao": "ultimaAtualizacao",
}

QUALITY_DEFAULTS = {
    "totalRegistros": 0,
    "totalLeads": 0,
    "registrosComCpfValido": 0,
    "registrosSemCpfValido": 0,
    "registrosComCelularValido": 0,
    "registrosSemCelularValido": 0,
    "registrosComEmailValido": 0,
    "registrosSemEmailValido": 0,
    "registrosSemChaveValida": 0,
    "duplicidadesCpf": 0,
    "duplicidadesCelular": 0,
    "duplicidadesEmail": 0,
    "duplicidadesTotais": 0,
    "percentualDuplicidade": 0,
    "leadsSemPessoa": 0,
    "leadsSemDataInscricao": 0,
    "leadsSemStatus": 0,
    "leadsSemDisparo": 0,
    "nuncaTrabalhados": 0,
    "ultimaAtualizacao": None,
}

SENSITIVE_IMPORT_FIELDS = {"detalhes_json", "payload", "cpf", "celular", "telefone", "nome", "nome_lead", "email", "cpf_raw", "celular_raw", "nome_raw", "email_raw"}


def _to_number(value: Any, *, integer: bool = True) -> Any:
    if value is None or value == "":
        return 0
    try:
        number = float(value)
        return int(number) if integer else number
    except (TypeError, ValueError):
        return 0


def map_qualidade_row(row: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    data = dict(QUALITY_DEFAULTS)
    if not row:
        return data
    for snake, camel in QUALITY_RESPONSE_KEYS.items():
        value = row.get(snake)
        if camel == "ultimaAtualizacao":
            data[camel] = value if value not in ("", None) else None
        else:
            data[camel] = _to_number(value, integer=(camel != "percentualDuplicidade"))
    if data["duplicidadesTotais"] == 0:
        data["duplicidadesTotais"] = data["duplicidadesCpf"] + data["duplicidadesCelular"] + data["duplicidadesEmail"]
    if data["totalRegistros"] == 0 and data["percentualDuplicidade"]:
        data["percentualDuplicidade"] = 0
    return data


def get_qualidade_dados(filters: Mapping[str, Any] | None = None, meta: Mapping[str, Any] | None = None) -> Tuple[Dict[str, Any], bool]:
    sql = f"""
    SELECT *
    FROM {bq.bq_ref(QUALITY_VIEW)}
    LIMIT 1
    """
    row = _single(sql, [], "gestao_qualidade_dados")
    return map_qualidade_row(row), False


def _history_param_date(value: Any, field: str) -> Optional[str]:
    return _date_or_none(value, field)


def parse_import_history_request(source: Mapping[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    filters: Dict[str, Any] = {}
    status = str(source.get("status") or "").strip()
    if status:
        filters["status"] = status[:80]
    nome = str(source.get("nomeArquivo") or source.get("nome_arquivo") or "").strip()
    if nome:
        filters["nomeArquivo"] = nome[:200]
    data_inicio = _history_param_date(source.get("dataInicio") or source.get("data_ini"), "dataInicio")
    data_fim = _history_param_date(source.get("dataFim") or source.get("data_fim"), "dataFim")
    if data_inicio and data_fim and data_inicio > data_fim:
        raise GestaoValidationError("dataInicio não pode ser maior que dataFim.")
    if data_inicio:
        filters["dataInicio"] = data_inicio
    if data_fim:
        filters["dataFim"] = data_fim
    try:
        page = max(1, int(source.get("page") or 1))
    except (TypeError, ValueError):
        page = 1
    try:
        page_size = int(source.get("pageSize") or source.get("limit") or 20)
    except (TypeError, ValueError):
        page_size = 20
    page_size = max(1, min(page_size, min(MAX_PAGE_SIZE, 100)))
    return filters, {"page": page, "pageSize": page_size, "offset": (page - 1) * page_size}


def _importacoes_where(filters: Mapping[str, Any], params: List[Any], alias: str = "i") -> str:
    where = ["1=1"]
    if filters.get("dataInicio"):
        where.append(f"DATE({alias}.criado_em) >= @dataInicio")
        params.append(bigquery.ScalarQueryParameter("dataInicio", "DATE", filters["dataInicio"]))
    if filters.get("dataFim"):
        where.append(f"DATE({alias}.criado_em) <= @dataFim")
        params.append(bigquery.ScalarQueryParameter("dataFim", "DATE", filters["dataFim"]))
    if filters.get("status"):
        where.append(f"UPPER(TRIM(CAST({alias}.status AS STRING))) = UPPER(@status)")
        params.append(bigquery.ScalarQueryParameter("status", "STRING", filters["status"]))
    if filters.get("nomeArquivo"):
        where.append(f"UPPER(CAST({alias}.nome_arquivo AS STRING)) LIKE CONCAT('%', UPPER(@nomeArquivo), '%')")
        params.append(bigquery.ScalarQueryParameter("nomeArquivo", "STRING", filters["nomeArquivo"]))
    return " AND ".join(where)


def _sanitize_import_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    safe = {k: v for k, v in dict(row).items() if k not in SENSITIVE_IMPORT_FIELDS and not k.endswith("_raw")}
    safe.pop("total_registros", None)
    return safe


def get_importacoes(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    page = int(meta.get("page") or 1)
    page_size = min(int(meta.get("pageSize") or meta.get("limit") or 20), min(MAX_PAGE_SIZE, 100))
    offset = int(meta.get("offset", (page - 1) * page_size))
    params: List[Any] = []
    where = _importacoes_where(filters, params)
    count_sql = f"SELECT COUNT(1) AS total FROM {bq.bq_ref(IMPORT_HISTORY_VIEW)} i WHERE {where}"
    count_row = _single(count_sql, list(params), "importacoes_historico_count")
    total = int(count_row.get("total") or 0)
    query_params = list(params) + [bigquery.ScalarQueryParameter("limit", "INT64", page_size), bigquery.ScalarQueryParameter("offset", "INT64", offset)]
    sql = f"""
    SELECT *
    FROM {bq.bq_ref(IMPORT_HISTORY_VIEW)} i
    WHERE {where}
    ORDER BY criado_em DESC
    LIMIT @limit OFFSET @offset
    """
    rows = [_sanitize_import_row(r) for r in _run(sql, query_params, "importacoes_historico")]
    return {"items": rows, "pagination": {"page": page, "pageSize": page_size, "total": total, "totalPages": math.ceil(total / page_size) if page_size else 0}}, False


def get_importacoes_historico(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    return get_importacoes(filters, meta)


def export_importacoes(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[str, bytes, int]:
    params: List[Any] = []
    where = _importacoes_where(filters, params)
    sql = f"""
    SELECT *
    FROM {bq.bq_ref(IMPORT_HISTORY_EXPORT_VIEW)} i
    WHERE {where}
    ORDER BY criado_em DESC
    LIMIT 5000
    """
    rows = [_sanitize_import_row(r) for r in _run(sql, params, "importacoes_historico_exportar")]
    keys: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in keys and key not in SENSITIVE_IMPORT_FIELDS:
                keys.append(key)
    if not keys:
        keys = ["upload_id", "id_importacao", "nome_arquivo", "tipo_arquivo", "tamanho_arquivo_bytes", "usuario", "status", "etapa", "mensagem", "criado_em", "iniciado_em", "finalizado_em", "duracao_ms", "total_linhas", "linhas_recebidas", "linhas_validas", "linhas_inseridas", "linhas_atualizadas", "linhas_ignoradas", "linhas_rejeitadas", "duplicados_arquivo", "duplicados_banco", "erros"]
    headers = [(key, key.replace("_", " ").title()) for key in keys if key not in SENSITIVE_IMPORT_FIELDS]
    return f"historico_importacoes_{date.today().isoformat()}.csv", _csv_response_bytes(headers, rows), len(rows)


def _sanitize_message(message: Optional[str]) -> Optional[str]:
    if not message:
        return None
    text = str(message).replace("\n", " ").replace("\r", " ")
    text = re.sub(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+", "[email-mascarado]", text)
    text = re.sub(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b", "[cpf-mascarado]", text)
    text = re.sub(r"(?<!\d)(?:\+?55\s*)?(?:\(?\d{2}\)?\s*)?9?\d{4}[-\s]?\d{4}(?!\d)", "[celular-mascarado]", text)
    for secret_word in ("token", "authorization", "credential", "password", "senha"):
        text = re.sub(rf"({secret_word}\s*[:=]\s*)\S+", rf"\1[redigido]", text, flags=re.IGNORECASE)
    return text[:500]


def _run_import_log_dml(sql: str, params: List[Any], operation: str, upload_id: str, etapa: str) -> None:
    started = perf_counter()
    correlation_id = next((getattr(p, "value", None) for p in params if getattr(p, "name", None) == "correlation_id"), None)
    try:
        bq._run_gestao_query(sql, params=params, operation_name=operation)
        logger.info(
            "import_log operation=%s upload_id=%s correlation_id=%s etapa=%s table=%s duration=%.3fs error_code=%s mensagem=%s",
            operation, upload_id, correlation_id, etapa, IMPORT_LOG_TABLE, perf_counter() - started, None, "ok",
        )
    except Exception as exc:
        logger.warning(
            "import_log_failed operation=%s upload_id=%s correlation_id=%s etapa=%s table=%s duration=%.3fs error_code=%s mensagem=%s",
            operation, upload_id, correlation_id, etapa, IMPORT_LOG_TABLE, perf_counter() - started, exc.__class__.__name__, _sanitize_message(str(exc)),
        )


def criar_log_importacao(*, upload_id: str, id_importacao: str, nome_arquivo: str, tipo_arquivo: str, tamanho_arquivo_bytes: int, usuario: str, correlation_id: str, mensagem: str = "Upload recebido.") -> None:
    sql = f"""
    INSERT INTO {bq.bq_ref(IMPORT_LOG_TABLE)}
    (upload_id, id_importacao, nome_arquivo, tipo_arquivo, tamanho_arquivo_bytes, usuario, status, etapa, mensagem, criado_em, iniciado_em, atualizado_em, correlation_id)
    VALUES (@upload_id, @id_importacao, @nome_arquivo, @tipo_arquivo, @tamanho_arquivo_bytes, @usuario, 'RECEBIDO', 'RECEBIMENTO', @mensagem, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), @correlation_id)
    """
    params = [
        bigquery.ScalarQueryParameter("upload_id", "STRING", upload_id),
        bigquery.ScalarQueryParameter("id_importacao", "STRING", id_importacao),
        bigquery.ScalarQueryParameter("nome_arquivo", "STRING", nome_arquivo),
        bigquery.ScalarQueryParameter("tipo_arquivo", "STRING", tipo_arquivo),
        bigquery.ScalarQueryParameter("tamanho_arquivo_bytes", "INT64", int(tamanho_arquivo_bytes or 0)),
        bigquery.ScalarQueryParameter("usuario", "STRING", usuario),
        bigquery.ScalarQueryParameter("mensagem", "STRING", _sanitize_message(mensagem)),
        bigquery.ScalarQueryParameter("correlation_id", "STRING", correlation_id),
    ]
    _run_import_log_dml(sql, params, "import_log_create", upload_id, "RECEBIMENTO")


def atualizar_log_importacao(*, upload_id: str, status: str, etapa: str, mensagem: Optional[str] = None, correlation_id: Optional[str] = None, detalhes_json: Optional[Mapping[str, Any]] = None, finalizado: bool = False, duracao_ms: Optional[int] = None, **counters: Any) -> None:
    allowed_status = {"RECEBIDO", "VALIDANDO", "PROCESSANDO", "CONCLUIDO", "CONCLUIDO_COM_REJEICOES", "ERRO"}
    if status not in allowed_status:
        status = "ERRO"
    counter_fields = ["total_linhas", "linhas_recebidas", "linhas_validas", "linhas_inseridas", "linhas_atualizadas", "linhas_ignoradas", "linhas_rejeitadas", "duplicados_arquivo", "duplicados_banco", "erros"]
    set_parts = ["status = @status", "etapa = @etapa", "mensagem = @mensagem", "atualizado_em = CURRENT_TIMESTAMP()"]
    params: List[Any] = [bigquery.ScalarQueryParameter("status", "STRING", status), bigquery.ScalarQueryParameter("etapa", "STRING", etapa), bigquery.ScalarQueryParameter("mensagem", "STRING", _sanitize_message(mensagem)), bigquery.ScalarQueryParameter("upload_id", "STRING", upload_id)]
    if correlation_id:
        set_parts.append("correlation_id = @correlation_id")
        params.append(bigquery.ScalarQueryParameter("correlation_id", "STRING", correlation_id))
    for field in counter_fields:
        if field in counters and counters[field] is not None:
            set_parts.append(f"{field} = @{field}")
            params.append(bigquery.ScalarQueryParameter(field, "INT64", int(counters[field] or 0)))
    if detalhes_json is not None:
        safe_details = {k: str(v)[:300] for k, v in detalhes_json.items() if k not in SENSITIVE_IMPORT_FIELDS}
        set_parts.append("detalhes_json = @detalhes_json")
        params.append(bigquery.ScalarQueryParameter("detalhes_json", "STRING", json.dumps(safe_details, ensure_ascii=False)))
    if duracao_ms is not None:
        set_parts.append("duracao_ms = @duracao_ms")
        params.append(bigquery.ScalarQueryParameter("duracao_ms", "INT64", int(duracao_ms)))
    if finalizado:
        set_parts.append("finalizado_em = CURRENT_TIMESTAMP()")
    sql = f"UPDATE {bq.bq_ref(IMPORT_LOG_TABLE)} SET {', '.join(set_parts)} WHERE upload_id = @upload_id"
    _run_import_log_dml(sql, params, "import_log_update", upload_id, etapa)


def registrar_importacao_upload(**kwargs: Any) -> None:
    # Compatibilidade com chamadas antigas: grava um único registro sintético quando necessário.
    upload_id = kwargs.get("upload_id") or kwargs.get("id_importacao")
    if not upload_id:
        return
    atualizar_log_importacao(
        upload_id=upload_id,
        status=kwargs.get("status") or "ERRO",
        etapa=kwargs.get("etapa") or "FINALIZADO",
        mensagem=kwargs.get("mensagem_erro"),
        finalizado=True,
        duracao_ms=int(float(kwargs.get("duracao_segundos") or 0) * 1000),
        total_linhas=kwargs.get("total_recebido"),
        linhas_recebidas=kwargs.get("total_recebido"),
        linhas_validas=kwargs.get("total_valido"),
        linhas_rejeitadas=kwargs.get("total_rejeitado"),
        linhas_inseridas=kwargs.get("total_inserido"),
        linhas_atualizadas=kwargs.get("total_atualizado"),
        linhas_ignoradas=kwargs.get("total_ignorado_antigo"),
        erros=1 if kwargs.get("status") == "ERRO" else 0,
    )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _date_or_none(value: Any, field: str) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()[:10]
    try:
        date.fromisoformat(text)
    except ValueError as exc:
        raise GestaoValidationError(f"{field} inválida. Use AAAA-MM-DD.") from exc
    return text


def _as_clean_list(value: Any) -> List[str]:
    if value is None:
        return []
    values: Iterable[Any]
    if isinstance(value, (list, tuple)):
        values = value
    else:
        text = str(value).strip()
        if not text:
            return []
        values = text.split("||") if "||" in text else text.split(",")
    out: List[str] = []
    for item in values:
        cleaned = str(item).strip()
        if cleaned and cleaned not in out:
            out.append(cleaned[:200])
    return out


def _bool_filter(value: Any) -> Optional[bool]:
    if value in (None, ""):
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "sim", "s", "yes", "matriculado"}:
        return True
    if text in {"0", "false", "nao", "não", "n", "no", "nao_matriculado"}:
        return False
    raise GestaoValidationError("matriculado deve ser sim ou não.")


def parse_filters(source: Mapping[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    filters: Dict[str, Any] = {}
    data_ini = _date_or_none(source.get("data_ini") or source.get("periodo_inicial"), "data_ini")
    data_fim = _date_or_none(source.get("data_fim") or source.get("periodo_final"), "data_fim")
    if data_ini and data_fim and data_ini > data_fim:
        raise GestaoValidationError("data_ini não pode ser maior que data_fim.")
    if data_ini:
        filters["data_ini"] = data_ini
    if data_fim:
        filters["data_fim"] = data_fim

    for key in FILTER_SPECS:
        values = _as_clean_list(source.get(key))
        if values:
            filters[key] = values

    # Compatibilidade: filtro polo também consulta unidade quando polo não existir.
    if "polo" not in filters and _as_clean_list(source.get("polo_unidade")):
        filters["polo"] = _as_clean_list(source.get("polo_unidade"))

    matriculado = _bool_filter(source.get("matriculado"))
    if matriculado is not None:
        filters["matriculado"] = matriculado

    for key in ("status_importacao", "nome_arquivo", "usuario", "motivo", "id_importacao", "tipo"):
        value = str(source.get(key) or "").strip()
        if value:
            filters[key] = value[:200]

    busca = str(source.get("busca") or source.get("q") or "").strip()
    if busca:
        filters["busca"] = busca[:120]

    granularidade = str(source.get("granularidade") or "dia").strip().lower()
    if granularidade not in {"dia", "semana", "mes", "mês"}:
        granularidade = "dia"

    def _int_param(name: str, default: int, minimum: int, maximum: int) -> int:
        try:
            value = int(source.get(name) or default)
        except (TypeError, ValueError):
            value = default
        return max(minimum, min(maximum, value))

    limit = _int_param("limit", DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE)
    offset = _int_param("offset", 0, 0, 1_000_000)
    order_dir = str(source.get("order_dir") or "DESC").upper()
    if order_dir not in {"ASC", "DESC"}:
        order_dir = "DESC"
    meta = {
        "limit": limit,
        "offset": offset,
        "order_by": str(source.get("order_by") or "").strip(),
        "order_dir": order_dir,
        "granularidade": "mes" if granularidade == "mês" else granularidade,
        "force_refresh": str(source.get("force_refresh") or source.get("refresh") or "").lower() in {"1", "true", "yes", "sim"},
    }
    return filters, meta


def _normalized_for_cache(filters: Mapping[str, Any], meta: Optional[Mapping[str, Any]] = None) -> str:
    payload = {"filters": filters, "meta": meta or {}}
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _cache_key(endpoint: str, filters: Mapping[str, Any], meta: Optional[Mapping[str, Any]] = None) -> str:
    normalized = _normalized_for_cache(filters, meta)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"gestao:{endpoint}:{bq.GCP_PROJECT_ID}.{bq.BQ_DATASET}:{digest}"


def invalidate_gestao_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()
        bq._gestao_cache.clear()
    logger.info("gestao_cache invalidado")


def _cacheable(filters: Mapping[str, Any], allow_personal: bool = False) -> bool:
    return allow_personal or not any(k in filters for k in _PERSONAL_FILTERS)


def _with_cache(endpoint: str, filters: Mapping[str, Any], meta: Optional[Mapping[str, Any]], force: bool, fn, *, allow_personal: bool = False) -> Tuple[Any, bool]:
    if force or not _cacheable(filters, allow_personal=allow_personal):
        return fn(), False
    key = _cache_key(endpoint, filters, meta)
    with _CACHE_LOCK:
        if key in _CACHE:
            logger.info("gestao_cache hit endpoint=%s key=%s", endpoint, key[:32])
            return _CACHE[key], True
        lock = _KEY_LOCKS.setdefault(key, threading.Lock())
    with lock:
        with _CACHE_LOCK:
            if key in _CACHE:
                logger.info("gestao_cache hit_after_lock endpoint=%s key=%s", endpoint, key[:32])
                return _CACHE[key], True
        logger.info("gestao_cache miss endpoint=%s key=%s", endpoint, key[:32])
        value = fn()
        with _CACHE_LOCK:
            _CACHE[key] = value
        return value, False


def _has(col: str) -> bool:
    return bq._has_view_col(col)


def _col(col: str, alias: str = "v") -> str:
    return f"{alias}.{col}"


def _text_col(*candidates: str, default: str = "Não informado", alias: str = "v") -> str:
    return bq._gestao_text_expr(*candidates, default=default, alias=alias)


def _timestamp_col(*candidates: str, alias: str = "v") -> str:
    return bq._gestao_timestamp_expr(*candidates, alias=alias)


def _date_from_ts(expr: str) -> str:
    return f"DATE({expr})"


def _empty_text_sql(expr: str) -> str:
    return f"(NULLIF(TRIM(CAST({expr} AS STRING)), '') IS NULL OR UPPER(TRIM(CAST({expr} AS STRING))) IN ('NULL','N/A','NA','SEM INFORMACAO','SEM INFORMAÇÃO','-'))"


def _filled_text_sql(expr: str) -> str:
    return f"NOT {_empty_text_sql(expr)}"


def _digits_sql(expr: str) -> str:
    return f"REGEXP_REPLACE(COALESCE(CAST({expr} AS STRING), ''), r'[^0-9]', '')"



def _digits_only(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _mask_last4(value: Any, prefix: str = "*******") -> str:
    digits = _digits_only(value)
    return f"{prefix}{digits[-4:]}" if digits else ""


def _mask_cpf(value: Any) -> str:
    digits = _digits_only(value)
    return f"***.***.***-{digits[-4:]}" if digits else ""


def _mask_email(value: Any) -> str:
    text = str(value or "").strip()
    if "@" not in text:
        return ""
    local, domain = text.split("@", 1)
    return f"{(local[:1] or '*')}***@{domain}"


def mask_rejection_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    masked: Dict[str, Any] = {}
    for key, value in dict(row).items():
        if key in {"payload", "detalhes_json"}:
            continue
        if key in {"cpf", "cpf_raw"}:
            masked[key] = _mask_cpf(value)
        elif key in {"celular", "celular_raw", "telefone"}:
            masked[key] = _mask_last4(value)
        elif key in {"email", "email_raw"}:
            masked[key] = _mask_email(value)
        elif key in {"nome_raw"}:
            masked[key] = "***" if value else ""
        else:
            masked[key] = value
    return masked

def _mask_sql(expr: str, kind: str) -> str:
    digits = _digits_sql(expr)
    if kind == "cpf":
        return f"IF({digits} = '', '***', CONCAT('***.***.***-', RIGHT({digits}, 4)))"
    if kind == "celular":
        return f"IF({digits} = '', '', CONCAT('*******', RIGHT({digits}, 4)))"
    return f"CAST({expr} AS STRING)"


def _email_mask_sql(expr: str) -> str:
    txt = f"CAST({expr} AS STRING)"
    return f"CASE WHEN {_empty_text_sql(expr)} OR STRPOS({txt}, '@') = 0 THEN '' ELSE CONCAT(SUBSTR({txt}, 1, LEAST(2, GREATEST(1, STRPOS({txt}, '@') - 1))), '***', SUBSTR({txt}, STRPOS({txt}, '@'))) END"


def _data_inscricao_ts(alias: str = "v") -> str:
    return _timestamp_col("data_inscricao", alias=alias)


def _date_bucket_expr(granularidade: str, date_expr: str) -> str:
    if granularidade == "semana":
        return f"DATE_TRUNC({date_expr}, WEEK(MONDAY))"
    if granularidade == "mes":
        return f"DATE_TRUNC({date_expr}, MONTH)"
    return date_expr


def _apply_filters(where: List[str], params: List[Any], filters: Mapping[str, Any], alias: str = "v") -> None:
    data_inscricao = _timestamp_col("data_inscricao", alias=alias)
    if filters.get("data_ini"):
        where.append(f"{data_inscricao} IS NOT NULL AND DATE({data_inscricao}) >= @data_ini")
        params.append(bigquery.ScalarQueryParameter("data_ini", "DATE", filters["data_ini"]))
    if filters.get("data_fim"):
        where.append(f"{data_inscricao} IS NOT NULL AND DATE({data_inscricao}) <= @data_fim")
        params.append(bigquery.ScalarQueryParameter("data_fim", "DATE", filters["data_fim"]))

    for key, values in filters.items():
        if key not in FILTER_SPECS:
            continue
        col, _kind = FILTER_SPECS[key]
        effective_col = col
        if key == "polo" and not _has("polo") and _has("unidade"):
            effective_col = "unidade"
        if not _has(effective_col):
            continue
        param_name = f"f_{key}"
        filled, include_empty = bq._split_empty_filter(list(values))
        conditions: List[str] = []
        col_expr = f"{alias}.{effective_col}"
        if filled:
            conditions.append(f"TRIM(CAST({col_expr} AS STRING)) IN UNNEST(@{param_name})")
            params.append(bigquery.ArrayQueryParameter(param_name, "STRING", filled))
        if include_empty:
            conditions.append(bq._empty_value_condition(col_expr))
        if conditions:
            where.append("(" + " OR ".join(conditions) + ")")

    if "matriculado" in filters:
        expr = bq._gestao_matriculado_expr(alias)
        where.append(expr if filters["matriculado"] else f"NOT {expr}")

    if filters.get("busca"):
        search_cols = [c for c in ("nome", "cpf", "celular", "telefone", "email") if _has(c)]
        if search_cols:
            pieces = [f"UPPER(CAST({alias}.{c} AS STRING)) LIKE CONCAT('%', UPPER(@busca), '%')" for c in search_cols]
            where.append("(" + " OR ".join(pieces) + ")")
            params.append(bigquery.ScalarQueryParameter("busca", "STRING", filters["busca"]))


def _where_sql(filters: Mapping[str, Any], params: List[Any], alias: str = "v") -> str:
    where = ["1=1"]
    _apply_filters(where, params, filters, alias=alias)
    return " AND ".join(where)


def _run(sql: str, params: Optional[List[Any]], operation: str) -> List[Dict[str, Any]]:
    started = perf_counter()
    rows = bq._rows_to_json_safe(bq._run_gestao_query(sql, params=params, operation_name=operation))
    logger.info("gestao_query operation=%s duration=%.3fs rows=%s", operation, perf_counter() - started, len(rows))
    return rows


def _single(sql: str, params: Optional[List[Any]], operation: str) -> Dict[str, Any]:
    rows = _run(sql, params, operation)
    return rows[0] if rows else {}


def _safe_select(col: str, alias_out: Optional[str] = None, bq_type: str = "STRING") -> str:
    return bq._select_col(col, alias=alias_out, bq_type=bq_type)


def _score_components(alias: str = "v") -> Dict[str, str]:
    matriculado = bq._gestao_matriculado_expr(alias)
    status_empty = bq._gestao_status_empty_expr(alias)
    data_inscricao = _timestamp_col("data_inscricao", alias=alias)
    ultima_acao = _timestamp_col("data_ultima_acao", "ultima_atividade", "data_disparo", alias=alias)
    dt_upload = _timestamp_col("dt_upload", "data_atualizacao", alias=alias)
    qtd = f"COALESCE(SAFE_CAST({alias}.qtd_acionamentos AS INT64), 0)" if _has("qtd_acionamentos") else "0"
    dias_inscricao = f"IF({data_inscricao} IS NULL, 0, GREATEST(DATE_DIFF(CURRENT_DATE(), DATE({data_inscricao}), DAY), 0))"
    dias_acao = f"IF({ultima_acao} IS NULL, 999, GREATEST(DATE_DIFF(CURRENT_DATE(), DATE({ultima_acao}), DAY), 0))"
    carga_recente = f"IF({dt_upload} IS NOT NULL AND DATE_DIFF(CURRENT_DATE(), DATE({dt_upload}), DAY) <= 3, 8, 0)"
    sem_origem = f"IF({_text_col('origem', default='', alias=alias)} = '', 4, 0)"
    sem_campanha = f"IF({_text_col('campanha', default='', alias=alias)} = '', 2, 0)"
    score = f"""
    CASE WHEN {matriculado} THEN 5 ELSE
      20
      + IF({status_empty}, 35, 0)
      + IF({ultima_acao} IS NULL, 25, 0)
      + LEAST({dias_inscricao} * 0.35, 20)
      + LEAST({dias_acao} * 0.60, 30)
      + IF({qtd} = 0, 10, IF({qtd} BETWEEN 1 AND 2, 5, -5))
      + {sem_origem}
      + {sem_campanha}
      + {carga_recente}
    END
    """
    prioridade = f"CASE WHEN {matriculado} THEN 'BAIXA' WHEN ({score}) >= 85 THEN 'ALTA' WHEN ({score}) >= 55 THEN 'MÉDIA' ELSE 'BAIXA' END"
    motivo = f"""
    CASE
      WHEN {matriculado} THEN 'Lead matriculado: mantido em baixa prioridade.'
      WHEN {status_empty} AND {ultima_acao} IS NULL THEN 'Nunca trabalhado, sem status e sem ação registrada.'
      WHEN {status_empty} THEN 'Sem status definido.'
      WHEN {ultima_acao} IS NULL THEN 'Sem ação comercial registrada.'
      WHEN {dias_acao} > 7 THEN 'Parado há mais de 7 dias.'
      ELSE 'Prioridade calculada por idade, ações, origem, campanha e carga recente.'
    END
    """
    return {"score": score, "prioridade": prioridade, "motivo": motivo, "dias_sem_acao": dias_acao, "ultima_acao": ultima_acao, "data_inscricao": data_inscricao, "qtd": qtd}


def get_resumo(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        params: List[Any] = []
        where = _where_sql(filters, params)
        status_empty = bq._gestao_status_empty_expr("v")
        matriculado = bq._gestao_matriculado_expr("v")
        data_inscricao = _timestamp_col("data_inscricao")
        data_disparo = _timestamp_col("data_disparo")
        primeiro_contato = _timestamp_col("data_primeiro_contato", "primeiro_contato", "data_disparo")
        data_matricula = _timestamp_col("data_matricula", "data_matriculado", "dt_matricula")
        ultima_acao = _timestamp_col("data_ultima_acao", "ultima_atividade", "data_disparo")
        dt_upload = _timestamp_col("dt_upload", "data_atualizacao")
        sql = f"""
        WITH base AS (
          SELECT v.* FROM {bq._tbl(bq.BQ_VIEW_LEADS)} v WHERE {where}
        ), max_upload AS (SELECT MAX({_timestamp_col('dt_upload', 'data_atualizacao')}) AS max_dt FROM base v)
        SELECT
          COUNT(*) AS total_leads,
          COUNTIF({data_inscricao} IS NOT NULL) AS novos_leads_periodo,
          COUNTIF({status_empty}) AS nunca_trabalhados,
          COUNTIF(NOT {status_empty} AND NOT {matriculado}) AS leads_em_carteira,
          COUNTIF({matriculado}) AS leads_matriculados,
          SAFE_DIVIDE(COUNTIF({matriculado}), COUNT(*)) * 100 AS taxa_geral_conversao,
          COUNTIF({status_empty}) AS leads_sem_status,
          COUNTIF(NOT {matriculado} AND ({ultima_acao} IS NULL OR DATE_DIFF(CURRENT_DATE(), DATE({ultima_acao}), DAY) > 7)) AS leads_parados_7_dias,
          AVG(IF({primeiro_contato} IS NOT NULL AND {data_inscricao} IS NOT NULL, TIMESTAMP_DIFF({primeiro_contato}, {data_inscricao}, HOUR), NULL)) AS media_horas_primeiro_contato,
          AVG(IF({data_matricula} IS NOT NULL AND {data_inscricao} IS NOT NULL, TIMESTAMP_DIFF({data_matricula}, {data_inscricao}, HOUR), NULL)) AS media_horas_ate_matricula,
          COUNTIF({data_disparo} IS NOT NULL) AS quantidade_disparos,
          COUNTIF({dt_upload} = (SELECT max_dt FROM max_upload)) AS leads_atualizados_carga_mais_recente,
          (SELECT max_dt FROM max_upload) AS ultima_atualizacao_dados
        FROM base v
        """
        return _single(sql, params, "gestao_resumo")
    return _with_cache("resumo", filters, {}, meta.get("force_refresh", False), load)


def get_funil(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        params: List[Any] = []
        where = _where_sql(filters, params)
        status_empty = bq._gestao_status_empty_expr("v")
        matriculado = bq._gestao_matriculado_expr("v")
        inscrito_expr = "TRUE" if not _has("status_inscricao") else "UPPER(TRIM(CAST(v.status_inscricao AS STRING))) IN ('INSCRITO','INSCRIÇÃO','INSCRICAO','INS')"
        sql = f"""
        WITH agg AS (
          SELECT
            COUNT(*) AS total,
            COUNTIF({status_empty}) AS nunca,
            COUNTIF(NOT {status_empty} AND NOT {matriculado}) AS carteira,
            COUNTIF({inscrito_expr}) AS inscritos,
            COUNTIF({matriculado}) AS matriculados
          FROM {bq._tbl(bq.BQ_VIEW_LEADS)} v WHERE {where}
        )
        SELECT 'Total de leads' AS etapa, total AS volume, 1 AS ordem FROM agg
        UNION ALL SELECT 'Nunca trabalhados', nunca, 2 FROM agg
        UNION ALL SELECT 'Em atendimento ou carteira', carteira, 3 FROM agg
        UNION ALL SELECT 'Inscritos', inscritos, 4 FROM agg
        UNION ALL SELECT 'Matriculados', matriculados, 5 FROM agg
        ORDER BY ordem
        """
        rows = _run(sql, params, "gestao_funil")
        total = float(rows[0]["volume"] or 0) if rows else 0
        prev = None
        for row in rows:
            volume = float(row.get("volume") or 0)
            row["pct_total"] = (volume / total * 100) if total else 0
            row["conversao_etapa_anterior"] = (volume / prev * 100) if prev else None
            row["perda_etapa_anterior"] = (prev - volume) if prev is not None else None
            prev = volume
        return {"etapas": rows, "regra": "Matriculados usam flag_matriculado quando disponível e status normalizado como alternativa; nunca trabalhados usam status vazio."}
    return _with_cache("funil", filters, {}, meta.get("force_refresh", False), load)


def get_evolucao(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    granularidade = meta.get("granularidade", "dia")
    def load():
        params: List[Any] = []
        where = _where_sql(filters, params)
        matriculado = bq._gestao_matriculado_expr("v")
        data_inscricao = _timestamp_col("data_inscricao")
        data_disparo = _timestamp_col("data_disparo")
        data_matricula = _timestamp_col("data_matricula", "data_matriculado", "dt_matricula")
        dt_upload = _timestamp_col("dt_upload", "data_atualizacao")
        bucket = _date_bucket_expr(granularidade, "evento_data")
        sql = f"""
        WITH base AS (SELECT v.* FROM {bq._tbl(bq.BQ_VIEW_LEADS)} v WHERE {where}), eventos AS (
          SELECT DATE({data_inscricao}) AS evento_data, 1 leads, 0 matriculas, 0 disparos, 0 atualizacoes FROM base v WHERE {data_inscricao} IS NOT NULL
          UNION ALL SELECT DATE({data_matricula}), 0, 1, 0, 0 FROM base v WHERE {matriculado} AND {data_matricula} IS NOT NULL
          UNION ALL SELECT DATE({data_disparo}), 0, 0, 1, 0 FROM base v WHERE {data_disparo} IS NOT NULL
          UNION ALL SELECT DATE({dt_upload}), 0, 0, 0, 1 FROM base v WHERE {dt_upload} IS NOT NULL
        )
        SELECT {bucket} AS periodo, SUM(leads) AS leads_recebidos, SUM(matriculas) AS matriculas, SUM(disparos) AS disparos, SUM(atualizacoes) AS atualizacoes,
               SAFE_DIVIDE(SUM(matriculas), NULLIF(SUM(leads), 0)) * 100 AS taxa_conversao
        FROM eventos
        WHERE evento_data IS NOT NULL
        GROUP BY periodo
        ORDER BY periodo
        LIMIT 500
        """
        return {"granularidade": granularidade, "series": _run(sql, params, "gestao_evolucao")}
    return _with_cache("evolucao", filters, {"granularidade": granularidade}, meta.get("force_refresh", False), load)


def _ranking_sql(dimension_expr: str, filters: Mapping[str, Any], limit: int, min_leads: int, operation: str) -> List[Dict[str, Any]]:
    params: List[Any] = [bigquery.ScalarQueryParameter("limit", "INT64", limit), bigquery.ScalarQueryParameter("min_leads", "INT64", min_leads)]
    where = _where_sql(filters, params)
    matriculado = bq._gestao_matriculado_expr("v")
    sql = f"""
    SELECT {dimension_expr} AS nome,
           COUNT(*) AS total_leads,
           COUNTIF({matriculado}) AS matriculas,
           SAFE_DIVIDE(COUNTIF({matriculado}), COUNT(*)) * 100 AS taxa_conversao_pct
    FROM {bq._tbl(bq.BQ_VIEW_LEADS)} v
    WHERE {where}
    GROUP BY nome
    HAVING total_leads >= @min_leads
    ORDER BY taxa_conversao_pct DESC, matriculas DESC, total_leads DESC
    LIMIT @limit
    """
    rows = _run(sql, params, operation)
    for idx, row in enumerate(rows, 1):
        row["posicao"] = idx
        row["melhor_resultado"] = idx == 1
    return rows


def get_rankings(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        limit = 10
        min_leads = MIN_RANKING_LEADS
        consultor = _text_col("consultor_comercial", default="Sem consultor")
        origem = _text_col("origem", default="Sem origem")
        curso = _text_col("curso", default="Sem curso")
        # volume usa min 1, conversão usa mínimo configurável.
        return {
            "minimo_leads_conversao": min_leads,
            "consultores_matriculas": _ranking_sql(consultor, filters, limit, 1, "gestao_ranking_consultores_mat"),
            "consultores_conversao": _ranking_sql(consultor, filters, limit, min_leads, "gestao_ranking_consultores_conv"),
            "origens_volume": _ranking_sql(origem, filters, limit, 1, "gestao_ranking_origens_vol"),
            "origens_conversao": _ranking_sql(origem, filters, limit, min_leads, "gestao_ranking_origens_conv"),
            "cursos_volume": _ranking_sql(curso, filters, limit, 1, "gestao_ranking_cursos_vol"),
            "cursos_conversao": _ranking_sql(curso, filters, limit, min_leads, "gestao_ranking_cursos_conv"),
        }
    return _with_cache("rankings", filters, {}, meta.get("force_refresh", False), load)


def get_produtividade(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        params: List[Any] = [bigquery.ScalarQueryParameter("limit", "INT64", meta["limit"]), bigquery.ScalarQueryParameter("offset", "INT64", meta["offset"])]
        where = _where_sql(filters, params)
        status_empty = bq._gestao_status_empty_expr("v")
        matriculado = bq._gestao_matriculado_expr("v")
        consultor = _text_col("consultor_comercial", default="Sem consultor")
        data_inscricao = _timestamp_col("data_inscricao")
        primeiro = _timestamp_col("data_primeiro_contato", "primeiro_contato", "data_disparo")
        matricula = _timestamp_col("data_matricula", "data_matriculado", "dt_matricula")
        ultima = _timestamp_col("data_ultima_acao", "ultima_atividade", "data_disparo")
        qtd = "COALESCE(SAFE_CAST(v.qtd_acionamentos AS INT64), 0)" if _has("qtd_acionamentos") else "0"
        score = _score_components()["score"]
        order_by = ORDERABLE_PRODUTIVIDADE.get(meta.get("order_by") or "taxa_conversao_pct", "taxa_conversao_pct")
        order_dir = meta.get("order_dir", "DESC")
        sql = f"""
        WITH agg AS (
          SELECT {consultor} AS consultor,
            COUNT(*) AS total_leads,
            COUNTIF({data_inscricao} IS NOT NULL) AS leads_novos,
            COUNTIF({status_empty}) AS leads_sem_status,
            COUNTIF(NOT {status_empty} AND NOT {matriculado}) AS leads_em_carteira,
            COUNTIF({matriculado}) AS matriculados,
            SAFE_DIVIDE(COUNTIF({matriculado}), COUNT(*)) * 100 AS taxa_conversao_pct,
            SUM({qtd}) AS quantidade_acionamentos,
            AVG(IF({primeiro} IS NOT NULL AND {data_inscricao} IS NOT NULL, TIMESTAMP_DIFF({primeiro}, {data_inscricao}, HOUR), NULL)) AS media_horas_primeiro_contato,
            AVG(IF({matricula} IS NOT NULL AND {data_inscricao} IS NOT NULL, TIMESTAMP_DIFF({matricula}, {data_inscricao}, HOUR), NULL)) AS media_horas_ate_matricula,
            MAX({ultima}) AS ultima_atividade,
            IF(MAX({ultima}) IS NULL, NULL, DATE_DIFF(CURRENT_DATE(), DATE(MAX({ultima}), 'America/Sao_Paulo'), DAY)) AS dias_sem_atividade,
            AVG({score}) AS score_medio_carteira
          FROM {bq._tbl(bq.BQ_VIEW_LEADS)} v WHERE {where}
          GROUP BY consultor
        ), numbered AS (SELECT agg.*, COUNT(*) OVER() AS total_registros FROM agg)
        SELECT numbered.*, CASE WHEN ultima_atividade IS NULL OR dias_sem_atividade > 14 THEN 'inativo' WHEN dias_sem_atividade > 7 THEN 'atenção' ELSE 'ativo' END AS situacao
        FROM numbered
        ORDER BY {order_by} {order_dir}
        LIMIT @limit OFFSET @offset
        """
        rows = _run(sql, params, "gestao_produtividade")
        return {"rows": rows, "total": rows[0].get("total_registros", 0) if rows else 0, "limit": meta["limit"], "offset": meta["offset"]}
    return _with_cache("produtividade", filters, {k: meta.get(k) for k in ("limit", "offset", "order_by", "order_dir")}, meta.get("force_refresh", False), load)



def _status_norm_expr(alias: str = "v") -> str:
    candidates = [c for c in ("status", "status_inscricao", "tipo_negocio") if _has(c)]
    if not candidates:
        return "''"
    return "COALESCE(" + ", ".join([f"NULLIF(UPPER(TRIM(CAST({alias}.{c} AS STRING))), '')" for c in candidates]) + ", '')"


def _status_ec_expr(alias: str = "v") -> str:
    parts = [f"UPPER(TRIM(CAST({alias}.{c} AS STRING))) = 'EC'" for c in ("status", "status_inscricao", "tipo_negocio") if _has(c)]
    return "(" + " OR ".join(parts) + ")" if parts else "FALSE"


def _status_excluded_expr(alias: str = "v") -> str:
    parts = [f"UPPER(TRIM(CAST({alias}.{c} AS STRING))) IN ('MAT','MATRICULADO','CANCELADO','CANCELADA','CANC','DESCARTADO','DESCARTADA','ENCERRADO','ENCERRADA','PERDIDO','PERDIDA')" for c in ("status", "status_inscricao", "tipo_negocio") if _has(c)]
    return "(" + " OR ".join(parts) + ")" if parts else "FALSE"


def _valid_phone_sql(alias: str = "v") -> str:
    tel_col = bq._first_existing_col("celular", "telefone")
    if not tel_col:
        return "FALSE"
    digits = _digits_sql(f"{alias}.{tel_col}")
    return f"({digits} != '' AND LENGTH({digits}) IN (10, 11) AND NOT REGEXP_CONTAINS({digits}, r'^(\\d)\\1+$'))"


def _fila_sql(filters: Mapping[str, Any], meta: Mapping[str, Any], *, export: bool = False) -> Tuple[str, List[Any]]:
    limit = min(int(meta.get("limit", DEFAULT_PAGE_SIZE)), 5000 if export else MAX_PAGE_SIZE)
    offset = max(0, int(meta.get("offset", 0)))
    params: List[Any] = [] if export else [bigquery.ScalarQueryParameter("limit", "INT64", limit), bigquery.ScalarQueryParameter("offset", "INT64", offset)]
    where = ["1=1"]
    _apply_filters(where, params, filters, alias="v")
    data_inscricao = _data_inscricao_ts("v")
    ultima = _timestamp_col("data_ultima_acao", "ultima_atividade", "data_disparo", alias="v")
    dt_upload = _timestamp_col("dt_upload", "data_atualizacao", alias="v")
    sem_status = bq._gestao_status_empty_expr("v")
    matriculado = bq._gestao_matriculado_expr("v")
    status_ec = _status_ec_expr("v")
    excluido = _status_excluded_expr("v")
    telefone_valido = _valid_phone_sql("v")
    status_norm = _status_norm_expr("v")
    polo_expr = "v.polo" if _has("polo") else "v.unidade" if _has("unidade") else "CAST(NULL AS STRING)"
    status_expr = "v.status" if _has("status") else "v.status_inscricao" if _has("status_inscricao") else "CAST(NULL AS STRING)"
    data_disparo = _timestamp_col("data_disparo", alias="v")
    data_ultima_acao_only = _timestamp_col("data_ultima_acao", "ultima_atividade", alias="v")
    nunca_trabalhado = f"({sem_status} AND {data_disparo} IS NULL AND {data_ultima_acao_only} IS NULL)"
    sem_status_trabalhado = f"({sem_status} AND ({data_disparo} IS NOT NULL OR {data_ultima_acao_only} IS NOT NULL))"
    grupo = f"""
      CASE
        WHEN {matriculado} THEN 99
        WHEN {nunca_trabalhado} THEN 1
        WHEN {sem_status_trabalhado} THEN 2
        WHEN {status_ec} THEN 3
        WHEN NOT {excluido} THEN 4
        ELSE 99
      END
    """
    prioridade = f"CASE WHEN ({grupo}) = 1 THEN 100 WHEN ({grupo}) = 2 THEN 85 WHEN ({grupo}) = 3 THEN 70 WHEN ({grupo}) = 4 THEN 40 ELSE 0 END"
    motivo = f"CASE WHEN ({grupo}) = 1 THEN 'Nunca trabalhado, sem status e sem ação registrada' WHEN ({grupo}) = 2 THEN 'Sem status, já teve disparo ou contato' WHEN ({grupo}) = 3 THEN 'Lead classificado como EC' WHEN ({grupo}) = 4 THEN 'Lead elegível para acompanhamento' ELSE 'Lead não elegível' END"
    sql = f"""
    WITH base AS (
      SELECT
        {_safe_select('nome')}, {_safe_select('celular')}, {_safe_select('curso')}, {polo_expr} AS polo,
        {_safe_select('origem')}, {_safe_select('campanha')}, {_safe_select('consultor_comercial')},
        {status_expr} AS status, {data_inscricao} AS data_inscricao, {ultima} AS data_ultima_acao, {dt_upload} AS dt_upload,
        IF({ultima} IS NULL, NULL, GREATEST(DATE_DIFF(CURRENT_DATE(), DATE({ultima}), DAY), 0)) AS dias_sem_acao,
        IF({data_inscricao} IS NULL, NULL, GREATEST(DATE_DIFF(CURRENT_DATE(), DATE({data_inscricao}), DAY), 0)) AS dias_desde_inscricao,
        {status_norm} AS status_normalizado,
        {grupo} AS grupo_prioridade,
        {prioridade} AS prioridade,
        {motivo} AS motivo_prioridade
      FROM {bq._tbl(bq.BQ_VIEW_LEADS)} v
      WHERE {' AND '.join(where)} AND NOT {matriculado} AND NOT {excluido} AND {telefone_valido}
    ), numbered AS (SELECT base.*, COUNT(*) OVER() AS total_registros FROM base WHERE grupo_prioridade < 99)
    SELECT * FROM numbered
    ORDER BY grupo_prioridade ASC, data_inscricao DESC NULLS LAST, dt_upload DESC NULLS LAST, prioridade DESC
    """
    if not export:
        sql += "\nLIMIT @limit OFFSET @offset"
    return sql, params


def get_fila(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    sql, params = _fila_sql(filters, meta)
    rows = _run(sql, params, "gestao_fila")
    total = rows[0].get("total_registros", 0) if rows else 0
    return {"items": rows, "rows": rows, "pagination": _pagination(meta, total), "total": total, "limit": meta["limit"], "offset": meta["offset"], "score_regra": score_rule_documentation()}, False


def _pagination(meta: Mapping[str, Any], total: int) -> Dict[str, Any]:
    page_size = int(meta.get("limit", DEFAULT_PAGE_SIZE))
    offset = int(meta.get("offset", 0))
    page = offset // page_size + 1 if page_size else 1
    return {"page": page, "page_size": page_size, "total": int(total or 0), "total_pages": math.ceil((total or 0) / page_size) if page_size else 0}


def _cpf_valid_sql(digits_expr: str) -> str:
    nums = [f"CAST(SUBSTR({digits_expr}, {i}, 1) AS INT64)" for i in range(1, 12)]
    d1 = "MOD(MOD(" + " + ".join([f"{nums[i]} * {10-i}" for i in range(9)]) + ", 11) * 10, 11)"
    d1 = f"IF({d1} = 10, 0, {d1})"
    d2 = "MOD(MOD(" + " + ".join([f"{nums[i]} * {11-i}" for i in range(10)]) + ", 11) * 10, 11)"
    d2 = f"IF({d2} = 10, 0, {d2})"
    return f"(LENGTH({digits_expr}) = 11 AND NOT REGEXP_CONTAINS({digits_expr}, r'^(\\d)\\1+$') AND {d1} = {nums[9]} AND {d2} = {nums[10]})"


def _quality_base_exprs(alias: str = "v") -> Dict[str, str]:
    cpf_col = bq._first_existing_col("cpf")
    tel_col = bq._first_existing_col("celular", "telefone")
    return {
        "cpf_col": cpf_col or "",
        "tel_col": tel_col or "",
        "cpf_digits": _digits_sql(f"{alias}.{cpf_col}") if cpf_col else "''",
        "tel_digits": _digits_sql(f"{alias}.{tel_col}") if tel_col else "''",
        "email_expr": f"{alias}.email" if _has("email") else "CAST(NULL AS STRING)",
        "origem_expr": f"{alias}.origem" if _has("origem") else "CAST(NULL AS STRING)",
        "curso_expr": f"{alias}.curso" if _has("curso") else "CAST(NULL AS STRING)",
        "consultor_expr": f"{alias}.consultor_comercial" if _has("consultor_comercial") else "CAST(NULL AS STRING)",
        "status_expr": f"{alias}.status" if _has("status") else "CAST(NULL AS STRING)",
    }


def _quality_conditions(alias: str = "v") -> Dict[str, str]:
    e = _quality_base_exprs(alias)
    cpf_valid = _cpf_valid_sql(e["cpf_digits"])
    data_raw = f"{alias}.data_inscricao" if _has("data_inscricao") else "CAST(NULL AS STRING)"
    data_ts = _timestamp_col("data_inscricao", alias=alias)
    dup_cpf = f"{e['cpf_digits']} IN (SELECT cpf_key FROM dup_cpf)" if e["cpf_col"] else "FALSE"
    dup_tel = f"{e['tel_digits']} IN (SELECT tel_key FROM dup_tel)" if e["tel_col"] else "FALSE"
    return {
        "sem_celular": f"{e['tel_digits']} = ''",
        "sem_email": _empty_text_sql(e["email_expr"]),
        "sem_cpf": f"{e['cpf_digits']} = ''",
        "sem_origem": _empty_text_sql(e["origem_expr"]),
        "sem_curso": _empty_text_sql(e["curso_expr"]),
        "sem_consultor": _empty_text_sql(e["consultor_expr"]),
        "sem_status": bq._gestao_status_empty_expr(alias),
        "telefone_invalido": f"{e['tel_digits']} != '' AND (LENGTH({e['tel_digits']}) NOT IN (10, 11) OR REGEXP_CONTAINS({e['tel_digits']}, r'^(\\d)\\1+$'))",
        "cpf_invalido": f"{e['cpf_digits']} != '' AND (LENGTH({e['cpf_digits']}) != 11 OR NOT {cpf_valid})",
        "duplicado_celular": dup_tel,
        "duplicado_cpf": dup_cpf,
        "sem_dt_upload": f"{_timestamp_col('dt_upload', 'data_atualizacao', alias=alias)} IS NULL",
        "data_invalida": f"NOT {_empty_text_sql(data_raw)} AND {data_ts} IS NULL",
    }


def get_qualidade(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        params: List[Any] = []
        where = _where_sql(filters, params)
        e = _quality_base_exprs("v")
        cpf_valid = _cpf_valid_sql(e["cpf_digits"])
        cond = _quality_conditions("v")
        dup_cpf_cte = f"SELECT {e['cpf_digits']} cpf_key, COUNT(*) qtd FROM base v WHERE {e['cpf_digits']} != '' AND {cpf_valid} GROUP BY cpf_key HAVING qtd > 1" if e["cpf_col"] else "SELECT '' cpf_key, 0 qtd WHERE FALSE"
        dup_tel_cte = f"SELECT {e['tel_digits']} tel_key, COUNT(*) qtd FROM base v WHERE {e['tel_digits']} != '' GROUP BY tel_key HAVING qtd > 1" if e["tel_col"] else "SELECT '' tel_key, 0 qtd WHERE FALSE"
        # registros rejeitados não depende de filtros de leads; conta a tabela se existir.
        sql = f"""
        WITH base AS (SELECT v.* FROM {bq._tbl(bq.BQ_VIEW_LEADS)} v WHERE {where}),
        dup_cpf AS ({dup_cpf_cte}), dup_tel AS ({dup_tel_cte})
        SELECT
          COUNTIF({cond['sem_celular']}) AS sem_celular,
          COUNTIF({cond['sem_email']}) AS sem_email,
          COUNTIF({cond['sem_cpf']}) AS sem_cpf,
          COUNTIF({cond['sem_origem']}) AS sem_origem,
          COUNTIF({cond['sem_curso']}) AS sem_curso,
          COUNTIF({cond['sem_consultor']}) AS sem_consultor,
          COUNTIF({cond['sem_status']}) AS sem_status,
          COUNTIF({cond['telefone_invalido']}) AS telefone_invalido,
          COUNTIF({cond['cpf_invalido']}) AS cpf_invalido,
          COALESCE((SELECT SUM(qtd - 1) FROM dup_tel), 0) AS duplicado_celular,
          COALESCE((SELECT SUM(qtd - 1) FROM dup_cpf), 0) AS duplicado_cpf,
          0 AS rejeitado,
          COUNTIF({cond['sem_dt_upload']}) AS sem_dt_upload,
          COUNTIF({cond['data_invalida']}) AS data_invalida
        FROM base v
        """
        indicadores = _single(sql, params, "gestao_qualidade")
        try:
            rej_sql = f"SELECT COALESCE(SUM(total_rejeicoes), 0) AS total FROM {bq.bq_ref(REJECTIONS_SUMMARY_VIEW)}"
            indicadores["rejeitado"] = _single(rej_sql, [], "gestao_qualidade_rejeitados").get("total", 0)
        except NotFound:
            indicadores["rejeitado"] = None
        # aliases legados
        indicadores.update({
            "leads_sem_telefone": indicadores.get("sem_celular"),
            "leads_sem_email": indicadores.get("sem_email"),
            "leads_sem_cpf": indicadores.get("sem_cpf"),
            "leads_sem_origem": indicadores.get("sem_origem"),
            "leads_sem_curso": indicadores.get("sem_curso"),
            "leads_sem_consultor": indicadores.get("sem_consultor"),
            "telefone_invalido": indicadores.get("telefone_invalido"),
            "cpf_invalido_ou_incompleto": indicadores.get("cpf_invalido"),
            "duplicados_por_cpf_excedentes": indicadores.get("duplicado_cpf"),
            "duplicados_por_telefone_excedentes": indicadores.get("duplicado_celular"),
            "registros_rejeitados_upload": indicadores.get("rejeitado"),
            "registros_sem_dt_upload": indicadores.get("sem_dt_upload"),
            "datas_nao_interpretadas": indicadores.get("data_invalida"),
        })
        return {"indicadores": indicadores, "items": [], "pagination": _pagination(meta, 0), "regras": {"telefone_invalido": "Remove caracteres não numéricos; inválido se preenchido com tamanho diferente de 10/11 ou todos os dígitos iguais.", "cpf_invalido": "Remove caracteres não numéricos; separa ausente de inválido/incompleto e valida dígitos verificadores para CPFs com 11 dígitos."}}
    return _with_cache("qualidade", filters, {}, meta.get("force_refresh", False), load)


def _quality_details_sql(tipo: str, filters: Mapping[str, Any], meta: Mapping[str, Any], *, export: bool = False) -> Tuple[str, List[Any]]:
    if tipo not in QUALITY_TYPES:
        raise GestaoValidationError("Tipo de indicador inválido.")
    params: List[Any] = [] if export else [bigquery.ScalarQueryParameter("limit", "INT64", meta.get("limit", DEFAULT_PAGE_SIZE)), bigquery.ScalarQueryParameter("offset", "INT64", meta.get("offset", 0))]
    where = _where_sql(filters, params)
    e = _quality_base_exprs("v")
    cond = _quality_conditions("v")
    data_insc = _timestamp_col("data_inscricao", alias="v")
    dt_upload = _timestamp_col("dt_upload", "data_atualizacao", alias="v")
    cpf_valid = _cpf_valid_sql(e["cpf_digits"])
    dup_cpf_cte = f"SELECT {e['cpf_digits']} cpf_key, COUNT(*) qtd FROM base v WHERE {e['cpf_digits']} != '' AND {cpf_valid} GROUP BY cpf_key HAVING qtd > 1" if e["cpf_col"] else "SELECT '' cpf_key, 0 qtd WHERE FALSE"
    dup_tel_cte = f"SELECT {e['tel_digits']} tel_key, COUNT(*) qtd FROM base v WHERE {e['tel_digits']} != '' GROUP BY tel_key HAVING qtd > 1" if e["tel_col"] else "SELECT '' tel_key, 0 qtd WHERE FALSE"
    condition = cond[tipo] if tipo != "rejeitado" else "FALSE"
    if tipo == "rejeitado":
        raise GestaoValidationError("Rejeições individuais exigem autorização específica e não são expostas neste endpoint.")
    else:
        sql = f"""
        WITH base AS (SELECT v.* FROM {bq._tbl(bq.BQ_VIEW_LEADS)} v WHERE {where}),
        dup_cpf AS ({dup_cpf_cte}), dup_tel AS ({dup_tel_cte})
        SELECT '{QUALITY_TYPES[tipo]}' AS motivo,
               {_mask_sql('v.cpf', 'cpf') if e['cpf_col'] else "''"} AS identificador,
               {_safe_select('nome')}, {_safe_select('curso')}, {_safe_select('consultor_comercial', 'consultor')},
               {data_insc} AS data_inscricao, {dt_upload} AS data_upload,
               {_safe_select('origem')}, {_safe_select('status')},
               {_mask_sql('v.celular', 'celular') if e['tel_col'] == 'celular' else "''"} AS celular_mascarado,
               {_email_mask_sql('v.email') if _has('email') else "''"} AS email_mascarado,
               COUNT(*) OVER() AS total_registros
        FROM base v
        WHERE {condition}
        ORDER BY data_inscricao DESC NULLS LAST, data_upload DESC NULLS LAST
        """
    if not export:
        sql += "\nLIMIT @limit OFFSET @offset"
    return sql, params


def get_qualidade_detalhes(filters: Mapping[str, Any], meta: Mapping[str, Any], tipo: str) -> Tuple[Dict[str, Any], bool]:
    sql, params = _quality_details_sql(tipo, filters, meta)
    rows = [mask_rejection_row(r) for r in _run(sql, params, "gestao_qualidade_detalhes")]
    total = rows[0].get("total_registros", 0) if rows else 0
    return {"indicadores": {}, "items": rows, "pagination": _pagination(meta, total)}, False


def _csv_response_bytes(headers: List[Tuple[str, str]], rows: List[Dict[str, Any]], delimiter: str = ";") -> bytes:
    output = io.StringIO()
    writer = csv.writer(output, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    writer.writerow([label for _key, label in headers])
    for row in rows:
        writer.writerow([row.get(key, "") for key, _label in headers])
    return output.getvalue().encode("utf-8-sig")


def export_qualidade(filters: Mapping[str, Any], meta: Mapping[str, Any], tipo: str) -> Tuple[str, bytes, int]:
    sql, params = _quality_details_sql(tipo, filters, {**meta, "limit": 5000, "offset": 0}, export=True)
    rows = [mask_rejection_row(r) for r in _run(sql, params, "gestao_qualidade_exportar")]
    headers = [("motivo", "Motivo"), ("identificador", "Identificador mascarado"), ("nome", "Nome"), ("curso", "Curso"), ("consultor", "Consultor"), ("data_inscricao", "Data de inscrição"), ("data_upload", "Data do upload"), ("origem", "Origem"), ("status", "Status")]
    filename = f"qualidade_{tipo}_{date.today().isoformat()}.csv"
    return filename, _csv_response_bytes(headers, rows), len(rows)


def export_fila(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[str, bytes, int]:
    sql, params = _fila_sql(filters, {**meta, "limit": 5000, "offset": 0}, export=True)
    rows = _run(sql, params, "gestao_fila_exportar")
    for row in rows:
        row["celular"] = _mask_last4(row.get("celular"))
    headers = [("nome", "Nome"), ("celular", "Celular"), ("curso", "Curso"), ("polo", "Unidade"), ("origem", "Origem"), ("campanha", "Campanha"), ("consultor_comercial", "Consultor"), ("status", "Status"), ("data_inscricao", "Data de inscrição"), ("data_ultima_acao", "Última ação"), ("grupo_prioridade", "Grupo de prioridade"), ("prioridade", "Prioridade"), ("motivo_prioridade", "Motivo da prioridade")]
    return f"fila_operacional_{date.today().isoformat()}.csv", _csv_response_bytes(headers, rows), len(rows)



def get_opcoes(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        data: Dict[str, List[str]] = {}
        for field in OPTION_FIELDS:
            effective = field
            if field == "polo" and not _has("polo") and _has("unidade"):
                effective = "unidade"
            if not _has(effective):
                data[field] = []
                continue
            params = [bigquery.ScalarQueryParameter("limit", "INT64", 200)]
            sql = f"""
            SELECT DISTINCT TRIM(CAST(v.{effective} AS STRING)) AS valor
            FROM {bq._tbl(bq.BQ_VIEW_LEADS)} v
            WHERE v.{effective} IS NOT NULL AND TRIM(CAST(v.{effective} AS STRING)) != ''
            ORDER BY valor
            LIMIT @limit
            """
            data[field] = [r["valor"] for r in _run(sql, params, f"gestao_opcoes_{field}")]
        return data
    return _with_cache("opcoes", {}, {}, meta.get("force_refresh", False), load)


def score_rule_documentation() -> List[Dict[str, Any]]:
    return [
        {"componente": "Grupo 1", "regra": "Nunca trabalhados: leads não matriculados, com status vazio, sem data_disparo e sem data_ultima_acao; ordenados por data_inscricao desc e dt_upload desc."},
        {"componente": "Grupo 2", "regra": "Depois vêm leads não matriculados com status vazio que já tiveram disparo ou contato, também por data_inscricao desc."},
        {"componente": "Grupo 3", "regra": "Depois vêm leads não matriculados classificados exatamente como EC em status, status_inscricao ou tipo_negocio."},
        {"componente": "Grupo 4", "regra": "Por fim vêm demais leads elegíveis, excluindo matriculados, cancelados, descartados, encerrados e sem telefone válido."},
        {"componente": "Score", "regra": "A prioridade 100/85/70/40 é explicativa; grupo_prioridade vem primeiro e data_inscricao desc decide a ordem dentro do grupo."},
    ]


def is_matriculado_row(row: Mapping[str, Any]) -> bool:
    flag = row.get("flag_matriculado")
    if isinstance(flag, bool) and flag:
        return True
    text_flag = str(row.get("matriculado") or "").strip().upper()
    status = str(row.get("status") or "").strip().upper()
    return text_flag in {"SIM", "S", "TRUE", "1", "MATRICULADO", "MAT"} or status in {"MAT", "MATRICULADO"}


def is_status_empty(value: Any) -> bool:
    return value is None or str(value).strip().upper() in EMPTY_TEXTS


def should_accept_upload_version(staging_dt_upload: datetime, fact_data_atualizacao: Optional[datetime]) -> bool:
    if fact_data_atualizacao is None:
        return True
    return staging_dt_upload >= fact_data_atualizacao



def normalize_phone(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def is_valid_phone(value: Any) -> bool:
    digits = normalize_phone(value)
    return len(digits) in {10, 11} and len(set(digits)) > 1


def normalize_cpf(value: Any) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def is_valid_cpf(value: Any) -> bool:
    cpf = normalize_cpf(value)
    if len(cpf) != 11 or len(set(cpf)) == 1:
        return False
    nums = [int(c) for c in cpf]
    s1 = sum(nums[i] * (10 - i) for i in range(9))
    d1 = (s1 * 10) % 11
    d1 = 0 if d1 == 10 else d1
    s2 = sum(nums[i] * (11 - i) for i in range(10))
    d2 = (s2 * 10) % 11
    d2 = 0 if d2 == 10 else d2
    return nums[9] == d1 and nums[10] == d2


def is_status_ec(*values: Any) -> bool:
    return any(str(v or "").strip().upper() == "EC" for v in values)


def parse_lead_date(value: Any) -> Optional[date]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19] if "%H" in fmt else text[:10], fmt).date()
        except ValueError:
            pass
    try:
        serial = int(float(text))
        if serial > 0:
            return date(1899, 12, 30) + timedelta(days=serial)
    except ValueError:
        pass
    return None


def prioritize_fila_rows(rows: List[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for raw in rows:
        row = dict(raw)
        if is_matriculado_row(row) or not is_valid_phone(row.get("celular") or row.get("telefone")):
            continue
        status = row.get("status")
        status_norm = str(status or row.get("status_inscricao") or row.get("tipo_negocio") or "").strip().upper()
        if status_norm in {"CANCELADO", "CANCELADA", "CANC", "DESCARTADO", "DESCARTADA", "ENCERRADO", "ENCERRADA", "PERDIDO", "PERDIDA", "MAT", "MATRICULADO"}:
            continue
        dt = parse_lead_date(row.get("data_inscricao"))
        data_disparo = parse_lead_date(row.get("data_disparo"))
        data_ultima_acao = parse_lead_date(row.get("data_ultima_acao") or row.get("ultima_atividade"))
        if is_status_empty(status) and data_disparo is None and data_ultima_acao is None:
            grupo, prioridade, motivo = 1, 100, "Nunca trabalhado, sem status e sem ação registrada"
        elif is_status_empty(status):
            grupo, prioridade, motivo = 2, 85, "Sem status, já teve disparo ou contato"
        elif is_status_ec(row.get("status"), row.get("status_inscricao"), row.get("tipo_negocio")):
            grupo, prioridade, motivo = 3, 70, "Lead classificado como EC"
        else:
            grupo, prioridade, motivo = 4, 40, "Lead elegível para acompanhamento"
        row.update({"grupo_prioridade": grupo, "prioridade": prioridade, "motivo_prioridade": motivo, "data_inscricao_normalizada": dt, "status_normalizado": status_norm, "dias_desde_inscricao": (date.today() - dt).days if dt else None})
        out.append(row)
    out.sort(key=lambda r: (r["grupo_prioridade"], -(r["data_inscricao_normalizada"].toordinal() if r.get("data_inscricao_normalizada") else 0)))
    return out
