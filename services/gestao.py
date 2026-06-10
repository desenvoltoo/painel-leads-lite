from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from collections import OrderedDict
from datetime import date, datetime, timezone
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
    "score": "score_prioridade",
    "score_prioridade": "score_prioridade",
    "nome": "nome",
    "data_inscricao": "data_inscricao",
    "data_ultima_acao": "data_ultima_acao",
    "dias_sem_acao": "dias_sem_acao",
    "prioridade": "prioridade",
}

class GestaoValidationError(ValueError):
    pass


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


def get_fila(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    params: List[Any] = [bigquery.ScalarQueryParameter("limit", "INT64", meta["limit"]), bigquery.ScalarQueryParameter("offset", "INT64", meta["offset"])]
    where = _where_sql(filters, params)
    comps = _score_components()
    polo_expr = "v.polo" if _has("polo") else "v.unidade" if _has("unidade") else "CAST(NULL AS STRING)"
    status_expr = "v.status" if _has("status") else "v.status_inscricao" if _has("status_inscricao") else "CAST(NULL AS STRING)"
    order_by = ORDERABLE_FILA.get(meta.get("order_by") or "score_prioridade", "score_prioridade")
    order_dir = meta.get("order_dir", "DESC")
    sql = f"""
    WITH scored AS (
      SELECT
        {_safe_select('nome')}, {_safe_select('celular')}, {_safe_select('curso')}, {polo_expr} AS polo,
        {_safe_select('origem')}, {_safe_select('campanha')}, {_safe_select('consultor_comercial')},
        {status_expr} AS status, {comps['data_inscricao']} AS data_inscricao, {comps['ultima_acao']} AS data_ultima_acao,
        {comps['dias_sem_acao']} AS dias_sem_acao, ROUND({comps['score']}, 2) AS score_prioridade,
        {comps['prioridade']} AS prioridade, {comps['motivo']} AS motivo_prioridade,
        COUNT(*) OVER() AS total_registros
      FROM {bq._tbl(bq.BQ_VIEW_LEADS)} v WHERE {where} AND NOT {bq._gestao_matriculado_expr('v')}
    )
    SELECT scored.* FROM scored
    ORDER BY {order_by} {order_dir}, data_inscricao DESC
    LIMIT @limit OFFSET @offset
    """
    rows = _run(sql, params, "gestao_fila")
    return {"rows": rows, "total": rows[0].get("total_registros", 0) if rows else 0, "limit": meta["limit"], "offset": meta["offset"], "score_regra": score_rule_documentation()}, False


def get_qualidade(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        params: List[Any] = []
        where = _where_sql(filters, params)
        cpf = bq._first_existing_col("cpf")
        tel = bq._first_existing_col("celular", "telefone")
        cpf_dup_cte = f"SELECT SUM(qtd - 1) FROM (SELECT TRIM(CAST(v.{cpf} AS STRING)) k, COUNT(*) qtd FROM base v WHERE TRIM(CAST(v.{cpf} AS STRING)) != '' GROUP BY k HAVING qtd > 1)" if cpf else "SELECT 0"
        tel_dup_cte = f"SELECT SUM(qtd - 1) FROM (SELECT REGEXP_REPLACE(CAST(v.{tel} AS STRING), r'\\D', '') k, COUNT(*) qtd FROM base v WHERE REGEXP_REPLACE(CAST(v.{tel} AS STRING), r'\\D', '') != '' GROUP BY k HAVING qtd > 1)" if tel else "SELECT 0"
        cpf_expr = f"REGEXP_REPLACE(CAST(v.{cpf} AS STRING), r'\\D', '')" if cpf else "''"
        tel_expr = f"REGEXP_REPLACE(CAST(v.{tel} AS STRING), r'\\D', '')" if tel else "''"
        sql = f"""
        WITH base AS (SELECT v.* FROM {bq._tbl(bq.BQ_VIEW_LEADS)} v WHERE {where})
        SELECT
          COUNTIF({_text_col('celular','telefone', default='')} = '') AS leads_sem_telefone,
          COUNTIF({_text_col('email', default='')} = '') AS leads_sem_email,
          COUNTIF({_text_col('cpf', default='')} = '') AS leads_sem_cpf,
          COUNTIF({_text_col('origem', default='')} = '') AS leads_sem_origem,
          COUNTIF({_text_col('curso', default='')} = '') AS leads_sem_curso,
          COUNTIF({_text_col('consultor_comercial', default='')} = '') AS leads_sem_consultor,
          COUNTIF({cpf_expr} != '' AND LENGTH({cpf_expr}) != 11) AS cpf_invalido_ou_incompleto,
          COUNTIF({tel_expr} != '' AND LENGTH({tel_expr}) < 10) AS telefone_invalido,
          COALESCE(({cpf_dup_cte}), 0) AS duplicados_por_cpf_excedentes,
          COALESCE(({tel_dup_cte}), 0) AS duplicados_por_telefone_excedentes,
          0 AS registros_rejeitados_upload,
          COUNTIF({_timestamp_col('dt_upload', 'data_atualizacao')} IS NULL) AS registros_sem_dt_upload,
          0 AS datas_nao_interpretadas
        FROM base v
        """
        return _single(sql, params, "gestao_qualidade")
    return _with_cache("qualidade", filters, {}, meta.get("force_refresh", False), load)


def _mask_cpf(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return f"***.***.***-{digits[-2:]}" if len(digits) >= 2 else "***"


def _mask_last4(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return f"***{digits[-4:]}" if digits else ""


def mask_email(value: Any) -> str:
    text = str(value or "").strip()
    if "@" not in text:
        return ""
    user, domain = text.split("@", 1)
    if not user:
        return f"***@{domain}"
    return f"{user[:1]}***@{domain}"


def mask_rejection_row(row: Dict[str, Any]) -> Dict[str, Any]:
    masked = dict(row)
    for key in list(masked):
        lk = key.lower()
        if lk == "payload":
            masked.pop(key, None)
        elif "cpf" in lk:
            masked[key] = _mask_cpf(masked[key])
        elif "celular" in lk or "telefone" in lk:
            masked[key] = _mask_last4(masked[key])
        elif "email" in lk:
            masked[key] = mask_email(masked[key])
    return masked


def get_importacoes(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    def load():
        limit = min(meta.get("limit", 20), 100)
        out = {"historico": [], "rejeicoes": [], "tabelas_disponiveis": {"logs_importacoes": False, "logs_rejeicoes_import": False}}
        try:
            params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
            sql = f"""
            SELECT nome_arquivo, dt_upload, usuario, total_recebido, total_valido, total_rejeitado,
                   total_inserido, total_atualizado, total_ignorado_antigo, total_sem_celular,
                   status, duracao_segundos, mensagem_erro_resumida, job_id_bigquery
            FROM {bq._tbl('logs_importacoes')}
            ORDER BY dt_upload DESC
            LIMIT @limit
            """
            out["historico"] = _run(sql, params, "gestao_importacoes_historico")
            out["tabelas_disponiveis"]["logs_importacoes"] = True
        except NotFound:
            logger.info("logs_importacoes não encontrada; use a migração SQL entregue.")
        try:
            params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
            sql = f"""
            SELECT dt_rejeicao, nome_arquivo, linha, motivo, cpf, celular, email
            FROM {bq._tbl('logs_rejeicoes_import')}
            ORDER BY dt_rejeicao DESC
            LIMIT @limit
            """
            out["rejeicoes"] = [mask_rejection_row(r) for r in _run(sql, params, "gestao_importacoes_rejeicoes")]
            out["tabelas_disponiveis"]["logs_rejeicoes_import"] = True
        except NotFound:
            logger.info("logs_rejeicoes_import não encontrada.")
        return out
    return _with_cache("importacoes", {}, {"limit": meta.get("limit", 20)}, meta.get("force_refresh", False), load)


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
        {"componente": "Matrícula", "regra": "Leads matriculados recebem score 5 e prioridade baixa."},
        {"componente": "Sem status", "regra": "+35 pontos quando status está vazio."},
        {"componente": "Sem ação", "regra": "+25 pontos quando não há última ação; dias sem ação também pesam."},
        {"componente": "Idade do lead", "regra": "+0,35 ponto por dia desde a inscrição, limitado a 20."},
        {"componente": "Dias sem ação", "regra": "+0,60 ponto por dia sem ação, limitado a 30."},
        {"componente": "Acionamentos", "regra": "+10 sem acionamento, +5 com 1 a 2 acionamentos, -5 com mais acionamentos."},
        {"componente": "Origem/campanha", "regra": "Pequeno acréscimo quando origem ou campanha não informadas."},
        {"componente": "Carga recente", "regra": "+8 quando dt_upload/data_atualizacao ocorreu nos últimos 3 dias."},
    ]


def is_matriculado_row(row: Mapping[str, Any]) -> bool:
    flag = row.get("flag_matriculado")
    if isinstance(flag, bool) and flag:
        return True
    text_flag = str(row.get("matriculado") or "").strip().upper()
    status = str(row.get("status") or "").strip().upper()
    return text_flag in {"SIM", "S", "TRUE", "1", "MATRICULADO", "MAT"} or status in {"MAT", "MATRICULADO"}


def is_status_empty(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def should_accept_upload_version(staging_dt_upload: datetime, fact_data_atualizacao: Optional[datetime]) -> bool:
    if fact_data_atualizacao is None:
        return True
    return staging_dt_upload >= fact_data_atualizacao
