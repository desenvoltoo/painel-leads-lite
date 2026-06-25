from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from google.cloud import bigquery

from services import bigquery as bq
try:
    from services.gestao import invalidate_gestao_cache
except Exception:  # pragma: no cover
    def invalidate_gestao_cache() -> None:
        return None

PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID") or os.getenv("GCP_PROJECT_ID") or bq.GCP_PROJECT_ID or "painel-universidade"
DATASET = os.getenv("BIGQUERY_DATASET") or os.getenv("BQ_DATASET") or bq.BQ_DATASET or "modelo_estrela"
VIEW_LEADS_PRIORIZADOS = "vw_leads_priorizados"
FINAL_STATUS = ("CONCLUIDO", "MAT", "CANCELADO")
ACTIVE_STATUS = ("PENDENTE", "EM_ATENDIMENTO", "AC", "EC", "NT", "IF", "NI", "COU")
ACTIVE_STATUS_EXCLUDED = FINAL_STATUS
TIPOS_DISPARO = {"ROBO", "URA", "MANUAL"}
STATUS_ATENDIMENTO_MAP = {
    "MAT": {"retorno": True, "positivo": True, "negativo": False, "matriculado": True},
    "AC": {"retorno": True, "positivo": True, "negativo": False, "matriculado": False},
    "EC": {"retorno": True, "positivo": True, "negativo": False, "matriculado": False},
    "IF": {"retorno": True, "positivo": True, "negativo": False, "matriculado": False},
    "NI": {"retorno": True, "positivo": False, "negativo": True, "matriculado": False},
    "NT": {"retorno": False, "positivo": False, "negativo": True, "matriculado": False},
    "COU": {"retorno": True, "positivo": False, "negativo": False, "matriculado": False},
    "CONCLUIDO": None,
    "CANCELADO": None,
}


def _ref(table: str) -> str:
    return f"`{PROJECT_ID}.{DATASET}.{table}`"


def _run(sql: str, params: Optional[List[Any]] = None, operation: str = "gestao_operacional") -> List[Dict[str, Any]]:
    return bq._rows_to_json_safe(bq._run_gestao_query(sql, params=params or [], operation_name=operation))


def _single(sql: str, params: Optional[List[Any]] = None, operation: str = "gestao_operacional") -> Dict[str, Any]:
    rows = _run(sql, params, operation)
    return rows[0] if rows else {}


def _int(value: Any, default: int, min_value: int = 0, max_value: int = 5000) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(min_value, min(parsed, max_value))


def _filters_meta(source: Mapping[str, Any]) -> Tuple[Dict[str, str], Dict[str, int]]:
    filters = {k: str(source.get(k) or "").strip() for k in ["campanha", "curso", "polo", "origem", "tipo_disparo", "nivel_prioridade", "status_lote", "consultor_disparo", "status_atendimento", "lote_id"] if str(source.get(k) or "").strip()}
    meta = {"limit": _int(source.get("limit"), 100, 1, 5000), "offset": _int(source.get("offset"), 0, 0, 1_000_000)}
    return filters, meta


def parse_operational_request(source: Mapping[str, Any]) -> Tuple[Dict[str, str], Dict[str, int]]:
    return _filters_meta(source)


def _add_eq(where: List[str], params: List[Any], alias: str, filters: Mapping[str, Any], fields: List[str]) -> None:
    for field in fields:
        value = str(filters.get(field) or "").strip()
        if value:
            where.append(f"UPPER(TRIM(CAST({alias}.{field} AS STRING))) = UPPER(TRIM(@{field}))")
            params.append(bigquery.ScalarQueryParameter(field, "STRING", value))


def create_operational_tables() -> Dict[str, Any]:
    sql = Path(__file__).resolve().parents[1].joinpath("sql/migrations/20260625_operacao_lotes_disparo.sql").read_text(encoding="utf-8")
    bq._run_gestao_query(sql, operation_name="create_operational_tables")
    return {"created": ["op_lotes_disparo", "op_lote_leads", "op_lead_eventos", "op_bigquery_sync", "op_regras_distribuicao", "op_config_operacional"]}


def get_dashboard() -> Tuple[Dict[str, Any], bool]:
    data = _single(f"""
    SELECT
      (SELECT COUNT(*) FROM {_ref(VIEW_LEADS_PRIORIZADOS)} l LEFT JOIN {_ref('op_lote_leads')} op ON l.sk_pessoa=op.sk_pessoa AND op.status_atendimento IN ('PENDENTE','EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU') WHERE op.sk_pessoa IS NULL AND COALESCE(l.flag_matriculado,FALSE)=FALSE) AS leads_disponiveis,
      (SELECT COUNT(*) FROM {_ref('op_lote_leads')} WHERE status_atendimento IN ('PENDENTE','EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU')) AS leads_em_lote,
      COUNTIF(status_lote='ABERTO') AS lotes_abertos,
      COUNTIF(status_lote='EM_ANDAMENTO') AS lotes_em_andamento,
      COUNTIF(status_lote='CONCLUIDO') AS lotes_concluidos,
      COALESCE(SUM(total_retorno),0) AS retornos,
      COALESCE(SUM(total_positivo),0) AS positivos,
      COALESCE(SUM(total_negativo),0) AS negativos,
      COALESCE(SUM(total_matriculas),0) AS matriculas,
      COALESCE(AVG(taxa_retorno),0) AS taxa_retorno,
      COALESCE(AVG(taxa_matricula),0) AS taxa_matricula
    FROM {_ref('op_lotes_disparo')}
    """, operation="operacional_dashboard")
    return data, False


def get_leads_disponiveis(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    where = ["op.sk_pessoa IS NULL", "COALESCE(l.flag_matriculado, FALSE) = FALSE"]
    params: List[Any] = []
    _add_eq(where, params, "l", filters, ["campanha", "curso", "polo", "origem", "tipo_disparo", "nivel_prioridade"])
    params += [bigquery.ScalarQueryParameter("limit", "INT64", _int(meta.get("limit"), 100, 1, 5000)), bigquery.ScalarQueryParameter("offset", "INT64", _int(meta.get("offset"), 0, 0, 1_000_000))]
    sql = f"""
    SELECT l.*
    FROM {_ref(VIEW_LEADS_PRIORIZADOS)} l
    LEFT JOIN {_ref('op_lote_leads')} op
      ON l.sk_pessoa = op.sk_pessoa
     AND op.status_atendimento IN ('PENDENTE','EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU')
    WHERE {' AND '.join(where)}
    ORDER BY l.score_prioridade DESC, l.nunca_disparado DESC, l.dias_sem_acao DESC, l.data_inscricao ASC
    LIMIT @limit OFFSET @offset
    """
    rows = _run(sql, params, "operacional_leads_disponiveis")
    return {"items": rows, "count": len(rows), "pagination": {"limit": meta.get("limit"), "offset": meta.get("offset")}}, False


def criar_lote(payload: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    quantidade = _int(payload.get("quantidade"), 0, 1, 5000)
    tipo = str(payload.get("tipo_disparo") or "").strip().upper()
    if tipo not in TIPOS_DISPARO:
        raise ValueError("tipo_disparo deve ser ROBO, URA ou MANUAL.")
    filtros = dict(payload.get("filtros") or {})
    for k in ["campanha", "curso", "polo", "origem", "nivel_prioridade"]:
        if payload.get(k) and k not in filtros:
            filtros[k] = payload.get(k)
    filtros["tipo_disparo"] = tipo if not filtros.get("tipo_disparo") else filtros["tipo_disparo"]
    leads_data, _ = get_leads_disponiveis(filtros, {"limit": quantidade, "offset": 0})
    leads = leads_data["items"]
    if not leads:
        raise ValueError("Nenhum lead disponível para os filtros informados.")
    lote_id = str(uuid.uuid4())
    base_params = [
        bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id),
        bigquery.ScalarQueryParameter("nome_lote", "STRING", str(payload.get("nome_lote") or "").strip()),
        bigquery.ScalarQueryParameter("campanha", "STRING", str(payload.get("campanha") or filtros.get("campanha") or "").strip()),
        bigquery.ScalarQueryParameter("tipo_disparo", "STRING", tipo),
        bigquery.ScalarQueryParameter("consultor_disparo", "STRING", str(payload.get("consultor_disparo") or "").strip()),
        bigquery.ScalarQueryParameter("quantidade_leads", "INT64", len(leads)),
        bigquery.ScalarQueryParameter("criado_por", "STRING", str(payload.get("criado_por") or "").strip()),
    ]
    _run(f"""INSERT INTO {_ref('op_lotes_disparo')}
    (lote_id,nome_lote,campanha,tipo_disparo,consultor_disparo,quantidade_leads,status_lote,total_retorno,total_positivo,total_negativo,total_matriculas,taxa_retorno,taxa_matricula,criado_por,created_at,updated_at)
    VALUES (@lote_id,@nome_lote,@campanha,@tipo_disparo,@consultor_disparo,@quantidade_leads,'ABERTO',0,0,0,0,0,0,@criado_por,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())""", base_params, "operacional_criar_lote")
    for lead in leads:
        params = [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id)] + [bigquery.ScalarQueryParameter(k, "STRING" if k not in {"sk_pessoa","score_prioridade"} else "INT64", lead.get(k)) for k in ["sk_pessoa","cpf","nome","celular","email","curso","modalidade","turno","polo","origem","tipo_negocio","campanha","canal","acao_comercial","tipo_disparo","score_prioridade","nivel_prioridade","etapa_operacional"]]
        params.append(bigquery.ScalarQueryParameter("consultor_disparo", "STRING", str(payload.get("consultor_disparo") or lead.get("consultor_disparo") or "")))
        _run(f"""INSERT INTO {_ref('op_lote_leads')}
        (lote_id,sk_pessoa,cpf,nome,celular,email,curso,modalidade,turno,polo,origem,tipo_negocio,campanha,canal,acao_comercial,tipo_disparo,consultor_disparo,status_atendimento,retorno,positivo,negativo,matriculado,observacao,data_inscricao,data_matricula,data_disparo,score_prioridade,nivel_prioridade,etapa_operacional,created_at,updated_at)
        SELECT @lote_id,@sk_pessoa,@cpf,@nome,@celular,@email,@curso,@modalidade,@turno,@polo,@origem,@tipo_negocio,@campanha,@canal,@acao_comercial,@tipo_disparo,@consultor_disparo,'PENDENTE',FALSE,FALSE,FALSE,FALSE,NULL,SAFE_CAST(NULL AS DATE),SAFE_CAST(NULL AS DATE),CURRENT_TIMESTAMP(),@score_prioridade,@nivel_prioridade,@etapa_operacional,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP()
        WHERE NOT EXISTS (SELECT 1 FROM {_ref('op_lote_leads')} WHERE sk_pessoa=@sk_pessoa AND status_atendimento IN ('PENDENTE','EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU'))""", params, "operacional_criar_lote_lead")
        _evento(lote_id, lead.get("sk_pessoa"), lead.get("cpf"), "LOTE_CRIADO", None, "PENDENTE", "Lead incluído no lote", payload.get("criado_por"))
    invalidate_gestao_cache()
    aviso = None if len(leads) >= quantidade else f"Lote criado com {len(leads)} leads disponíveis de {quantidade} solicitados."
    return {"lote_id": lote_id, "quantidade_leads": len(leads), "quantidade_liberada": len(leads), "status_lote": "ABERTO", "aviso": aviso}, False


def get_lotes(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    where, params = ["1=1"], []
    _add_eq(where, params, "l", filters, ["status_lote", "consultor_disparo", "tipo_disparo", "campanha"])
    params += [bigquery.ScalarQueryParameter("limit", "INT64", _int(meta.get("limit"), 100)), bigquery.ScalarQueryParameter("offset", "INT64", _int(meta.get("offset"), 0))]
    rows = _run(f"SELECT * FROM {_ref('op_lotes_disparo')} l WHERE {' AND '.join(where)} ORDER BY created_at DESC LIMIT @limit OFFSET @offset", params, "operacional_lotes")
    return {"items": rows, "count": len(rows)}, False


def get_lote_detalhe(lote_id: str) -> Tuple[Dict[str, Any], bool]:
    p = [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id)]
    lote = _single(f"SELECT * FROM {_ref('op_lotes_disparo')} WHERE lote_id=@lote_id", p, "operacional_lote")
    leads = _run(f"SELECT * FROM {_ref('op_lote_leads')} WHERE lote_id=@lote_id ORDER BY updated_at DESC LIMIT 500", p, "operacional_lote_leads")
    eventos = _run(f"SELECT * FROM {_ref('op_lead_eventos')} WHERE lote_id=@lote_id ORDER BY created_at DESC LIMIT 50", p, "operacional_lote_eventos")
    metricas = _metrics(lote_id)
    return {"lote": lote, "leads": leads, "metricas": metricas, "eventos": eventos}, False


def _evento(lote_id, sk_pessoa, cpf, tipo, anterior, novo, desc, usuario):
    params = [bigquery.ScalarQueryParameter("evento_id", "STRING", str(uuid.uuid4())), bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id), bigquery.ScalarQueryParameter("sk_pessoa", "INT64", sk_pessoa), bigquery.ScalarQueryParameter("cpf", "STRING", cpf), bigquery.ScalarQueryParameter("tipo_evento", "STRING", tipo), bigquery.ScalarQueryParameter("status_anterior", "STRING", anterior), bigquery.ScalarQueryParameter("status_novo", "STRING", novo), bigquery.ScalarQueryParameter("descricao", "STRING", desc), bigquery.ScalarQueryParameter("usuario", "STRING", str(usuario or ""))]
    _run(f"INSERT INTO {_ref('op_lead_eventos')} (evento_id,lote_id,sk_pessoa,cpf,tipo_evento,status_anterior,status_novo,descricao,usuario,created_at) VALUES (@evento_id,@lote_id,@sk_pessoa,@cpf,@tipo_evento,@status_anterior,@status_novo,@descricao,@usuario,CURRENT_TIMESTAMP())", params, "operacional_evento")


def start_lote(lote_id: str) -> Tuple[Dict[str, Any], bool]:
    p = [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id)]
    _run(f"UPDATE {_ref('op_lotes_disparo')} SET status_lote='EM_ANDAMENTO', started_at=COALESCE(started_at,CURRENT_TIMESTAMP()), updated_at=CURRENT_TIMESTAMP() WHERE lote_id=@lote_id AND status_lote!='CONCLUIDO'", p, "operacional_start_lote")
    _evento(lote_id, None, None, "LOTE_INICIADO", None, "EM_ANDAMENTO", "Lote iniciado", None)
    invalidate_gestao_cache()
    return {"lote_id": lote_id, "status_lote": "EM_ANDAMENTO"}, False


def _metrics(lote_id: str) -> Dict[str, Any]:
    return _single(f"SELECT COUNT(*) total, COUNTIF(retorno) total_retorno, COUNTIF(positivo) total_positivo, COUNTIF(negativo) total_negativo, COUNTIF(matriculado) total_matriculas, SAFE_DIVIDE(COUNTIF(retorno), COUNT(*))*100 taxa_retorno, SAFE_DIVIDE(COUNTIF(matriculado), COUNT(*))*100 taxa_matricula FROM {_ref('op_lote_leads')} WHERE lote_id=@lote_id", [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id)], "operacional_metricas")


def finish_lote(lote_id: str, payload: Optional[Mapping[str, Any]] = None) -> Tuple[Dict[str, Any], bool]:
    payload = payload or {}
    p = [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id)]
    pendentes = _single(f"SELECT COUNT(*) qtd FROM {_ref('op_lote_leads')} WHERE lote_id=@lote_id AND status_atendimento='PENDENTE'", p, "operacional_finish_pendentes").get("qtd") or 0
    if pendentes and not bool(payload.get("confirmacao_forcada")):
        raise ValueError("Ainda existem leads PENDENTE. Envie confirmacao_forcada=true para finalizar.")
    if pendentes:
        final_status = str(payload.get("status_pendentes") or "PENDENTE_FINALIZADO").strip().upper()
        if final_status not in {"CONCLUIDO", "PENDENTE_FINALIZADO"}:
            final_status = "PENDENTE_FINALIZADO"
        _run(f"UPDATE {_ref('op_lote_leads')} SET status_atendimento=@status, updated_at=CURRENT_TIMESTAMP() WHERE lote_id=@lote_id AND status_atendimento='PENDENTE'", p + [bigquery.ScalarQueryParameter("status", "STRING", final_status)], "operacional_finish_pendentes_update")
    existing = _single(f"SELECT status_lote FROM {_ref('op_lotes_disparo')} WHERE lote_id=@lote_id", p, "operacional_finish_check")
    if existing.get("status_lote") == "CONCLUIDO":
        return {"lote_id": lote_id, "status_lote": "CONCLUIDO", "aviso": "Lote já estava concluído."}, False
    m = recalcular_metricas_lote(lote_id)
    params = p + [bigquery.ScalarQueryParameter("total", "INT64", m.get("total") or 0), bigquery.ScalarQueryParameter("ret", "INT64", m.get("total_retorno") or 0), bigquery.ScalarQueryParameter("pos", "INT64", m.get("total_positivo") or 0), bigquery.ScalarQueryParameter("neg", "INT64", m.get("total_negativo") or 0), bigquery.ScalarQueryParameter("mat", "INT64", m.get("total_matriculas") or 0), bigquery.ScalarQueryParameter("txr", "FLOAT64", m.get("taxa_retorno") or 0), bigquery.ScalarQueryParameter("txm", "FLOAT64", m.get("taxa_matricula") or 0)]
    _run(f"UPDATE {_ref('op_lotes_disparo')} SET status_lote='CONCLUIDO', quantidade_leads=@total,total_retorno=@ret,total_positivo=@pos,total_negativo=@neg,total_matriculas=@mat,taxa_retorno=@txr,taxa_matricula=@txm,finished_at=CURRENT_TIMESTAMP(),updated_at=CURRENT_TIMESTAMP() WHERE lote_id=@lote_id AND status_lote!='CONCLUIDO'", params, "operacional_finish_lote")
    _run(f"INSERT INTO {_ref('op_bigquery_sync')} (sync_id,lote_id,status_sync,tentativas,linhas_processadas,erro,created_at,synced_at) VALUES (@sync_id,@lote_id,'CONCLUIDO',1,@total,NULL,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())", [bigquery.ScalarQueryParameter("sync_id", "STRING", str(uuid.uuid4())), bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id), bigquery.ScalarQueryParameter("total", "INT64", m.get("total") or 0)], "operacional_sync")
    _evento(lote_id, None, None, "SINCRONIZACAO_BQ", None, "CONCLUIDO", "Sincronização operacional registrada", None)
    _evento(lote_id, None, None, "LOTE_FINALIZADO", None, "CONCLUIDO", "Lote finalizado", None)
    invalidate_gestao_cache()
    return {"lote_id": lote_id, "status_lote": "CONCLUIDO", "metricas": m}, False


def get_meus_leads(consultor_disparo: str, filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    f = dict(filters); f["consultor_disparo"] = consultor_disparo
    where, params = ["1=1"], []
    _add_eq(where, params, "l", f, ["consultor_disparo", "status_atendimento", "campanha", "tipo_disparo", "lote_id"])
    params += [bigquery.ScalarQueryParameter("limit", "INT64", _int(meta.get("limit"), 100)), bigquery.ScalarQueryParameter("offset", "INT64", _int(meta.get("offset"), 0))]
    rows = _run(f"SELECT * FROM {_ref('op_lote_leads')} l WHERE {' AND '.join(where)} ORDER BY updated_at DESC LIMIT @limit OFFSET @offset", params, "operacional_meus_leads")
    return {"items": rows, "count": len(rows)}, False


def update_lead_status(sk_pessoa: int, payload: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    lote_id = str(payload.get("lote_id") or "").strip(); status = str(payload.get("status_atendimento") or "").strip().upper()
    if not lote_id or status not in STATUS_ATENDIMENTO_MAP:
        raise ValueError("lote_id e status_atendimento válido são obrigatórios.")
    old = _single(f"SELECT status_atendimento,cpf,retorno,positivo,negativo,matriculado FROM {_ref('op_lote_leads')} WHERE lote_id=@lote_id AND sk_pessoa=@sk_pessoa", [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id), bigquery.ScalarQueryParameter("sk_pessoa", "INT64", sk_pessoa)], "operacional_status_old")
    flags = STATUS_ATENDIMENTO_MAP[status] or {k: bool(old.get(k)) for k in ["retorno", "positivo", "negativo", "matriculado"]}
    params = [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id), bigquery.ScalarQueryParameter("sk_pessoa", "INT64", sk_pessoa), bigquery.ScalarQueryParameter("status", "STRING", status), bigquery.ScalarQueryParameter("observacao", "STRING", str(payload.get("observacao") or "")), *(bigquery.ScalarQueryParameter(k, "BOOL", v) for k, v in flags.items())]
    _run(f"UPDATE {_ref('op_lote_leads')} SET status_atendimento=@status, observacao=@observacao, retorno=@retorno, positivo=@positivo, negativo=@negativo, matriculado=@matriculado, updated_at=CURRENT_TIMESTAMP() WHERE lote_id=@lote_id AND sk_pessoa=@sk_pessoa", params, "operacional_update_status")
    _evento(lote_id, sk_pessoa, old.get("cpf"), "LEAD_STATUS_ALTERADO", old.get("status_atendimento"), status, payload.get("observacao") or "Status atualizado", payload.get("usuario"))
    recalcular_metricas_lote(lote_id)
    invalidate_gestao_cache()
    return {"lote_id": lote_id, "sk_pessoa": sk_pessoa, "status_atendimento": status, **flags}, False


def liberar_proximos_leads(payload: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """Cria automaticamente um lote com os próximos leads priorizados."""
    enriched = dict(payload or {})
    enriched.setdefault("nome_lote", f"Liberação {str(enriched.get('tipo_disparo') or '').upper()}")
    return criar_lote(enriched)


def get_esteira_operacional() -> Tuple[Dict[str, Any], bool]:
    data = _single(f"""
    SELECT
      (SELECT COUNT(*) FROM {_ref(VIEW_LEADS_PRIORIZADOS)} l
       LEFT JOIN {_ref('op_lote_leads')} op ON l.sk_pessoa=op.sk_pessoa AND op.status_atendimento IN ('PENDENTE','EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU')
       WHERE op.sk_pessoa IS NULL AND COALESCE(l.flag_matriculado,FALSE)=FALSE) AS leads_disponiveis,
      (SELECT COUNT(*) FROM {_ref('op_lote_leads')} WHERE status_atendimento='PENDENTE') AS leads_pendentes_em_lote,
      (SELECT COUNT(*) FROM {_ref('op_lote_leads')} WHERE status_atendimento IN ('EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU')) AS leads_em_atendimento,
      (SELECT COUNT(*) FROM {_ref('op_lote_leads')} WHERE status_atendimento IN ('CONCLUIDO','MAT','CANCELADO')) AS leads_finalizados,
      (SELECT COUNT(*) FROM {_ref('op_lote_leads')} WHERE matriculado OR status_atendimento='MAT') AS leads_matriculados,
      COUNTIF(status_lote='ABERTO') AS lotes_abertos,
      COUNTIF(status_lote='EM_ANDAMENTO') AS lotes_em_andamento,
      COUNTIF(status_lote='CONCLUIDO') AS lotes_concluidos,
      COALESCE(AVG(taxa_retorno),0) AS taxa_retorno,
      COALESCE(AVG(taxa_matricula),0) AS taxa_matricula
    FROM {_ref('op_lotes_disparo')}
    """, operation="operacional_esteira")
    return data, False


def get_fila_por_prioridade(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    return get_leads_disponiveis(filters, meta)


def recalcular_metricas_lote(lote_id: str) -> Dict[str, Any]:
    m = _metrics(lote_id)
    params = [
        bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id),
        bigquery.ScalarQueryParameter("total", "INT64", m.get("total") or 0),
        bigquery.ScalarQueryParameter("ret", "INT64", m.get("total_retorno") or 0),
        bigquery.ScalarQueryParameter("pos", "INT64", m.get("total_positivo") or 0),
        bigquery.ScalarQueryParameter("neg", "INT64", m.get("total_negativo") or 0),
        bigquery.ScalarQueryParameter("mat", "INT64", m.get("total_matriculas") or 0),
        bigquery.ScalarQueryParameter("txr", "FLOAT64", m.get("taxa_retorno") or 0),
        bigquery.ScalarQueryParameter("txm", "FLOAT64", m.get("taxa_matricula") or 0),
    ]
    _run(f"""UPDATE {_ref('op_lotes_disparo')}
    SET quantidade_leads=@total,total_retorno=@ret,total_positivo=@pos,total_negativo=@neg,total_matriculas=@mat,taxa_retorno=@txr,taxa_matricula=@txm,updated_at=CURRENT_TIMESTAMP()
    WHERE lote_id=@lote_id""", params, "operacional_recalcular_lote")
    return m


def criar_regra_distribuicao(payload: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    regra_id = str(uuid.uuid4())
    tipo = str(payload.get("tipo_disparo") or "").strip().upper()
    if tipo not in TIPOS_DISPARO:
        raise ValueError("tipo_disparo deve ser ROBO, URA ou MANUAL.")
    params = [bigquery.ScalarQueryParameter("regra_id", "STRING", regra_id)]
    for k in ["nome_regra","consultor_disparo","campanha","curso","polo","origem","nivel_prioridade"]:
        params.append(bigquery.ScalarQueryParameter(k, "STRING", str(payload.get(k) or "").strip()))
    params += [
        bigquery.ScalarQueryParameter("tipo_disparo", "STRING", tipo),
        bigquery.ScalarQueryParameter("quantidade_por_lote", "INT64", _int(payload.get("quantidade_por_lote"), 100, 1, 5000)),
        bigquery.ScalarQueryParameter("limite_lotes_ativos", "INT64", _int(payload.get("limite_lotes_ativos"), 1, 1, 100)),
        bigquery.ScalarQueryParameter("ativo", "BOOL", bool(payload.get("ativo", True))),
    ]
    _run(f"""INSERT INTO {_ref('op_regras_distribuicao')}
    (regra_id,nome_regra,tipo_disparo,consultor_disparo,campanha,curso,polo,origem,nivel_prioridade,quantidade_por_lote,limite_lotes_ativos,ativo,created_at,updated_at)
    VALUES (@regra_id,@nome_regra,@tipo_disparo,@consultor_disparo,@campanha,@curso,@polo,@origem,@nivel_prioridade,@quantidade_por_lote,@limite_lotes_ativos,@ativo,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())""", params, "operacional_regra_criar")
    _evento(None, None, None, "REGRA_CRIADA", None, "ATIVA" if payload.get("ativo", True) else "INATIVA", f"Regra {regra_id} criada", payload.get("usuario"))
    invalidate_gestao_cache()
    return {"regra_id": regra_id, "tipo_disparo": tipo, "ativo": bool(payload.get("ativo", True))}, False


def listar_regras_distribuicao() -> Tuple[Dict[str, Any], bool]:
    rows = _run(f"SELECT * FROM {_ref('op_regras_distribuicao')} ORDER BY updated_at DESC LIMIT 500", operation="operacional_regras_listar")
    return {"items": rows, "count": len(rows)}, False


def ativar_desativar_regra(regra_id: str, payload: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    ativo = bool(payload.get("ativo"))
    _run(f"UPDATE {_ref('op_regras_distribuicao')} SET ativo=@ativo, updated_at=CURRENT_TIMESTAMP() WHERE regra_id=@regra_id", [bigquery.ScalarQueryParameter("regra_id", "STRING", regra_id), bigquery.ScalarQueryParameter("ativo", "BOOL", ativo)], "operacional_regra_toggle")
    invalidate_gestao_cache()
    return {"regra_id": regra_id, "ativo": ativo}, False


def executar_regras_distribuicao() -> Tuple[Dict[str, Any], bool]:
    regras = _run(f"SELECT * FROM {_ref('op_regras_distribuicao')} WHERE ativo=TRUE ORDER BY updated_at DESC", operation="operacional_regras_ativas")
    resultados = []
    for r in regras:
        params = [bigquery.ScalarQueryParameter("tipo_disparo", "STRING", r.get("tipo_disparo") or ""), bigquery.ScalarQueryParameter("consultor_disparo", "STRING", r.get("consultor_disparo") or ""), bigquery.ScalarQueryParameter("campanha", "STRING", r.get("campanha") or "")]
        ativos = _single(f"""SELECT COUNT(*) qtd FROM {_ref('op_lotes_disparo')}
        WHERE status_lote IN ('ABERTO','EM_ANDAMENTO') AND UPPER(COALESCE(tipo_disparo,''))=UPPER(@tipo_disparo)
          AND UPPER(COALESCE(consultor_disparo,''))=UPPER(@consultor_disparo) AND UPPER(COALESCE(campanha,''))=UPPER(@campanha)""", params, "operacional_regra_lotes_ativos").get("qtd") or 0
        if ativos >= (r.get("limite_lotes_ativos") or 1):
            resultados.append({"regra_id": r.get("regra_id"), "criado": False, "motivo": "limite_lotes_ativos atingido"})
            continue
        payload = {"modo":"AUTOMATICO","tipo_disparo":r.get("tipo_disparo"),"consultor_disparo":r.get("consultor_disparo"),"campanha":r.get("campanha"),"quantidade":r.get("quantidade_por_lote") or 100,"criado_por":"REGRA_AUTOMATICA","filtros":{"curso":r.get("curso"),"polo":r.get("polo"),"origem":r.get("origem"),"nivel_prioridade":r.get("nivel_prioridade")}}
        try:
            lote, _ = liberar_proximos_leads(payload)
            resultados.append({"regra_id": r.get("regra_id"), "criado": True, **lote})
        except ValueError as exc:
            resultados.append({"regra_id": r.get("regra_id"), "criado": False, "motivo": str(exc)})
        _evento(None, None, None, "REGRA_EXECUTADA", None, None, f"Regra {r.get('regra_id')} executada", "REGRA_AUTOMATICA")
    invalidate_gestao_cache()
    return {"items": resultados, "count": len(resultados)}, False
