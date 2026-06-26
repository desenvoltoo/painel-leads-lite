from __future__ import annotations

import base64
import csv
import io
import json
import os
import uuid
from datetime import datetime
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
VIEW_LEADS_PRIORIZADOS = "vw_op_leads_disponiveis"
FALLBACK_VIEW_LEADS_PRIORIZADOS = "vw_leads_priorizados"
FINAL_STATUS = ("CONCLUIDO", "MAT", "CANCELADO")
ACTIVE_STATUS = ("PENDENTE", "EM_ATENDIMENTO", "AC", "EC", "NT", "IF", "NI", "COU")
ACTIVE_STATUS_EXCLUDED = FINAL_STATUS
OP_TABLES = ("op_lotes_disparo", "op_lote_leads", "op_lead_eventos", "op_bigquery_sync", "op_regras_distribuicao", "op_config_operacional")

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


def _active_status_sql() -> str:
    return "'" + "','".join(ACTIVE_STATUS) + "'"


def _final_status_sql() -> str:
    return "'" + "','".join(FINAL_STATUS) + "'"


def _available_leads_predicate(alias: str = "l", redisparo: bool = False) -> str:
    """Lead livre: não matriculado e sem lote ativo; tipo_disparo nunca restringe a fila."""
    disparo = (
        f"{alias}.data_disparo IS NOT NULL AND DATE(SAFE_CAST({alias}.data_disparo AS TIMESTAMP)) < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
        if redisparo
        else f"{alias}.data_disparo IS NULL"
    )
    return f"COALESCE({alias}.flag_matriculado, FALSE) = FALSE AND ({disparo})"


def _missing_operational_tables() -> List[str]:
    params = [bigquery.ArrayQueryParameter("tables", "STRING", list(OP_TABLES))]
    rows = _run(f"""
    SELECT table_name
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name IN UNNEST(@tables)
    """, params, "operacional_check_tables")
    existing = {str(r.get("table_name")) for r in rows}
    return [table for table in OP_TABLES if table not in existing]


def _zero_operational_indicators(missing: List[str]) -> Dict[str, Any]:
    return {
        "leads_disponiveis": 0,
        "leads_redisparo": 0,
        "leads_em_lote": 0,
        "leads_pendentes_em_lote": 0,
        "leads_em_atendimento": 0,
        "leads_finalizados": 0,
        "leads_matriculados": 0,
        "lotes_abertos": 0,
        "lotes_em_andamento": 0,
        "lotes_concluidos": 0,
        "retornos": 0,
        "positivos": 0,
        "negativos": 0,
        "matriculas": 0,
        "taxa_retorno": 0,
        "taxa_matricula": 0,
        "warning": "Tabelas operacionais ausentes. Execute /api/gestao/operacional/admin/create-tables para criar a estrutura.",
        "missing_tables": missing,
    }

def _ref(table: str) -> str:
    return f"`{PROJECT_ID}.{DATASET}.{table}`"

def _lead_source_ref() -> str:
    """Prefer the operational availability view and fall back to prioritized leads."""
    try:
        row = _single(
            f"""SELECT COUNT(*) AS qtd
            FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name=@view_name""",
            [bigquery.ScalarQueryParameter("view_name", "STRING", VIEW_LEADS_PRIORIZADOS)],
            "operacional_check_available_view",
        )
        if int(row.get("qtd") or 0) > 0:
            return _ref(VIEW_LEADS_PRIORIZADOS)
    except Exception:
        pass
    return _ref(FALLBACK_VIEW_LEADS_PRIORIZADOS)


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


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _build_lote_nome(usuario: str, mes_leads: str = "") -> str:
    safe_usuario = "_".join(_clean_text(usuario).upper().split()) or "SISTEMA"
    safe_mes = "_".join(_clean_text(mes_leads).upper().split()) or datetime.utcnow().strftime("%Y%m")
    return f"LOTE_{datetime.utcnow().strftime('%Y%m%d')}_LEADS_{safe_mes}_EXPORTADO_POR_{safe_usuario}"


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _extract_lote_row(rows: List[Dict[str, Any]], fallback: Mapping[str, Any]) -> Dict[str, Any]:
    if not rows:
        raise ValueError("A procedure oficial não retornou dados do lote criado.")
    row = dict(rows[0] or {})
    lote_id = _clean_text(row.get("lote_id") or row.get("id_lote"))
    if not lote_id:
        raise ValueError("A procedure oficial não retornou lote_id.")
    usuario = _clean_text(fallback.get("usuario") or fallback.get("criado_por") or fallback.get("consultor_disparo"))
    mes_leads = _clean_text(row.get("mes_leads") or row.get("mes_dos_leads") or fallback.get("mes_leads"))
    nome_lote = _clean_text(row.get("nome_lote")) or _build_lote_nome(usuario, mes_leads)
    nome_arquivo = _clean_text(row.get("nome_arquivo_exportado")) or f"{nome_lote}.csv"
    if not nome_arquivo.lower().endswith(".csv"):
        nome_arquivo = f"{nome_arquivo}.csv"
    return {
        **row,
        "lote_id": lote_id,
        "nome_lote": nome_lote,
        "nome_arquivo_exportado": nome_arquivo,
        "mes_leads": mes_leads,
        "quantidade_solicitada": _to_int(row.get("quantidade_solicitada") or fallback.get("quantidade")),
        "quantidade_liberada": _to_int(row.get("quantidade_liberada") or row.get("quantidade_leads") or row.get("total_leads")),
    }


def _filters_meta(source: Mapping[str, Any]) -> Tuple[Dict[str, str], Dict[str, int]]:
    filters = {k: str(source.get(k) or "").strip() for k in ["campanha", "curso", "polo", "origem", "nivel_prioridade", "status_lote", "consultor_disparo", "status_atendimento", "lote_id"] if str(source.get(k) or "").strip()}
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
    missing = _missing_operational_tables()
    if missing:
        return _zero_operational_indicators(missing), False
    active = _active_status_sql()
    final = _final_status_sql()
    available = _available_leads_predicate("l")
    data = _single(f"""
    SELECT
      (SELECT COUNT(*) FROM {_lead_source_ref()} l
       WHERE {available}
         AND NOT EXISTS (
           SELECT 1 FROM {_ref('op_lote_leads')} op
           WHERE op.sk_pessoa = l.sk_pessoa
             AND op.status_atendimento IN ({active})
         )) AS leads_disponiveis,
      (SELECT COUNT(*) FROM {_lead_source_ref()} l
       WHERE COALESCE(l.flag_matriculado,FALSE)=FALSE
         AND (COALESCE(l.nunca_disparado,FALSE)=FALSE OR TRIM(COALESCE(l.consultor_disparo,'')) != '' OR l.data_disparo IS NOT NULL)
         AND NOT EXISTS (
           SELECT 1 FROM {_ref('op_lote_leads')} op
           WHERE op.sk_pessoa = l.sk_pessoa
             AND op.status_atendimento IN ({active}, {final})
         )) AS leads_redisparo,
      (SELECT COUNT(*) FROM {_ref('op_lote_leads')} WHERE status_atendimento IN ({active})) AS leads_em_lote,
      (SELECT COUNT(*) FROM {_ref('op_lote_leads')} WHERE status_atendimento='PENDENTE') AS leads_pendentes_em_lote,
      (SELECT COUNT(*) FROM {_ref('op_lote_leads')} WHERE status_atendimento IN ('EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU')) AS leads_em_atendimento,
      (SELECT COUNT(*) FROM {_ref('op_lote_leads')} WHERE status_atendimento IN ({final})) AS leads_finalizados,
      (SELECT COUNT(*) FROM {_ref('op_lote_leads')} WHERE matriculado OR status_atendimento='MAT') AS leads_matriculados,
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
    where = ["op.sk_pessoa IS NULL", _available_leads_predicate("l")]
    params: List[Any] = []
    _add_eq(where, params, "l", filters, ["campanha", "curso", "polo", "origem", "nivel_prioridade"])
    params += [bigquery.ScalarQueryParameter("limit", "INT64", _int(meta.get("limit"), 100, 1, 5000)), bigquery.ScalarQueryParameter("offset", "INT64", _int(meta.get("offset"), 0, 0, 1_000_000))]
    select_cols = """
      l.sk_pessoa,l.cpf,l.nome,l.celular,l.email,l.curso,l.modalidade,l.turno,l.polo,l.origem,
      l.tipo_negocio,l.campanha,l.canal,l.acao_comercial,l.consultor_disparo,l.tipo_disparo,
      l.data_inscricao,l.data_matricula,l.data_disparo,l.score_prioridade,l.nivel_prioridade,
      l.etapa_operacional,l.nunca_disparado,l.dias_sem_acao,l.flag_matriculado
    """
    total = _single(f"""SELECT COUNT(1) AS total
    FROM {_lead_source_ref()} l
    LEFT JOIN {_ref('op_lote_leads')} op
      ON l.sk_pessoa = op.sk_pessoa
     AND op.status_atendimento IN ('PENDENTE','EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU')
    WHERE {' AND '.join(where)}""", params, "operacional_leads_disponiveis_total").get("total") or 0
    sql = f"""
    SELECT {select_cols}
    FROM {_lead_source_ref()} l
    LEFT JOIN {_ref('op_lote_leads')} op
      ON l.sk_pessoa = op.sk_pessoa
     AND op.status_atendimento IN ('PENDENTE','EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU')
    WHERE {' AND '.join(where)}
    ORDER BY l.data_inscricao DESC NULLS LAST,
      l.score_prioridade DESC,
      CASE UPPER(COALESCE(l.nivel_prioridade,'')) WHEN 'ALTA' THEN 1 WHEN 'MÉDIA' THEN 2 WHEN 'MEDIA' THEN 2 WHEN 'NORMAL' THEN 3 ELSE 4 END,
      COALESCE(l.dias_sem_acao, 0) DESC,
      l.sk_pessoa DESC
    LIMIT @limit OFFSET @offset
    """
    rows = _run(sql, params, "operacional_leads_disponiveis")
    return {"items": rows, "count": len(rows), "total": int(total), "pagination": {"limit": meta.get("limit"), "offset": meta.get("offset"), "total": int(total)}}, False


def criar_lote(payload: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    quantidade = _int(payload.get("quantidade"), 0, 1, 5000)
    tipo = str(payload.get("tipo_disparo") or "").strip().upper()
    if tipo not in TIPOS_DISPARO:
        raise ValueError("tipo_disparo deve ser ROBO, URA ou MANUAL.")
    modo = str(payload.get("modo") or "NOVO").strip().upper()
    usuario = _clean_text(payload.get("usuario") or payload.get("criado_por") or payload.get("consultor_disparo") or "sistema")
    filtros = dict(payload.get("filtros") or {})
    for k in ["campanha", "curso", "polo", "origem", "nivel_prioridade"]:
        if payload.get(k) and k not in filtros:
            filtros[k] = payload.get(k)
    mes_leads = _clean_text(payload.get("mes_leads") or filtros.get("mes_leads"))
    nome_lote = _clean_text(payload.get("nome_lote")) or _build_lote_nome(usuario, mes_leads)
    filtros_json = dict(filtros)
    filtros_json.setdefault("nome_lote", nome_lote)

    params = [
        bigquery.ScalarQueryParameter("usuario", "STRING", usuario),
        bigquery.ScalarQueryParameter("quantidade", "INT64", quantidade),
        bigquery.ScalarQueryParameter("tipo_disparo", "STRING", tipo),
        bigquery.ScalarQueryParameter("consultor_disparo", "STRING", _clean_text(payload.get("consultor_disparo"))),
        bigquery.ScalarQueryParameter("campanha", "STRING", _clean_text(payload.get("campanha") or filtros.get("campanha"))),
        bigquery.ScalarQueryParameter("modo", "STRING", modo),
        bigquery.ScalarQueryParameter("filtros_json", "STRING", json.dumps(filtros_json, ensure_ascii=False, default=str)),
    ]
    rows = _run(f"""
    CALL {_ref('sp_op_criar_lote')}(
      @usuario,
      @quantidade,
      @tipo_disparo,
      @consultor_disparo,
      @campanha,
      @modo,
      @filtros_json
    )
    """, params, "operacional_sp_criar_lote")
    data = _extract_lote_row(rows, {**dict(payload or {}), "usuario": usuario, "quantidade": quantidade, "nome_lote": nome_lote, "mes_leads": mes_leads})
    invalidate_gestao_cache()
    return {
        "success": True,
        "lote_id": data["lote_id"],
        "nome_lote": data["nome_lote"],
        "nome_arquivo_exportado": data["nome_arquivo_exportado"],
        "mes_leads": data.get("mes_leads") or mes_leads,
        "quantidade_solicitada": data["quantidade_solicitada"],
        "quantidade_liberada": data["quantidade_liberada"],
        "download_url": f"/api/gestao/lotes/{data['lote_id']}/csv",
    }, False

def get_lotes(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    where, params = ["1=1"], []
    _add_eq(where, params, "r", filters, ["status_lote", "consultor_disparo", "tipo_disparo", "campanha"])
    params += [bigquery.ScalarQueryParameter("limit", "INT64", _int(meta.get("limit"), 100)), bigquery.ScalarQueryParameter("offset", "INT64", _int(meta.get("offset"), 0))]
    rows = _run(f"""
    SELECT
      r.lote_id,
      r.nome_lote,
      r.nome_arquivo_exportado,
      r.mes_leads,
      r.exportado_por,
      r.campanha,
      r.tipo_disparo,
      r.consultor_disparo,
      r.quantidade_leads,
      r.status_lote,
      f.etapa_fluxo,
      f.proxima_acao,
      r.total_retorno,
      r.total_positivo,
      r.total_negativo,
      r.total_matriculas,
      r.taxa_retorno,
      r.taxa_matricula,
      r.exportado_em,
      r.started_at,
      r.importado_em,
      r.finished_at
    FROM {_ref('vw_op_lotes_resumo')} r
    LEFT JOIN {_ref('vw_op_fluxo_lotes')} f
      ON f.lote_id = r.lote_id
    WHERE {' AND '.join(where)}
    ORDER BY r.exportado_em DESC NULLS LAST, r.started_at DESC NULLS LAST, r.lote_id DESC
    LIMIT @limit OFFSET @offset
    """, params, "operacional_lotes")
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
    return marcar_lote_disparado(lote_id, "")


def marcar_lote_disparado(lote_id: str, usuario: str) -> Tuple[Dict[str, Any], bool]:
    lote_id = _clean_text(lote_id)
    if not lote_id:
        raise ValueError("lote_id é obrigatório.")
    params = [
        bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id),
        bigquery.ScalarQueryParameter("usuario", "STRING", _clean_text(usuario) or "sistema"),
    ]
    rows = _run(f"""
    CALL {_ref('sp_op_marcar_lote_disparado')}(
      @lote_id,
      @usuario
    )
    """, params, "operacional_sp_marcar_lote_disparado")
    invalidate_gestao_cache()
    data = dict(rows[0]) if rows else {}
    return {
        "success": True,
        "lote_id": data.get("lote_id") or lote_id,
        "status_lote": data.get("status_lote") or "EM_ANDAMENTO",
        "etapa_fluxo": data.get("etapa_fluxo") or "DISPARO_EM_ANDAMENTO",
        "proxima_acao": data.get("proxima_acao"),
    }, False


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
    return get_dashboard()


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


EXPORT_COLUMNS_OPERACIONAL = [
    "lote_id", "sk_pessoa", "nome", "cpf", "celular", "email", "curso", "modalidade", "turno",
    "polo", "origem", "campanha", "canal", "acao_comercial", "tipo_disparo", "consultor_disparo",
    "data_inscricao", "score_prioridade", "nivel_prioridade", "etapa_operacional", "status_atendimento",
]


def get_operacao_dashboard() -> Tuple[Dict[str, Any], bool]:
    return get_dashboard()


def get_lotes_select() -> Tuple[Dict[str, Any], bool]:
    rows = _run(f"""
    SELECT lote_id,nome_lote,consultor_disparo,tipo_disparo,status_lote,campanha,created_at
    FROM {_ref('op_lotes_disparo')}
    ORDER BY created_at DESC
    LIMIT 500
    """, operation="operacional_lotes_select")
    return {"items": rows, "count": len(rows)}, False


def preview_proximo_lote(filters: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    quantidade = _int(filters.get("quantidade") or filters.get("limit"), 100, 1, 5000)
    clean = {k: v for k, v in dict(filters or {}).items() if k in {"campanha", "curso", "polo", "origem", "nivel_prioridade"}}
    data, cached = get_leads_disponiveis(clean, {"limit": quantidade, "offset": _int(filters.get("offset"), 0)})
    leads = data.get("items", [])
    return {
        "total_disponivel": len(leads),
        "quantidade_preview": len(leads),
        "leads": leads,
        "items": leads,
        "count": len(leads),
        "ordenacao": ["data_inscricao DESC", "score_prioridade DESC", "nunca_disparado DESC", "dias_sem_acao DESC", "sk_pessoa DESC"],
    }, cached


def _rows_to_csv_b64(rows: List[Dict[str, Any]]) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=EXPORT_COLUMNS_OPERACIONAL, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k) for k in EXPORT_COLUMNS_OPERACIONAL})
    return base64.b64encode(out.getvalue().encode("utf-8-sig")).decode("ascii")

def _rows_to_xlsx_b64(rows: List[Dict[str, Any]]) -> str:
    try:
        from openpyxl import Workbook
    except Exception as exc:
        raise ValueError("Exportação XLSX não está disponível neste ambiente. Selecione CSV.") from exc
    wb = Workbook()
    ws = wb.active
    ws.title = "lote"
    ws.append(EXPORT_COLUMNS_OPERACIONAL)
    for row in rows:
        ws.append([row.get(k) for k in EXPORT_COLUMNS_OPERACIONAL])
    out = io.BytesIO()
    wb.save(out)
    return base64.b64encode(out.getvalue()).decode("ascii")


def get_lote_csv(lote_id: str) -> Tuple[str, bytes, int]:
    lote_id = _clean_text(lote_id)
    if not lote_id:
        raise ValueError("lote_id é obrigatório.")
    params = [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id)]
    rows = _run(
        f"SELECT * FROM {_ref('vw_op_export_lote_csv')} WHERE lote_id=@lote_id",
        params,
        "operacional_export_lote_csv",
    )
    if not rows:
        raise ValueError("Nenhum registro encontrado para exportação do lote informado.")

    first = rows[0]
    nome_lote = _clean_text(first.get("nome_lote")) or f"lote_{lote_id}"
    filename = _clean_text(first.get("nome_arquivo_exportado")) or f"{nome_lote}.csv"
    if not filename.lower().endswith(".csv"):
        filename = f"{filename}.csv"

    metadata_cols = {"nome_arquivo_exportado"}
    headers = [key for key in rows[0].keys() if key not in metadata_cols]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=headers, extrasaction="ignore", delimiter=";")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key) for key in headers})
    return filename, out.getvalue().encode("utf-8-sig"), len(rows)


def exportar_proximo_lote(payload: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    lote, cached = criar_lote(payload)
    filename, content, rows_count = get_lote_csv(lote["lote_id"])
    return {
        **lote,
        "filename": filename,
        "content_type": "text/csv; charset=utf-8",
        "base64": base64.b64encode(content).decode("ascii"),
        "rows_count": rows_count,
    }, cached


def _read_upload_rows(file: Any) -> List[Dict[str, Any]]:
    name = (getattr(file, "filename", "") or "").lower()
    raw = file.read()
    if hasattr(file, "seek"):
        file.seek(0)
    if name.endswith((".xlsx", ".xls")):
        try:
            import pandas as pd
            return pd.read_excel(io.BytesIO(raw)).fillna("").to_dict("records")
        except Exception as exc:
            raise ValueError(f"Não foi possível ler XLSX: {exc}")
    text = raw.decode("utf-8-sig", errors="replace")
    sample = text[:2048]
    dialect = csv.Sniffer().sniff(sample, delimiters=",;\t") if sample.strip() else csv.excel
    return list(csv.DictReader(io.StringIO(text), dialect=dialect))


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "sim", "s", "yes", "y", "mat", "matriculado"}

def importar_lote_disparado(file: Any, lote_id: str, usuario: str = "") -> Tuple[Dict[str, Any], bool]:
    if not lote_id:
        raise ValueError("lote_id é obrigatório.")
    rows = _read_upload_rows(file)
    updated = rejected = not_found = errors = 0
    error_list: List[str] = []
    lote = _single(f"SELECT lote_id FROM {_ref('op_lotes_disparo')} WHERE lote_id=@lote_id", [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id)], "operacional_import_lote_exists")
    if not lote:
        raise ValueError("Lote não encontrado.")
    for idx, row in enumerate(rows, start=2):
        try:
            normalized = {str(k).strip().lower(): v for k, v in row.items()}
            status = str(normalized.get("status_atendimento") or normalized.get("status") or "").strip().upper()
            if _truthy(normalized.get("matriculado")):
                status = "MAT"
            if status not in STATUS_ATENDIMENTO_MAP:
                rejected += 1; error_list.append(f"Linha {idx}: status inválido."); continue
            sk = normalized.get("sk_pessoa")
            cpf = str(normalized.get("cpf") or "").strip()
            celular = str(normalized.get("celular") or "").strip()
            if sk:
                found = _single(f"SELECT sk_pessoa,cpf,status_atendimento FROM {_ref('op_lote_leads')} WHERE lote_id=@lote_id AND sk_pessoa=@sk_pessoa", [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id), bigquery.ScalarQueryParameter("sk_pessoa", "INT64", int(float(sk)))], "operacional_import_find_sk")
            elif cpf:
                found = _single(f"SELECT sk_pessoa,cpf,status_atendimento FROM {_ref('op_lote_leads')} WHERE lote_id=@lote_id AND REGEXP_REPLACE(COALESCE(cpf,''), r'[^0-9]', '')=REGEXP_REPLACE(@cpf, r'[^0-9]', '')", [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id), bigquery.ScalarQueryParameter("cpf", "STRING", cpf)], "operacional_import_find_cpf")
            elif celular:
                found = _single(f"SELECT sk_pessoa,cpf,status_atendimento FROM {_ref('op_lote_leads')} WHERE lote_id=@lote_id AND REGEXP_REPLACE(COALESCE(celular,''), r'[^0-9]', '')=REGEXP_REPLACE(@celular, r'[^0-9]', '')", [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id), bigquery.ScalarQueryParameter("celular", "STRING", celular)], "operacional_import_find_celular")
            else:
                rejected += 1; error_list.append(f"Linha {idx}: informe sk_pessoa, cpf ou celular."); continue
            if not found:
                not_found += 1; continue
            update_lead_status(int(found["sk_pessoa"]), {"lote_id": lote_id, "status_atendimento": status, "observacao": normalized.get("observacao") or "", "usuario": usuario})
            updated += 1
        except Exception as exc:
            errors += 1; error_list.append(f"Linha {idx}: {exc}")
    recalcular_metricas_lote(lote_id)
    _run(f"INSERT INTO {_ref('op_bigquery_sync')} (sync_id,lote_id,status_sync,tentativas,linhas_processadas,erro,created_at,synced_at) VALUES (@sync_id,@lote_id,'CONCLUIDO',1,@total,NULL,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())", [bigquery.ScalarQueryParameter("sync_id", "STRING", str(uuid.uuid4())), bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id), bigquery.ScalarQueryParameter("total", "INT64", updated)], "operacional_import_sync")
    _evento(lote_id, None, None, "LOTE_RESULTADO_IMPORTADO", None, None, f"Resultado importado: {updated} atualizados", usuario)
    invalidate_gestao_cache()
    return {"lote_id": lote_id, "linhas_lidas": len(rows), "linhas_atualizadas": updated, "linhas_rejeitadas": rejected, "nao_encontrados": not_found, "erros": error_list}, False


def importar_novos_leads(file: Any, metadata: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    rows = _read_upload_rows(file)
    cols = {c.lower().strip() for c in (rows[0].keys() if rows else [])}
    missing = [c for c in ["nome", "curso"] if c not in cols]
    if "cpf" not in cols and "celular" not in cols:
        missing.append("cpf ou celular")
    if "polo" not in cols and "unidade" not in cols:
        missing.append("unidade/polo")
    if missing:
        raise ValueError("Colunas mínimas ausentes: " + ", ".join(missing))
    return {"linhas_lidas": len(rows), "linhas_validas": len(rows), "linhas_rejeitadas": 0, "mensagem": "Arquivo validado. Use POST /api/upload para a carga oficial com dt_upload preenchido pelo backend.", "erros": [], "metadata": dict(metadata or {})}, False


def get_fila_leads(filters: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    f, m = _filters_meta(filters)
    return get_leads_disponiveis(f, m)


def finalizar_lote(lote_id: str) -> Tuple[Dict[str, Any], bool]:
    return finish_lote(lote_id, {"confirmacao_forcada": True})


def cancelar_lote(lote_id: str) -> Tuple[Dict[str, Any], bool]:
    p = [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id)]
    _run(f"UPDATE {_ref('op_lotes_disparo')} SET status_lote='CANCELADO', updated_at=CURRENT_TIMESTAMP() WHERE lote_id=@lote_id", p, "operacional_cancelar_lote")
    _run(f"UPDATE {_ref('op_lote_leads')} SET status_atendimento='CANCELADO', updated_at=CURRENT_TIMESTAMP() WHERE lote_id=@lote_id AND status_atendimento IN ('PENDENTE','EM_ATENDIMENTO','AC','EC','NT','IF','NI','COU')", p, "operacional_cancelar_lote_leads")
    _evento(lote_id, None, None, "LOTE_CANCELADO", None, "CANCELADO", "Lote cancelado", None)
    invalidate_gestao_cache()
    return {"lote_id": lote_id, "status_lote": "CANCELADO"}, False


def get_operacao_logs(filters: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    limit = _int(filters.get("limit"), 100, 1, 1000)
    params = [bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    eventos = _run(f"SELECT 'evento' AS origem, created_at, tipo_evento AS tipo, lote_id, sk_pessoa, descricao, usuario FROM {_ref('op_lead_eventos')} ORDER BY created_at DESC LIMIT @limit", params, "operacional_logs_eventos")
    sync = _run(f"SELECT 'bigquery_sync' AS origem, created_at, status_sync AS tipo, lote_id, NULL AS sk_pessoa, erro AS descricao, NULL AS usuario FROM {_ref('op_bigquery_sync')} ORDER BY created_at DESC LIMIT @limit", params, "operacional_logs_sync")
    return {"items": sorted(eventos + sync, key=lambda r: str(r.get("created_at") or ""), reverse=True)[:limit], "count": min(len(eventos) + len(sync), limit)}, False
