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
    filters = {k: str(source.get(k) or "").strip() for k in ["campanha", "curso", "polo", "origem", "nivel_prioridade", "status_lote", "consultor_disparo", "status_atendimento", "lote_id", "q", "busca", "somente_pendentes"] if str(source.get(k) or "").strip()}
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


DASHBOARD_CARD_FIELDS = [
    "leads_novos_disponiveis", "leads_redisparo_disponiveis", "leads_em_lotes",
    "leads_pendentes", "leads_em_atendimento", "total_lotes", "lotes_abertos",
    "lotes_em_andamento", "lotes_importados", "lotes_concluidos", "lotes_cancelados",
    "retornos", "positivos", "negativos", "matriculas", "taxa_retorno_pct",
    "taxa_matricula_pct",
]

def get_dashboard() -> Tuple[Dict[str, Any], bool]:
    data = _single(f"""
    SELECT {", ".join(DASHBOARD_CARD_FIELDS)}
    FROM {_ref('vw_op_dashboard_cards')}
    LIMIT 1
    """, operation="operacional_dashboard_cards")
    missing = [field for field in DASHBOARD_CARD_FIELDS if field not in data]
    if missing:
        data["missing_fields"] = missing
    # Backward-compatible aliases for older front-end/tests; keep None when absent so UI can show — instead of 0.
    aliases = {
        "leads_disponiveis": "leads_novos_disponiveis",
        "leads_redisparo": "leads_redisparo_disponiveis",
        "leads_em_lote": "leads_em_lotes",
        "leads_pendentes_em_lote": "leads_pendentes",
        "taxa_retorno": "taxa_retorno_pct",
        "taxa_matricula": "taxa_matricula_pct",
    }
    for old, canonical in aliases.items():
        data.setdefault(old, data.get(canonical))
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
    params += [
        bigquery.ScalarQueryParameter("limit", "INT64", _int(meta.get("limit"), 100)),
        bigquery.ScalarQueryParameter("offset", "INT64", _int(meta.get("offset"), 0)),
    ]
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
      COALESCE(f.etapa_fluxo, r.etapa_fluxo) AS etapa_fluxo,
      COALESCE(f.proxima_acao, r.proxima_acao) AS proxima_acao,
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
    LEFT JOIN {_ref('vw_op_fluxo_lotes')} f USING (lote_id)
    WHERE {' AND '.join(where)}
    ORDER BY COALESCE(r.exportado_em, r.started_at, r.finished_at) DESC
    LIMIT @limit OFFSET @offset
    """, params, "operacional_lotes_views")
    return {"items": rows, "count": len(rows)}, False


def marcar_lote_disparado(lote_id: str, usuario: str = "") -> Tuple[Dict[str, Any], bool]:
    lote_id = _clean_text(lote_id)
    usuario = _clean_text(usuario) or "sistema"
    if not lote_id:
        raise ValueError("lote_id é obrigatório.")
    params = [
        bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id),
        bigquery.ScalarQueryParameter("usuario", "STRING", usuario),
    ]
    _run(f"""
    CALL {_ref('sp_op_marcar_lote_disparado')}(
      @lote_id,
      @usuario
    )
    """, params, "operacional_sp_marcar_lote_disparado")
    invalidate_gestao_cache()
    return {
        "lote_id": lote_id,
        "status_lote": "EM_ANDAMENTO",
        "etapa_fluxo": "DISPARO_EM_ANDAMENTO",
    }, False


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
    "status_inscricao", "data_inscricao", "origem", "unidade", "tipo_negocio", "curso",
    "modalidade", "turno", "nome", "cpf", "celular", "email", "data_ultima_acao",
    "qtd_acionamentos", "status", "data_disparo", "peca_disparo", "texto_disparo",
    "consultor_disparo", "tipo_disparo", "campanha", "observacao", "data_matricula",
    "matriculado", "canal", "acao_comercial", "consultor_comercial",
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
        f"""SELECT {", ".join(EXPORT_COLUMNS_OPERACIONAL)}, nome_arquivo_exportado, nome_lote
        FROM {_ref('vw_op_export_lote_csv')}
        WHERE lote_id=@lote_id
        ORDER BY data_inscricao DESC, score_prioridade DESC""",
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

    headers = EXPORT_COLUMNS_OPERACIONAL
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



def get_consultor_momento(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    where, params = ["1=1"], []
    _add_eq(where, params, "c", filters, ["consultor_disparo"])
    params.append(bigquery.ScalarQueryParameter("limit", "INT64", _int(meta.get("limit"), 500, 1, 1000)))
    rows = _run(f"""
    SELECT consultor_disparo,total_leads_em_lote,total_lotes,leads_em_lotes_abertos,leads_em_disparo,
           leads_com_retorno_importado,leads_finalizados,pendentes,em_atendimento,retornos,positivos,
           negativos,matriculas,ultima_movimentacao
    FROM {_ref('vw_op_consultor_momento')} c
    WHERE {' AND '.join(where)}
    ORDER BY ultima_movimentacao DESC NULLS LAST, consultor_disparo
    LIMIT @limit
    """, params, "operacional_consultor_momento")
    return {"items": rows, "count": len(rows)}, False


def get_lote_atual_leads(filters: Mapping[str, Any], meta: Mapping[str, Any]) -> Tuple[Dict[str, Any], bool]:
    where, params = ["1=1"], []
    _add_eq(where, params, "l", filters, ["lote_id", "consultor_disparo", "status_atendimento", "curso", "campanha"])
    if str(filters.get("somente_pendentes") or "").lower() in {"1", "true", "sim", "s"}:
        where.append("UPPER(COALESCE(l.status_atendimento,''))='PENDENTE'")
    q = _clean_text(filters.get("q") or filters.get("busca"))
    if q:
        params.append(bigquery.ScalarQueryParameter("q", "STRING", q))
        params.append(bigquery.ScalarQueryParameter("digits", "STRING", "".join(ch for ch in q if ch.isdigit())))
        where.append("""(UPPER(COALESCE(l.nome,'')) LIKE CONCAT('%', UPPER(@q), '%')
          OR REGEXP_REPLACE(COALESCE(l.cpf,''), r'[^0-9]', '') = @digits
          OR REGEXP_REPLACE(COALESCE(l.celular,''), r'[^0-9]', '') = @digits)""")
    params += [bigquery.ScalarQueryParameter("limit", "INT64", _int(meta.get("limit"), 100, 1, 500)), bigquery.ScalarQueryParameter("offset", "INT64", _int(meta.get("offset"), 0))]
    cols = "lote_id,sk_pessoa,nome,cpf,celular,email,curso,polo,campanha,status_atendimento,retorno,positivo,negativo,matriculado,observacao,data_inscricao,data_disparo,ultimo_evento,ultimo_evento_em,ultimo_evento_por,consultor_disparo,data_matricula"
    rows = _run(f"SELECT {cols} FROM {_ref('vw_op_lote_atual_leads')} l WHERE {' AND '.join(where)} ORDER BY data_inscricao DESC LIMIT @limit OFFSET @offset", params, "operacional_lote_atual_leads")
    total = _to_int(_single(f"SELECT COUNT(1) AS total FROM {_ref('vw_op_lote_atual_leads')} l WHERE {' AND '.join(where)}", params[:-2], "operacional_lote_atual_total").get("total"))
    return {"items": rows, "count": len(rows), "pagination": {"total": total, "limit": _int(meta.get("limit"), 100), "offset": _int(meta.get("offset"), 0)}}, False


def atualizar_lead_lote(lote_id: str, sk_pessoa: str, payload: Mapping[str, Any], usuario: str) -> Tuple[Dict[str, Any], bool]:
    lote_id = _clean_text(lote_id)
    if not lote_id or not sk_pessoa:
        raise ValueError("lote_id e sk_pessoa são obrigatórios.")
    status = _clean_text(payload.get("status_atendimento")).upper()
    if status and status not in STATUS_ATENDIMENTO_MAP:
        raise ValueError("status_atendimento inválido.")
    params = [
        bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id),
        bigquery.ScalarQueryParameter("sk_pessoa", "INT64", int(sk_pessoa)),
        bigquery.ScalarQueryParameter("usuario", "STRING", _clean_text(usuario) or "sistema"),
        bigquery.ScalarQueryParameter("status_atendimento", "STRING", status),
        bigquery.ScalarQueryParameter("observacao", "STRING", _clean_text(payload.get("observacao"))),
        bigquery.ScalarQueryParameter("matriculado", "BOOL", bool(payload.get("matriculado", False))),
        bigquery.ScalarQueryParameter("data_matricula", "DATE", payload.get("data_matricula") or None),
        bigquery.ScalarQueryParameter("consultor_disparo", "STRING", _clean_text(payload.get("consultor_disparo"))),
    ]
    _run(f"""CALL {_ref('sp_op_atualizar_lead_lote')}(
      @lote_id,@sk_pessoa,@usuario,@status_atendimento,@observacao,@matriculado,@data_matricula,@consultor_disparo
    )""", params, "operacional_sp_atualizar_lead_lote")
    invalidate_gestao_cache()
    updated, _ = get_lote_atual_leads({"lote_id": lote_id, "busca": str(sk_pessoa)}, {"limit": 1, "offset": 0})
    return {"success": True, "lote_id": lote_id, "sk_pessoa": int(sk_pessoa), "lead": (updated.get("items") or [{}])[0]}, False

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


RETORNO_ALIASES = {
    "status": "status_retorno",
    "retorno_status": "status_retorno",
    "status_atendimento": "status_retorno",
    "obs": "observacao",
    "telefone": "celular",
}
RETORNO_COLUMNS = ["sk_pessoa", "cpf", "celular", "nome", "status_retorno", "observacao", "data_contato"]
VALID_RETORNO_STATUS = {"AC", "EC", "NT", "IF", "MAT", "NI", "COU"}


def _normalize_return_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key, value in (row or {}).items():
        col = str(key or "").strip().lower()
        col = RETORNO_ALIASES.get(col, col)
        if col in RETORNO_COLUMNS and col not in normalized:
            normalized[col] = "" if value is None else str(value).strip()
    status = str(normalized.get("status_retorno") or "").strip().upper()
    normalized["status_retorno"] = status
    return {col: normalized.get(col, "") for col in RETORNO_COLUMNS}


def importar_retorno_lote(file: Any, lote_id: str, usuario: str = "") -> Tuple[Dict[str, Any], bool]:
    lote_id = _clean_text(lote_id)
    if not lote_id:
        raise ValueError("O usuário deve selecionar um lote.")
    rows = _read_upload_rows(file)
    if not rows:
        raise ValueError("Arquivo vazio.")
    lote = _single(
        f"SELECT lote_id, nome_lote FROM {_ref('op_lotes_disparo')} WHERE lote_id=@lote_id",
        [bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id)],
        "retorno_lote_exists",
    )
    if not lote:
        raise ValueError("Lote não encontrado.")

    import_batch_id = str(uuid.uuid4())
    staging_rows: List[Dict[str, Any]] = []
    errors: List[str] = []
    for idx, raw in enumerate(rows, start=2):
        row = _normalize_return_row(raw)
        if row["status_retorno"] not in VALID_RETORNO_STATUS:
            errors.append(f"Linha {idx}: status_retorno inválido.")
            continue
        if not (row.get("sk_pessoa") or row.get("cpf") or row.get("celular")):
            errors.append(f"Linha {idx}: informe sk_pessoa, cpf ou celular.")
            continue
        staging_rows.append({
            "lote_id": lote_id,
            "import_batch_id": import_batch_id,
            "usuario": _clean_text(usuario) or "sistema",
            **row,
        })
    if errors:
        raise ValueError("; ".join(errors[:10]))

    client = bq.get_bq_client()
    table_id = f"{PROJECT_ID}.{DATASET}.op_lote_retorno_staging"
    insert_errors = client.insert_rows_json(table_id, staging_rows, ignore_unknown_values=True)
    if insert_errors:
        raise RuntimeError(f"Falha ao inserir retorno na staging: {insert_errors}")

    proc_rows = _run(
        f"""CALL `{PROJECT_ID}.{DATASET}.sp_op_processar_retorno_lote`(@lote_id, @import_batch_id, @usuario)""",
        [
            bigquery.ScalarQueryParameter("lote_id", "STRING", lote_id),
            bigquery.ScalarQueryParameter("import_batch_id", "STRING", import_batch_id),
            bigquery.ScalarQueryParameter("usuario", "STRING", _clean_text(usuario) or "sistema"),
        ],
        "retorno_lote_processar",
    )
    summary = dict(proc_rows[0] if proc_rows else {})
    data = {
        "success": True,
        "lote_id": lote_id,
        "nome_lote": summary.get("nome_lote") or lote.get("nome_lote"),
        "import_batch_id": import_batch_id,
        "linhas_recebidas": len(staging_rows),
        "leads_atualizados": _to_int(summary.get("leads_atualizados")),
        "total_retorno": _to_int(summary.get("total_retorno")),
        "total_positivo": _to_int(summary.get("total_positivo")),
        "total_negativo": _to_int(summary.get("total_negativo")),
        "total_matriculas": _to_int(summary.get("total_matriculas")),
    }
    invalidate_gestao_cache()
    return data, False


def buscar_leads(q: str, limit: int = 20) -> Tuple[Dict[str, Any], bool]:
    term = _clean_text(q)
    if not term:
        return {"items": []}, False
    digits = "".join(ch for ch in term if ch.isdigit())
    params = [bigquery.ScalarQueryParameter("q", "STRING", term), bigquery.ScalarQueryParameter("digits", "STRING", digits), bigquery.ScalarQueryParameter("limit", "INT64", _int(limit, 20, 1, 50))]
    rows = _run(f"""
    SELECT l.*
    FROM {_ref('vw_leads_painel_lite')} l
    WHERE UPPER(COALESCE(l.nome,'')) LIKE CONCAT('%', UPPER(@q), '%')
       OR REGEXP_REPLACE(COALESCE(l.cpf,''), r'[^0-9]', '') = @digits
       OR REGEXP_REPLACE(COALESCE(l.celular,''), r'[^0-9]', '') = @digits
       OR UPPER(COALESCE(l.email,'')) = UPPER(@q)
       OR CAST(l.sk_pessoa AS STRING) = @q
       OR EXISTS (SELECT 1 FROM {_ref('op_lote_leads')} ol WHERE ol.sk_pessoa=l.sk_pessoa AND CAST(ol.lote_id AS STRING)=@q)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY l.sk_pessoa ORDER BY l.data_inscricao DESC)=1
    ORDER BY l.data_inscricao DESC
    LIMIT @limit
    """, params, "leads_buscar")
    return {"items": rows}, False


def get_lead_lotes(sk_pessoa: str) -> Tuple[Dict[str, Any], bool]:
    p = [bigquery.ScalarQueryParameter("sk_pessoa", "INT64", int(sk_pessoa))]
    rows = _run(f"""
    SELECT ol.lote_id, ld.nome_lote, ol.status_atendimento, ol.retorno, ol.positivo, ol.negativo, ol.matriculado,
           ld.exportado_em AS data_exportacao, ol.data_disparo, ld.importado_em AS data_importacao_retorno,
           ol.ultimo_evento, ol.ultimo_evento_em, ol.ultimo_evento_por
    FROM {_ref('op_lote_leads')} ol
    LEFT JOIN {_ref('op_lotes_disparo')} ld ON ld.lote_id=ol.lote_id
    WHERE ol.sk_pessoa=@sk_pessoa
    QUALIFY ROW_NUMBER() OVER(PARTITION BY ol.lote_id ORDER BY ol.updated_at DESC)=1
    ORDER BY COALESCE(ol.updated_at, ld.updated_at) DESC
    """, p, "lead_lotes")
    return {"items": rows}, False


def get_lead_timeline(sk_pessoa: str) -> Tuple[Dict[str, Any], bool]:
    p = [bigquery.ScalarQueryParameter("sk_pessoa", "INT64", int(sk_pessoa))]
    return {"items": _run(f"SELECT * FROM {_ref('op_lead_timeline')} WHERE sk_pessoa=@sk_pessoa ORDER BY created_at DESC LIMIT 500", p, "lead_timeline")}, False


def get_lead_eventos(sk_pessoa: str) -> Tuple[Dict[str, Any], bool]:
    p = [bigquery.ScalarQueryParameter("sk_pessoa", "INT64", int(sk_pessoa))]
    return {"items": _run(f"SELECT * FROM {_ref('op_lead_eventos')} WHERE sk_pessoa=@sk_pessoa ORDER BY created_at DESC LIMIT 500", p, "lead_eventos")}, False


def _api_response(data=None, message="OK", success=True):
    return {"success": success, "message": message, "data": data}


def listar_perfis() -> Tuple[Dict[str, Any], bool]:
    rows = _run(f"SELECT * FROM {_ref('op_perfis_painel')} WHERE UPPER(COALESCE(codigo_perfil, perfil, nome, '')) IN ('ADMIN','GESTOR','OPERADOR','LEITURA') ORDER BY nome", operation="usuarios_perfis")
    if not rows:
        rows = [{"perfil_id": p, "codigo_perfil": p, "nome": p} for p in ["ADMIN", "GESTOR", "OPERADOR", "LEITURA"]]
    return _api_response(rows), False


def listar_usuarios() -> Tuple[Dict[str, Any], bool]:
    return _api_response(_run(f"SELECT * FROM {_ref('vw_op_usuarios_painel')} ORDER BY nome LIMIT 500", operation="usuarios_listar")), False


def auditoria_usuario(usuario_id: str) -> Tuple[Dict[str, Any], bool]:
    p=[bigquery.ScalarQueryParameter("usuario_id","STRING",usuario_id)]
    return _api_response(_run(f"SELECT * FROM {_ref('op_auditoria_painel')} WHERE usuario_id=@usuario_id OR entidade_id=@usuario_id ORDER BY created_at DESC LIMIT 200", p, "usuarios_auditoria")), False


def registrar_auditoria(acao: str, usuario_id: str, autor: str, detalhes: Mapping[str, Any] | None = None) -> None:
    p=[bigquery.ScalarQueryParameter("auditoria_id","STRING",str(uuid.uuid4())), bigquery.ScalarQueryParameter("usuario_id","STRING",usuario_id), bigquery.ScalarQueryParameter("acao","STRING",acao), bigquery.ScalarQueryParameter("autor","STRING",autor or "sistema"), bigquery.ScalarQueryParameter("detalhes","STRING",json.dumps(detalhes or {}, ensure_ascii=False, default=str))]
    _run(f"INSERT INTO {_ref('op_auditoria_painel')} (auditoria_id, usuario_id, entidade_id, acao, usuario, detalhes, created_at) VALUES (@auditoria_id, @usuario_id, @usuario_id, @acao, @autor, @detalhes, CURRENT_TIMESTAMP())", p, "usuarios_auditoria_insert")


def salvar_usuario(payload: Mapping[str, Any], autor: str, usuario_id: str | None = None) -> Tuple[Dict[str, Any], bool]:
    nome=_clean_text(payload.get("nome")); email=_clean_text(payload.get("email")).lower(); perfil_id=_clean_text(payload.get("perfil_id")); ativo=bool(payload.get("ativo", True)); status=_clean_text(payload.get("status_usuario")) or ("ATIVO" if ativo else "INATIVO")
    if not nome or not email or not perfil_id: raise ValueError("nome, email e perfil_id são obrigatórios.")
    if usuario_id:
        p=[bigquery.ScalarQueryParameter("usuario_id","STRING",usuario_id), bigquery.ScalarQueryParameter("nome","STRING",nome), bigquery.ScalarQueryParameter("email","STRING",email), bigquery.ScalarQueryParameter("perfil_id","STRING",perfil_id), bigquery.ScalarQueryParameter("ativo","BOOL",ativo), bigquery.ScalarQueryParameter("status_usuario","STRING",status)]
        _run(f"UPDATE {_ref('op_usuarios_painel')} SET nome=@nome,email=@email,perfil_id=@perfil_id,ativo=@ativo,status_usuario=@status_usuario,updated_at=CURRENT_TIMESTAMP() WHERE usuario_id=@usuario_id", p, "usuarios_update")
        registrar_auditoria("EDICAO_USUARIO", usuario_id, autor, {"email": email, "perfil_id": perfil_id})
    else:
        usuario_id=str(uuid.uuid4()); password_hash=_clean_text(payload.get("password_hash"))
        p=[bigquery.ScalarQueryParameter("usuario_id","STRING",usuario_id), bigquery.ScalarQueryParameter("nome","STRING",nome), bigquery.ScalarQueryParameter("email","STRING",email), bigquery.ScalarQueryParameter("perfil_id","STRING",perfil_id), bigquery.ScalarQueryParameter("ativo","BOOL",ativo), bigquery.ScalarQueryParameter("status_usuario","STRING",status), bigquery.ScalarQueryParameter("primeiro_acesso","BOOL",True), bigquery.ScalarQueryParameter("password_hash","STRING",password_hash)]
        _run(f"INSERT INTO {_ref('op_usuarios_painel')} (usuario_id,nome,email,perfil_id,ativo,status_usuario,primeiro_acesso,password_hash,created_at,updated_at) VALUES (@usuario_id,@nome,@email,@perfil_id,@ativo,@status_usuario,@primeiro_acesso,@password_hash,CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP())", p, "usuarios_insert")
        registrar_auditoria("CRIACAO_USUARIO", usuario_id, autor, {"email": email, "perfil_id": perfil_id})
    return _api_response({"usuario_id": usuario_id}, "Usuário salvo."), False


def alterar_status_usuario(usuario_id: str, ativo: bool, autor: str) -> Tuple[Dict[str, Any], bool]:
    p=[bigquery.ScalarQueryParameter("usuario_id","STRING",usuario_id), bigquery.ScalarQueryParameter("ativo","BOOL",ativo), bigquery.ScalarQueryParameter("status_usuario","STRING","ATIVO" if ativo else "INATIVO")]
    _run(f"UPDATE {_ref('op_usuarios_painel')} SET ativo=@ativo,status_usuario=@status_usuario,updated_at=CURRENT_TIMESTAMP() WHERE usuario_id=@usuario_id", p, "usuarios_status")
    registrar_auditoria("ATIVACAO_USUARIO" if ativo else "DESATIVACAO_USUARIO", usuario_id, autor)
    return _api_response({"usuario_id": usuario_id, "ativo": ativo}, "Status atualizado."), False


def resetar_senha_usuario(usuario_id: str, password_hash: str, autor: str) -> Tuple[Dict[str, Any], bool]:
    p=[bigquery.ScalarQueryParameter("usuario_id","STRING",usuario_id), bigquery.ScalarQueryParameter("password_hash","STRING",password_hash)]
    _run(f"UPDATE {_ref('op_usuarios_painel')} SET password_hash=@password_hash,primeiro_acesso=TRUE,updated_at=CURRENT_TIMESTAMP() WHERE usuario_id=@usuario_id", p, "usuarios_reset_senha")
    registrar_auditoria("RESET_SENHA", usuario_id, autor)
    return _api_response({"usuario_id": usuario_id}, "Senha resetada."), False
