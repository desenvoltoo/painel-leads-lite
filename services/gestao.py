# -*- coding: utf-8 -*-
"""PostgreSQL services for gestão endpoints."""
from __future__ import annotations
import csv, io, json, math, re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Tuple

from services import database as bq

MAX_PAGE_SIZE = 100
OPTION_FIELDS = ["status","curso","modalidade","turno","polo","origem","consultor_disparo","consultor_comercial","canal","campanha","tipo_disparo","tipo_negocio"]
_CACHE: dict = {}

class GestaoValidationError(ValueError): pass

def utc_now_iso(): return datetime.now(timezone.utc).isoformat()
def invalidate_gestao_cache(): _CACHE.clear()
def _sanitize_message(msg):
    if msg is None: return None
    s=str(msg); s=re.sub(r"\d{3}\.?\d{3}\.?\d{3}-?\d{2}","[cpf-mascarado]",s); s=re.sub(r"[\w.%-]+@[\w.-]+","[email-mascarado]",s); s=re.sub(r"\b\d{10,11}\b","[celular-mascarado]",s); s=re.sub(r"(token|senha|password)=\S+",r"\1=[redacted]",s,flags=re.I); return s

def parse_filters(args):
    args=dict(args or {}); filters={}
    for k in OPTION_FIELDS+["busca","data_inicio","data_fim","data_ini","matriculado","data_disparo_mes","data_disparo_situacao"]:
        v=args.get(k)
        if isinstance(v, list): v=[x for x in v if str(x).strip()]
        if v not in (None,"",[]): filters[k]=v
    def d(k):
        if k in filters:
            try: date.fromisoformat(str(filters[k]))
            except Exception: raise GestaoValidationError("Data inválida.")
    d("data_ini"); d("data_inicio"); d("data_fim")
    ini=filters.get("data_ini") or filters.get("data_inicio"); fim=filters.get("data_fim")
    if ini and fim and date.fromisoformat(str(ini)) > date.fromisoformat(str(fim)): raise GestaoValidationError("Período inválido.")
    limit=max(1,min(MAX_PAGE_SIZE,int(args.get("limit") or args.get("pageSize") or 50)))
    page=max(1,int(args.get("page") or 1)); offset=max(0,int(args.get("offset") or (page-1)*limit))
    order_by=str(args.get("order_by") or "data_inscricao"); order_dir="ASC" if str(args.get("order_dir") or "DESC").upper()=="ASC" else "DESC"
    return filters,{"limit":limit,"pageSize":limit,"page":page,"offset":offset,"order_by":order_by,"order_dir":order_dir}

def parse_import_history_request(args):
    args=dict(args or {}); filters={}
    for k in ["status","nomeArquivo","dataInicio","dataFim"]:
        if args.get(k): filters[k]=args[k]
    limit=max(1,min(MAX_PAGE_SIZE,int(args.get("pageSize") or args.get("limit") or 50))); page=max(1,int(args.get("page") or 1))
    return filters,{"limit":limit,"pageSize":limit,"page":page,"offset":(page-1)*limit,"order_by":args.get("order_by","criado_em"),"order_dir":"ASC" if str(args.get("order_dir","DESC")).upper()=="ASC" else "DESC"}

def _cache_key(name, filters, meta): return (name, tuple(sorted((k,str(v)) for k,v in filters.items())), tuple(sorted((k,str(v)) for k,v in meta.items())))
def _with_cache(name, filters, meta, personal, loader):
    if personal or any(k in filters for k in ("busca","cpf","celular","email","nome")): return loader(), False
    key=_cache_key(name,filters,meta)
    if key in _CACHE: return _CACHE[key], True
    data=loader(); _CACHE[key]=data; return data, False

def _run(sql, params=None, op="gestao_query"): return bq._run_gestao_query(sql, params or {}, op)
def _has(col):
    try: return bq._has_view_col(col)
    except Exception: return True

def is_status_empty(v): return v is None or str(v).strip().upper() in {"","SEM INFORMAÇÃO","SEM INFORMACAO","NULL","N/A"}
def is_status_ec(v): return str(v or "").strip().upper()=="EC"
def is_valid_phone(v):
    d=re.sub(r"\D","",str(v or "")); return len(d) in (10,11) and len(set(d))>1
def is_valid_cpf(v):
    s=re.sub(r"\D","",str(v or ""))
    if len(s)!=11 or len(set(s))==1: return False
    def calc(n):
        sm=sum(int(s[i])*(n+1-i) for i in range(n-1)); r=(sm*10)%11; return 0 if r==10 else r
    return calc(10)==int(s[9]) and calc(11)==int(s[10])
def is_matriculado_row(r): return bool(r.get("flag_matriculado")) or str(r.get("status","")).strip().upper()=="MAT" or str(r.get("status_inscricao","")).strip().upper() in {"MATRICULADO","MATRÍCULADO","MATRICULADOS"} or bool(r.get("data_matricula")) or str(r.get("matriculado","")).lower() in {"sim","true","1"}
def should_accept_upload_version(source_dt, current_dt): return current_dt is None or source_dt >= current_dt
def score_rule_documentation(): return [{"regra":"Prioriza não matriculados, com celular válido, sem status e dt_upload mais recente; cargas antigas não sobrescrevem novas."},{"regra":"Matriculados e status finais saem da fila."}]

def _mask_cpf(v):
    d=re.sub(r"\D","",str(v or "")); return "***.***.***-"+d[-4:] if d else ""
def _mask_phone(v):
    d=re.sub(r"\D","",str(v or "")); return "*******"+d[-4:] if d else ""
def _mask_email(v):
    s=str(v or ""); return (s[0]+"***@"+s.split("@",1)[1]) if "@" in s and s else ""
def mask_rejection_row(row):
    out={}
    for k,v in (row or {}).items():
        if k == "payload": continue
        lk=k.lower()
        if "cpf" in lk: out[k]=_mask_cpf(v)
        elif "celular" in lk or "phone" in lk: out[k]=_mask_phone(v)
        elif "email" in lk: out[k]=_mask_email(v)
        else: out[k]=v
    return out

def _matriculado_sql(): return "(flag_matriculado = true OR upper(trim(coalesce(status::text,''))) = 'MAT' OR upper(trim(coalesce(status_inscricao::text,''))) IN ('MATRICULADO','MATRÍCULADO','MATRICULADOS') OR data_matricula IS NOT NULL)"
def _inscrito_sql(): return f"(upper(trim(coalesce(status_inscricao::text,''))) = 'INSCRITO' OR {_matriculado_sql()})"
def _trabalhado_sql(): return f"(nullif(trim(coalesce(status::text,'')),'') IS NOT NULL OR data_ultima_acao IS NOT NULL OR data_disparo IS NOT NULL OR coalesce(qtd_acionamentos,0)>0 OR {_inscrito_sql()})"
def _valid_phone_sql(): return "nullif(regexp_replace(coalesce(celular::text, ''), '[^0-9]', '', 'g'), '') IS NOT NULL"

def get_resumo(filters, meta):
    def load():
        sql=f"SELECT COUNT(*) total_leads, COUNT(*) FILTER (WHERE {_valid_phone_sql()}) total_validos, COUNT(*) FILTER (WHERE {_matriculado_sql()}) matriculados, MAX(data_atualizacao) ultima_atualizacao FROM {bq._view_table_id()} v WHERE 1=1"
        rows=_run(sql,{},"gestao_resumo"); return rows[0] if rows else {}
    return _with_cache("resumo",filters,meta,False,load)

def get_funil(filters, meta):
    def load():
        sql=f"SELECT COUNT(*) FILTER (WHERE {_valid_phone_sql()}) total_valido, COUNT(*) FILTER (WHERE {_valid_phone_sql()} AND {_trabalhado_sql()}) trabalhados, COUNT(*) FILTER (WHERE {_valid_phone_sql()} AND {_inscrito_sql()}) inscritos, COUNT(*) FILTER (WHERE {_valid_phone_sql()} AND {_matriculado_sql()}) matriculados FROM {bq._view_table_id()} v WHERE 1=1"
        r=(_run(sql,{},"gestao_funil") or [{}])[0]; vals=[int(r.get(k) or 0) for k in ["total_valido","trabalhados","inscritos","matriculados"]]
        vals=[vals[0], min(vals[1],vals[0]), min(vals[2],vals[1]), min(vals[3],vals[2])]
        names=["Total válido","Trabalhados","Inscritos","Matriculados"]; items=[]
        for i,(n,v) in enumerate(zip(names,vals)):
            prev=vals[i-1] if i else v; items.append({"etapa":n,"total":v,"conversao": min(100, round((v/prev*100) if prev else 0,2)),"perda": max(prev-v,0)})
        return {"items":items,"nunca_trabalhados":max(vals[0]-vals[1],0)}
    return _with_cache("funil",filters,meta,False,load)

def get_evolucao(filters, meta):
    return _with_cache("evolucao",filters,meta,False,lambda: {"items": _run(f"SELECT date_trunc('day', data_inscricao)::date AS data, COUNT(*) total FROM {bq._view_table_id()} v WHERE data_inscricao IS NOT NULL GROUP BY 1 ORDER BY 1 DESC LIMIT 90",{},"gestao_evolucao")})
def get_rankings(filters, meta):
    rows=_run(f"SELECT curso, COUNT(*) total_leads, COUNT(*) FILTER (WHERE {_matriculado_sql()}) matriculados FROM {bq._view_table_id()} v GROUP BY curso ORDER BY total_leads DESC NULLS LAST LIMIT 20",{},"gestao_rankings")
    return {"cursos":rows}, False
def get_produtividade(filters, meta):
    rows=_run(f"SELECT coalesce(consultor_comercial, consultor_disparo, 'Sem consultor') consultor, COUNT(*) total_leads, COUNT(*) FILTER (WHERE {_trabalhado_sql()}) trabalhados FROM {bq._view_table_id()} v GROUP BY 1 ORDER BY total_leads DESC LIMIT 50",{},"gestao_produtividade")
    return {"items":rows}, False

def prioritize_fila_rows(rows):
    def dt(v):
        for fmt in ("%Y-%m-%d","%d/%m/%Y"):
            try: return datetime.strptime(str(v or ""),fmt)
            except Exception: pass
        return datetime.min
    out=[]
    for r in rows:
        if not is_valid_phone(r.get("celular")) or is_matriculado_row(r): continue
        st=str(r.get("status") or "").strip().upper()
        if st in {"CANCELADO","DESCARTADO","ENCERRADO"}: continue
        g=1 if is_status_empty(st) else (2 if is_status_ec(st) else 3)
        rr=dict(r); rr["grupo_prioridade"]=g; rr["prioridade"]={1:"ALTA",2:"MEDIA",3:"NORMAL"}[g]; out.append(rr)
    return sorted(out,key=lambda r:(r["grupo_prioridade"], -dt(r.get("data_inscricao")).timestamp()))

def _fila_sql(limit, offset):
    return f"SELECT *, CASE WHEN nullif(trim(coalesce(status::text,'')),'') IS NULL THEN 1 WHEN upper(trim(status::text))='EC' THEN 2 ELSE 3 END grupo_prioridade FROM {bq._view_table_id()} v WHERE NOT {_matriculado_sql()} AND {_valid_phone_sql()} AND upper(trim(coalesce(status::text,''))) NOT IN ('CANCELADO','DESCARTADO','ENCERRADO') ORDER BY grupo_prioridade ASC, data_inscricao DESC NULLS LAST LIMIT :limit OFFSET :offset"
def get_fila(filters, meta):
    rows=_run(_fila_sql(meta.get("limit",50),meta.get("offset",0)), {"limit":meta.get("limit",50),"offset":meta.get("offset",0)}, "gestao_fila")
    return {"items":rows,"pagination":{"page":meta.get("page",1),"page_size":meta.get("limit",50),"total":len(rows),"total_pages":1}}, False

def get_qualidade(filters, meta): return {"indicadores": get_qualidade_dados(filters,meta)[0]}, False
def get_qualidade_dados(filters, meta):
    sql=f"""SELECT COUNT(*) total_registros,
SUM(CASE WHEN nullif(celular::text,'') IS NULL THEN 1 ELSE 0 END) sem_celular,
SUM(CASE WHEN nullif(celular::text,'') IS NOT NULL AND NOT ({_valid_phone_sql()}) THEN 1 ELSE 0 END) celular_invalido,
SUM(CASE WHEN nullif(email::text,'') IS NULL THEN 1 ELSE 0 END) sem_email,
SUM(CASE WHEN nullif(cpf::text,'') IS NULL THEN 1 ELSE 0 END) sem_cpf,
SUM(CASE WHEN length(regexp_replace(coalesce(cpf::text,''),'[^0-9]','','g')) BETWEEN 1 AND 10 THEN 1 ELSE 0 END) cpf_incompleto,
SUM(CASE WHEN nullif(origem::text,'') IS NULL THEN 1 ELSE 0 END) sem_origem,
SUM(CASE WHEN nullif(curso::text,'') IS NULL THEN 1 ELSE 0 END) sem_curso,
SUM(CASE WHEN nullif(coalesce(consultor_comercial,consultor_disparo)::text,'') IS NULL THEN 1 ELSE 0 END) sem_consultor,
SUM(CASE WHEN nullif(trim(coalesce(status::text,'')),'') IS NULL THEN 1 ELSE 0 END) sem_status,
SUM(CASE WHEN data_inscricao IS NULL THEN 1 ELSE 0 END) sem_data_inscricao,
SUM(CASE WHEN data_atualizacao IS NULL THEN 1 ELSE 0 END) sem_data_atualizacao,
(SELECT coalesce(SUM(qtd-1),0) FROM (SELECT regexp_replace(coalesce(cpf::text,''),'[^0-9]','','g') k, COUNT(*) qtd FROM {bq._view_table_id()} WHERE nullif(cpf::text,'') IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1) d) duplicados_cpf,
(SELECT coalesce(SUM(qtd-1),0) FROM (SELECT regexp_replace(coalesce(celular::text,''),'[^0-9]','','g') k, COUNT(*) qtd FROM {bq._view_table_id()} WHERE nullif(celular::text,'') IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1) d) duplicados_celular,
0 total_rejeitados FROM {bq._view_table_id()} v"""
    return (_run(sql,{},"gestao_qualidade") or [{}])[0], False

def _quality_details_sql(tipo, filters, meta):
    allowed={"sem_status":"nullif(trim(coalesce(status::text,'')),'') IS NULL","duplicado_cpf":"dup_cpf.qtd > 1","duplicados_cpf":"dup_cpf.qtd > 1"}
    if tipo not in allowed: raise GestaoValidationError("Tipo de qualidade inválido.")
    sql=f"SELECT v.nome, v.curso, v.status, v.data_inscricao, '***' identificador FROM {bq._view_table_id()} v LEFT JOIN (SELECT cpf, COUNT(*) qtd FROM {bq._view_table_id()} GROUP BY cpf) dup_cpf ON dup_cpf.cpf=v.cpf WHERE {allowed[tipo]} LIMIT :limit OFFSET :offset"
    return sql,{"limit":meta.get("limit",50),"offset":meta.get("offset",0)}
def get_qualidade_detalhes(filters, meta, tipo):
    sql,params=_quality_details_sql(tipo,filters,meta); rows=[mask_rejection_row(r) for r in _run(sql,params,"gestao_qualidade_detalhes")]
    return {"items":rows,"pagination":{"page":meta.get("page",1),"page_size":meta.get("limit",50),"total":len(rows),"total_pages":1}}, False

def map_qualidade_row(r):
    return {"totalRegistros":int(r.get("total_registros") or 0),"totalLeads":int(r.get("total_leads") or 0),"duplicidadesCpf":int(r.get("duplicidades_cpf") or 0),"duplicidadesCelular":int(r.get("duplicidades_celular") or 0),"duplicidadesEmail":int(r.get("duplicidades_email") or 0),"duplicidadesTotais":int(r.get("duplicidades_cpf") or 0)+int(r.get("duplicidades_celular") or 0)+int(r.get("duplicidades_email") or 0),"percentualDuplicidade":float(r.get("percentual_duplicidade") or 0),"ultimaAtualizacao":r.get("ultima_atualizacao")}

def get_importacoes_historico(filters, meta): return get_importacoes(filters, meta)
def get_importacoes(filters, meta):
    where=[]; params={}
    if filters.get("status"): where.append("status = :status"); params["status"]=filters["status"]
    if filters.get("nomeArquivo"): where.append("nome_arquivo ILIKE :nome"); params["nome"]="%"+filters["nomeArquivo"]+"%"
    wh=(" WHERE "+" AND ".join(where)) if where else ""
    count=_run(f"SELECT COUNT(*) total FROM {bq._safe_ident(bq.DB_SCHEMA)}.vw_historico_importacoes{wh}",params,"import_history_count")[0]["total"]
    params.update({"limit":meta.get("pageSize",50),"offset":meta.get("offset",0)})
    rows=_run(f"SELECT * FROM {bq._safe_ident(bq.DB_SCHEMA)}.vw_historico_importacoes{wh} ORDER BY criado_em DESC LIMIT :limit OFFSET :offset",params,"import_history")
    safe=[{k:v for k,v in r.items() if k.lower() not in {"payload","email","cpf","celular"}} for r in rows]
    ps=meta.get("pageSize",50); return {"items":safe,"pagination":{"page":meta.get("page",1),"pageSize":ps,"total":int(count),"totalPages":math.ceil(int(count)/ps) if ps else 0}}, False

def get_rejeicoes(filters, meta):
    rows=_run(f"SELECT motivo, linha, criado_em FROM {bq._safe_ident(bq.DB_SCHEMA)}.logs_rejeicoes_import ORDER BY criado_em DESC LIMIT :limit OFFSET :offset", {"limit":meta.get("limit",50),"offset":meta.get("offset",0)}, "gestao_rejeicoes")
    return {"items":[mask_rejection_row(r) for r in rows],"pagination":{"page":meta.get("page",1),"page_size":meta.get("limit",50),"total":len(rows),"total_pages":1}}, False

def get_opcoes(filters, meta): return bq.query_options(), False

def _csv_bytes(rows, headers=None):
    bio=io.StringIO(); w=csv.DictWriter(bio, fieldnames=headers or sorted({k for r in rows for k in r}), delimiter=';', extrasaction='ignore'); w.writeheader(); w.writerows(rows); return ('\ufeff'+bio.getvalue()).encode('utf-8')
def export_qualidade(filters, meta, tipo="sem_status"):
    data,_=get_qualidade_detalhes(filters,meta,tipo); rows=data["items"]; return f"qualidade_{tipo}_{datetime.now():%Y%m%d_%H%M%S}.csv", _csv_bytes(rows, ["motivo","identificador","nome","curso","consultor","data_inscricao","data_upload","origem","status"]), len(rows)
def export_importacoes(filters, meta):
    if not meta: meta={"pageSize":100,"offset":0,"page":1}
    rows=[{k:v for k,v in r.items() if k.lower() not in {"payload","cpf","email","celular"}} for r in _run(f"SELECT * FROM {bq._safe_ident(bq.DB_SCHEMA)}.vw_historico_importacoes ORDER BY criado_em DESC LIMIT 1000",{},"import_export")]
    return f"historico_importacoes_{datetime.now():%Y%m%d_%H%M%S}.csv", _csv_bytes(rows), len(rows)
def export_fila(filters, meta):
    rows=_run(_fila_sql(meta.get("limit",100),meta.get("offset",0)), {"limit":meta.get("limit",100),"offset":meta.get("offset",0)}, "gestao_fila_exportar")
    rows=[mask_rejection_row(r) for r in rows]; return f"fila_{datetime.now():%Y%m%d_%H%M%S}.csv", _csv_bytes(rows), len(rows)
def export_rejeicoes(filters, meta): data,_=get_rejeicoes(filters,meta); return f"rejeicoes_{datetime.now():%Y%m%d_%H%M%S}.csv", _csv_bytes(data["items"]), len(data["items"])
def export_produtividade(filters, meta): data,_=get_produtividade(filters,meta); return f"produtividade_{datetime.now():%Y%m%d_%H%M%S}.csv", _csv_bytes(data["items"]), len(data["items"])

def criar_log_importacao(**kw):
    params = {
        "upload_id": kw.get("upload_id"),
        "id_importacao": kw.get("id_importacao"),
        "nome_arquivo": kw.get("nome_arquivo"),
        "tipo_arquivo": kw.get("tipo_arquivo"),
        "tamanho_arquivo_bytes": int(kw.get("tamanho_arquivo_bytes") or 0),
        "usuario": kw.get("usuario") or "desconhecido",
        "correlation_id": kw.get("correlation_id"),
    }
    schema = bq._safe_ident(bq.DB_SCHEMA)
    bq._run_gestao_query(
        f"""
        INSERT INTO {schema}.logs_importacoes (upload_id, id_importacao, nome_arquivo, tipo_arquivo, tamanho_arquivo_bytes, usuario, status, etapa, mensagem, correlation_id, criado_em, iniciado_em, atualizado_em)
        VALUES (:upload_id, :id_importacao, :nome_arquivo, :tipo_arquivo, :tamanho_arquivo_bytes, :usuario, 'RECEBIDO', 'UPLOAD', 'Upload recebido', :correlation_id, now(), now(), now())
        ON CONFLICT (upload_id) DO UPDATE SET id_importacao = EXCLUDED.id_importacao, nome_arquivo = EXCLUDED.nome_arquivo, tipo_arquivo = EXCLUDED.tipo_arquivo, tamanho_arquivo_bytes = EXCLUDED.tamanho_arquivo_bytes, usuario = EXCLUDED.usuario, status = EXCLUDED.status, etapa = EXCLUDED.etapa, mensagem = EXCLUDED.mensagem, correlation_id = EXCLUDED.correlation_id, atualizado_em = now()
        """,
        params,
        "import_log_create",
    )
    return {"upload_id": kw.get("upload_id"), "success": True}


def atualizar_log_importacao(upload_id, **kw):
    allowed = {"status", "etapa", "mensagem", "total_linhas", "linhas_recebidas", "linhas_validas", "linhas_inseridas", "linhas_atualizadas", "linhas_ignoradas", "linhas_rejeitadas", "duplicados_arquivo", "duplicados_banco", "erros", "duracao_ms", "correlation_id"}
    params = {"upload_id": upload_id}
    sets = ["atualizado_em = now()"]
    for key, value in kw.items():
        if key in allowed:
            sets.append(f"{key} = :{key}")
            params[key] = value
    if "detalhes_json" in kw:
        sets.append("detalhes_json = CAST(:detalhes_json AS jsonb)")
        params["detalhes_json"] = json.dumps(kw.get("detalhes_json") or {}, ensure_ascii=False)
    if kw.get("finalizado"):
        sets.append("finalizado_em = COALESCE(finalizado_em, now())")
    schema = bq._safe_ident(bq.DB_SCHEMA)
    bq._run_gestao_query(f"UPDATE {schema}.logs_importacoes SET {', '.join(sets)} WHERE upload_id = :upload_id", params, "import_log_update")
    return {"success": True}
