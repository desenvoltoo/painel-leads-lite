# -*- coding: utf-8 -*-
"""Serviço da Gestão Operacional usando PostgreSQL/Supabase.

Este módulo evita respostas de sucesso simuladas: operações somente retornam
sucesso depois de uma alteração real no banco. Estruturas opcionais são
identificadas em tempo de execução para manter compatibilidade entre ambientes.
"""
from __future__ import annotations

import os
import uuid
from typing import Any, Dict

from . import database as db


class DatabaseSchemaError(RuntimeError):
    pass


SCHEMA = os.getenv("DB_SCHEMA", db.DB_SCHEMA).strip() or "modelo_estrela"
SCHEMA_IDENT = db._safe_ident(SCHEMA)


def classify_database_error(exc: Exception):
    return {
        "error_type": "DATABASE_ERROR",
        "message": "Erro técnico ao consultar o PostgreSQL.",
        "details": str(exc),
    }


def parse_operational_request(args=None, json_payload=None):
    return dict(args or {}), dict(json_payload or {})


def _result(data: Any):
    return data, False


def _rows(sql: str, params: Dict[str, Any] | None = None, name: str = "gestao_operacional"):
    return db._run_gestao_query(sql, params or {}, name)


def _relation_exists(name: str) -> bool:
    rows = _rows(
        """
        SELECT EXISTS (
          SELECT 1 FROM information_schema.tables
          WHERE table_schema = :schema AND table_name = :name
          UNION ALL
          SELECT 1 FROM information_schema.views
          WHERE table_schema = :schema AND table_name = :name
        ) AS existe
        """,
        {"schema": SCHEMA, "name": name},
        "gestao_relation_exists",
    )
    return bool(rows and rows[0].get("existe"))


def _columns(name: str) -> set[str]:
    if not _relation_exists(name):
        return set()
    rows = _rows(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :name
        """,
        {"schema": SCHEMA, "name": name},
        "gestao_relation_columns",
    )
    return {str(row.get("column_name")) for row in rows}


def _require_relation(name: str) -> set[str]:
    cols = _columns(name)
    if not cols:
        raise DatabaseSchemaError(f"Estrutura obrigatória não encontrada: {SCHEMA}.{name}.")
    return cols


def _first_col(columns: set[str], *candidates: str) -> str | None:
    return next((candidate for candidate in candidates if candidate in columns), None)


def _limit(filters=None, meta=None, default=200, maximum=1000):
    raw = (meta or {}).get("limit") or (filters or {}).get("limit") or default
    try:
        return max(1, min(int(raw), maximum))
    except Exception:
        return default


def _offset(filters=None, meta=None):
    raw = (meta or {}).get("offset") or (filters or {}).get("offset") or 0
    try:
        return max(0, int(raw))
    except Exception:
        return 0


def _unsupported(action: str):
    raise ValueError(
        f"A operação '{action}' ainda não possui implementação segura no PostgreSQL. "
        "Nenhuma alteração foi realizada."
    )


def _audit(action: str, entity_id: Any = None, user: str = "SISTEMA", details: str = "") -> None:
    table = "op_auditoria_painel"
    cols = _columns(table)
    if not cols:
        return
    values = {
        "acao": action,
        "evento": action,
        "tipo_evento": action,
        "entidade_id": str(entity_id or ""),
        "referencia_id": str(entity_id or ""),
        "lote_id": str(entity_id or ""),
        "usuario": user,
        "usuario_email": user,
        "criado_por": user,
        "detalhes": details,
        "descricao": details,
    }
    selected = [(key, value) for key, value in values.items() if key in cols and value not in (None, "")]
    if not selected:
        return
    names = ", ".join(db._safe_ident(key) for key, _ in selected)
    binds = ", ".join(f":{key}" for key, _ in selected)
    params = {key: value for key, value in selected}
    try:
        _rows(
            f"INSERT INTO {SCHEMA_IDENT}.{table} ({names}) VALUES ({binds})",
            params,
            "gestao_auditoria_insert",
        )
    except Exception:
        pass


def create_operational_tables():
    return _result({"created": [], "message": "Estruturas são gerenciadas por migrações SQL."})


def get_dashboard(*_args, **_kwargs):
    dashboard = {}
    if _relation_exists("vw_op_dashboard_cards"):
        rows = _rows(f"SELECT * FROM {SCHEMA_IDENT}.vw_op_dashboard_cards LIMIT 1", name="gestao_dashboard_view")
        dashboard = rows[0] if rows else {}

    defaults = {
        "leads_novos_disponiveis": 0, "leads_redisparo_disponiveis": 0,
        "leads_em_lotes": 0, "leads_pendentes": 0, "leads_em_atendimento": 0,
        "total_lotes": 0, "lotes_abertos": 0, "lotes_em_andamento": 0,
        "lotes_importados": 0, "lotes_concluidos": 0, "lotes_cancelados": 0,
        "retornos": 0, "positivos": 0, "negativos": 0, "matriculas": 0,
        "taxa_retorno_pct": 0, "taxa_matricula_pct": 0,
    }
    defaults.update(dashboard or {})
    if not dashboard and _relation_exists("leads_painel_lite"):
        cols = _columns("leads_painel_lite")
        matricula_col = _first_col(cols, "flag_matriculado", "matriculado")
        matricula_expr = f"COUNT(*) FILTER (WHERE COALESCE({matricula_col}, false))" if matricula_col else "0"
        base = _rows(
            f"SELECT COUNT(*)::bigint AS total, ({matricula_expr})::bigint AS matriculas FROM {SCHEMA_IDENT}.leads_painel_lite",
            name="gestao_dashboard_base_fallback",
        )
        if base:
            defaults["leads_novos_disponiveis"] = int(base[0].get("total") or 0)
            defaults["matriculas"] = int(base[0].get("matriculas") or 0)
    return _result(defaults)


def get_consultor_momento(filters=None, meta=None):
    filters = filters or {}
    if not _relation_exists("vw_op_consultor_momento"):
        return _result({"items": [], "total": 0, "warning": "View vw_op_consultor_momento não encontrada."})
    limit, offset = _limit(filters, meta), _offset(filters, meta)
    consultor = str(filters.get("consultor_disparo") or "").strip()
    sql = f"SELECT * FROM {SCHEMA_IDENT}.vw_op_consultor_momento WHERE 1=1"
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if consultor:
        sql += " AND COALESCE(consultor_disparo::text, '') ILIKE :consultor"
        params["consultor"] = f"%{consultor}%"
    sql += " ORDER BY matriculas DESC NULLS LAST, trabalhados DESC NULLS LAST, consultor_disparo LIMIT :limit OFFSET :offset"
    items = _rows(sql, params, "gestao_consultores")
    return _result({"items": items, "total": len(items)})


def get_lotes(filters=None, meta=None):
    filters = filters or {}
    relation = "vw_op_lotes_resumo" if _relation_exists("vw_op_lotes_resumo") else "op_lotes_disparo"
    cols = _require_relation(relation)
    limit, offset = _limit(filters, meta), _offset(filters, meta)
    status_col = _first_col(cols, "status_lote", "status")
    id_col = _first_col(cols, "lote_id", "id")
    order_col = _first_col(cols, "exportado_em", "created_at", "criado_em", id_col or "")
    sql = f"SELECT * FROM {SCHEMA_IDENT}.{relation} WHERE 1=1"
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if status_col and filters.get("status"):
        sql += f" AND {status_col}::text = :status"
        params["status"] = str(filters["status"])
    if filters.get("q") or filters.get("busca"):
        q = str(filters.get("q") or filters.get("busca"))
        searchable = [c for c in ("nome_lote", "tipo_disparo", "consultor_disparo") if c in cols]
        if searchable:
            sql += " AND (" + " OR ".join(f"COALESCE({c}::text,'') ILIKE :q" for c in searchable) + ")"
            params["q"] = f"%{q}%"
    if order_col:
        sql += f" ORDER BY {order_col} DESC NULLS LAST"
    sql += " LIMIT :limit OFFSET :offset"
    items = _rows(sql, params, "gestao_lotes")
    return _result({"items": items, "total": len(items)})


def get_lotes_select(*_args, **_kwargs):
    data, _ = get_lotes({"limit": 500}, {})
    return _result({"items": data.get("items", [])})


def get_lote_detalhe(lote_id):
    relation = "vw_op_lotes_resumo" if _relation_exists("vw_op_lotes_resumo") else "op_lotes_disparo"
    cols = _require_relation(relation)
    id_col = _first_col(cols, "lote_id", "id")
    if not id_col:
        raise DatabaseSchemaError(f"{relation} não possui identificador de lote.")
    rows = _rows(
        f"SELECT * FROM {SCHEMA_IDENT}.{relation} WHERE {id_col}::text = :lote_id LIMIT 1",
        {"lote_id": str(lote_id)},
        "gestao_lote_detalhe",
    )
    if not rows:
        raise ValueError("Lote não encontrado.")
    return _result(rows[0])


def _change_lote_status(lote_id, target: str, allowed: set[str] | None = None, user: str = "SISTEMA"):
    table = "op_lotes_disparo"
    cols = _require_relation(table)
    id_col = _first_col(cols, "lote_id", "id")
    status_col = _first_col(cols, "status_lote", "status")
    if not id_col or not status_col:
        raise DatabaseSchemaError("Tabela de lotes sem colunas de identificação/status.")
    current_rows = _rows(
        f"SELECT {status_col}::text AS status FROM {SCHEMA_IDENT}.{table} WHERE {id_col}::text=:lote_id LIMIT 1",
        {"lote_id": str(lote_id)},
        "gestao_lote_status_atual",
    )
    if not current_rows:
        raise ValueError("Lote não encontrado.")
    current = str(current_rows[0].get("status") or "").upper()
    if current == target:
        return _result({"success": True, "lote_id": str(lote_id), "status_lote": target, "unchanged": True})
    if allowed and current not in allowed:
        raise ValueError(f"Não é possível alterar o lote de {current or 'SEM_STATUS'} para {target}.")

    assignments = [f"{status_col}=:status"]
    params: Dict[str, Any] = {"status": target, "lote_id": str(lote_id)}
    timestamp_candidates = {
        "EM_ANDAMENTO": ("disparado_em", "iniciado_em", "updated_at"),
        "CONCLUIDO": ("concluido_em", "finalizado_em", "updated_at"),
        "CANCELADO": ("cancelado_em", "updated_at"),
    }
    stamp_col = _first_col(cols, *timestamp_candidates.get(target, ("updated_at",)))
    if stamp_col:
        assignments.append(f"{stamp_col}=CURRENT_TIMESTAMP")
    user_col = _first_col(cols, "atualizado_por", "usuario_atualizacao")
    if user_col:
        assignments.append(f"{user_col}=:usuario")
        params["usuario"] = user

    result = _rows(
        f"UPDATE {SCHEMA_IDENT}.{table} SET {', '.join(assignments)} WHERE {id_col}::text=:lote_id",
        params,
        "gestao_lote_change_status",
    )
    if int((result or {}).get("rowcount") or 0) != 1:
        raise RuntimeError("O lote não foi atualizado.")
    _audit(f"LOTE_{target}", lote_id, user, f"Status alterado de {current} para {target}.")
    return _result({"success": True, "lote_id": str(lote_id), "status_anterior": current, "status_lote": target})


def start_lote(lote_id, *_args, **_kwargs):
    return _change_lote_status(lote_id, "EM_ANDAMENTO", {"ABERTO", "EXPORTADO", "IMPORTADO"})


def marcar_lote_disparado(lote_id, usuario="SISTEMA"):
    return _change_lote_status(lote_id, "EM_ANDAMENTO", {"ABERTO", "EXPORTADO", "IMPORTADO"}, usuario)


def finish_lote(lote_id, payload=None):
    user = str((payload or {}).get("usuario") or (payload or {}).get("atualizado_por") or "SISTEMA")
    return _change_lote_status(lote_id, "CONCLUIDO", {"EM_ANDAMENTO", "IMPORTADO"}, user)


def cancelar_lote(lote_id, usuario="SISTEMA"):
    return _change_lote_status(lote_id, "CANCELADO", {"ABERTO", "EXPORTADO", "EM_ANDAMENTO"}, usuario)


def buscar_leads(filters=None, meta=None):
    if isinstance(filters, str):
        filters, meta = {"q": filters}, {"limit": meta or 20}
    filters = dict(filters or {})
    q = str(filters.get("q") or filters.get("busca") or "").strip()
    digits = "".join(ch for ch in q if ch.isdigit())
    limit = _limit(filters, meta, default=100, maximum=500)
    relation = "vw_leads_painel_lite"
    cols = _require_relation(relation)
    params: Dict[str, Any] = {"limit": limit}
    sql = f"SELECT * FROM {SCHEMA_IDENT}.{relation} WHERE 1=1"
    if q:
        clauses = []
        for col in ("nome", "email"):
            if col in cols:
                clauses.append(f"COALESCE({col}::text,'') ILIKE :q")
        params["q"] = f"%{q}%"
        if digits:
            for col in ("cpf", "celular"):
                if col in cols:
                    clauses.append(f"regexp_replace(COALESCE({col}::text,''), '[^0-9]', '', 'g') LIKE :digits")
            params["digits"] = f"%{digits}%"
        if clauses:
            sql += " AND (" + " OR ".join(clauses) + ")"
    order_col = _first_col(cols, "data_atualizacao", "data_inscricao", "sk_pessoa")
    if order_col:
        sql += f" ORDER BY {order_col} DESC NULLS LAST"
    sql += " LIMIT :limit"
    items = _rows(sql, params, "gestao_buscar_leads")
    return _result({"items": items, "total": len(items)})


def get_leads_disponiveis(filters=None, meta=None):
    relation = "vw_op_fila_priorizada" if _relation_exists("vw_op_fila_priorizada") else "vw_leads_painel_lite"
    cols = _require_relation(relation)
    filters = dict(filters or {})
    limit, offset = _limit(filters, meta), _offset(filters, meta)
    sql = f"SELECT * FROM {SCHEMA_IDENT}.{relation} WHERE 1=1"
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    for key in ("curso", "polo", "origem", "consultor_disparo", "tipo_disparo", "status"):
        if key in cols and filters.get(key):
            sql += f" AND COALESCE({key}::text,'') ILIKE :{key}"
            params[key] = f"%{filters[key]}%"
    order_col = _first_col(cols, "score_prioridade", "data_inscricao", "sk_pessoa")
    if order_col:
        direction = "DESC" if order_col == "score_prioridade" else "DESC NULLS LAST"
        sql += f" ORDER BY {order_col} {direction}"
    sql += " LIMIT :limit OFFSET :offset"
    items = _rows(sql, params, "gestao_fila_leads")
    return _result({"items": items, "total": len(items), "source": relation})


def get_lote_atual_leads(filters=None, meta=None):
    filters = dict(filters or {})
    relation = "vw_op_lote_atual_leads" if _relation_exists("vw_op_lote_atual_leads") else "op_lote_leads"
    cols = _require_relation(relation)
    limit, offset = _limit(filters, meta), _offset(filters, meta)
    sql = f"SELECT * FROM {SCHEMA_IDENT}.{relation} WHERE 1=1"
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    for key in ("lote_id", "consultor_disparo", "status"):
        if key in cols and filters.get(key):
            sql += f" AND {key}::text = :{key}"
            params[key] = str(filters[key])
    order_col = _first_col(cols, "score_prioridade", "data_inscricao", "sk_pessoa")
    if order_col:
        sql += f" ORDER BY {order_col} DESC NULLS LAST"
    sql += " LIMIT :limit OFFSET :offset"
    items = _rows(sql, params, "gestao_lote_atual_leads")
    return _result({"items": items, "total": len(items)})


def _lead_history(relation: str, sk_pessoa):
    cols = _require_relation(relation)
    key = _first_col(cols, "sk_pessoa", "pessoa_id", "lead_id")
    if not key:
        raise DatabaseSchemaError(f"{relation} não possui identificador de lead.")
    order_col = _first_col(cols, "created_at", "criado_em", "data_evento", "evento_em")
    sql = f"SELECT * FROM {SCHEMA_IDENT}.{relation} WHERE {key}::text=:lead_id"
    if order_col:
        sql += f" ORDER BY {order_col} DESC NULLS LAST"
    rows = _rows(sql, {"lead_id": str(sk_pessoa)}, f"gestao_{relation}")
    return _result(rows)


def get_lead_timeline(sk_pessoa):
    return _lead_history("op_lead_timeline", sk_pessoa)


def get_lead_lotes(sk_pessoa):
    relation = "op_lote_leads" if _relation_exists("op_lote_leads") else "op_lote_lead"
    return _lead_history(relation, sk_pessoa)


def get_lead_eventos(sk_pessoa):
    return _lead_history("op_lead_eventos", sk_pessoa)


def get_logs_auditoria(kind, args=None, _user_ctx=None):
    args = dict(args or {})
    limit, offset = _limit(args, {}, default=50, maximum=500), _offset(args, {})
    mapping = {
        "importacoes": "vw_historico_importacoes" if _relation_exists("vw_historico_importacoes") else "logs_importacoes",
        "rejeicoes": "vw_gestao_rejeicoes_import" if _relation_exists("vw_gestao_rejeicoes_import") else "logs_rejeicoes_import",
        "auditoria": "op_auditoria_painel", "eventos_leads": "op_lead_eventos",
        "timeline": "op_lead_timeline", "debug_fila": "vw_op_fila_priorizada",
        "database_sync": "op_database_sync",
    }
    relation = mapping.get(kind)
    if not relation or not _relation_exists(relation):
        return _result({"success": True, "ok": True, "data": [], "items": [], "total": 0, "limit": limit, "offset": offset})
    cols = _columns(relation)
    order_col = _first_col(cols, "created_at", "criado_em", "data_evento", "updated_at")
    sql = f"SELECT * FROM {SCHEMA_IDENT}.{relation}"
    if order_col:
        sql += f" ORDER BY {order_col} DESC NULLS LAST"
    sql += " LIMIT :limit OFFSET :offset"
    items = _rows(sql, {"limit": limit, "offset": offset}, f"gestao_logs_{kind}")
    total_rows = _rows(f"SELECT COUNT(*)::bigint AS total FROM {SCHEMA_IDENT}.{relation}", name=f"gestao_logs_{kind}_count")
    total = int((total_rows or [{}])[0].get("total") or 0)
    return _result({"success": True, "ok": True, "data": items, "items": items, "total": total, "limit": limit, "offset": offset})


def listar_usuarios(*_args, **_kwargs):
    table = "op_usuarios_painel"
    cols = _require_relation(table)
    safe_cols = [c for c in ("usuario_id", "nome", "email", "perfil_id", "status_usuario", "ativo", "primeiro_acesso", "created_at", "updated_at") if c in cols]
    order_col = _first_col(cols, "nome", "email", "created_at")
    sql = f"SELECT {', '.join(safe_cols) if safe_cols else '*'} FROM {SCHEMA_IDENT}.{table}"
    if order_col:
        sql += f" ORDER BY {order_col}"
    items = _rows(sql, name="gestao_usuarios_listar")
    return _result({"success": True, "items": items, "data": items, "total": len(items)})


def salvar_usuario(payload, ator="SISTEMA", usuario_id=None):
    payload = dict(payload or {})
    table = "op_usuarios_painel"
    cols = _require_relation(table)
    email = str(payload.get("email") or "").strip().lower()
    nome = str(payload.get("nome") or "").strip()
    if not email or "@" not in email:
        raise ValueError("Informe um e-mail válido.")
    if not nome:
        raise ValueError("Nome do usuário é obrigatório.")
    allowed = {k: v for k, v in payload.items() if k in cols and k not in {"usuario_id", "created_at", "updated_at"}}
    allowed.update({k: v for k, v in {"email": email, "nome": nome}.items() if k in cols})
    if usuario_id:
        stamp = ", updated_at=CURRENT_TIMESTAMP" if "updated_at" in cols else ""
        assignments = ", ".join(f"{db._safe_ident(k)}=:{k}" for k in allowed)
        result = _rows(
            f"UPDATE {SCHEMA_IDENT}.{table} SET {assignments}{stamp} WHERE usuario_id::text=:usuario_id",
            {**allowed, "usuario_id": str(usuario_id)},
            "gestao_usuario_update",
        )
        if int((result or {}).get("rowcount") or 0) != 1:
            raise ValueError("Usuário não encontrado.")
        saved_id = str(usuario_id)
    else:
        saved_id = str(uuid.uuid4())
        if "usuario_id" in cols:
            allowed["usuario_id"] = saved_id
        if "ativo" in cols and "ativo" not in allowed:
            allowed["ativo"] = True
        if "status_usuario" in cols and "status_usuario" not in allowed:
            allowed["status_usuario"] = "ATIVO"
        if "primeiro_acesso" in cols and "primeiro_acesso" not in allowed:
            allowed["primeiro_acesso"] = True
        names = ", ".join(db._safe_ident(k) for k in allowed)
        binds = ", ".join(f":{k}" for k in allowed)
        try:
            _rows(f"INSERT INTO {SCHEMA_IDENT}.{table} ({names}) VALUES ({binds})", allowed, "gestao_usuario_insert")
        except Exception as exc:
            if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
                raise ValueError("Já existe um usuário com este e-mail.") from exc
            raise
    _audit("USUARIO_SALVO", saved_id, ator, email)
    return _result({"success": True, "usuario_id": saved_id, "message": "Usuário salvo com sucesso."})


def alterar_status_usuario(usuario_id, ativo, ator="SISTEMA"):
    table = "op_usuarios_painel"
    cols = _require_relation(table)
    assignments, params = [], {"usuario_id": str(usuario_id), "ativo": bool(ativo)}
    if "ativo" in cols:
        assignments.append("ativo=:ativo")
    if "status_usuario" in cols:
        assignments.append("status_usuario=:status")
        params["status"] = "ATIVO" if ativo else "INATIVO"
    if "updated_at" in cols:
        assignments.append("updated_at=CURRENT_TIMESTAMP")
    if not assignments:
        raise DatabaseSchemaError("Tabela de usuários não possui coluna de status.")
    result = _rows(
        f"UPDATE {SCHEMA_IDENT}.{table} SET {', '.join(assignments)} WHERE usuario_id::text=:usuario_id",
        params,
        "gestao_usuario_status",
    )
    if int((result or {}).get("rowcount") or 0) != 1:
        raise ValueError("Usuário não encontrado.")
    _audit("USUARIO_ATIVADO" if ativo else "USUARIO_DESATIVADO", usuario_id, ator)
    return _result({"success": True, "usuario_id": str(usuario_id), "ativo": bool(ativo)})


def resetar_senha_usuario(usuario_id, password_hash, ator="SISTEMA"):
    cols = _require_relation("op_usuarios_painel")
    if "password_hash" not in cols:
        raise DatabaseSchemaError("Tabela de usuários não possui password_hash.")
    assignments = ["password_hash=:password_hash"]
    if "primeiro_acesso" in cols:
        assignments.append("primeiro_acesso=true")
    if "updated_at" in cols:
        assignments.append("updated_at=CURRENT_TIMESTAMP")
    result = _rows(
        f"UPDATE {SCHEMA_IDENT}.op_usuarios_painel SET {', '.join(assignments)} WHERE usuario_id::text=:usuario_id",
        {"usuario_id": str(usuario_id), "password_hash": password_hash},
        "gestao_usuario_reset_senha",
    )
    if int((result or {}).get("rowcount") or 0) != 1:
        raise ValueError("Usuário não encontrado.")
    _audit("USUARIO_SENHA_RESETADA", usuario_id, ator)
    return _result({"success": True, "usuario_id": str(usuario_id), "message": "Senha redefinida."})


def listar_perfis(*_args, **_kwargs):
    if _relation_exists("op_perfis_painel"):
        items = _rows(f"SELECT * FROM {SCHEMA_IDENT}.op_perfis_painel ORDER BY 1", name="gestao_perfis")
    else:
        items = [{"perfil_id": "ADMIN", "nome": "Administrador"}, {"perfil_id": "OPERADOR", "nome": "Operador"}]
    return _result({"success": True, "items": items, "data": items})


def auditoria_usuario(usuario_id):
    if not _relation_exists("op_auditoria_painel"):
        return _result({"success": True, "items": [], "data": []})
    cols = _columns("op_auditoria_painel")
    user_col = _first_col(cols, "usuario_id", "entidade_id", "referencia_id")
    if not user_col:
        return _result({"success": True, "items": [], "data": []})
    items = _rows(
        f"SELECT * FROM {SCHEMA_IDENT}.op_auditoria_painel WHERE {user_col}::text=:usuario_id ORDER BY 1 DESC LIMIT 200",
        {"usuario_id": str(usuario_id)},
        "gestao_usuario_auditoria",
    )
    return _result({"success": True, "items": items, "data": items})


def buscar_usuario_login(email, *_args, **_kwargs):
    cols = _require_relation("op_usuarios_painel")
    if "email" not in cols:
        raise DatabaseSchemaError("Tabela de usuários sem coluna email.")
    rows = _rows(
        f"SELECT * FROM {SCHEMA_IDENT}.op_usuarios_painel WHERE LOWER(email::text)=LOWER(:email) LIMIT 1",
        {"email": str(email or "").strip()},
        "gestao_usuario_login",
    )
    return rows[0] if rows else None


def registrar_login_usuario(usuario_id, *_args, **_kwargs):
    cols = _require_relation("op_usuarios_painel")
    assignments = []
    for col in ("ultimo_login_em", "last_login_at", "updated_at"):
        if col in cols:
            assignments.append(f"{col}=CURRENT_TIMESTAMP")
            break
    if not assignments:
        return _result({"success": True, "unchanged": True})
    _rows(
        f"UPDATE {SCHEMA_IDENT}.op_usuarios_painel SET {', '.join(assignments)} WHERE usuario_id::text=:usuario_id",
        {"usuario_id": str(usuario_id)},
        "gestao_usuario_login_registrar",
    )
    return _result({"success": True})


def atualizar_password_hash_usuario(usuario_id, password_hash, *_args, **_kwargs):
    return resetar_senha_usuario(usuario_id, password_hash, "SISTEMA")


# Operações ainda sem contrato SQL confiável: nunca retornam sucesso falso.
def criar_lote(*_args, **_kwargs): return _unsupported("criar lote operacional")
def get_meus_leads(*_args, **_kwargs): return _unsupported("consultar meus leads")
def update_lead_status(*_args, **_kwargs): return _unsupported("atualizar status do lead")
def liberar_proximos_leads(*_args, **_kwargs): return _unsupported("liberar próximos leads")
def executar_regras_distribuicao(*_args, **_kwargs): return _unsupported("executar regras de distribuição")
def get_esteira_operacional(*_args, **_kwargs): return _unsupported("consultar esteira operacional")
def get_fila_por_prioridade(filters=None, meta=None): return get_leads_disponiveis(filters, meta)
def criar_regra_distribuicao(*_args, **_kwargs): return _unsupported("criar regra de distribuição")
def listar_regras_distribuicao(*_args, **_kwargs): return _result({"items": [], "total": 0})
def ativar_desativar_regra(*_args, **_kwargs): return _unsupported("alterar regra de distribuição")
def preview_proximo_lote(filters=None, *_args, **_kwargs): return get_leads_disponiveis(dict(filters or {}), {"limit": 50})
def exportar_proximo_lote(*_args, **_kwargs): return _unsupported("exportar próximo lote")
def get_lote_csv(*_args, **_kwargs): return _unsupported("baixar CSV do lote")
def importar_lote_disparado(*_args, **_kwargs): return _unsupported("importar lote disparado")
def importar_novos_leads(*_args, **_kwargs): return _unsupported("importar novos leads; utilize /api/upload")
def get_operacao_logs(args=None, *_args, **_kwargs): return get_logs_auditoria("auditoria", args)
def importar_retorno_lote(*_args, **_kwargs): return _unsupported("importar retorno do lote")
def atualizar_lead_lote(*_args, **_kwargs): return _unsupported("atualizar lead no lote")
