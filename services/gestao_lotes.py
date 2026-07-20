# -*- coding: utf-8 -*-
"""Fluxo transacional de lotes da Gestão Operacional."""
from __future__ import annotations

import base64
import csv
import io
import json
import re
import unicodedata
import uuid
from datetime import datetime
from typing import Any

import pandas as pd
from sqlalchemy import text

from . import database as db
from . import gestao_operacional as core

SCHEMA = core.SCHEMA
SCHEMA_IDENT = core.SCHEMA_IDENT
DatabaseSchemaError = core.DatabaseSchemaError


def _result(data):
    return data, False


def _normalize(value: Any) -> str:
    value = unicodedata.normalize("NFKD", str(value or ""))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _first_relation(*names: str) -> str | None:
    return next((name for name in names if core._relation_exists(name)), None)


def _source_relation() -> str:
    relation = _first_relation("vw_op_fila_priorizada", "vw_leads_painel_lite", "leads_painel_lite")
    if not relation:
        raise DatabaseSchemaError("Fila/base de leads não encontrada.")
    return relation


def _association_relation() -> str:
    relation = _first_relation("op_lote_leads", "op_lotes_leads", "op_lote_lead")
    if not relation:
        raise DatabaseSchemaError("Tabela de associação entre lotes e leads não encontrada.")
    return relation


def _filters(payload: dict) -> dict:
    nested = payload.get("filtros") if isinstance(payload.get("filtros"), dict) else {}
    keys = {
        "curso", "polo", "unidade", "origem", "modalidade", "turno", "campanha",
        "canal", "tipo_disparo", "tipo_negocio", "consultor_disparo", "consultor", "status",
    }
    return {**nested, **{k: v for k, v in payload.items() if k in keys}}


def _apply_filters(sql: str, params: dict, columns: set[str], filters: dict) -> str:
    aliases = {"unidade": "polo", "consultor": "consultor_disparo"}
    used = set()
    for raw_key, value in filters.items():
        key = aliases.get(raw_key, raw_key)
        if key in used or key not in columns or value in (None, "", []):
            continue
        used.add(key)
        values = value if isinstance(value, list) else [value]
        values = [str(v).strip() for v in values if str(v).strip()]
        if not values:
            continue
        clauses = []
        for index, item in enumerate(values):
            pname = f"f_{key}_{index}"
            clauses.append(f"COALESCE({key}::text,'') ILIKE :{pname}")
            params[pname] = item
        sql += " AND (" + " OR ".join(clauses) + ")"
    return sql


def _quantity(payload: dict, default: int = 100) -> int:
    try:
        value = int(payload.get("quantidade") or payload.get("limit") or default)
    except Exception as exc:
        raise ValueError("Quantidade do lote é inválida.") from exc
    return max(1, min(value, 50000))


def _lead_select(payload: dict, count: bool = False):
    source = _source_relation()
    source_cols = core._columns(source)
    lead_col = core._first_col(source_cols, "sk_pessoa", "lead_id", "pessoa_id", "id")
    if not lead_col:
        raise DatabaseSchemaError("A base de leads não possui identificador de lead.")
    association = _first_relation("op_lote_leads", "op_lotes_leads", "op_lote_lead")
    select = "COUNT(*)::bigint AS total" if count else "*"
    sql = f"SELECT {select} FROM {SCHEMA_IDENT}.{source} s WHERE 1=1"
    params = {}
    sql = _apply_filters(sql, params, source_cols, _filters(payload))
    if association:
        assoc_cols = core._columns(association)
        assoc_lead = core._first_col(assoc_cols, "sk_pessoa", "lead_id", "pessoa_id")
        if assoc_lead:
            sql += (
                f" AND NOT EXISTS (SELECT 1 FROM {SCHEMA_IDENT}.{association} a "
                f"WHERE a.{assoc_lead}::text=s.{lead_col}::text)"
            )
    if not count:
        order_col = core._first_col(source_cols, "score_prioridade", "data_inscricao", lead_col)
        if order_col:
            sql += f" ORDER BY s.{order_col} DESC NULLS LAST"
        params["limit"] = _quantity(payload)
        sql += " LIMIT :limit"
    return sql, params, source, source_cols, lead_col


def preview_proximo_lote(filters=None, meta=None):
    payload = dict(filters or {})
    if meta:
        payload.update({k: v for k, v in dict(meta).items() if k not in payload})
    count_sql, count_params, *_ = _lead_select(payload, count=True)
    item_sql, item_params, *_ = _lead_select(payload, count=False)
    total_rows = core._rows(count_sql, count_params, "gestao_preview_lote_count")
    items = core._rows(item_sql, item_params, "gestao_preview_lote_items")
    total = int((total_rows or [{}])[0].get("total") or 0)
    return _result({
        "items": items,
        "leads": items,
        "total": total,
        "total_disponivel": total,
        "quantidade_preview": len(items),
    })


def _insert(conn, table: str, values: dict, columns: set[str]):
    selected = {key: value for key, value in values.items() if key in columns and value is not None}
    if not selected:
        raise DatabaseSchemaError(f"Nenhuma coluna compatível para inserir em {table}.")
    names = ", ".join(db._safe_ident(key) for key in selected)
    binds = ", ".join(f":{key}" for key in selected)
    return conn.execute(
        text(f"INSERT INTO {SCHEMA_IDENT}.{table} ({names}) VALUES ({binds})"),
        selected,
    ).rowcount


def criar_lote(payload, *_args, **_kwargs):
    payload = dict(payload or {})
    quantity = _quantity(payload)
    filters = _filters(payload)
    source_sql, source_params, _source, source_cols, source_lead_col = _lead_select(
        {**payload, "quantidade": quantity}, count=False
    )
    lot_table = "op_lotes_disparo"
    lot_cols = core._require_relation(lot_table)
    assoc_table = _association_relation()
    assoc_cols = core._columns(assoc_table)
    lot_id_col = core._first_col(lot_cols, "lote_id", "id")
    assoc_lot_col = core._first_col(assoc_cols, "lote_id", "id_lote")
    assoc_lead_col = core._first_col(assoc_cols, "sk_pessoa", "lead_id", "pessoa_id")
    if not all((lot_id_col, assoc_lot_col, assoc_lead_col, source_lead_col)):
        raise DatabaseSchemaError("As estruturas operacionais não possuem identificadores compatíveis.")

    user = str(payload.get("usuario") or payload.get("criado_por") or "SISTEMA")
    lot_id = str(uuid.uuid4())
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    lot_name = str(payload.get("nome_lote") or f"LOTE_{stamp}_{user.split('@')[0]}").strip()
    values = {
        lot_id_col: lot_id,
        "nome_lote": lot_name,
        "status_lote": "ABERTO",
        "status": "ABERTO",
        "tipo_disparo": payload.get("tipo_disparo") or filters.get("tipo_disparo"),
        "consultor_disparo": payload.get("consultor_disparo") or filters.get("consultor_disparo"),
        "campanha": payload.get("campanha") or filters.get("campanha"),
        "quantidade": quantity,
        "total_leads": quantity,
        "criado_por": user,
        "usuario": user,
        "filtros_json": json.dumps(filters, ensure_ascii=False, default=str),
        "filtros": json.dumps(filters, ensure_ascii=False, default=str),
    }

    with db.get_engine().begin() as conn:
        selected_rows = conn.execute(text(source_sql), source_params).mappings().all()
        lead_ids = [str(row[source_lead_col]) for row in selected_rows if row.get(source_lead_col) is not None]
        if not lead_ids:
            raise ValueError("Nenhum lead disponível atende aos filtros selecionados.")
        _insert(conn, lot_table, values, lot_cols)
        inserted = 0
        for lead_id in lead_ids:
            inserted += int(_insert(conn, assoc_table, {
                assoc_lot_col: lot_id,
                assoc_lead_col: lead_id,
                "status": "PENDENTE",
                "status_lead": "PENDENTE",
                "criado_por": user,
                "usuario": user,
            }, assoc_cols) or 0)
        if inserted != len(lead_ids):
            raise RuntimeError("Nem todos os leads foram associados; a criação foi desfeita.")
        count_col = core._first_col(lot_cols, "quantidade", "total_leads", "qtd_leads")
        if count_col:
            conn.execute(
                text(f"UPDATE {SCHEMA_IDENT}.{lot_table} SET {count_col}=:total WHERE {lot_id_col}::text=:lote_id"),
                {"total": inserted, "lote_id": lot_id},
            )

    core._audit("LOTE_CRIADO", lot_id, user, json.dumps({"quantidade": len(lead_ids), "filtros": filters}, ensure_ascii=False))
    return _result({
        "success": True,
        "ok": True,
        "lote_id": lot_id,
        "nome_lote": lot_name,
        "status_lote": "ABERTO",
        "quantidade_liberada": len(lead_ids),
        "download_url": f"/api/gestao/lotes/{lot_id}/csv",
        "nome_arquivo_exportado": f"{_normalize(lot_name) or 'lote'}.csv",
    })


def _lot_rows(lote_id) -> list[dict]:
    export_view = _first_relation("vw_op_export_lote_csv")
    if export_view:
        cols = core._columns(export_view)
        lot_col = core._first_col(cols, "lote_id", "id_lote")
        if lot_col:
            return core._rows(
                f"SELECT * FROM {SCHEMA_IDENT}.{export_view} WHERE {lot_col}::text=:lote_id",
                {"lote_id": str(lote_id)},
                "gestao_lote_csv_view",
            )
    assoc = _association_relation()
    assoc_cols = core._columns(assoc)
    assoc_lot = core._first_col(assoc_cols, "lote_id", "id_lote")
    assoc_lead = core._first_col(assoc_cols, "sk_pessoa", "lead_id", "pessoa_id")
    leads = _first_relation("vw_leads_painel_lite", "leads_painel_lite")
    if not leads:
        raise DatabaseSchemaError("Base de leads não encontrada para exportação.")
    lead_cols = core._columns(leads)
    lead_id = core._first_col(lead_cols, "sk_pessoa", "lead_id", "pessoa_id", "id")
    if not all((assoc_lot, assoc_lead, lead_id)):
        raise DatabaseSchemaError("Não foi possível relacionar os leads do lote.")
    return core._rows(
        f"SELECT l.* FROM {SCHEMA_IDENT}.{assoc} a "
        f"JOIN {SCHEMA_IDENT}.{leads} l ON l.{lead_id}::text=a.{assoc_lead}::text "
        f"WHERE a.{assoc_lot}::text=:lote_id",
        {"lote_id": str(lote_id)},
        "gestao_lote_csv_join",
    )


def get_lote_csv(lote_id):
    detail, _ = core.get_lote_detalhe(lote_id)
    rows = _lot_rows(lote_id)
    if not rows:
        raise ValueError("O lote não possui leads para exportação.")
    output = io.StringIO(newline="")
    output.write("\ufeff")
    writer = csv.DictWriter(output, fieldnames=db.EXPORT_ORDER, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for row in rows:
        normalized = dict(row)
        normalized.setdefault("unidade", normalized.get("polo"))
        normalized.setdefault("matriculado", normalized.get("flag_matriculado"))
        writer.writerow({column: normalized.get(column, "") for column in db.EXPORT_ORDER})
    lot_name = str(detail.get("nome_lote") or f"lote_{lote_id}")
    return f"{_normalize(lot_name) or 'lote'}.csv", output.getvalue(), len(rows)


def exportar_proximo_lote(payload):
    created, _ = criar_lote(payload)
    filename, content, count = get_lote_csv(created["lote_id"])
    return _result({
        **created,
        "filename": filename,
        "base64": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "content_type": "text/csv",
        "rows": count,
    })


def _read_file(file) -> pd.DataFrame:
    filename = str(getattr(file, "filename", "") or "").lower()
    raw = file.read()
    if not raw:
        raise ValueError("O arquivo está vazio.")
    if filename.endswith((".xlsx", ".xls")):
        frame = pd.read_excel(io.BytesIO(raw), dtype=str)
    else:
        frame = None
        last_error = None
        for encoding in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                frame = pd.read_csv(io.StringIO(raw.decode(encoding)), sep=None, engine="python", dtype=str)
                break
            except Exception as exc:
                last_error = exc
        if frame is None:
            raise ValueError("Não foi possível ler o CSV.") from last_error
    frame = frame.fillna("")
    frame.columns = [_normalize(column) for column in frame.columns]
    return frame.loc[:, ~frame.columns.duplicated()]


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "sim", "s", "yes", "matriculado"}


def importar_retorno_lote(file, lote_id, usuario="SISTEMA"):
    frame = _read_file(file)
    if frame.empty:
        raise ValueError("O arquivo não possui linhas para importar.")
    assoc = _association_relation()
    assoc_cols = core._columns(assoc)
    assoc_lot = core._first_col(assoc_cols, "lote_id", "id_lote")
    assoc_lead = core._first_col(assoc_cols, "sk_pessoa", "lead_id", "pessoa_id")
    lead_table = _first_relation("leads_painel_lite")
    if not lead_table:
        raise DatabaseSchemaError("Tabela editável de leads não encontrada.")
    lead_cols = core._columns(lead_table)
    lead_id = core._first_col(lead_cols, "sk_pessoa", "lead_id", "pessoa_id", "id")
    if not all((assoc_lot, assoc_lead, lead_id)):
        raise DatabaseSchemaError("Não foi possível identificar os leads do lote.")

    key = next((column for column in ("sk_pessoa", "lead_id", "pessoa_id", "cpf", "celular", "email") if column in frame.columns), None)
    if not key:
        raise ValueError("O arquivo precisa conter sk_pessoa, CPF, celular ou e-mail.")
    db_key = lead_id if key in {"sk_pessoa", "lead_id", "pessoa_id"} else key
    if db_key not in lead_cols:
        raise ValueError(f"A coluna de identificação '{key}' não existe na base.")

    mutable = [column for column in (
        "status_inscricao", "status", "observacao", "data_ultima_acao", "qtd_acionamentos",
        "data_disparo", "peca_disparo", "texto_disparo", "consultor_disparo", "tipo_disparo",
        "campanha", "data_matricula", "flag_matriculado", "matriculado", "canal",
        "acao_comercial", "consultor_comercial",
    ) if column in lead_cols]
    updated = 0
    totals = {"retorno": 0, "positivo": 0, "negativo": 0, "matriculas": 0}
    batch_id = str(uuid.uuid4())

    with db.get_engine().begin() as conn:
        for row in frame.to_dict("records"):
            identifier = str(row.get(key) or "").strip()
            if not identifier:
                continue
            values = {}
            for column in mutable:
                source_column = "matriculado" if column == "flag_matriculado" and "matriculado" in row else column
                raw_value = row.get(source_column)
                if source_column not in row or str(raw_value or "").strip() == "":
                    continue
                if column in {"flag_matriculado", "matriculado"}:
                    raw_value = _truthy(raw_value)
                elif column == "qtd_acionamentos":
                    try:
                        raw_value = int(float(str(raw_value).replace(",", ".")))
                    except Exception:
                        continue
                values[column] = raw_value
            if not values:
                continue
            assignments = ", ".join(f"{db._safe_ident(column)}=:{column}" for column in values)
            result = conn.execute(text(
                f"UPDATE {SCHEMA_IDENT}.{lead_table} l SET {assignments} "
                f"WHERE l.{db_key}::text=:identifier AND EXISTS ("
                f"SELECT 1 FROM {SCHEMA_IDENT}.{assoc} a "
                f"WHERE a.{assoc_lot}::text=:lote_id AND a.{assoc_lead}::text=l.{lead_id}::text)"
            ), {**values, "identifier": identifier, "lote_id": str(lote_id)})
            updated += int(result.rowcount or 0)
            status_text = f"{row.get('status', '')} {row.get('status_inscricao', '')}".lower()
            totals["retorno"] += int("retorno" in status_text)
            totals["positivo"] += int(any(token in status_text for token in ("positivo", "interessado", "matricula")))
            totals["negativo"] += int(any(token in status_text for token in ("negativo", "sem interesse", "nao interessado")))
            totals["matriculas"] += int(_truthy(row.get("matriculado")) or bool(str(row.get("data_matricula") or "").strip()))
        if updated <= 0:
            raise ValueError("Nenhum lead do arquivo foi encontrado no lote selecionado.")
        lot_cols = core._columns("op_lotes_disparo")
        lot_id_col = core._first_col(lot_cols, "lote_id", "id")
        lot_status_col = core._first_col(lot_cols, "status_lote", "status")
        if lot_id_col and lot_status_col:
            conn.execute(text(
                f"UPDATE {SCHEMA_IDENT}.op_lotes_disparo SET {lot_status_col}='IMPORTADO' "
                f"WHERE {lot_id_col}::text=:lote_id"
            ), {"lote_id": str(lote_id)})

    detail, _ = core.get_lote_detalhe(lote_id)
    core._audit("LOTE_RETORNO_IMPORTADO", lote_id, usuario, json.dumps({
        "batch_id": batch_id, "linhas": len(frame), "atualizados": updated,
    }, ensure_ascii=False))
    return _result({
        "success": True,
        "ok": True,
        "lote_id": str(lote_id),
        "nome_lote": detail.get("nome_lote"),
        "import_batch_id": batch_id,
        "linhas_recebidas": int(len(frame)),
        "leads_atualizados": updated,
        "total_retorno": totals["retorno"],
        "total_positivo": totals["positivo"],
        "total_negativo": totals["negativo"],
        "total_matriculas": totals["matriculas"],
    })


def importar_lote_disparado(file, lote_id, usuario="SISTEMA"):
    return importar_retorno_lote(file, lote_id, usuario)
