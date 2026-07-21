# -*- coding: utf-8 -*-
"""Registro explícito da rota de prévia de importação."""
from __future__ import annotations

import json
import logging
import re
import unicodedata
from typing import Any

from flask import jsonify, request
from services import database as db

logger = logging.getLogger(__name__)

PERSONAL_FIELDS = {"nome", "cpf", "celular", "email"}
ACADEMIC_FIELDS = {
    "curso", "modalidade", "turno", "polo", "origem", "tipo_negocio",
    "data_inscricao", "data_matricula",
}
OPERATIONAL_FIELDS = {
    "consultor_comercial", "consultor_disparo", "status", "status_inscricao",
    "campanha", "canal", "acao_comercial", "tipo_disparo", "peca_disparo",
    "texto_disparo", "observacao", "qtd_acionamentos", "matriculado",
    "flag_matriculado", "data_ultima_acao", "data_disparo",
}
ALIASES = {
    "unidade": "polo", "campus": "polo", "telefone": "celular",
    "telefone_celular": "celular", "whatsapp": "celular", "phone": "celular",
    "fone": "celular", "documento": "cpf", "cpf_aluno": "cpf",
    "consultor": "consultor_comercial", "consultor_venda": "consultor_comercial",
    "consultor_do_disparo": "consultor_disparo", "acao": "acao_comercial",
    "obs": "observacao", "flag_matriculado": "matriculado",
}


def _normalize_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return ALIASES.get(text, text)


def _digits(value: Any) -> str | None:
    text = re.sub(r"\D", "", str(value or ""))
    return text or None


def _clean(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def _comparable(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).strip().casefold()


def register_upload_preview_routes(app) -> None:
    if "api_upload_preview_real" in app.view_functions:
        return

    def api_upload_preview_real():
        try:
            payload = request.get_json(silent=True) or {}
            raw_rows = payload.get("rows") or []
            raw_columns = payload.get("columns") or []
            if not isinstance(raw_rows, list):
                return jsonify({"ok": False, "error": {"code": "INVALID_ROWS", "message": "Linhas inválidas."}}), 400
            if len(raw_rows) > 5000:
                return jsonify({"ok": False, "error": {"code": "PREVIEW_LIMIT", "message": "A prévia aceita até 5.000 linhas."}}), 400

            columns = []
            for item in raw_columns:
                name = _normalize_name(item)
                if name and name not in columns:
                    columns.append(name)
            present = set(columns)
            operational_present = sorted(present & OPERATIONAL_FIELDS)

            rows = []
            for index, raw in enumerate(raw_rows, start=2):
                if not isinstance(raw, dict):
                    continue
                mapped = {_normalize_name(k): _clean(v) for k, v in raw.items()}
                row = {
                    "linha": index,
                    "cpf": _digits(mapped.get("cpf")),
                    "celular": _digits(mapped.get("celular")),
                }
                for field in OPERATIONAL_FIELDS:
                    row[field] = mapped.get(field)
                rows.append(row)

            if not rows:
                return jsonify({"ok": True, "data": {
                    "total": 0, "novos": 0, "existentes": 0, "alterados": 0,
                    "sem_mudanca": 0, "ambiguos": 0, "rejeitados": 0,
                    "limpezas": 0, "campos_operacionais": operational_present,
                }})

            sql = """
            WITH entrada AS (
                SELECT *
                FROM jsonb_to_recordset(CAST(:rows_json AS jsonb)) AS r(
                    linha integer, cpf text, celular text,
                    consultor_comercial text, consultor_disparo text,
                    status text, status_inscricao text, campanha text, canal text,
                    acao_comercial text, tipo_disparo text, peca_disparo text,
                    texto_disparo text, observacao text, qtd_acionamentos text,
                    matriculado text, data_ultima_acao text, data_disparo text
                )
            ),
            base_unificada AS (
                SELECT
                    'LEAD:' || COALESCE(l.sk_pessoa_dim::text, l.sk_pessoa::text) AS entidade,
                    l.sk_pessoa_dim,
                    regexp_replace(COALESCE(l.cpf::text, ''), '[^0-9]', '', 'g') AS cpf_limpo,
                    regexp_replace(COALESCE(l.celular::text, ''), '[^0-9]', '', 'g') AS celular_limpo,
                    l.consultor_comercial,
                    l.consultor_disparo,
                    l.status,
                    l.status_inscricao,
                    l.campanha,
                    l.canal,
                    l.acao_comercial,
                    l.tipo_disparo,
                    l.peca_disparo,
                    l.texto_disparo,
                    l.observacao,
                    l.qtd_acionamentos::text AS qtd_acionamentos,
                    l.flag_matriculado::text AS matriculado,
                    l.data_ultima_acao::text AS data_ultima_acao,
                    l.data_disparo::text AS data_disparo,
                    1 AS prioridade
                FROM modelo_estrela.leads_painel_lite l

                UNION ALL

                SELECT
                    'DIM:' || p.sk_pessoa::text AS entidade,
                    p.sk_pessoa AS sk_pessoa_dim,
                    regexp_replace(COALESCE(p.cpf::text, ''), '[^0-9]', '', 'g') AS cpf_limpo,
                    regexp_replace(COALESCE(p.celular::text, ''), '[^0-9]', '', 'g') AS celular_limpo,
                    l.consultor_comercial,
                    l.consultor_disparo,
                    l.status,
                    l.status_inscricao,
                    l.campanha,
                    l.canal,
                    l.acao_comercial,
                    l.tipo_disparo,
                    l.peca_disparo,
                    l.texto_disparo,
                    l.observacao,
                    l.qtd_acionamentos::text AS qtd_acionamentos,
                    l.flag_matriculado::text AS matriculado,
                    l.data_ultima_acao::text AS data_ultima_acao,
                    l.data_disparo::text AS data_disparo,
                    2 AS prioridade
                FROM modelo_estrela.dim_pessoa p
                LEFT JOIN modelo_estrela.leads_painel_lite l
                  ON l.sk_pessoa_dim = p.sk_pessoa
            ),
            candidatos AS (
                SELECT
                    e.linha,
                    b.*,
                    CASE
                        WHEN e.cpf IS NOT NULL AND b.cpf_limpo = e.cpf THEN 'CPF'
                        WHEN e.celular IS NOT NULL AND b.celular_limpo = e.celular THEN 'CELULAR'
                    END AS tipo_match
                FROM entrada e
                JOIN base_unificada b
                  ON (e.cpf IS NOT NULL AND b.cpf_limpo = e.cpf)
                  OR (e.celular IS NOT NULL AND b.celular_limpo = e.celular)
            ),
            contagens AS (
                SELECT
                    e.linha,
                    COUNT(DISTINCT c.entidade) FILTER (WHERE c.tipo_match = 'CPF') AS cpf_matches,
                    COUNT(DISTINCT c.entidade) FILTER (WHERE c.tipo_match = 'CELULAR') AS celular_matches
                FROM entrada e
                LEFT JOIN candidatos c ON c.linha = e.linha
                GROUP BY e.linha
            ),
            escolhido AS (
                SELECT DISTINCT ON (c.linha)
                    c.*
                FROM candidatos c
                JOIN contagens x ON x.linha = c.linha
                WHERE
                    (x.cpf_matches = 1 AND c.tipo_match = 'CPF')
                    OR (x.cpf_matches = 0 AND x.celular_matches = 1 AND c.tipo_match = 'CELULAR')
                ORDER BY c.linha, c.prioridade, c.entidade
            )
            SELECT
                e.*,
                COALESCE(x.cpf_matches, 0) AS cpf_matches,
                COALESCE(x.celular_matches, 0) AS celular_matches,
                esc.entidade,
                esc.sk_pessoa_dim,
                esc.consultor_comercial AS atual_consultor_comercial,
                esc.consultor_disparo AS atual_consultor_disparo,
                esc.status AS atual_status,
                esc.status_inscricao AS atual_status_inscricao,
                esc.campanha AS atual_campanha,
                esc.canal AS atual_canal,
                esc.acao_comercial AS atual_acao_comercial,
                esc.tipo_disparo AS atual_tipo_disparo,
                esc.peca_disparo AS atual_peca_disparo,
                esc.texto_disparo AS atual_texto_disparo,
                esc.observacao AS atual_observacao,
                esc.qtd_acionamentos AS atual_qtd_acionamentos,
                esc.matriculado AS atual_matriculado,
                esc.data_ultima_acao AS atual_data_ultima_acao,
                esc.data_disparo AS atual_data_disparo
            FROM entrada e
            LEFT JOIN contagens x ON x.linha = e.linha
            LEFT JOIN escolhido esc ON esc.linha = e.linha
            ORDER BY e.linha
            """

            result = db._run_gestao_query(
                sql,
                {"rows_json": json.dumps(rows, ensure_ascii=False, default=str)},
                "upload_preview_real",
            )

            metrics = {
                "total": len(rows), "novos": 0, "existentes": 0,
                "alterados": 0, "sem_mudanca": 0, "ambiguos": 0,
                "rejeitados": 0, "limpezas": 0,
            }
            examples = {"ambiguos": [], "rejeitados": []}

            for item in result:
                cpf = item.get("cpf")
                celular = item.get("celular")
                cpf_matches = int(item.get("cpf_matches") or 0)
                celular_matches = int(item.get("celular_matches") or 0)
                entidade = item.get("entidade")

                if not cpf and not celular:
                    metrics["rejeitados"] += 1
                    if len(examples["rejeitados"]) < 5:
                        examples["rejeitados"].append(item.get("linha"))
                    continue

                if cpf_matches > 1 or (cpf_matches == 0 and celular_matches > 1):
                    metrics["ambiguos"] += 1
                    if len(examples["ambiguos"]) < 5:
                        examples["ambiguos"].append(item.get("linha"))
                    continue

                if not entidade:
                    metrics["novos"] += 1
                    continue

                metrics["existentes"] += 1
                changed = False
                for field in operational_present:
                    incoming = item.get(field)
                    current = item.get(f"atual_{field}")
                    if incoming is None and current not in (None, ""):
                        metrics["limpezas"] += 1
                    if _comparable(incoming) != _comparable(current):
                        changed = True
                metrics["alterados" if changed else "sem_mudanca"] += 1

            return jsonify({"ok": True, "data": {
                **metrics,
                "campos_operacionais": operational_present,
                "campos_preservados": sorted(present & (PERSONAL_FIELDS | ACADEMIC_FIELDS)),
                "exemplos": examples,
            }})
        except Exception as exc:
            logger.exception("Falha na prévia real da importação.")
            return jsonify({"ok": False, "error": {
                "code": "UPLOAD_PREVIEW_ERROR",
                "message": "Não foi possível comparar o arquivo com o banco.",
                "details": str(exc),
            }}), 500

    app.add_url_rule(
        "/api/upload/preview",
        endpoint="api_upload_preview_real",
        view_func=api_upload_preview_real,
        methods=["POST"],
    )
