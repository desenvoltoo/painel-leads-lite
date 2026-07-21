# -*- coding: utf-8 -*-
"""Extensão automática para simular importações contra o PostgreSQL.

O Python importa ``usercustomize`` depois de ``sitecustomize``. Assim, a rota
é registrada em toda instância Flask sem alterar o app principal.
"""
from __future__ import annotations

import json
import logging
import re
import unicodedata
from typing import Any

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


def _install_upload_preview_extension() -> None:
    try:
        from flask import Flask, jsonify, request
        from services import database as db
    except Exception:
        logger.exception("Não foi possível preparar a prévia real de importação.")
        return

    original_init = Flask.__init__
    if getattr(original_init, "_upload_preview_patched", False):
        return

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)

        if "api_upload_preview_real" in self.view_functions:
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
                    row = {"linha": index, "cpf": _digits(mapped.get("cpf")), "celular": _digits(mapped.get("celular"))}
                    for field in OPERATIONAL_FIELDS:
                        row[field] = mapped.get(field)
                    rows.append(row)

                if not rows:
                    return jsonify({"ok": True, "data": {"total": 0, "novos": 0, "existentes": 0, "alterados": 0, "sem_mudanca": 0, "ambiguos": 0, "rejeitados": 0, "limpezas": 0, "campos_operacionais": operational_present}})

                sql = """
                WITH entrada AS (
                    SELECT *
                    FROM jsonb_to_recordset(CAST(:rows_json AS jsonb)) AS r(
                        linha integer,
                        cpf text,
                        celular text,
                        consultor_comercial text,
                        consultor_disparo text,
                        status text,
                        status_inscricao text,
                        campanha text,
                        canal text,
                        acao_comercial text,
                        tipo_disparo text,
                        peca_disparo text,
                        texto_disparo text,
                        observacao text,
                        qtd_acionamentos text,
                        matriculado text,
                        data_ultima_acao text,
                        data_disparo text
                    )
                ),
                cpf_match AS (
                    SELECT e.linha, MIN(p.sk_pessoa) AS sk_pessoa, COUNT(*) AS qtd
                    FROM entrada e
                    JOIN modelo_estrela.dim_pessoa p
                      ON e.cpf IS NOT NULL
                     AND regexp_replace(COALESCE(p.cpf::text, ''), '[^0-9]', '', 'g') = e.cpf
                    GROUP BY e.linha
                ),
                celular_match AS (
                    SELECT e.linha, MIN(p.sk_pessoa) AS sk_pessoa, COUNT(*) AS qtd
                    FROM entrada e
                    JOIN modelo_estrela.dim_pessoa p
                      ON e.celular IS NOT NULL
                     AND regexp_replace(COALESCE(p.celular::text, ''), '[^0-9]', '', 'g') = e.celular
                    GROUP BY e.linha
                ),
                resolvida AS (
                    SELECT
                        e.*,
                        CASE
                            WHEN COALESCE(cm.qtd, 0) = 1 THEN cm.sk_pessoa
                            WHEN COALESCE(cm.qtd, 0) = 0 AND COALESCE(tm.qtd, 0) = 1 THEN tm.sk_pessoa
                            ELSE NULL
                        END AS sk_pessoa_dim,
                        COALESCE(cm.qtd, 0) AS cpf_matches,
                        COALESCE(tm.qtd, 0) AS celular_matches
                    FROM entrada e
                    LEFT JOIN cpf_match cm ON cm.linha = e.linha
                    LEFT JOIN celular_match tm ON tm.linha = e.linha
                )
                SELECT
                    r.*,
                    l.consultor_comercial AS atual_consultor_comercial,
                    l.consultor_disparo AS atual_consultor_disparo,
                    l.status AS atual_status,
                    l.status_inscricao AS atual_status_inscricao,
                    l.campanha AS atual_campanha,
                    l.canal AS atual_canal,
                    l.acao_comercial AS atual_acao_comercial,
                    l.tipo_disparo AS atual_tipo_disparo,
                    l.peca_disparo AS atual_peca_disparo,
                    l.texto_disparo AS atual_texto_disparo,
                    l.observacao AS atual_observacao,
                    l.qtd_acionamentos::text AS atual_qtd_acionamentos,
                    l.flag_matriculado::text AS atual_matriculado,
                    l.data_ultima_acao::text AS atual_data_ultima_acao,
                    l.data_disparo::text AS atual_data_disparo
                FROM resolvida r
                LEFT JOIN modelo_estrela.leads_painel_lite l
                  ON l.sk_pessoa_dim = r.sk_pessoa_dim
                ORDER BY r.linha
                """
                result = db._run_gestao_query(sql, {"rows_json": json.dumps(rows, ensure_ascii=False, default=str)}, "upload_preview_real")

                metrics = {"total": len(rows), "novos": 0, "existentes": 0, "alterados": 0, "sem_mudanca": 0, "ambiguos": 0, "rejeitados": 0, "limpezas": 0}
                examples = {"ambiguos": [], "rejeitados": []}

                for item in result:
                    cpf = item.get("cpf")
                    celular = item.get("celular")
                    cpf_matches = int(item.get("cpf_matches") or 0)
                    celular_matches = int(item.get("celular_matches") or 0)
                    sk = item.get("sk_pessoa_dim")

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
                    if not sk:
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
                    if changed:
                        metrics["alterados"] += 1
                    else:
                        metrics["sem_mudanca"] += 1

                return jsonify({
                    "ok": True,
                    "data": {
                        **metrics,
                        "campos_operacionais": operational_present,
                        "campos_preservados": sorted(present & (PERSONAL_FIELDS | ACADEMIC_FIELDS)),
                        "exemplos": examples,
                    },
                })
            except Exception as exc:
                logger.exception("Falha na prévia real da importação.")
                return jsonify({"ok": False, "error": {"code": "UPLOAD_PREVIEW_ERROR", "message": "Não foi possível comparar o arquivo com o banco.", "details": str(exc)}}), 500

        self.add_url_rule(
            "/api/upload/preview",
            endpoint="api_upload_preview_real",
            view_func=api_upload_preview_real,
            methods=["POST"],
        )

    patched_init._upload_preview_patched = True
    Flask.__init__ = patched_init
    logger.info("Extensão de prévia real de importação preparada.")


_install_upload_preview_extension()
