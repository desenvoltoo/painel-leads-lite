"""Inicialização dos serviços do Painel de Leads Lite.

Aplica adaptações de compatibilidade sobre o módulo histórico
``services.database`` sem exigir mudanças amplas nas rotas Flask.
"""

from __future__ import annotations

import re
from typing import Any, Dict

from . import database as _database
from .upload_pipeline import process_upload_dataframe


# Garante que ``from services.database import process_upload_dataframe`` use
# a implementação compatível com FUNCTION e PROCEDURE do PostgreSQL.
_database.process_upload_dataframe = process_upload_dataframe


# O frontend antigo classifica qualquer valor numérico com 11 dígitos como CPF.
# Telefones celulares brasileiros também possuem 11 dígitos, então a busca era
# enviada exclusivamente ao campo CPF e não encontrava o celular existente.
def _normalize_quick_search_filters(filters: Dict[str, Any] | None) -> Dict[str, Any]:
    normalized = dict(filters or {})

    # Quando a busca rápida chegou como CPF, converte para a busca geral. A busca
    # geral do banco compara o mesmo número normalizado tanto com CPF quanto com
    # celular, aceitando máscara, DDD e prefixo +55.
    cpf_value = str(normalized.get("cpf") or "").strip()
    cpf_digits = re.sub(r"[^0-9]", "", cpf_value)
    if cpf_digits and not normalized.get("busca"):
        normalized.pop("cpf", None)
        normalized["busca"] = cpf_digits

    # Mantém celular específico, mas remove caracteres de apresentação.
    celular_value = str(normalized.get("celular") or "").strip()
    celular_digits = re.sub(r"[^0-9]", "", celular_value)
    if celular_digits:
        normalized["celular"] = celular_digits

    return normalized


_original_query_leads = _database.query_leads
_original_query_leads_count = _database.query_leads_count
_original_export_leads_rows = _database.export_leads_rows


def query_leads(filters=None, *args, **kwargs):
    return _original_query_leads(
        _normalize_quick_search_filters(filters), *args, **kwargs
    )


def query_leads_count(filters=None, *args, **kwargs):
    return _original_query_leads_count(
        _normalize_quick_search_filters(filters), *args, **kwargs
    )


def export_leads_rows(filters=None, *args, **kwargs):
    return _original_export_leads_rows(
        _normalize_quick_search_filters(filters), *args, **kwargs
    )


# As rotas importam estas funções diretamente de services.database depois que o
# pacote services é inicializado, portanto substituímos as referências aqui.
_database.query_leads = query_leads
_database.query_leads_count = query_leads_count
_database.export_leads_rows = export_leads_rows

__all__ = [
    "process_upload_dataframe",
    "query_leads",
    "query_leads_count",
    "export_leads_rows",
]
