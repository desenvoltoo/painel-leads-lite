"""Inicialização dos serviços do Painel de Leads Lite.

Aplica adaptações de compatibilidade sobre os módulos históricos sem exigir
mudanças amplas nas rotas Flask.
"""

from __future__ import annotations

import re
from typing import Any, Dict

from . import database as _database
from .upload_pipeline import process_upload_dataframe


# Garante que ``from services.database import process_upload_dataframe`` use
a# implementação compatível com FUNCTION e PROCEDURE do PostgreSQL.
_database.process_upload_dataframe = process_upload_dataframe


# O frontend antigo classifica qualquer valor numérico com 11 dígitos como CPF.
# Telefones celulares brasileiros também possuem 11 dígitos, então a busca era
# enviada exclusivamente ao campo CPF e não encontrava o celular existente.
def _normalize_quick_search_filters(filters: Dict[str, Any] | None) -> Dict[str, Any]:
    normalized = dict(filters or {})

    cpf_value = str(normalized.get("cpf") or "").strip()
    cpf_digits = re.sub(r"[^0-9]", "", cpf_value)
    if cpf_digits and not normalized.get("busca"):
        normalized.pop("cpf", None)
        normalized["busca"] = cpf_digits

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


_database.query_leads = query_leads
_database.query_leads_count = query_leads_count
_database.export_leads_rows = export_leads_rows


# Substitui apenas o fluxo de lotes por uma implementação transacional. As rotas
# continuam importando os mesmos nomes de ``services.gestao_operacional``.
from . import gestao_operacional as _gestao_operacional
from . import gestao_lotes as _gestao_lotes

for _name in (
    "preview_proximo_lote",
    "criar_lote",
    "exportar_proximo_lote",
    "get_lote_csv",
    "importar_retorno_lote",
    "importar_lote_disparado",
):
    setattr(_gestao_operacional, _name, getattr(_gestao_lotes, _name))


__all__ = [
    "process_upload_dataframe",
    "query_leads",
    "query_leads_count",
    "export_leads_rows",
]
