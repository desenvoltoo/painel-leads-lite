"""Inicialização dos serviços do Painel de Leads Lite.

Aplica adaptações de compatibilidade sobre os módulos históricos sem exigir
mudanças amplas nas rotas Flask.
"""

from __future__ import annotations

import re
import sys
import threading
import time
from typing import Any, Dict

from . import database as _database
from .upload_pipeline import process_upload_dataframe


_BLANK_MARKERS = {
    "",
    "\\n",
    "\\\\n",
    "\\N",
    "\\\\N",
    "null",
    "none",
    "nan",
    "nat",
}


def _normalize_blank_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if _database.pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.lower() in {item.lower() for item in _BLANK_MARKERS}:
            return None
        return cleaned
    return value


# Aceita telefone2 no upload e elimina marcadores artificiais antes da staging.
_database.UPLOAD_ALIASES.setdefault(
    "telefone2",
    [
        "telefone2",
        "telefone_2",
        "telefone_secundario",
        "telefone_secundário",
        "celular2",
        "celular_2",
        "whatsapp2",
    ],
)

_original_prepare_upload_dataframe = _database._prepare_upload_dataframe


def _prepare_upload_dataframe_sem_marcadores(df, filename: str, upload_id: str):
    if df is not None and not df.empty:
        df = df.copy()
        for column in df.columns:
            df[column] = df[column].map(_normalize_blank_value)
    prepared = _original_prepare_upload_dataframe(df, filename, upload_id)
    if prepared is not None and not prepared.empty:
        for column in prepared.columns:
            prepared[column] = prepared[column].map(_normalize_blank_value)
    return prepared


_database._prepare_upload_dataframe = _prepare_upload_dataframe_sem_marcadores


# Inclui telefone2 imediatamente depois de celular nas exportações.
if not any(output == "telefone2" for _, output in _database.EXPORT_COLUMNS):
    position = next(
        (
            index + 1
            for index, (_, output) in enumerate(_database.EXPORT_COLUMNS)
            if output == "celular"
        ),
        len(_database.EXPORT_COLUMNS),
    )
    _database.EXPORT_COLUMNS.insert(position, ("telefone2", "telefone2"))
_database.EXPORT_ORDER = [output for _, output in _database.EXPORT_COLUMNS]


# O app possui uma lista histórica própria usada no CSV em lote. Como o módulo
# services é carregado enquanto app.py ainda está sendo importado, aguardamos a
# conclusão da importação e acrescentamos telefone2 sem alterar as rotas.
def _patch_app_export_order() -> None:
    for _ in range(200):
        patched = False
        for module_name in ("app", "__main__"):
            module = sys.modules.get(module_name)
            order = getattr(module, "EXPORT_ORDER", None) if module else None
            if isinstance(order, list):
                if "telefone2" not in order:
                    try:
                        order.insert(order.index("celular") + 1, "telefone2")
                    except ValueError:
                        order.append("telefone2")
                patched = True
        if patched:
            return
        time.sleep(0.05)


threading.Thread(
    target=_patch_app_export_order,
    name="patch-app-export-telefone2",
    daemon=True,
).start()


# Garante que ``from services.database import process_upload_dataframe`` use
# a implementação compatível com FUNCTION e PROCEDURE do PostgreSQL.
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


def _clean_export_rows(rows):
    cleaned = []
    for row in rows or []:
        cleaned.append({key: _normalize_blank_value(value) for key, value in dict(row).items()})
    return cleaned


_original_query_leads = _database.query_leads
_original_query_leads_count = _database.query_leads_count
_original_export_leads_rows = _database.export_leads_rows
_original_rows_dataframe_export_order = _database._rows_dataframe_export_order


def query_leads(filters=None, *args, **kwargs):
    return _original_query_leads(
        _normalize_quick_search_filters(filters), *args, **kwargs
    )


def query_leads_count(filters=None, *args, **kwargs):
    return _original_query_leads_count(
        _normalize_quick_search_filters(filters), *args, **kwargs
    )


def export_leads_rows(filters=None, *args, **kwargs):
    rows = _original_export_leads_rows(
        _normalize_quick_search_filters(filters), *args, **kwargs
    )
    return _clean_export_rows(rows)


def export_leads_rows_iter(filters=None, *args, **kwargs):
    yield from export_leads_rows(filters, *args, **kwargs)


def _rows_dataframe_export_order(rows):
    df = _original_rows_dataframe_export_order(_clean_export_rows(rows))
    for column in df.columns:
        df[column] = df[column].map(_normalize_blank_value)
    return df


_database.query_leads = query_leads
_database.query_leads_count = query_leads_count
_database.export_leads_rows = export_leads_rows
_database.export_leads_rows_iter = export_leads_rows_iter
_database._rows_dataframe_export_order = _rows_dataframe_export_order


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
