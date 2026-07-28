# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

from . import database as db

NULL_MARKERS = {
    "\\N", "/N", "//N", "*//N*", "NULL", "NONE", "NAN", "N/A", "NA", "<NA>"
}

DATE_COLUMNS = {
    "data_inscricao",
    "data_matricula",
    "data_ultima_acao",
    "data_ultima_interacao",
    "data_disparo",
    "data_atualizacao",
}


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return None
    if text.upper() in NULL_MARKERS:
        return None
    return value


def _format_date_value(value: Any) -> Any:
    """Normaliza datas para ISO, evitando ambiguidade DD/MM x MM/DD no PostgreSQL."""
    value = _clean_value(value)
    if value is None:
        return None

    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    # Datas numéricas do Excel.
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            parsed = pd.to_datetime(value, unit="D", origin="1899-12-30", errors="raise")
            return parsed.strftime("%Y-%m-%d")
        except Exception:
            return value

    text = str(value).strip()
    if not text:
        return None

    # Remove somente a parte de horário quando a data já estiver em formato conhecido.
    candidate = text.replace("T", " ").split(" ", 1)[0]

    # Arquivos operacionais usam padrão brasileiro. A tentativa dayfirst=False fica
    # apenas como fallback para valores ISO/americanos inequivocamente válidos.
    for dayfirst in (True, False):
        try:
            parsed = pd.to_datetime(candidate, dayfirst=dayfirst, errors="raise")
            return parsed.strftime("%Y-%m-%d")
        except Exception:
            continue

    # Mantém valor inválido para que a rotina oficial possa rejeitar e registrar o erro.
    return value


def _clean_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame
    cleaned = frame.copy()
    for column in cleaned.columns:
        if str(column).strip().lower() in DATE_COLUMNS:
            cleaned[column] = cleaned[column].map(_format_date_value)
        else:
            cleaned[column] = cleaned[column].map(_clean_value)
    return cleaned.astype(object).where(pd.notnull(cleaned), None)


def apply_upload_null_guard() -> None:
    current = db._prepare_upload_dataframe
    if getattr(current, "_upload_null_guard", False):
        return

    def guarded_prepare(df, filename: str, upload_id: str):
        prepared = current(_clean_dataframe(df), filename, upload_id)
        return _clean_dataframe(prepared)

    guarded_prepare._upload_null_guard = True
    db._prepare_upload_dataframe = guarded_prepare
