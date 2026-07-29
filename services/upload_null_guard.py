# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime
import re
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

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
BR_DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")


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
    """Normaliza qualquer data válida para ISO sem trocar dia e mês.

    Ordem obrigatória:
    1. objetos datetime/date;
    2. serial numérico do Excel;
    3. ISO YYYY-MM-DD, preservado exatamente;
    4. brasileiro DD/MM/YYYY;
    5. fallback controlado.

    A função é idempotente: aplicar duas vezes em 2026-04-01 continua
    retornando 2026-04-01, nunca 2026-01-04.
    """
    value = _clean_value(value)
    if value is None:
        return None

    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    # Datas numéricas nativas do Excel.
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            parsed = pd.to_datetime(value, unit="D", origin="1899-12-30", errors="raise")
            return parsed.strftime("%Y-%m-%d")
        except Exception:
            return value

    text = str(value).strip()
    if not text:
        return None

    candidate = text.replace("T", " ").split(" ", 1)[0]

    # ISO nunca pode passar por dayfirst=True, pois isso reinverte datas
    # como 2026-04-01 para 2026-01-04 na segunda limpeza do dataframe.
    if ISO_DATE_RE.fullmatch(candidate):
        try:
            parsed = datetime.strptime(candidate, "%Y-%m-%d")
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            return value

    # Arquivos brasileiros em texto usam DD/MM/YYYY.
    if BR_DATE_RE.fullmatch(candidate):
        try:
            parsed = datetime.strptime(candidate, "%d/%m/%Y")
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            return value

    # Fallback para outros formatos não ambíguos; padrão brasileiro primeiro.
    for dayfirst in (True, False):
        try:
            parsed = pd.to_datetime(candidate, dayfirst=dayfirst, errors="raise")
            return parsed.strftime("%Y-%m-%d")
        except Exception:
            continue

    # Mantém valor inválido para a rotina oficial rejeitar e registrar o erro.
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
