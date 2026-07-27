# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any
import pandas as pd

from . import database as db

NULL_MARKERS = {
    "\\N", "/N", "//N", "*//N*", "NULL", "NONE", "NAN", "N/A", "NA", "<NA>"
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


def _clean_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return frame
    cleaned = frame.copy()
    for column in cleaned.columns:
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
