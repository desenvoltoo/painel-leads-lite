from datetime import date

import pytest

from app import InvalidDataDisparoFilter, _get_filters_from_payload
from services import database as bq


def test_get_filters_from_payload_accepts_data_disparo_filters():
    filters, _meta = _get_filters_from_payload({
        "data_disparo_mes": "2026-06",
        "data_disparo_situacao": "preenchidas",
    })

    assert filters["data_disparo_mes"] == "2026-06"
    assert filters["data_disparo_situacao"] == "preenchidas"


@pytest.mark.parametrize("payload", [
    {"data_disparo_mes": "06/2026"},
    {"data_disparo_mes": "2026-13"},
    {"data_disparo_situacao": "qualquer"},
])
def test_get_filters_from_payload_rejects_invalid_data_disparo_filters(payload):
    with pytest.raises(InvalidDataDisparoFilter):
        _get_filters_from_payload(payload)


def _param_map(params):
    return {param.name: param for param in params}


@pytest.fixture(autouse=True)
def data_disparo_columns(monkeypatch):
    monkeypatch.setattr(bq, "_has_view_col", lambda col: col in {"data_disparo", "data_inscricao"})
    monkeypatch.setattr(bq, "_view_table_id", lambda: "public.vw_leads_painel_lite")


def test_apply_filters_data_disparo_month_generates_date_range_params():
    params = []
    sql = bq._apply_filters("SELECT 1 FROM view v WHERE 1=1", {"data_disparo_mes": "2026-06"}, params)

    assert "DATE(v.data_disparo) >= @data_disparo_ini" in sql
    assert "DATE(v.data_disparo) < @data_disparo_fim" in sql
    mapped = _param_map(params)
    assert mapped["data_disparo_ini"].value == date(2026, 6, 1)
    assert mapped["data_disparo_fim"].value == date(2026, 7, 1)


def test_apply_filters_data_disparo_vazias_generates_is_null():
    params = []
    sql = bq._apply_filters("SELECT 1 FROM view v WHERE 1=1", {"data_disparo_situacao": "vazias"}, params)

    assert "v.data_disparo IS NULL" in sql


def test_apply_filters_data_disparo_preenchidas_generates_is_not_null():
    params = []
    sql = bq._apply_filters("SELECT 1 FROM view v WHERE 1=1", {"data_disparo_situacao": "preenchidas"}, params)

    assert "v.data_disparo IS NOT NULL" in sql


def test_apply_filters_data_disparo_vazias_ignores_month():
    params = []
    sql = bq._apply_filters(
        "SELECT 1 FROM view v WHERE 1=1",
        {"data_disparo_situacao": "vazias", "data_disparo_mes": "2026-06"},
        params,
    )

    assert "v.data_disparo IS NULL" in sql
    assert "@data_disparo_ini" not in sql
    assert "@data_disparo_fim" not in sql
    assert "data_disparo_ini" not in _param_map(params)
    assert "data_disparo_fim" not in _param_map(params)
