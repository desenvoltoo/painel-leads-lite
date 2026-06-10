from datetime import datetime, timezone, timedelta
from io import BytesIO

import pytest

from app import create_app
from services import gestao
from services import bigquery as bq


@pytest.fixture()
def app(monkeypatch, tmp_path):
    monkeypatch.setenv("FLASK_SECRET_KEY", "test-secret")
    monkeypatch.setenv("UPLOAD_DIR", str(tmp_path / "uploads"))
    monkeypatch.setenv("EXPORT_DIR", str(tmp_path / "exports"))
    app = create_app()
    app.config.update(TESTING=True)
    return app


@pytest.fixture()
def client(app):
    return app.test_client()


def login(client):
    with client.session_transaction() as sess:
        sess["username"] = "matheus"
        sess.permanent = True


def test_gestao_requires_auth(client):
    resp = client.get("/gestao")
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_gestao_api_requires_auth(client):
    resp = client.get("/api/gestao/resumo")
    assert resp.status_code == 401
    assert resp.get_json()["ok"] is False


def test_gestao_page_opens_authenticated(client):
    login(client)
    resp = client.get("/gestao")
    assert resp.status_code == 200
    assert b"Gest\xc3\xa3o Operacional" in resp.data


def test_resumo_endpoint_success_and_meta(client, monkeypatch):
    login(client)

    def fake(filters, meta):
        return {"total_leads": 10}, False

    monkeypatch.setattr("app.gestao_get_resumo", fake)
    resp = client.get("/api/gestao/resumo?curso=Direito&limit=9999")
    body = resp.get_json()
    assert resp.status_code == 200
    assert body["ok"] is True
    assert body["data"]["total_leads"] == 10
    assert body["meta"]["filters"]["curso"] == ["Direito"]


def test_bigquery_error_is_safe(client, monkeypatch):
    login(client)

    def fake(filters, meta):
        raise RuntimeError("secret technical detail")

    monkeypatch.setattr("app.gestao_get_resumo", fake)
    resp = client.get("/api/gestao/resumo")
    body = resp.get_json()
    assert resp.status_code == 500
    assert body["error"]["code"] == "GESTAO_QUERY_ERROR"
    assert "secret" not in body["error"]["message"]


def test_filter_validation_invalid_period():
    with pytest.raises(gestao.GestaoValidationError):
        gestao.parse_filters({"data_ini": "2026-06-11", "data_fim": "2026-06-10"})


def test_filter_validation_invalid_date(client):
    login(client)
    resp = client.get("/api/gestao/resumo?data_ini=not-a-date")
    assert resp.status_code == 400
    assert resp.get_json()["error"]["code"] == "GESTAO_INVALID_FILTER"


def test_pagination_and_order_are_bounded():
    filters, meta = gestao.parse_filters({"limit": "999999", "offset": "-10", "order_dir": "sideways", "order_by": "total_leads"})
    assert meta["limit"] == gestao.MAX_PAGE_SIZE
    assert meta["offset"] == 0
    assert meta["order_dir"] == "DESC"
    assert meta["order_by"] == "total_leads"


def test_cache_by_filters_and_invalidation(monkeypatch):
    gestao.invalidate_gestao_cache()
    calls = {"n": 0}

    def load():
        calls["n"] += 1
        return {"value": calls["n"]}

    data1, cached1 = gestao._with_cache("unit", {"curso": ["A"]}, {}, False, load)
    data2, cached2 = gestao._with_cache("unit", {"curso": ["A"]}, {}, False, load)
    data3, cached3 = gestao._with_cache("unit", {"curso": ["B"]}, {}, False, load)
    gestao.invalidate_gestao_cache()
    data4, cached4 = gestao._with_cache("unit", {"curso": ["A"]}, {}, False, load)
    assert (data1["value"], cached1) == (1, False)
    assert (data2["value"], cached2) == (1, True)
    assert (data3["value"], cached3) == (2, False)
    assert (data4["value"], cached4) == (3, False)


def test_no_cache_for_personal_filters():
    calls = {"n": 0}

    def load():
        calls["n"] += 1
        return {"value": calls["n"]}

    a, ca = gestao._with_cache("personal", {"busca": "maria@example.com"}, {}, False, load)
    b, cb = gestao._with_cache("personal", {"busca": "maria@example.com"}, {}, False, load)
    assert a["value"] == 1 and b["value"] == 2
    assert ca is False and cb is False


def test_matriculado_and_status_rules():
    assert gestao.is_matriculado_row({"flag_matriculado": True}) is True
    assert gestao.is_matriculado_row({"status": " mat "}) is True
    assert gestao.is_matriculado_row({"matriculado": "sim"}) is True
    assert gestao.is_matriculado_row({"status": "PENDENTE"}) is False
    assert gestao.is_status_empty(None) is True
    assert gestao.is_status_empty("   ") is True
    assert gestao.is_status_empty("MAT") is False


def test_score_rule_documents_required_components():
    docs = " ".join(item["regra"] for item in gestao.score_rule_documentation()).lower()
    assert "matriculados" in docs
    assert "sem status" in docs
    assert "dt_upload" in docs


def test_upload_invalid_extension(client):
    login(client)
    resp = client.post("/api/upload", data={"file": (BytesIO(b"x"), "leads.txt")}, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "Formato" in resp.get_json()["error"]


def test_upload_empty_csv(client):
    login(client)
    resp = client.post("/api/upload", data={"file": (BytesIO(b"nome;celular\n"), "leads.csv")}, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "vazio" in resp.get_json()["error"].lower()


def test_dt_upload_injection(monkeypatch):
    import pandas as pd

    schema = [bq.bigquery.SchemaField("nome", "STRING"), bq.bigquery.SchemaField("celular", "STRING"), bq.bigquery.SchemaField("dt_upload", "TIMESTAMP")]
    upload_ts = datetime(2026, 6, 10, 12, tzinfo=timezone.utc)
    df = pd.DataFrame([{"nome": "Ana", "celular": "11999999999"}])
    out = bq._coerce_df_to_staging_schema(df, staging_schema=schema, upload_ts=upload_ts)
    assert "dt_upload" in out.columns
    assert out["dt_upload"].iloc[0] == upload_ts


def test_old_upload_cannot_replace_newer_version():
    older = datetime(2026, 6, 9, tzinfo=timezone.utc)
    newer = datetime(2026, 6, 10, tzinfo=timezone.utc)
    assert gestao.should_accept_upload_version(newer, older) is True
    assert gestao.should_accept_upload_version(older, newer) is False


def test_masks_personal_data():
    row = {"cpf": "12345678901", "celular": "11987654321", "email": "maria.silva@example.com", "payload": {"raw": "secret"}}
    masked = gestao.mask_rejection_row(row)
    assert masked["cpf"].endswith("01")
    assert masked["celular"].endswith("4321")
    assert masked["email"] == "m***@example.com"
    assert "payload" not in masked


def test_json_safe_bigquery_values():
    from decimal import Decimal

    now = datetime(2026, 6, 10, 12, 0, tzinfo=timezone.utc)
    assert bq._json_safe_value(Decimal("10.5")) == 10.5
    assert "2026" in bq._json_safe_value(now)
