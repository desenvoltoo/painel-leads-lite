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


def test_phone_and_cpf_validation_and_masks():
    assert gestao.is_valid_phone("(11) 98765-4321") is True
    assert gestao.is_valid_phone("11111111111") is False
    assert gestao.is_valid_phone("123") is False
    assert gestao.is_valid_cpf("529.982.247-25") is True
    assert gestao.is_valid_cpf("111.111.111-11") is False
    assert gestao.is_valid_cpf("123") is False
    masked = gestao.mask_rejection_row({"cpf_raw": "52998224725", "celular_raw": "11987654321", "email_raw": "maria@example.com", "payload": "segredo"})
    assert masked["cpf_raw"] == "***.***.***-4725"
    assert masked["celular_raw"] == "*******4321"
    assert masked["email_raw"] == "m***@example.com"
    assert "payload" not in masked


def test_status_empty_and_ec_rules():
    assert gestao.is_status_empty(None) is True
    assert gestao.is_status_empty("") is True
    assert gestao.is_status_empty(" SEM INFORMAÇÃO ") is True
    assert gestao.is_status_empty("MAT") is False
    assert gestao.is_status_ec(" ec ") is True
    assert gestao.is_status_ec("EC") is True
    assert gestao.is_status_ec("E C") is False


def test_fila_priority_order_with_dates_and_exclusions():
    rows = [
        {"nome": "lead EC antigo", "status": "ec", "celular": "11999999996", "data_inscricao": "01/06/2026"},
        {"nome": "lead comum recente", "status": "ABERTO", "celular": "11999999995", "data_inscricao": "2026-06-10"},
        {"nome": "lead matriculado", "status": "MAT", "celular": "11999999994", "data_inscricao": "2026-06-11"},
        {"nome": "lead antigo sem status", "status": "   ", "celular": "11999999998", "data_inscricao": "01/06/2026"},
        {"nome": "lead EC recente", "status": "EC", "celular": "11999999997", "data_inscricao": "2026-06-09"},
        {"nome": "lead novo sem status", "status": None, "celular": "11999999999", "data_inscricao": "10/06/2026"},
    ]
    ordered = gestao.prioritize_fila_rows(rows)
    assert [r["nome"] for r in ordered] == [
        "lead novo sem status",
        "lead antigo sem status",
        "lead EC recente",
        "lead EC antigo",
        "lead comum recente",
    ]
    assert all(r["nome"] != "lead matriculado" for r in ordered)


def test_quality_sql_uses_duplicate_excedent_and_closed_type(monkeypatch):
    monkeypatch.setattr(gestao.bq, "_first_existing_col", lambda *cols: cols[0])
    monkeypatch.setattr(gestao, "_has", lambda col: True)
    sql, params = gestao._quality_details_sql("duplicado_cpf", {}, {"limit": 10, "offset": 0})
    assert "SUM(qtd - 1)" not in sql  # detail lists rows; summary uses excedent aggregation
    assert "dup_cpf" in sql
    with pytest.raises(gestao.GestaoValidationError):
        gestao._quality_details_sql("campo_livre", {}, {"limit": 10, "offset": 0})


def test_exports_generate_csv_with_masked_data(monkeypatch):
    monkeypatch.setattr(gestao.bq, "_first_existing_col", lambda *cols: cols[0])
    monkeypatch.setattr(gestao, "_has", lambda col: True)
    monkeypatch.setattr(gestao, "_run", lambda sql, params, op: [{"motivo": "Sem status", "identificador": "***.***.***-4725", "nome": "Ana", "curso": "Direito", "consultor": "João", "data_inscricao": "2026-06-10", "data_upload": "2026-06-10", "origem": "Site", "status": ""}])
    filename, content, count = gestao.export_qualidade({}, {"limit": 10, "offset": 0}, "sem_status")
    assert filename.startswith("qualidade_sem_status_")
    assert count == 1
    text = content.decode("utf-8-sig")
    assert "Identificador mascarado" in text
    assert "52998224725" not in text


def test_importacoes_missing_table_returns_clear_payload(monkeypatch):
    from google.api_core.exceptions import NotFound
    def fake_run(sql, params, op):
        raise NotFound("missing")
    monkeypatch.setattr(gestao, "_run", fake_run)
    data, cached = gestao.get_importacoes({}, {"limit": 20, "offset": 0, "order_dir": "DESC", "order_by": "dt_upload", "force_refresh": True})
    assert cached is False
    assert data["items"] == []
    assert data["tabelas_disponiveis"]["logs_importacoes"] is False
    assert "migração" in data["message"]


def test_importacoes_csv_has_no_payload(monkeypatch):
    monkeypatch.setattr(gestao, "_run", lambda sql, params, op: [{"id_importacao": "1", "nome_arquivo": "leads.csv", "usuario": "matheus", "dt_upload": "2026-06-10", "payload": "segredo"}])
    filename, content, count = gestao.export_importacoes({}, {"order_dir": "DESC", "order_by": "dt_upload"})
    assert filename.startswith("historico_importacoes_")
    assert count == 1
    assert "payload" not in content.decode("utf-8-sig").lower()


def test_fila_export_uses_same_order_function(monkeypatch):
    captured = {}
    def fake_run(sql, params, op):
        captured["sql"] = sql
        return [{"nome": "A", "celular": "11987654321", "grupo_prioridade": 1, "prioridade": 100, "motivo_prioridade": "Lead recente sem status"}]
    monkeypatch.setattr(gestao.bq, "_first_existing_col", lambda *cols: cols[0])
    monkeypatch.setattr(gestao, "_has", lambda col: True)
    monkeypatch.setattr(gestao, "_run", fake_run)
    filename, content, count = gestao.export_fila({}, {"limit": 10, "offset": 0})
    assert "ORDER BY grupo_prioridade ASC, data_inscricao DESC NULLS LAST" in captured["sql"]
    assert "*******4321" in content.decode("utf-8-sig")
    assert count == 1
