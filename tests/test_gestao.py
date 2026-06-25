from datetime import date, datetime, timezone
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
        {"nome": "lead A", "status": None, "celular": "11999999999", "data_inscricao": "2026-06-11"},
        {"nome": "lead B", "status": "   ", "celular": "11999999998", "data_inscricao": "10/06/2026"},
        {"nome": "lead C", "status": "EC", "celular": "11999999997", "data_inscricao": "2026-06-11"},
        {"nome": "lead D", "status": "ec", "celular": "11999999996", "data_inscricao": "01/06/2026"},
        {"nome": "lead E", "status": "ABERTO", "celular": "11999999995", "data_inscricao": "2026-06-11"},
        {"nome": "lead F", "status": "MAT", "celular": "11999999994", "data_inscricao": "2026-06-11"},
        {"nome": "lead G", "status": None, "celular": "", "data_inscricao": "2026-06-11"},
        {"nome": "lead H", "status": "CANCELADO", "celular": "11999999993", "data_inscricao": "2026-06-11"},
    ]
    ordered = gestao.prioritize_fila_rows(rows)
    assert [r["nome"] for r in ordered] == ["lead A", "lead B", "lead C", "lead D", "lead E"]
    assert {"lead F", "lead G", "lead H"}.isdisjoint({r["nome"] for r in ordered})
    assert [r["grupo_prioridade"] for r in ordered] == [1, 1, 2, 2, 3]


def test_quality_sql_uses_duplicate_excedent_and_closed_type(monkeypatch):
    monkeypatch.setattr(gestao.bq, "_first_existing_col", lambda *cols: cols[0])
    monkeypatch.setattr(gestao.bq, "_view_columns", lambda: set(gestao.OPTION_FIELDS) | {"nome", "cpf", "celular", "email", "data_inscricao", "dt_upload", "flag_matriculado"})
    monkeypatch.setattr(gestao.bq, "_has_view_col", lambda col: True)
    monkeypatch.setattr(gestao, "_has", lambda col: True)
    sql, params = gestao._quality_details_sql("duplicado_cpf", {}, {"limit": 10, "offset": 0})
    assert "SUM(qtd - 1)" not in sql  # detail lists rows; summary uses excedent aggregation
    assert "dup_cpf" in sql
    with pytest.raises(gestao.GestaoValidationError):
        gestao._quality_details_sql("campo_livre", {}, {"limit": 10, "offset": 0})


def test_exports_generate_csv_with_masked_data(monkeypatch):
    monkeypatch.setattr(gestao.bq, "_first_existing_col", lambda *cols: cols[0])
    monkeypatch.setattr(gestao.bq, "_view_columns", lambda: set(gestao.OPTION_FIELDS) | {"nome", "cpf", "celular", "email", "data_inscricao", "dt_upload", "flag_matriculado"})
    monkeypatch.setattr(gestao.bq, "_has_view_col", lambda col: True)
    monkeypatch.setattr(gestao, "_has", lambda col: True)
    monkeypatch.setattr(gestao, "_run", lambda sql, params, op: [{"motivo": "Sem status", "identificador": "***.***.***-4725", "nome": "Ana", "curso": "Direito", "consultor": "João", "data_inscricao": "2026-06-10", "data_upload": "2026-06-10", "origem": "Site", "status": ""}])
    filename, content, count = gestao.export_qualidade({}, {"limit": 10, "offset": 0}, "sem_status")
    assert filename.startswith("qualidade_sem_status_")
    assert count == 1
    text = content.decode("utf-8-sig")
    assert "Identificador mascarado" in text
    assert "52998224725" not in text


def test_qualidade_map_explicit_snake_to_camel_and_nulls():
    mapped = gestao.map_qualidade_row({
        "total_registros": "10",
        "total_leads": None,
        "duplicidades_cpf": 2,
        "duplicidades_celular": 3,
        "duplicidades_email": 1,
        "percentual_duplicidade": None,
        "ultima_atualizacao": None,
    })
    assert mapped["totalRegistros"] == 10
    assert mapped["totalLeads"] == 0
    assert mapped["duplicidadesTotais"] == 6
    assert mapped["percentualDuplicidade"] == 0
    assert mapped["ultimaAtualizacao"] is None


def test_historico_uses_official_view_and_count_query(monkeypatch):
    calls = []
    def fake_run(sql, params, op):
        calls.append((sql, params, op))
        if "COUNT" in sql:
            return [{"total": 1}]
        return [{"upload_id": "u1", "nome_arquivo": "leads.csv", "criado_em": "2026-06-10", "payload": "segredo", "email": "a@b.com"}]
    monkeypatch.setattr(gestao, "_run", fake_run)
    data, cached = gestao.get_importacoes({"status": "CONCLUIDO", "nomeArquivo": "leads"}, {"page": 1, "pageSize": 20, "offset": 0})
    assert cached is False
    assert data["pagination"] == {"page": 1, "pageSize": 20, "total": 1, "totalPages": 1}
    assert data["items"][0]["upload_id"] == "u1"
    assert "payload" not in data["items"][0]
    assert "email" not in data["items"][0]
    assert "vw_historico_importacoes" in calls[0][0]
    assert "ORDER BY criado_em DESC" in calls[1][0]


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
        captured["op"] = op
        return [{"nome": "A", "celular": "11987654321", "grupo_prioridade": 1, "prioridade": "ALTA", "motivo_prioridade": "Lead recente sem status"}]
    monkeypatch.setattr(gestao.bq, "_first_existing_col", lambda *cols: cols[0])
    monkeypatch.setattr(gestao.bq, "_view_columns", lambda: set(gestao.OPTION_FIELDS) | {"nome", "cpf", "celular", "email", "data_inscricao", "data_ultima_acao", "dt_upload", "flag_matriculado"})
    monkeypatch.setattr(gestao.bq, "_has_view_col", lambda col: True)
    monkeypatch.setattr(gestao, "_has", lambda col: True)
    monkeypatch.setattr(gestao, "_run", fake_run)
    filename, content, count = gestao.export_fila({}, {"limit": 10, "offset": 0})
    assert "ORDER BY grupo_prioridade ASC, data_inscricao DESC NULLS LAST" in captured["sql"]
    assert captured["op"] == "gestao_fila_exportar"
    assert "*******4321" in content.decode("utf-8-sig")
    assert count == 1


def test_parse_import_history_request_bounded_and_camel_case():
    filters, meta = gestao.parse_import_history_request({"page": "2", "pageSize": "999", "status": "ERRO", "dataInicio": "2026-06-01", "dataFim": "2026-06-10", "nomeArquivo": "leads"})
    assert filters == {"status": "ERRO", "nomeArquivo": "leads", "dataInicio": "2026-06-01", "dataFim": "2026-06-10"}
    assert meta["page"] == 2
    assert meta["pageSize"] <= 100
    assert meta["offset"] == meta["pageSize"]


def test_export_importacoes_csv_semicolon_bom_and_no_sensitive(monkeypatch):
    monkeypatch.setattr(gestao, "_run", lambda sql, params, op: [{"upload_id": "1", "nome_arquivo": "leads, \"junho\".csv", "mensagem": "linha1\nlinha2", "payload": "segredo", "cpf": "123"}])
    filename, content, count = gestao.export_importacoes({}, {})
    assert filename.startswith("historico_importacoes_")
    assert count == 1
    assert content.startswith(b"\xef\xbb\xbf")
    text = content.decode("utf-8-sig")
    assert ";" in text.splitlines()[0]
    assert "payload" not in text.lower()
    assert "123" not in text
    assert '"leads, ""junho"".csv"' in text


def test_upload_log_uses_insert_then_update_same_upload(monkeypatch):
    calls = []
    monkeypatch.setattr(gestao.bq, "_run_gestao_query", lambda sql, params=None, operation_name="": calls.append((sql, params or [], operation_name)))
    gestao.criar_log_importacao(upload_id="u1", id_importacao="i1", nome_arquivo="leads.csv", tipo_arquivo="csv", tamanho_arquivo_bytes=10, usuario="user", correlation_id="c1")
    gestao.atualizar_log_importacao(upload_id="u1", status="CONCLUIDO", etapa="FINALIZADO", mensagem="ok", finalizado=True, linhas_recebidas=1)
    assert calls[0][2] == "import_log_create"
    assert "INSERT INTO" in calls[0][0] and "logs_importacoes" in calls[0][0]
    assert calls[1][2] == "import_log_update"
    assert "UPDATE" in calls[1][0] and "WHERE upload_id = @upload_id" in calls[1][0]
    assert [p.value for p in calls[1][1] if p.name == "upload_id"] == ["u1"]


def test_fila_sem_status_recency_before_ec_regardless_previous_action():
    rows = [
        {"nome": "sem status trabalhado recente", "status": "", "celular": "11999999996", "data_inscricao": "2026-06-10", "data_disparo": "2026-06-10"},
        {"nome": "sem status antigo", "status": None, "celular": "11999999999", "data_inscricao": "2026-06-01", "data_disparo": None, "data_ultima_acao": None},
        {"nome": "ec recente", "status": "EC", "celular": "11999999998", "data_inscricao": "2026-06-11"},
    ]
    ordered = gestao.prioritize_fila_rows(rows)
    assert [r["nome"] for r in ordered] == ["sem status trabalhado recente", "sem status antigo", "ec recente"]


def test_bq_param_logging_redacts_personal_values():
    params = [
        bq.bigquery.ScalarQueryParameter("email", "STRING", "ana@example.com"),
        bq.bigquery.ScalarQueryParameter("status", "STRING", "CONCLUIDO"),
    ]
    formatted = bq._format_bq_params_for_log(params)
    assert formatted[0]["value"] == "[REDACTED]"
    assert formatted[1]["value"] == "[SET]"
    assert "ana@example.com" not in str(formatted)


def test_sanitize_message_masks_personal_and_secret_values():
    msg = gestao._sanitize_message("Erro CPF 123.456.789-09 email ana@example.com celular 11999998888 token=abc")
    assert msg is not None
    assert "123.456.789-09" not in msg
    assert "ana@example.com" not in msg
    assert "11999998888" not in msg
    assert "abc" not in msg
    assert "[cpf-mascarado]" in msg


def test_qualidade_dados_route_success_shape(client, monkeypatch):
    login(client)
    monkeypatch.setattr("app.gestao_get_qualidade_dados", lambda filters, meta: ({"totalRegistros": 1, "ultimaAtualizacao": None}, False))
    resp = client.get("/api/gestao/qualidade-dados")
    body = resp.get_json()
    assert resp.status_code == 200
    assert body == {"success": True, "data": {"totalRegistros": 1, "ultimaAtualizacao": None}}


def test_importacoes_historico_route_empty_is_success(client, monkeypatch):
    login(client)
    monkeypatch.setattr("app.gestao_get_importacoes", lambda filters, meta: ({"items": [], "pagination": {"page": 1, "pageSize": 20, "total": 0, "totalPages": 0}}, False))
    resp = client.get("/api/importacoes/historico")
    body = resp.get_json()
    assert resp.status_code == 200
    assert body["success"] is True
    assert body["data"] == []
    assert body["pagination"]["total"] == 0


def test_parse_lead_date_accepts_required_formats():
    assert gestao.parse_lead_date("2026-06-11") == date(2026, 6, 11)
    assert gestao.parse_lead_date("11/06/2026") == date(2026, 6, 11)
    assert gestao.parse_lead_date("2026-06-11 13:45:00") == date(2026, 6, 11)
    assert gestao.parse_lead_date("46284") == date(2026, 9, 19)
    assert gestao.parse_lead_date(None) is None


def test_build_funil_etapas_cumulative_conversions_and_losses():
    etapas = gestao.build_funil_etapas(200, 120, 80, 20)
    assert [e["volume"] for e in etapas] == [200, 120, 80, 20]
    assert [e["perda_etapa_anterior"] for e in etapas] == [None, 80, 40, 60]
    assert etapas[1]["conversao_etapa_anterior"] == 60
    assert round(etapas[2]["conversao_etapa_anterior"], 1) == 66.7
    assert etapas[3]["conversao_etapa_anterior"] == 25


def test_build_funil_etapas_handles_zero_nulls_and_inconsistent_values():
    etapas = gestao.build_funil_etapas(None, 10, 30, -1)
    assert [e["volume"] for e in etapas] == [0, 0, 0, 0]
    assert all((e["conversao_etapa_anterior"] or 0) <= 100 for e in etapas)
    assert all((e["perda_etapa_anterior"] or 0) >= 0 for e in etapas)


def test_fila_api_contract_empty_and_error(client, monkeypatch):
    login(client)
    monkeypatch.setattr("app.gestao_get_fila", lambda filters, meta: ({"items": [], "pagination": {"page": 1, "page_size": 25, "total": 0, "total_pages": 0}}, False))
    resp = client.get("/api/gestao/fila?limit=25")
    body = resp.get_json()
    assert resp.status_code == 200
    assert body["ok"] is True
    assert body["data"]["items"] == []
    assert body["data"]["pagination"]["page"] == 1
    def boom(filters, meta):
        raise RuntimeError("invalidQuery secret")
    monkeypatch.setattr("app.gestao_get_fila", boom)
    resp = client.get("/api/gestao/fila?limit=25")
    body = resp.get_json()
    assert resp.status_code == 500
    assert body["ok"] is False
    assert body["error"]["code"] == "GESTAO_FILA_QUERY_ERROR"
    assert body["error"]["message"] == "Não foi possível carregar a fila operacional."


def test_get_fila_contract_and_pagination(monkeypatch):
    def fake_run(sql, params, op):
        assert op == "query_gestao_fila_operacional"
        return [{"nome": "A", "total_registros": 1, "total_antes_filtros": 8, "total_depois_filtros": 5, "view_utilizada": "vw_leads_painel_lite", "colunas_detectadas": "status,status_inscricao,tipo_negocio"}]
    monkeypatch.setattr(gestao.bq, "_view_columns", lambda: {"nome", "celular", "status", "status_inscricao", "tipo_negocio", "data_inscricao", "dt_upload"})
    monkeypatch.setattr(gestao.bq, "_has_view_col", lambda col: col in {"nome", "celular", "status", "status_inscricao", "tipo_negocio", "data_inscricao", "dt_upload"})
    monkeypatch.setattr(gestao.bq, "_first_existing_col", lambda *cols: next((c for c in cols if c in {"celular", "status", "status_inscricao", "tipo_negocio", "data_inscricao", "dt_upload"}), None))
    monkeypatch.setattr(gestao, "_has", lambda col: col in {"nome", "celular", "status", "status_inscricao", "tipo_negocio", "data_inscricao", "dt_upload"})
    monkeypatch.setattr(gestao, "_run", fake_run)
    data, cached = gestao.get_fila({}, {"limit": 25, "offset": 0})
    assert cached is False
    assert list(data.keys()) == ["items", "pagination"]
    assert data["pagination"] == {"page": 1, "page_size": 25, "total": 1, "total_pages": 1}


def test_operacional_leads_disponiveis_endpoint(client, monkeypatch):
    login(client)
    def fake(filters, meta):
        assert filters["curso"] == "Direito"
        return {"items": [{"sk_pessoa": 1, "nome": "Ana"}], "count": 1}, False
    monkeypatch.setattr("app.gestao_op_get_leads_disponiveis", fake)
    resp = client.get("/api/gestao/operacional/leads-disponiveis?curso=Direito&limit=10")
    body = resp.get_json()
    assert resp.status_code == 200
    assert body["ok"] is True
    assert body["data"]["count"] == 1


def test_operacional_criar_lote_endpoint(client, monkeypatch):
    login(client)
    def fake(payload):
        assert payload["tipo_disparo"] == "ROBO"
        return {"lote_id": "l1", "quantidade_leads": 2, "status_lote": "ABERTO"}, False
    monkeypatch.setattr("app.gestao_op_criar_lote", fake)
    resp = client.post("/api/gestao/operacional/lotes", json={"tipo_disparo": "ROBO", "quantidade": 2})
    body = resp.get_json()
    assert resp.status_code == 201
    assert body["data"]["lote_id"] == "l1"


def test_operacional_get_lotes_endpoint(client, monkeypatch):
    login(client)
    def fake(filters, meta):
        assert filters["status_lote"] == "ABERTO"
        return {"items": [{"lote_id": "l1"}], "count": 1}, False
    monkeypatch.setattr("app.gestao_op_get_lotes", fake)
    resp = client.get("/api/gestao/operacional/lotes?status_lote=ABERTO")
    assert resp.status_code == 200
    assert resp.get_json()["data"]["items"][0]["lote_id"] == "l1"


def test_operacional_update_lead_status_endpoint(client, monkeypatch):
    login(client)
    def fake(sk_pessoa, payload):
        assert sk_pessoa == 123
        assert payload["status_atendimento"] == "AC"
        return {"sk_pessoa": sk_pessoa, "status_atendimento": "AC", "retorno": True}, False
    monkeypatch.setattr("app.gestao_op_update_lead_status", fake)
    resp = client.patch("/api/gestao/operacional/leads/123/status", json={"lote_id": "l1", "status_atendimento": "AC"})
    assert resp.status_code == 200
    assert resp.get_json()["data"]["retorno"] is True


def test_operacional_finish_lote_endpoint(client, monkeypatch):
    login(client)
    def fake(lote_id):
        assert lote_id == "l1"
        return {"lote_id": lote_id, "status_lote": "CONCLUIDO"}, False
    monkeypatch.setattr("app.gestao_op_finish_lote", fake)
    resp = client.post("/api/gestao/operacional/lotes/l1/finish")
    assert resp.status_code == 200
    assert resp.get_json()["data"]["status_lote"] == "CONCLUIDO"


def test_operacional_liberar_proximos_leads_prioriza_e_retorna_quantidade(monkeypatch):
    leads = [
        {"sk_pessoa": 2, "cpf": "2", "nome": "B", "celular": "119", "email": "b@x.com", "curso": "Direito", "modalidade": None, "turno": None, "polo": "SP", "origem": "Site", "tipo_negocio": None, "campanha": "C", "canal": None, "acao_comercial": None, "tipo_disparo": "ROBO", "score_prioridade": 90, "nivel_prioridade": "ALTA", "etapa_operacional": "NOVO"},
        {"sk_pessoa": 1, "cpf": "1", "nome": "A", "celular": "118", "email": "a@x.com", "curso": "Direito", "modalidade": None, "turno": None, "polo": "SP", "origem": "Site", "tipo_negocio": None, "campanha": "C", "canal": None, "acao_comercial": None, "tipo_disparo": "ROBO", "score_prioridade": 80, "nivel_prioridade": "MEDIA", "etapa_operacional": "NOVO"},
    ]
    calls = []
    import services.gestao_operacional as op
    monkeypatch.setattr(op, "get_leads_disponiveis", lambda filters, meta: ({"items": leads[:meta["limit"]]}, False))
    monkeypatch.setattr(op, "_run", lambda sql, params=None, operation="": calls.append((sql, params or [], operation)) or [])
    monkeypatch.setattr(op, "_evento", lambda *args, **kwargs: calls.append(("EVENT", args, "evento")))
    monkeypatch.setattr(op, "invalidate_gestao_cache", lambda: None)
    data, _ = op.liberar_proximos_leads({"tipo_disparo": "ROBO", "quantidade": 2, "campanha": "C"})
    assert data["quantidade_liberada"] == 2
    assert any("NOT EXISTS" in c[0] and "PENDENTE" in c[0] for c in calls if isinstance(c[0], str))


def test_operacional_update_status_recalcula_lote(monkeypatch):
    import services.gestao_operacional as op
    calls = []
    monkeypatch.setattr(op, "_single", lambda *a, **k: {"status_atendimento": "PENDENTE", "cpf": "123", "retorno": False, "positivo": False, "negativo": False, "matriculado": False})
    monkeypatch.setattr(op, "_run", lambda sql, params=None, operation="": calls.append(operation) or [])
    monkeypatch.setattr(op, "_evento", lambda *a, **k: calls.append("evento"))
    monkeypatch.setattr(op, "recalcular_metricas_lote", lambda lote_id: calls.append(f"recalc:{lote_id}") or {})
    monkeypatch.setattr(op, "invalidate_gestao_cache", lambda: calls.append("cache"))
    data, _ = op.update_lead_status(123, {"lote_id": "l1", "status_atendimento": "AC"})
    assert data["retorno"] is True
    assert "recalc:l1" in calls
    assert "cache" in calls


def test_operacional_finalizar_lote_exige_confirmacao_com_pendentes(monkeypatch):
    import services.gestao_operacional as op
    monkeypatch.setattr(op, "_single", lambda sql, *a, **k: {"qtd": 1} if "COUNT(*) qtd" in sql else {"status_lote": "ABERTO"})
    with pytest.raises(ValueError):
        op.finish_lote("l1", {})


def test_operacional_executar_regra_automatica_respeita_limite(monkeypatch):
    import services.gestao_operacional as op
    regras = [{"regra_id": "r1", "tipo_disparo": "ROBO", "consultor_disparo": "", "campanha": "C", "limite_lotes_ativos": 1, "quantidade_por_lote": 500}]
    monkeypatch.setattr(op, "_run", lambda sql, params=None, operation="": regras if operation == "operacional_regras_ativas" else [])
    monkeypatch.setattr(op, "_single", lambda *a, **k: {"qtd": 1})
    monkeypatch.setattr(op, "_evento", lambda *a, **k: None)
    monkeypatch.setattr(op, "invalidate_gestao_cache", lambda: None)
    data, _ = op.executar_regras_distribuicao()
    assert data["items"][0]["criado"] is False
    assert "limite" in data["items"][0]["motivo"]


def test_operacional_recalcular_metricas_lote_atualiza_totais(monkeypatch):
    import services.gestao_operacional as op
    calls = []
    monkeypatch.setattr(op, "_metrics", lambda lote_id: {"total": 2, "total_retorno": 1, "total_positivo": 1, "total_negativo": 0, "total_matriculas": 1, "taxa_retorno": 50.0, "taxa_matricula": 50.0})
    monkeypatch.setattr(op, "_run", lambda sql, params=None, operation="": calls.append((sql, {p.name: p.value for p in params}, operation)) or [])
    m = op.recalcular_metricas_lote("l1")
    assert m["total_matriculas"] == 1
    assert calls[0][1]["txr"] == 50.0
    assert calls[0][2] == "operacional_recalcular_lote"


def test_operacional_fila_order_and_new_lead_rules(monkeypatch):
    from services import gestao_operacional as op
    captured = {}
    def fake(sql, params=None, operation=''):
        captured['sql'] = sql
        return []
    monkeypatch.setattr(op, '_run', fake)
    data, cached = op.get_fila_leads({}, {'limit': 10, 'offset': 0})
    sql = captured['sql']
    assert 'l.data_inscricao DESC' in sql
    assert 'l.score_prioridade DESC' in sql
    assert 'l.nunca_disparado DESC' in sql
    assert 'l.data_disparo IS NULL' in sql
    assert 'COALESCE(l.flag_matriculado, FALSE) = FALSE' in sql
    assert 'COALESCE(l.nunca_disparado, FALSE) = TRUE' in sql
    assert "TRIM(l.consultor_disparo) = ''" in sql
    assert "TRIM(l.tipo_disparo) = ''" in sql
    assert data['items'] == [] and cached is False


def test_operacional_lotes_select_endpoint(client, monkeypatch):
    login(client)
    monkeypatch.setattr('app.gestao_op_get_lotes_select', lambda: ({'items': [{'lote_id': 'L1'}]}, False))
    resp = client.get('/api/gestao/operacional/lotes-select')
    assert resp.status_code == 200
    assert resp.get_json()['data'][0]['lote_id'] == 'L1'


def test_operacional_preview_endpoint(client, monkeypatch):
    login(client)
    monkeypatch.setattr('app.gestao_op_preview_proximo_lote', lambda filters: ({'items': [], 'total_disponivel': 0}, False))
    resp = client.get('/api/gestao/operacional/preview-proximo-lote?quantidade=5')
    assert resp.status_code == 200
    assert resp.get_json()['data']['total_disponivel'] == 0


def test_operacional_export_endpoint(client, monkeypatch):
    login(client)
    monkeypatch.setattr('app.gestao_op_exportar_proximo_lote', lambda payload: ({'lote_id': 'L1', 'quantidade_exportada': 1, 'download_url': '/x'}, False))
    resp = client.post('/api/gestao/operacional/exportar-proximo-lote', json={'tipo_disparo': 'ROBO', 'quantidade': 1})
    assert resp.status_code == 201
    assert resp.get_json()['data']['lote_id'] == 'L1'


def test_operacional_importar_lote_disparado_endpoint(client, monkeypatch):
    login(client)
    monkeypatch.setattr('app.gestao_op_importar_lote_disparado', lambda file, lote_id, usuario: ({'lote_id': lote_id, 'linhas_lidas': 1, 'linhas_atualizadas': 1, 'linhas_rejeitadas': 0, 'erros': []}, False))
    resp = client.post('/api/gestao/operacional/importar-lote-disparado', data={'lote_id': 'L1', 'usuario': 'u', 'file': (BytesIO(b'sk_pessoa,status_atendimento\n1,AC\n'), 'r.csv')}, content_type='multipart/form-data')
    assert resp.status_code == 200
    assert resp.get_json()['data']['linhas_atualizadas'] == 1


def test_operacional_importar_novos_leads_validation_endpoint(client, monkeypatch):
    login(client)
    monkeypatch.setattr('app.gestao_op_importar_novos_leads', lambda file, metadata: ({'linhas_lidas': 1, 'linhas_validas': 1, 'message': 'ok'}, False))
    resp = client.post('/api/gestao/operacional/importar-novos-leads', data={'file': (BytesIO(b'nome,cpf,curso,polo\nA,1,C,P\n'), 'n.csv')}, content_type='multipart/form-data')
    assert resp.status_code == 200
    assert resp.get_json()['data']['upload_url'] == '/api/upload'


def test_operacional_create_tables_error_has_details(client, monkeypatch):
    login(client)
    def boom():
        raise RuntimeError('op_lotes_disparo not found')
    monkeypatch.setattr('app.gestao_op_create_tables', boom)
    resp = client.post('/api/gestao/operacional/admin/create-tables')
    assert resp.status_code == 500
    assert resp.get_json()['error']['code'] == 'GESTAO_OPERACIONAL_CREATE_TABLES_ERROR'
