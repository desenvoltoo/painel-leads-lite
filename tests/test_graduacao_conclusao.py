import pandas as pd

from services import database as db


def test_prepare_upload_dataframe_mapeia_graduacao_e_conclusao():
    df = pd.DataFrame(
        [
            {
                "Nome": "Lead Teste",
                "CPF": "12345678901",
                "Celular": "11999999999",
                "Formação": "Administração",
                "Ano Conclusão": "2025",
            }
        ]
    )

    prepared = db._prepare_upload_dataframe(
        df,
        filename="novos_campos.xlsx",
        upload_id="upload-academico-001",
    )

    assert len(prepared) == 1
    assert prepared.loc[0, "graduacao"] == "Administração"
    assert prepared.loc[0, "conclusao"] == "2025"
    assert prepared.loc[0, "upload_id"] == "upload-academico-001"


def test_campos_academicos_estao_na_api_e_exportacao():
    assert "graduacao" in db.LEADS_COLUMNS
    assert "conclusao" in db.LEADS_COLUMNS
    assert "graduacao" in db.EXPORT_ORDER
    assert "conclusao" in db.EXPORT_ORDER


def test_filtros_sql_incluem_graduacao_e_conclusao(monkeypatch):
    monkeypatch.setattr(db, "_has_view_col", lambda column: True)

    params = []
    sql = db._apply_filters(
        "SELECT * FROM modelo_estrela.vw_leads_painel_lite v WHERE 1=1",
        {
            "graduacao": ["Administração"],
            "conclusao": ["2025"],
        },
        params,
    )

    assert "v.graduacao::text = ANY(@f_graduacao)" in sql
    assert "v.conclusao::text = ANY(@f_conclusao)" in sql

    values = {param.name: param.value for param in params}
    assert values["f_graduacao"] == ["Administração"]
    assert values["f_conclusao"] == ["2025"]
