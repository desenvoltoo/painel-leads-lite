# -*- coding: utf-8 -*-
import os
from typing import Any, Dict, List, Optional

def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

def _has_bq() -> bool:
    try:
        from google.cloud import bigquery  # noqa: F401
        return True
    except Exception:
        return False

def _mock_rows(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Dados de exemplo para o painel funcionar sem BigQuery configurado
    sample = [
        {
            "data_inscricao": "2026-01-02",
            "nome": "Ana Silva",
            "cpf": "000.000.000-00",
            "celular": "11999990000",
            "email": "ana@email.com",
            "origem": "Meta Ads",
            "polo": "Tatuapé",
            "curso": "Administração",
            "status": "NOVO",
            "consultor": "Joyce",
        },
        {
            "data_inscricao": "2026-01-06",
            "nome": "Bruno Costa",
            "cpf": "111.111.111-11",
            "celular": "11988880000",
            "email": "bruno@email.com",
            "origem": "Google Ads",
            "polo": "São Miguel",
            "curso": "Enfermagem",
            "status": "CONTATADO",
            "consultor": "Fabio",
        },
        {
            "data_inscricao": "2026-01-08",
            "nome": "Carla Souza",
            "cpf": "222.222.222-22",
            "celular": "11977770000",
            "email": "carla@email.com",
            "origem": "Orgânico",
            "polo": "República",
            "curso": "RH",
            "status": "MATRICULADO",
            "consultor": "Guilherme",
        },
    ]

    # Filtro simples no mock
    def ok(r):
        for k in ("status", "curso", "polo", "origem"):
            if filters.get(k) and str(r.get(k, "")).lower() != str(filters[k]).lower():
                return False
        # data range
        di, df = filters.get("data_ini"), filters.get("data_fim")
        if di and r["data_inscricao"] < di:
            return False
        if df and r["data_inscricao"] > df:
            return False
        return True

    limit = int(filters.get("limit") or 500)
    return [r for r in sample if ok(r)][:limit]

def query_leads(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Consulta a VIEW no BigQuery usando parâmetros.
    Espera variáveis:
      GCP_PROJECT_ID, BQ_DATASET, BQ_VIEW_LEADS
    """
    if not _has_bq():
        return _mock_rows(filters)

    project = _env("GCP_PROJECT_ID", "")
    dataset = _env("BQ_DATASET", "marts")
    view = _env("BQ_VIEW_LEADS", "vw_leads_painel")

    # Se projeto/credenciais não estiverem configurados, cai no mock
    if project.strip() == "":
        return _mock_rows(filters)

    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=project)

        sql = f"""
        SELECT
          data_inscricao,
          nome,
          cpf,
          celular,
          email,
          origem,
          polo,
          curso,
          status,
          consultor
        FROM `{project}.{dataset}.{view}`
        WHERE 1=1
          AND (@status IS NULL OR UPPER(status) = UPPER(@status))
          AND (@curso  IS NULL OR UPPER(curso)  = UPPER(@curso))
          AND (@polo   IS NULL OR UPPER(polo)   = UPPER(@polo))
          AND (@origem IS NULL OR UPPER(origem) = UPPER(@origem))
          AND (@data_ini IS NULL OR DATE(data_inscricao) >= DATE(@data_ini))
          AND (@data_fim IS NULL OR DATE(data_inscricao) <= DATE(@data_fim))
        ORDER BY DATE(data_inscricao) DESC
        LIMIT @limit
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("status", "STRING", filters.get("status")),
                bigquery.ScalarQueryParameter("curso", "STRING", filters.get("curso")),
                bigquery.ScalarQueryParameter("polo", "STRING", filters.get("polo")),
                bigquery.ScalarQueryParameter("origem", "STRING", filters.get("origem")),
                bigquery.ScalarQueryParameter("data_ini", "STRING", filters.get("data_ini")),
                bigquery.ScalarQueryParameter("data_fim", "STRING", filters.get("data_fim")),
                bigquery.ScalarQueryParameter("limit", "INT64", int(filters.get("limit") or 500)),
            ]
        )

        rows = client.query(sql, job_config=job_config).result()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({k: r.get(k) for k in r.keys()})
        return out

    except Exception as e:
        print("🚨 BigQuery ERROR:", repr(e))
        return _mock_rows(filters)
