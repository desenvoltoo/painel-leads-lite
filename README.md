# Painel Leads Lite (Flask + BigQuery)

Painel leve e intuitivo para consulta de leads diretamente em uma VIEW do BigQuery.

## 1) Rodar local

```bash
pip install -r requirements.txt
python app.py
```

Acesse: http://localhost:8080

> Se BigQuery não estiver configurado, a API devolve dados de exemplo.

## 2) Configurar BigQuery

Defina as variáveis de ambiente:

- `GOOGLE_APPLICATION_CREDENTIALS` = caminho do JSON do Service Account
- `GCP_PROJECT_ID` = seu projeto (ex: painel-universidade)
- `BQ_DATASET` = dataset (ex: marts)
- `BQ_VIEW_LEADS` = view (ex: vw_leads_painel)

A VIEW deve conter (ou mapear para) as colunas:
- `data_inscricao` (DATE ou STRING)
- `nome`
- `cpf`
- `celular`
- `email`
- `origem`
- `polo`
- `curso`
- `status`
- `consultor`

## 3) Deploy (Cloud Run)

Exemplo (ajuste projeto e região):

```bash
gcloud run deploy painel-leads-lite \
  --source . \
  --region us-central1 \
  --allow-unauthenticated
```

## 4) GitHub

Estrutura já pronta pra subir como repositório.
