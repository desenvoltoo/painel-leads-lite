@"
# Painel Leads Lite

## Rodar local
1) Criar venv e instalar deps:
\`\`\`powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
\`\`\`

2) Variáveis de ambiente (Windows):
\`\`\`powershell
setx GCP_PROJECT_ID "painel-universidade"
setx BQ_DATASET "modelo_estrela"
setx BQ_VIEW_LEADS "vw_leads_painel_lite"
setx BQ_DATE_FIELD "data_inscricao"
setx BQ_UPLOAD_TABLE "stg_leads_upload"
setx BQ_PROMOTE_PROC "sp_promote_stg_leads_upload"
\`\`\`

3) Rodar:
\`\`\`powershell
python app.py
\`\`\`
"@ | Out-File -Encoding utf8 README.md
