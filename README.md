# Painel Leads Lite

Painel Flask para operação de leads com BigQuery, upload controlado e dashboard gerencial responsivo.

## Visão geral

O projeto contém dois fluxos principais:

- `/`: painel operacional de leads já existente.
- `/gestao`: Gestão Operacional com KPIs, funil, evolução temporal, rankings, produtividade, fila inteligente, qualidade de dados e histórico de importações.

A página `/gestao` carrega a estrutura HTML primeiro e busca dados por endpoints JSON protegidos, evitando injetar um grande objeto no template.

## Arquitetura

- `app.py`: fábrica Flask, autenticação, rotas públicas/protegidas, upload e endpoints JSON.
- `services/bigquery.py`: cliente BigQuery, consultas do painel legado, upload para staging e procedure.
- `services/gestao.py`: validação de filtros, cache, regras de KPIs, funil, rankings, produtividade, fila, qualidade e importações.
- `templates/gestao.html`: layout do dashboard gerencial.
- `static/js/gestao.js`: estado, filtros, API, cards, gráficos, tabelas, mensagens e exportação CSV client-side via DataTables.
- `static/css/gestao.css`: design responsivo com variáveis CSS.
- `sql/migrations/20260610_gestao_import_logs.sql`: criação idempotente da tabela de logs de importação e view mascarada de rejeições.

## Autenticação

Todas as rotas, exceto `/login`, `/health`, `/api/auth/login` e arquivos estáticos, exigem sessão autenticada. APIs administrativas retornam `401` em JSON quando a sessão expira.

## Endpoints principais

### Leads e upload

- `GET /api/leads`
- `POST /api/leads/search`
- `GET /api/options`
- `GET /api/export/xlsx`
- `POST /api/export/batch`
- `POST /api/upload` — única rota oficial de upload.
- `GET /api/upload/status`

### Gestão Operacional

- `GET /api/gestao/resumo`
- `GET /api/gestao/funil`
- `GET /api/gestao/evolucao`
- `GET /api/gestao/rankings`
- `GET /api/gestao/produtividade`
- `GET /api/gestao/fila`
- `GET /api/gestao/qualidade`
- `GET /api/gestao/importacoes`
- `GET /api/gestao/opcoes`

Formato de sucesso:

```json
{
  "ok": true,
  "data": {},
  "meta": {
    "generated_at": "2026-06-10T00:00:00Z",
    "filters": {},
    "cached": false
  }
}
```

Formato de erro:

```json
{
  "ok": false,
  "error": {
    "code": "GESTAO_QUERY_ERROR",
    "message": "Não foi possível carregar os dados."
  }
}
```

## Filtros da Gestão

Filtros aceitos:

- `data_ini`, `data_fim`;
- `consultor_comercial`, `consultor_disparo`;
- `curso`, `polo`, `modalidade`, `turno`;
- `origem`, `campanha`, `canal`, `tipo_negocio`, `tipo_disparo`;
- `status`, incluindo `__EMPTY__` para vazio;
- `matriculado=sim|nao`;
- `busca` por nome, CPF, celular ou e-mail;
- `granularidade=dia|semana|mes` para evolução;
- `limit`, `offset`, `order_by`, `order_dir` em tabelas.

Datas inválidas e `data_ini > data_fim` retornam erro `GESTAO_INVALID_FILTER`.

## Regras de cálculo

- Lead nunca trabalhado: status vazio.
- Matriculado: `flag_matriculado` quando disponível ou status/valor textual normalizado (`MAT`, `MATRICULADO`, `SIM`, etc.).
- Carteira: status preenchido e não matriculado.
- Conversão: matriculados / total de leads.
- Duplicidade: quantidade excedente por chave, calculada como `SUM(qtd - 1)` em grupos com mais de uma ocorrência.
- Rankings de conversão: exigem mínimo configurável de leads (`GESTAO_MIN_RANKING_LEADS`, padrão 10).

## Regra de `dt_upload`

- `dt_upload` é injetado pelo backend em cada upload.
- A tabela fato deve usar `data_atualizacao` como a versão do `dt_upload` aceito.
- A procedure de importação deve aceitar atualização somente quando `S.dt_upload >= F.data_atualizacao`.
- `data_inscricao`, `data_disparo`, `data_ultima_acao`, `dt_upload` e `data_atualizacao` têm significados distintos.

## Score da fila operacional

O score é calculado no BigQuery a partir de campos disponíveis:

- matriculados: score 5 e prioridade baixa;
- sem status: +35;
- sem última ação: +25;
- dias desde inscrição: +0,35 por dia até 20;
- dias sem ação: +0,60 por dia até 30;
- acionamentos: +10 sem ação, +5 com 1 a 2, -5 com mais;
- origem/campanha ausentes: pequeno acréscimo;
- carga recente por `dt_upload` ou `data_atualizacao`: +8 nos últimos 3 dias.

## Cache

- TTL: `GESTAO_CACHE_TTL_SECONDS`.
- Tamanho: `GESTAO_CACHE_MAXSIZE`.
- Chave: endpoint + filtros normalizados + paginação/granularidade.
- `force_refresh=1` ignora cache.
- Upload concluído invalida o cache.
- Filtros pessoais como `busca`, CPF, celular, e-mail e nome não são cacheados.

## Variáveis de ambiente

- `FLASK_SECRET_KEY`
- `SESSION_TTL_SECONDS`
- `COOKIE_SECURE`
- `SESSION_COOKIE_NAME`
- `GCP_PROJECT_ID`
- `BQ_DATASET`
- `BQ_LOCATION`
- `BQ_VIEW_LEADS`
- `BQ_STAGING_TABLE`
- `BQ_PROCEDURE`
- `BQ_QUERY_TIMEOUT_SECONDS`
- `GESTAO_CACHE_TTL_SECONDS`
- `GESTAO_CACHE_MAXSIZE`
- `GESTAO_DEFAULT_PAGE_SIZE`
- `GESTAO_MAX_PAGE_SIZE`
- `GESTAO_MIN_RANKING_LEADS`
- `UPLOAD_DIR`
- `EXPORT_DIR`

## Upload

A rota oficial é `POST /api/upload`. Ela aceita CSV, XLSX e XLS, valida extensão, lê conteúdo com pandas, normaliza colunas no serviço BigQuery, injeta `dt_upload`, carrega `stg_leads_site` e dispara a procedure assíncrona.

Rotas antigas via GCS retornam `410` com orientação para usar `/api/upload`.

## Migração SQL opcional

Aplique manualmente quando quiser habilitar o histórico de importações:

```bash
bq query --use_legacy_sql=false < sql/migrations/20260610_gestao_import_logs.sql
```

O script é idempotente e não executa operações destrutivas.

## Executar localmente

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
flask --app app:create_app run --debug
```

## Testes

```bash
python -m compileall .
pytest -q
```

Os testes usam mocks e não dependem de conexão real com o BigQuery.

## Deploy Cloud Run

O repositório possui `Dockerfile` e `cloudbuild.yaml`. Configure variáveis de ambiente no Cloud Run, faça o build da imagem e publique mantendo as permissões da service account para BigQuery.
