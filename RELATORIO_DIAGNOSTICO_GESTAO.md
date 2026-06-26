# Relatório de Diagnóstico — Gestão Operacional de Leads

Data da análise: 2026-06-26  
Projeto BigQuery informado: `painel-universidade`  
Dataset BigQuery informado: `modelo_estrela`

> Escopo: análise estática do projeto Flask + BigQuery, sem alterações funcionais. O único artefato criado é este relatório.

## 1. Mapa de arquivos

### Raiz da aplicação

| Arquivo | Papel observado |
|---|---|
| `app.py` | Aplicação Flask principal: cria app, configura sessão/autenticação, registra páginas, APIs de gestão, APIs operacionais, upload oficial, exportações e endpoints legados desativados. |
| `wsgi.py` | Ponto de entrada WSGI. |
| `startup_diagnostics.py` | Utilitários de diagnóstico/configuração usados por `app.py`. |
| `requirements.txt` | Dependências Python. |
| `Dockerfile`, `cloudbuild.yaml` | Empacotamento/deploy. |
| `README.md` | Documentação geral do projeto. |
| `docs/gestao.md` | Documentação específica de gestão. |
| `pyrightconfig.json` | Configuração de análise estática. |

### Templates

| Template | Papel observado |
|---|---|
| `templates/index.html` | Tela principal/consulta de leads. |
| `templates/gestao.html` | Tela de gestão operacional: navegação por módulos, dashboard, importação de leads, geração de lote, importação de retorno, lotes e logs. |
| `templates/login.html` | Tela de login. |

### Estáticos

| Arquivo | Papel observado |
|---|---|
| `static/js/app.js` | JavaScript da tela principal. |
| `static/js/gestao.js` | JavaScript da tela de gestão operacional; chama APIs `/api/gestao/operacional/*` e o upload oficial `/api/upload`. |
| `static/css/styles.css` | CSS global/tela principal. |
| `static/css/gestao.css` | CSS específico da tela de gestão operacional. |

### Serviços Python

| Arquivo | Papel observado |
|---|---|
| `services/bigquery.py` | Camada BigQuery geral: cliente, consultas de leads, opções, exportações, staging/upload, jobs de exportação e status de job. |
| `services/gestao.py` | Camada de gestão analítica: resumo, funil, evolução, rankings, produtividade, fila, qualidade, importações, rejeições, cache e logs de importação. |
| `services/gestao_operacional.py` | Camada operacional: dashboard de lotes, leads disponíveis, criação/exportação/importação/finalização/cancelamento de lotes, regras de distribuição, logs operacionais. |

### SQL e testes

| Caminho | Papel observado |
|---|---|
| `sql/gestao_views.sql`, `sql/gestao_operacional_views.sql` | Views de apoio para gestão e operação. |
| `sql/migrations/20260625_operacao_lotes_disparo.sql` | Estrutura operacional atual usada pelo endpoint administrativo de criação de tabelas. |
| `sql/migrations/20260610_gestao_import_logs.sql`, `sql/migrations/create_logs_importacoes.sql` | Estruturas de logs de importação. |
| `tests/conftest.py`, `tests/test_gestao.py` | Testes existentes focados em gestão. |

## 2. Rotas encontradas

### Páginas e autenticação

| Método | Rota | Função |
|---|---|---|
| `GET` | `/` | Renderiza `index.html`. |
| `GET` | `/gestao` | Renderiza `gestao.html`. |
| `GET` | `/gestao/exportar-prioritarios` | Exporta fila prioritária por CSV via serviço de gestão. |
| `GET` | `/login` | Renderiza `login.html`. |
| `POST` | `/api/auth/login` | Autentica usuário em memória e grava sessão Flask. |
| `POST` | `/logout` | Limpa sessão e cookie. |
| `GET` | `/health` | Healthcheck básico. |

### APIs analíticas de gestão

| Método | Rota | Serviço chamado |
|---|---|---|
| `GET` | `/api/gestao/resumo` | `services.gestao.get_resumo` |
| `GET` | `/api/gestao/funil` | `services.gestao.get_funil` |
| `GET` | `/api/gestao/evolucao` | `services.gestao.get_evolucao` |
| `GET` | `/api/gestao/rankings` | `services.gestao.get_rankings` |
| `GET` | `/api/gestao/produtividade` | `services.gestao.get_produtividade` |
| `GET` | `/api/gestao/fila` | `services.gestao.get_fila` |
| `GET` | `/api/gestao/qualidade-dados` | `services.gestao.get_qualidade_dados` |
| `GET` | `/api/gestao/qualidade` | `services.gestao.get_qualidade` |
| `GET` | `/api/gestao/qualidade/detalhes` | `services.gestao.get_qualidade_detalhes` |
| `GET` | `/api/gestao/qualidade/exportar` | `services.gestao.export_qualidade` |
| `GET` | `/api/gestao/importacoes` | `services.gestao.get_importacoes_historico` |
| `GET` | `/api/gestao/importacoes/exportar` | `services.gestao.export_importacoes` |
| `GET` | `/api/gestao/fila/exportar` | `services.gestao.export_fila` |
| `GET` | `/api/gestao/produtividade/exportar` | `services.gestao.export_produtividade` |
| `GET` | `/api/gestao/rejeicoes` | `services.gestao.get_rejeicoes` |
| `GET` | `/api/gestao/rejeicoes/exportar` | `services.gestao.export_rejeicoes` |
| `GET` | `/api/gestao/opcoes` | `services.gestao.get_opcoes` |

### APIs legadas/paralelas de histórico de importações

| Método | Rota | Observação |
|---|---|---|
| `GET` | `/api/importacoes/historico` | Retorna formato `success/data/pagination`, paralelo a `/api/gestao/importacoes`. |
| `GET` | `/api/importacoes/historico/exportar` | Exporta histórico, paralelo a `/api/gestao/importacoes/exportar`. |

### APIs operacionais de gestão de lotes

| Método | Rota | Serviço chamado / uso |
|---|---|---|
| `GET` | `/api/gestao/operacional/dashboard` | `get_dashboard` |
| `GET` | `/api/gestao/operacional/lotes-select` | `get_lotes_select` |
| `GET` | `/api/gestao/operacional/preview-proximo-lote` | `preview_proximo_lote` |
| `POST` | `/api/gestao/operacional/exportar-proximo-lote` | Cria lote e devolve CSV/XLSX para download. |
| `POST` | `/api/gestao/operacional/importar-lote-disparado` | Importa retorno de lote disparado. |
| `POST` | `/api/gestao/operacional/importar-novos-leads` | Valida arquivo, mas orienta usar `/api/upload`; não é a rota oficial de upload. |
| `GET` | `/api/gestao/operacional/fila-leads` | Alias para leads disponíveis. |
| `GET` | `/api/gestao/operacional/leads-disponiveis` | Alias para leads disponíveis. |
| `GET` | `/api/gestao/operacional/logs` | Logs operacionais. |
| `POST` | `/api/gestao/operacional/liberar-proximos-leads` | Fluxo operacional alternativo de liberação. |
| `POST` | `/api/gestao/operacional/executar-regras-distribuicao` | Executa regras automáticas. |
| `GET` | `/api/gestao/operacional/esteira` | Esteira operacional. |
| `GET` | `/api/gestao/operacional/fila-prioridade` | Fila por prioridade. |
| `GET` | `/api/gestao/operacional/regras-distribuicao` | Lista regras. |
| `POST` | `/api/gestao/operacional/regras-distribuicao` | Cria regra. |
| `PATCH` | `/api/gestao/operacional/regras-distribuicao/<regra_id>` | Ativa/desativa regra. |
| `POST` | `/api/gestao/operacional/lotes` | Cria lote. |
| `GET` | `/api/gestao/operacional/lotes` | Lista lotes. |
| `GET` | `/api/gestao/operacional/lotes/<lote_id>` | Detalhe do lote. |
| `POST` | `/api/gestao/operacional/lotes/<lote_id>/start` | Inicia lote. |
| `POST` | `/api/gestao/operacional/lotes/<lote_id>/cancel` | Cancela lote. |
| `POST` | `/api/gestao/operacional/lotes/<lote_id>/finish` | Finaliza lote. |
| `GET` | `/api/gestao/operacional/meus-leads` | Lista leads de consultor. |
| `PATCH` | `/api/gestao/operacional/leads/<sk_pessoa>/status` | Atualiza status de lead no lote. |
| `POST` | `/api/gestao/operacional/admin/create-tables` | Cria estruturas operacionais atuais por migration local. |

### APIs da tela principal, exportação e upload

| Método | Rota | Observação |
|---|---|---|
| `GET` | `/api/leads` | Consulta leads com filtros por query string. |
| `POST` | `/api/leads/search` | Consulta leads por JSON. |
| `GET` | `/api/kpis` | KPIs simples sobre amostra/limite consultado. |
| `POST` | `/api/kpis/search` | KPIs simples por JSON. |
| `GET` | `/api/options` | Opções de filtros. |
| `GET` | `/api/export/xlsx` | Exportação XLSX síncrona. |
| `POST` | `/api/export/batch` | Exportação CSV em lote assíncrona. |
| `GET` | `/api/export/batch/status` | Status do job de exportação. |
| `GET` | `/api/export/batch/download` | Download do arquivo exportado. |
| `GET` | `/api/upload-url` | Desativada; orienta usar `POST /api/upload`. |
| `POST` | `/api/process-upload` | Desativada; orienta usar `POST /api/upload`. |
| `POST` | `/api/upload` | Rota oficial de upload CSV/XLSX/XLS. |
| `GET` | `/api/upload/status` | Consulta status do job BigQuery do upload. |

## 3. Templates principais e assets da tela de gestão

A tela de gestão está concentrada em `templates/gestao.html` e possui módulos para início, importação de leads, geração de lote, importação de retorno, lotes e logs. O template carrega os assets `static/css/styles.css`, `static/css/gestao.css`, `static/js/app.js` e `static/js/gestao.js`.

O JavaScript operacional está concentrado em `static/js/gestao.js`. Ele usa um helper `opFetch()` que prefixa chamadas com `/api/gestao/operacional/`, exceto a importação oficial de novos leads, que chama diretamente `/api/upload`.

## 4. Serviços Python que acessam BigQuery

| Serviço | Acesso BigQuery observado | Pontos principais |
|---|---|---|
| `services/bigquery.py` | Usa `google.cloud.bigquery.Client`, executa queries, carrega staging e gerencia export jobs. | É a camada base de BigQuery e contém o fluxo oficial de upload via `process_upload_dataframe()`. |
| `services/gestao.py` | Usa `services.bigquery._run_gestao_query()` e parâmetros BigQuery. | Consulta views/tabelas analíticas legadas como `vw_leads_painel_lite`, `f_lead`, dimensões, `logs_importacoes` e `logs_rejeicoes_import`. |
| `services/gestao_operacional.py` | Usa `services.bigquery._run_gestao_query()` e parâmetros BigQuery. | Implementa lotes e operação com tabelas como `op_lotes_disparo`, `op_lote_leads`, `op_lead_eventos`, `op_bigquery_sync`, `op_regras_distribuicao` e fallback de views. |

## 5. Fluxo atual de upload

1. A tela `gestao.html`, módulo “Importar Leads”, declara que usa diretamente `POST /api/upload`.
2. `static/js/gestao.js` envia `FormData` de `#opImportarNovosForm` para `/api/upload`.
3. `app.py` valida presença do arquivo, nome e extensão permitida (`.csv`, `.xlsx`, `.xls`).
4. A rota cria `importacao_id/upload_id`, registra log de importação em `logs_importacoes`, lê o arquivo em DataFrame, valida arquivo vazio, atualiza etapas do log e chama `services.bigquery.process_upload_dataframe(df, filename=filename)`.
5. Após o processamento, atualiza o log como `CONCLUIDO` ou `CONCLUIDO_COM_REJEICOES`, invalida cache de gestão e retorna `202` com dados do job/procedure.
6. `/api/upload/status` consulta status do job BigQuery por `job_id`.
7. Rotas antigas `/api/upload-url` e `/api/process-upload` estão desativadas com HTTP `410` e orientam usar `POST /api/upload`.
8. Existe também `/api/gestao/operacional/importar-novos-leads`, mas o serviço apenas valida colunas mínimas e retorna mensagem orientando o uso de `POST /api/upload`; a tela atual não usa essa rota para importar novos leads.

## 6. Fluxo atual de geração/exportação de lote

1. A tela `gestao.html`, módulo “Gerar Lote”, apresenta filtros, quantidade, tipo de disparo e formato.
2. `static/js/gestao.js` usa `GET /api/gestao/operacional/preview-proximo-lote` para pré-visualizar leads.
3. Ao submeter o formulário, chama `POST /api/gestao/operacional/exportar-proximo-lote`.
4. `app.py` chama `services.gestao_operacional.exportar_proximo_lote(payload)`.
5. `exportar_proximo_lote()` chama `criar_lote(payload)`, busca o detalhe do lote, monta linhas enriquecidas com `lote_id/status_atendimento`, registra evento `LOTE_EXPORTADO` e devolve CSV ou XLSX em base64.
6. A rota decodifica o conteúdo e responde com `send_file()` para download.
7. O serviço atual implementa criação com `INSERT` direto em `op_lotes_disparo` e `op_lote_leads`, usando a view operacional disponível ou fallback, em vez de chamar explicitamente a procedure oficial `sp_op_criar_lote`.

## 7. Fluxo atual de importação de retorno

1. A tela `gestao.html`, módulo “Importar Retorno”, seleciona lote e arquivo CSV/XLSX.
2. `static/js/gestao.js` envia o formulário para `POST /api/gestao/operacional/importar-lote-disparado`.
3. `app.py` valida a presença do arquivo e chama `services.gestao_operacional.importar_lote_disparado(file, lote_id, usuario)`.
4. O serviço lê as linhas, valida `lote_id`, localiza cada lead por `sk_pessoa`, CPF ou celular, normaliza status e chama `update_lead_status()` por linha.
5. Após processar as linhas, recalcula métricas do lote, grava um registro em `op_bigquery_sync`, registra evento `LOTE_RESULTADO_IMPORTADO`, invalida cache e retorna contadores de atualizados, rejeitados e não encontrados.
6. O fluxo atual faz atualizações linha a linha em Python/BigQuery e não chama explicitamente a procedure oficial `sp_op_processar_retorno_lote`.

## 8. Duplicidades, inconsistências e problemas encontrados

### 8.1 Rotas duplicadas ou paralelas

- `GET /api/gestao/operacional/fila-leads` e `GET /api/gestao/operacional/leads-disponiveis` chamam o mesmo serviço de leads disponíveis.
- `GET /api/importacoes/historico` e `GET /api/gestao/importacoes` consultam o mesmo domínio, mas retornam contratos diferentes (`success` versus `ok/meta`).
- `GET /api/importacoes/historico/exportar` e `GET /api/gestao/importacoes/exportar` exportam o mesmo domínio com pequenas diferenças de headers/contrato de erro.
- Há múltiplos fluxos de exportação: exportação XLSX da tela principal, exportação batch CSV, exportação de fila prioritária e exportação operacional de lote.
- Há rotas antigas de upload desativadas corretamente, mas ainda expostas: `/api/upload-url` e `/api/process-upload`.

### 8.2 Desalinhamento com fontes e procedures oficiais informadas

- O serviço operacional atual ainda usa estruturas como `op_lead_eventos`, `op_bigquery_sync`, `op_regras_distribuicao`, `op_config_operacional` e fallback `vw_leads_priorizados`, enquanto o contexto informado aponta para fontes oficiais como `op_lead_timeline`, `op_auditoria_painel`, `vw_op_lotes_resumo`, `vw_op_fluxo_lotes`, `vw_op_export_lote_csv` e `vw_op_debug_fila`.
- A criação de lote está implementada em Python com `INSERT` direto, não como chamada a `sp_op_criar_lote`.
- A marcação de disparo/início, processamento de retorno, recálculo de métricas e finalização de lote estão implementados diretamente no serviço, não centralizados nas procedures oficiais `sp_op_marcar_lote_disparado`, `sp_op_processar_retorno_lote`, `sp_op_recalcular_metricas_lote` e `sp_op_finalizar_lote`.
- O dashboard operacional calcula métricas por SQL direto em tabelas, não consome `vw_op_dashboard_resumo`.
- A listagem/detalhe de lotes usa tabela direta, não `vw_op_lotes_resumo`/`vw_op_fluxo_lotes`.
- A exportação de lote monta CSV a partir do detalhe do lote em Python, não de `vw_op_export_lote_csv`.

### 8.3 Segurança, dados pessoais e logs

- A tela de gestão exibe CPF, celular e e-mail em tabelas operacionais. Isso pode contrariar a diretriz de não expor dados pessoais completos em telas quando não necessário.
- Usuários estão hardcoded em memória no `create_app()`. Existe contexto de tabelas oficiais de usuários/perfis (`op_usuarios_painel`, `op_perfis_painel`, `vw_op_usuarios_painel`), mas a autenticação ainda não usa BigQuery.
- `app.secret_key` possui fallback de desenvolvimento no código. Não é um segredo real de produção, mas em produção deve ser obrigatório via variável de ambiente.
- O log de upload registra nome do arquivo e metadados; não foi observado log explícito de CPF/e-mail/celular nesse trecho, mas é necessário manter auditoria sobre qualquer log de erro contendo payload/linha.

### 8.4 Modelo de dados e risco operacional

- O código atual possui fallback para `vw_leads_priorizados`; se a base oficial for apenas `vw_op_leads_disponiveis`/`vw_op_leads_redisparo`, esse fallback pode mascarar divergências.
- `OP_TABLES` não inclui as tabelas oficiais `op_lead_timeline`, `op_auditoria_painel`, `op_usuarios_painel`, `op_perfis_painel`; inclui estruturas provavelmente anteriores.
- O endpoint `/api/gestao/operacional/admin/create-tables` executa migration local com tabelas operacionais antigas. Mesmo com proteção por token em produção, pode criar/alterar estruturas desalinhadas com o modelo oficial.
- A importação de retorno faz várias consultas/updates por linha, o que tende a ser lento/caro no BigQuery e aumenta risco de parcialidade em caso de erro no meio do arquivo.
- `finish_lote()` possui compatibilidade com assinatura antiga no app (`try/except TypeError`), indicando transição incompleta de contrato.

## 9. Autenticação, sessão e usuários

- A autenticação fica em `app.py`, dentro de `create_app()`.
- Usuários são um dicionário em memória com hashes Werkzeug para `matheus` e `miguel`.
- A sessão usa cookie Flask com configurações `SESSION_COOKIE_HTTPONLY`, `SESSION_COOKIE_SECURE`, `SESSION_COOKIE_SAMESITE`, `PERMANENT_SESSION_LIFETIME` e nome configurável.
- Um `before_request` aplica `g.correlation_id`.
- Outro `before_request` bloqueia todos os caminhos exceto `/static/*`, `/login`, `/logout`, `/health` e `/api/auth/login`.
- Para APIs sem sessão, retorna `401` JSON com `redirect_to=/login`; para páginas, redireciona para `/login`.
- Não foi encontrado uso das fontes oficiais `op_usuarios_painel`, `op_perfis_painel` ou `vw_op_usuarios_painel` na autenticação atual.

## 10. Plano de refatoração em etapas

### Etapa 0 — Congelamento e contratos

1. Documentar contrato atual dos endpoints usados por `static/js/gestao.js`.
2. Definir contrato alvo baseado nas fontes e procedures oficiais.
3. Criar testes de caracterização para os fluxos atuais: dashboard, preview/exportação de lote, importação de retorno, upload oficial e autenticação.
4. Definir política de mascaramento de CPF, celular e e-mail por endpoint/tela.

### Etapa 1 — Alinhar camada BigQuery operacional às fontes oficiais sem trocar UI

1. Criar funções novas em `services/gestao_operacional.py` para consumir:
   - `vw_op_dashboard_resumo`
   - `vw_op_leads_disponiveis`
   - `vw_op_leads_redisparo`
   - `vw_op_lotes_resumo`
   - `vw_op_fluxo_lotes`
   - `vw_op_export_lote_csv`
   - `vw_op_debug_fila`
2. Manter nomes de funções usados pelo Flask para reduzir quebra, mas trocar internals gradualmente.
3. Remover fallback automático para `vw_leads_priorizados` após validação em ambiente.
4. Retornar apenas dados pessoais mascarados onde a tela não exigir o dado completo.

### Etapa 2 — Centralizar mutações em procedures oficiais

1. Alterar criação de lote para chamar `sp_op_criar_lote` com parâmetros BigQuery.
2. Alterar início/marcação de disparo para `sp_op_marcar_lote_disparado`.
3. Alterar importação de retorno para staging + `sp_op_processar_retorno_lote`, evitando update linha a linha.
4. Alterar recálculo para `sp_op_recalcular_metricas_lote`.
5. Alterar finalização para `sp_op_finalizar_lote`.
6. Registrar auditoria em `op_auditoria_painel`/`op_lead_timeline`, conforme contrato oficial.

### Etapa 3 — Simplificar rotas e remover duplicidades

1. Escolher rota canônica para leads disponíveis: preferencialmente `/api/gestao/operacional/leads-disponiveis`; manter `/fila-leads` temporariamente como alias com aviso/documentação.
2. Escolher rota canônica para histórico de importações: preferencialmente `/api/gestao/importacoes`; manter `/api/importacoes/historico` temporariamente para compatibilidade.
3. Manter `POST /api/upload` como única rota oficial de upload; qualquer rota antiga deve continuar apenas orientando esse uso.
4. Consolidar contratos de erro (`ok/error` versus `success/error`).

### Etapa 4 — Autenticação e autorização oficial

1. Substituir usuários hardcoded por consulta a `vw_op_usuarios_painel`.
2. Usar `op_usuarios_painel` e `op_perfis_painel` como fontes oficiais de usuário/perfil.
3. Introduzir autorização por perfil nas rotas administrativas, exportação, importação e finalização/cancelamento de lote.
4. Tornar `FLASK_SECRET_KEY` obrigatório em produção.

### Etapa 5 — UI e observabilidade

1. Ajustar `static/js/gestao.js` aos contratos finais.
2. Exibir mensagens específicas de procedure/job e correlação.
3. Mostrar auditoria/timeline a partir das fontes oficiais.
4. Criar telas/estados para erros de fila (`vw_op_debug_fila`).
5. Padronizar carregamento, paginação e download.

## 11. Riscos de quebra

| Risco | Impacto | Mitigação recomendada |
|---|---|---|
| Troca de tabelas diretas por views oficiais muda nomes/tipos de colunas. | Quebra JS, templates ou exports. | Criar camada adaptadora no serviço mantendo contrato antigo até migração da UI. |
| Procedures oficiais podem ter assinatura diferente do payload atual. | Falha ao criar/finalizar/importar lote. | Validar assinatura em ambiente BigQuery e cobrir com testes/mocks. |
| Remoção de fallback `vw_leads_priorizados`. | Ambientes sem views oficiais ficam sem operação. | Fazer feature flag temporária e checklist de deploy SQL. |
| Mascaramento de dados pessoais. | Usuários podem perder dados necessários para operação/exportação. | Definir permissões por perfil e liberar dado completo apenas onde indispensável. |
| Importação de retorno migrada para staging/procedure. | Contrato de arquivo pode mudar. | Documentar layout, validar arquivo antes de chamar procedure e retornar relatório detalhado. |
| Autenticação via BigQuery. | Login pode ficar dependente de disponibilidade/permissões BigQuery. | Cache curto de usuários/perfis e fallback operacional controlado apenas para admin emergencial. |
| Consolidar rotas duplicadas. | Clientes antigos podem quebrar. | Deprecar com aliases temporários e logs de uso antes de remover. |
| Endpoint `admin/create-tables` desalinhado com modelo oficial. | Pode criar estruturas obsoletas. | Desabilitar em produção ou trocar para verificação idempotente sem criação destrutiva. |

## 12. Ordem recomendada de implementação

1. Criar testes de caracterização dos endpoints atuais de gestão operacional.
2. Mapear assinaturas reais das procedures oficiais e colunas das views oficiais no BigQuery.
3. Implementar adaptadores de leitura para views oficiais, mantendo contratos JSON atuais.
4. Migrar dashboard, lotes, fluxo de lotes e exportação para views oficiais.
5. Migrar criação de lote para `sp_op_criar_lote`.
6. Migrar marcação de disparo/início para `sp_op_marcar_lote_disparado`.
7. Migrar importação de retorno para staging + `sp_op_processar_retorno_lote`.
8. Migrar recálculo/finalização para `sp_op_recalcular_metricas_lote` e `sp_op_finalizar_lote`.
9. Consolidar rotas duplicadas com aliases temporários.
10. Implementar autenticação/autorização via usuários e perfis oficiais.
11. Aplicar mascaramento de dados pessoais e revisão de logs.
12. Remover fallbacks/estruturas antigas após uma janela de compatibilidade.

## 13. Comandos usados na análise

```bash
pwd && find .. -name AGENTS.md -print
cat AGENTS.md && rg --files -g '!venv' -g '!__pycache__' | sed -n '1,200p'
sed -n '1,260p' app.py
sed -n '1,260p' services/gestao.py
sed -n '1,280p' services/gestao_operacional.py
rg -n "@app\.route|def [a-zA-Z0-9_]+\(" app.py services/bigquery.py services/gestao.py services/gestao_operacional.py templates/gestao.html static/js/gestao.js static/css/gestao.css | sed -n '1,260p'
sed -n '416,1425p' app.py
sed -n '804,960p' app.py
sed -n '514,604p' services/gestao_operacional.py
rg -n "ENDPOINT|fetch\(|/api/gestao/operacional|/api/upload|exportar|importar|lote" static/js/gestao.js templates/gestao.html | sed -n '1,260p'
python - <<'PY'
from app import create_app
app=create_app()
for r in sorted(app.url_map.iter_rules(), key=lambda x: str(x)):
    print(','.join(sorted(r.methods - {'HEAD','OPTIONS'})), r.rule, r.endpoint)
PY
```

Observação: o comando Python para introspecção do `url_map` falhou porque o ambiente não tinha `flask` instalado no momento da análise. As rotas foram levantadas por inspeção estática de `app.py`.
