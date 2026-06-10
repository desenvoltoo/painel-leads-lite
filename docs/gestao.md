# Gestão Operacional

## Regras de dados

- O lead é identificado pelo celular normalizado.
- `dt_upload` é preenchido no backend quando o arquivo é recebido.
- `data_atualizacao` representa a versão aceita na fato e deve receber o `dt_upload` da carga aceita.
- Uma carga só pode atualizar um lead existente quando `S.dt_upload >= F.data_atualizacao`.
- `data_inscricao`, `data_disparo`, `data_ultima_acao` e `data_atualizacao` têm significados diferentes e não devem ser trocados por `CURRENT_TIMESTAMP()`.

## Endpoints JSON protegidos

Todos retornam `{ ok, data, meta }` ou erro padronizado:

- `GET /api/gestao/resumo`
- `GET /api/gestao/funil`
- `GET /api/gestao/evolucao`
- `GET /api/gestao/rankings`
- `GET /api/gestao/produtividade`
- `GET /api/gestao/fila`
- `GET /api/gestao/qualidade`
- `GET /api/gestao/importacoes`
- `GET /api/gestao/opcoes`

## Filtros globais

Os filtros aceitos incluem período, consultores, curso, polo/unidade, modalidade, turno, origem, campanha, canal, tipo de negócio, status, matriculado e busca. A validação de datas bloqueia `data_ini > data_fim`. Os valores são enviados ao BigQuery por parâmetros.

## KPIs

- Total: quantidade de leads filtrados.
- Novos no período: registros com `data_inscricao` no filtro.
- Nunca trabalhados / sem status: status vazio.
- Carteira: status preenchido e não matriculado.
- Matriculados: `flag_matriculado` quando disponível, ou status/marcadores textuais normalizados.
- Conversão: matriculados / total.
- Parados > 7 dias: sem última ação ou última ação anterior a 7 dias, excluindo matriculados.
- Carga mais recente: registros com maior `dt_upload`/`data_atualizacao` no recorte.

## Score da fila

O score é calculado no backend/BigQuery e o frontend apenas exibe. Componentes:

- matriculados recebem score 5 e prioridade baixa;
- sem status: +35;
- sem ação: +25;
- idade desde inscrição: +0,35 por dia, limitado a 20;
- dias sem ação: +0,60 por dia, limitado a 30;
- acionamentos: +10 sem acionamento, +5 com 1 a 2, -5 com mais;
- origem/campanha ausentes adicionam pequeno peso;
- carga recente por `dt_upload`/`data_atualizacao`: +8 nos últimos 3 dias.

## Cache

O cache é por endpoint + filtros normalizados + paginação/granularidade, com TTL por `GESTAO_CACHE_TTL_SECONDS` e tamanho por `GESTAO_CACHE_MAXSIZE`. Respostas com filtros pessoais (`busca`, CPF, celular, e-mail, nome) não são cacheadas. O upload invalida o cache.

## Importações

A tabela `logs_importacoes` é criada pelo script `sql/migrations/20260610_gestao_import_logs.sql`. A interface não mostra `payload` completo e mascara CPF, celular e e-mail nas rejeições.
