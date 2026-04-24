## Melhorias implementadas (exportação em lote, busca e paginação)

### 1) Exportação em lote assíncrona

- Novo endpoint: `POST /api/export/batch`
  - Inicia um job assíncrono de exportação.
  - Processa os leads em lotes (`batch_size`, padrão 1000).
  - Ordenação aplicada no backend:
    1. `data_disparo` vazia (`NULL`/string vazia) primeiro
    2. Depois `data_disparo` da mais antiga para a mais recente.
- Novo endpoint: `GET /api/export/batch/status?job_id=...`
  - Retorna progresso (`processed`, `total`, `current_batch`, `total_batches`).
- Novo endpoint: `GET /api/export/batch/download?job_id=...`
  - Faz download do arquivo final único (`.csv`) após conclusão.

### 2) Busca e ordenação do painel

- `GET /api/leads` e `POST /api/leads/search` agora usam por padrão:
  - `order_by=data_disparo`
  - `order_dir=ASC`
- A consulta respeita a regra de ordenação da exportação.
- Frontend aplica debounce nos filtros de texto/data/seleções para reduzir chamadas.

### 3) Paginação com total

- A API já retorna `total`; frontend agora usa `limit + offset`:
  - Botões próxima/anterior página.
  - Exibição de faixa: `Mostrando X-Y de N leads`.

### 4) UI/UX

- Tabela com melhorias responsivas e barra de paginação.
- Indicador de loading durante buscas.
- Botão **Exportar em lote** com barra e texto de progresso por lote.

### SQL de referência para índices (quando banco relacional)

> Este projeto usa BigQuery. Em PostgreSQL/MySQL, use índices equivalentes para os filtros mais usados.

```sql
-- PostgreSQL (exemplo)
CREATE INDEX IF NOT EXISTS idx_leads_nome ON leads (nome);
CREATE INDEX IF NOT EXISTS idx_leads_email ON leads (email);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads (status);
CREATE INDEX IF NOT EXISTS idx_leads_data_disparo ON leads (data_disparo);
```

### Como testar rapidamente

1. Abra o painel e aplique filtros; valide paginação e ordenação por `data_disparo`.
2. Clique em **Exportar em lote**.
3. Acompanhe progresso no card de exportação.
4. Ao concluir, o download do CSV é iniciado automaticamente.

