# Painel de Chips

Aplicação Flask migrada para PostgreSQL/Supabase self-hosted.

## Variáveis de ambiente

- `DATABASE_URL`: URL PostgreSQL obrigatória. Se ausente, o erro é: `Variável DATABASE_URL não configurada.`
- `DB_SCHEMA`: schema do banco, padrão `chips`.
- `FLASK_SECRET_KEY`: chave de sessão do Flask.

## Healthcheck

`GET /health` executa `SELECT 1` no PostgreSQL e responde:

```json
{"ok": true, "db": "connected"}
```
