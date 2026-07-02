# Migração da GCP para VPS Hostinger

Deploy atual: VPS Hostinger. Deploy antigo: Google Cloud Run/BigQuery, obsoleto.

## Checklist seguro
1. Congelar alterações na GCP durante a janela de migração.
2. Exportar dados necessários da origem antiga e guardar backup verificável.
3. Importar para PostgreSQL local ou Supabase.
4. Rodar migrations idempotentes do projeto.
5. Validar contagens por status, curso, data de inscrição e matriculados.
6. Validar endpoints `/api/health`, `/api/leads`, `/api/leads/options`, `/api/gestao/resumo`, `/api/gestao/fila`, `/api/gestao/qualidade`, `/api/gestao/importacoes`.
7. Apontar domínio no DNS da Hostinger para a VPS.
8. Monitorar `journalctl -u painel-leads-lite -f` e logs do Nginx.
9. Somente após produção validada, desligar Cloud Run.
10. Remover triggers/builds/schedulers antigos.
11. Remover segredos antigos após confirmar que não são usados.
12. Remover datasets/recursos BigQuery somente após backup e aceite final.

## Rastros legados
Termos como GCP, BigQuery, Cloud Run e cloudbuild devem permanecer apenas nesta documentação de migração/legado, nunca como dependência de runtime ou deploy principal.

## Quando apagar recursos GCP
Apenas depois de: aplicação respondendo por HTTPS na VPS, dados conferidos, equipe validando `/` e `/gestao`, backups realizados e rollback definido.

## Backup PostgreSQL
```bash
set -a; . ./.env; set +a
./scripts/backup_postgres.sh
```
Para Supabase, também use backups do painel/Supabase CLI ou `pg_dump` com a connection string.
