-- Índices para priorização operacional de disparos.
-- Execute no PostgreSQL/Supabase antes do deploy da nova ordenação.

CREATE INDEX IF NOT EXISTS ix_leads_painel_disparo_prioridade
ON modelo_estrela.leads_painel_lite (
  (CASE WHEN data_disparo IS NULL THEN 0 ELSE 1 END),
  data_inscricao DESC,
  data_atualizacao DESC,
  sk_pessoa
);

CREATE INDEX IF NOT EXISTS ix_f_lead_disparo_prioridade
ON modelo_estrela.f_lead (
  (CASE WHEN data_disparo IS NULL THEN 0 ELSE 1 END),
  data_inscricao DESC,
  data_atualizacao DESC,
  sk_pessoa
);
