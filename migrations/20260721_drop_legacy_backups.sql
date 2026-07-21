BEGIN;

DROP TABLE IF EXISTS modelo_estrela.bkp_dim_campanha_etapa8;
DROP TABLE IF EXISTS modelo_estrela.bkp_dim_consultor_etapa8;
DROP TABLE IF EXISTS modelo_estrela.bkp_dim_curso_antes_dedup;
DROP TABLE IF EXISTS modelo_estrela.bkp_dim_disparo_etapa8;
DROP TABLE IF EXISTS modelo_estrela.bkp_dim_pessoa_antes_sincronizacao;
DROP TABLE IF EXISTS modelo_estrela.bkp_dim_status_etapa8;
DROP TABLE IF EXISTS modelo_estrela.bkp_dim_tipo_negocio_antes_dedup;
DROP TABLE IF EXISTS modelo_estrela.bkp_f_lead_antes_dedup_dimensoes;
DROP TABLE IF EXISTS modelo_estrela.bkp_f_lead_antes_etapa14;
DROP TABLE IF EXISTS modelo_estrela.bkp_f_lead_antes_sincronizacao_etapa13;
DROP TABLE IF EXISTS modelo_estrela.bkp_f_lead_duplicados_etapa7;
DROP TABLE IF EXISTS modelo_estrela.bkp_leads_painel_novas_pessoas;
DROP TABLE IF EXISTS modelo_estrela.bkp_painel_sem_dim_pessoa_etapa11;
DROP TABLE IF EXISTS modelo_estrela.bkp_painel_sem_fato_etapa10;

ALTER TABLE IF EXISTS modelo_estrela.dim_status DROP COLUMN IF EXISTS rn;

COMMIT;
