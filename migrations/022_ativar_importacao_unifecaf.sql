-- Importação UniFECAF: staging -> dimensões -> fato, com dois modos.

CREATE TABLE IF NOT EXISTS unifecaf.op_importacao_progresso (
  upload_id text PRIMARY KEY,
  modo text NOT NULL,
  rotina text NOT NULL,
  arquivo text,
  status text NOT NULL DEFAULT 'AGUARDANDO',
  etapa text NOT NULL DEFAULT 'AGUARDANDO',
  linhas_total integer NOT NULL DEFAULT 0,
  linhas_processadas integer NOT NULL DEFAULT 0,
  linhas_inseridas integer NOT NULL DEFAULT 0,
  linhas_ignoradas integer NOT NULL DEFAULT 0,
  linhas_rejeitadas integer NOT NULL DEFAULT 0,
  duplicados_arquivo integer NOT NULL DEFAULT 0,
  existentes_por_celular integer NOT NULL DEFAULT 0,
  existentes_por_cpf integer NOT NULL DEFAULT 0,
  progresso numeric(5,2) NOT NULL DEFAULT 0,
  mensagem text,
  erro text,
  criado_em timestamptz NOT NULL DEFAULT now(),
  iniciado_em timestamptz,
  atualizado_em timestamptz NOT NULL DEFAULT now(),
  finalizado_em timestamptz
);

ALTER TABLE unifecaf.op_importacao_progresso DISABLE ROW LEVEL SECURITY;
CREATE INDEX IF NOT EXISTS idx_unifecaf_progresso_status ON unifecaf.op_importacao_progresso(status, atualizado_em DESC);
CREATE INDEX IF NOT EXISTS idx_unifecaf_stg_upload_processado ON unifecaf.stg_leads(upload_id, processado);

CREATE OR REPLACE FUNCTION unifecaf.fn_digits(p_value text)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT NULLIF(regexp_replace(COALESCE(p_value,''), '[^0-9]', '', 'g'), '');
$$;

CREATE OR REPLACE FUNCTION unifecaf.fn_bool(p_value text)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT lower(trim(COALESCE(p_value,''))) IN ('1','true','t','sim','s','yes','y','matriculado');
$$;

CREATE OR REPLACE FUNCTION unifecaf.fn_ts(p_value text)
RETURNS timestamp LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE v text := trim(COALESCE(p_value,''));
BEGIN
  IF v = '' THEN RETURN NULL; END IF;
  BEGIN RETURN v::timestamp; EXCEPTION WHEN others THEN NULL; END;
  BEGIN RETURN to_timestamp(v,'DD/MM/YYYY HH24:MI:SS'); EXCEPTION WHEN others THEN NULL; END;
  BEGIN RETURN to_date(v,'DD/MM/YYYY')::timestamp; EXCEPTION WHEN others THEN RETURN NULL; END;
END; $$;

CREATE OR REPLACE FUNCTION unifecaf.fn_atualizar_progresso_importacao(
  p_upload_id text, p_status text, p_etapa text, p_progresso numeric,
  p_linhas_processadas integer DEFAULT NULL, p_linhas_inseridas integer DEFAULT NULL,
  p_linhas_ignoradas integer DEFAULT NULL, p_linhas_rejeitadas integer DEFAULT NULL,
  p_mensagem text DEFAULT NULL, p_erro text DEFAULT NULL
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=unifecaf,public AS $$
BEGIN
  UPDATE unifecaf.op_importacao_progresso SET
    status=COALESCE(p_status,status), etapa=COALESCE(p_etapa,etapa),
    progresso=LEAST(100,GREATEST(0,COALESCE(p_progresso,progresso))),
    linhas_processadas=COALESCE(p_linhas_processadas,linhas_processadas),
    linhas_inseridas=COALESCE(p_linhas_inseridas,linhas_inseridas),
    linhas_ignoradas=COALESCE(p_linhas_ignoradas,linhas_ignoradas),
    linhas_rejeitadas=COALESCE(p_linhas_rejeitadas,linhas_rejeitadas),
    mensagem=COALESCE(p_mensagem,mensagem), erro=COALESCE(p_erro,erro),
    iniciado_em=CASE WHEN p_status='PROCESSANDO' THEN COALESCE(iniciado_em,now()) ELSE iniciado_em END,
    finalizado_em=CASE WHEN p_status IN ('CONCLUIDO','ERRO') THEN now() ELSE finalizado_em END,
    atualizado_em=now()
  WHERE upload_id=p_upload_id;
END; $$;

CREATE OR REPLACE FUNCTION unifecaf.sp_processar_upload(p_upload_id text, p_atualizar boolean)
RETURNS TABLE(linhas_recebidas bigint, linhas_inseridas bigint, linhas_atualizadas bigint, existentes_por_celular bigint, existentes_por_cpf bigint, duplicados_no_arquivo bigint, linhas_rejeitadas bigint, mensagem text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=unifecaf,public AS $$
DECLARE
  v_total bigint:=0; v_insert bigint:=0; v_update bigint:=0; v_cel bigint:=0; v_cpf bigint:=0; v_dup bigint:=0; v_rej bigint:=0;
BEGIN
  SELECT count(*) INTO v_total FROM unifecaf.stg_leads WHERE upload_id=p_upload_id;
  IF v_total=0 THEN RAISE EXCEPTION 'Upload % sem linhas na staging UniFECAF', p_upload_id; END IF;

  UPDATE unifecaf.logs_importacoes SET status='PROCESSANDO', etapa='NORMALIZANDO', iniciado_em=COALESCE(iniciado_em,now()), atualizado_em=now() WHERE upload_id=p_upload_id;

  CREATE TEMP TABLE tmp_u ON COMMIT DROP AS
  SELECT s.*, unifecaf.fn_digits(s.celular) cel_norm, unifecaf.fn_digits(s.cpf) cpf_norm,
         row_number() OVER (PARTITION BY COALESCE(unifecaf.fn_digits(s.celular), 'CPF:'||COALESCE(unifecaf.fn_digits(s.cpf),'SEM:'||s.id::text)) ORDER BY s.id) rn
  FROM unifecaf.stg_leads s WHERE s.upload_id=p_upload_id;

  SELECT count(*) INTO v_dup FROM tmp_u WHERE rn>1;
  SELECT count(*) INTO v_rej FROM tmp_u WHERE rn=1 AND cel_norm IS NULL AND cpf_norm IS NULL;

  INSERT INTO unifecaf.logs_rejeicoes_import(upload_id,linha_arquivo,campo,motivo,valor_original,payload)
  SELECT p_upload_id,linha_arquivo,'celular/cpf','SEM_IDENTIFICADOR',NULL,to_jsonb(t) FROM tmp_u t WHERE rn=1 AND cel_norm IS NULL AND cpf_norm IS NULL;

  CREATE TEMP TABLE tmp_valid ON COMMIT DROP AS SELECT * FROM tmp_u WHERE rn=1 AND (cel_norm IS NOT NULL OR cpf_norm IS NOT NULL);

  SELECT count(*) INTO v_cel FROM tmp_valid t WHERE t.cel_norm IS NOT NULL AND EXISTS (SELECT 1 FROM unifecaf.dim_pessoa p WHERE unifecaf.fn_digits(p.celular)=t.cel_norm);
  SELECT count(*) INTO v_cpf FROM tmp_valid t WHERE t.cpf_norm IS NOT NULL AND NOT EXISTS (SELECT 1 FROM unifecaf.dim_pessoa p WHERE t.cel_norm IS NOT NULL AND unifecaf.fn_digits(p.celular)=t.cel_norm) AND EXISTS (SELECT 1 FROM unifecaf.dim_pessoa p WHERE unifecaf.fn_digits(p.cpf)=t.cpf_norm);

  INSERT INTO unifecaf.dim_origem(origem) SELECT DISTINCT trim(origem) FROM tmp_valid t WHERE nullif(trim(origem),'') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM unifecaf.dim_origem d WHERE lower(trim(d.origem))=lower(trim(t.origem)));
  INSERT INTO unifecaf.dim_unidade(unidade) SELECT DISTINCT trim(unidade) FROM tmp_valid t WHERE nullif(trim(unidade),'') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM unifecaf.dim_unidade d WHERE lower(trim(d.unidade))=lower(trim(t.unidade)));
  INSERT INTO unifecaf.dim_tipo_negocio(tipo_negocio) SELECT DISTINCT trim(tipo_negocio) FROM tmp_valid t WHERE nullif(trim(tipo_negocio),'') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM unifecaf.dim_tipo_negocio d WHERE lower(trim(d.tipo_negocio))=lower(trim(t.tipo_negocio)));
  INSERT INTO unifecaf.dim_curso(curso,modalidade) SELECT DISTINCT trim(curso),nullif(trim(modalidade),'') FROM tmp_valid t WHERE nullif(trim(curso),'') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM unifecaf.dim_curso d WHERE lower(trim(d.curso))=lower(trim(t.curso)) AND coalesce(lower(trim(d.modalidade)),'')=coalesce(lower(trim(t.modalidade)),''));
  INSERT INTO unifecaf.dim_campanha(campanha) SELECT DISTINCT trim(campanha) FROM tmp_valid t WHERE nullif(trim(campanha),'') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM unifecaf.dim_campanha d WHERE lower(trim(d.campanha))=lower(trim(t.campanha)));
  INSERT INTO unifecaf.dim_disparo(tipo_disparo,peca_disparo,texto_disparo) SELECT DISTINCT nullif(trim(tipo_disparo),''),nullif(trim(peca_disparo),''),texto_disparo FROM tmp_valid t WHERE nullif(trim(tipo_disparo),'') IS NOT NULL OR nullif(trim(peca_disparo),'') IS NOT NULL OR nullif(trim(texto_disparo),'') IS NOT NULL;
  INSERT INTO unifecaf.dim_consultor(nome,tipo) SELECT DISTINCT trim(consultor_comercial),'COMERCIAL' FROM tmp_valid t WHERE nullif(trim(consultor_comercial),'') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM unifecaf.dim_consultor d WHERE d.tipo='COMERCIAL' AND lower(trim(d.nome))=lower(trim(t.consultor_comercial)));
  INSERT INTO unifecaf.dim_consultor(nome,tipo) SELECT DISTINCT trim(consultor_disparo),'DISPARO' FROM tmp_valid t WHERE nullif(trim(consultor_disparo),'') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM unifecaf.dim_consultor d WHERE d.tipo='DISPARO' AND lower(trim(d.nome))=lower(trim(t.consultor_disparo)));
  INSERT INTO unifecaf.dim_status(status,observacao,matriculado) SELECT DISTINCT coalesce(nullif(trim(status),''),'SEM_STATUS'),observacao,unifecaf.fn_bool(matriculado) FROM tmp_valid t WHERE NOT EXISTS (SELECT 1 FROM unifecaf.dim_status d WHERE lower(trim(d.status))=lower(coalesce(nullif(trim(t.status),''),'SEM_STATUS')) AND d.matriculado=unifecaf.fn_bool(t.matriculado) AND coalesce(d.observacao,'')=coalesce(t.observacao,''));

  IF p_atualizar THEN
    UPDATE unifecaf.dim_pessoa p SET
      nome=COALESCE(NULLIF(trim(t.nome),''),p.nome), cpf=COALESCE(NULLIF(trim(t.cpf),''),p.cpf), celular=COALESCE(NULLIF(trim(t.celular),''),p.celular), email=COALESCE(NULLIF(trim(t.email),''),p.email), atualizado_em=now()
    FROM tmp_valid t WHERE (t.cel_norm IS NOT NULL AND unifecaf.fn_digits(p.celular)=t.cel_norm) OR (t.cpf_norm IS NOT NULL AND unifecaf.fn_digits(p.cpf)=t.cpf_norm);
    GET DIAGNOSTICS v_update=ROW_COUNT;
  END IF;

  INSERT INTO unifecaf.dim_pessoa(cpf,nome,celular,email)
  SELECT nullif(trim(t.cpf),''),coalesce(nullif(trim(t.nome),''),'SEM NOME'),nullif(trim(t.celular),''),nullif(trim(t.email),'') FROM tmp_valid t
  WHERE NOT EXISTS (SELECT 1 FROM unifecaf.dim_pessoa p WHERE (t.cel_norm IS NOT NULL AND unifecaf.fn_digits(p.celular)=t.cel_norm) OR (t.cpf_norm IS NOT NULL AND unifecaf.fn_digits(p.cpf)=t.cpf_norm));

  CREATE TEMP TABLE tmp_resolved ON COMMIT DROP AS
  SELECT t.*,
    (SELECT p.sk_pessoa FROM unifecaf.dim_pessoa p WHERE (t.cel_norm IS NOT NULL AND unifecaf.fn_digits(p.celular)=t.cel_norm) OR (t.cpf_norm IS NOT NULL AND unifecaf.fn_digits(p.cpf)=t.cpf_norm) ORDER BY CASE WHEN t.cel_norm IS NOT NULL AND unifecaf.fn_digits(p.celular)=t.cel_norm THEN 0 ELSE 1 END,p.sk_pessoa LIMIT 1) skp
  FROM tmp_valid t;

  IF p_atualizar THEN
    UPDATE unifecaf.f_leads f SET
      data_inscricao=COALESCE(unifecaf.fn_ts(t.data_inscricao),f.data_inscricao), data_matricula=COALESCE(unifecaf.fn_ts(t.data_matricula),f.data_matricula),
      data_ultima_interacao=COALESCE(unifecaf.fn_ts(t.data_ultima_interacao),f.data_ultima_interacao), data_disparo=COALESCE(unifecaf.fn_ts(t.data_disparo),f.data_disparo),
      qtd_acionamentos=COALESCE(NULLIF(regexp_replace(coalesce(t.qtd_acionamentos,''),'[^0-9]','','g'),'')::int,f.qtd_acionamentos), atualizado_em=now()
    FROM tmp_resolved t WHERE f.sk_pessoa=t.skp;
  END IF;

  INSERT INTO unifecaf.f_leads(sk_pessoa,sk_curso,sk_unidade,sk_origem,sk_tipo_negocio,sk_status,sk_campanha,sk_disparo,sk_consultor_comercial,sk_consultor_disparo,data_inscricao,data_matricula,data_ultima_interacao,data_disparo,qtd_acionamentos)
  SELECT t.skp,
    (SELECT sk_curso FROM unifecaf.dim_curso d WHERE lower(trim(d.curso))=lower(trim(t.curso)) AND coalesce(lower(trim(d.modalidade)),'')=coalesce(lower(trim(t.modalidade)),'') ORDER BY sk_curso LIMIT 1),
    (SELECT sk_unidade FROM unifecaf.dim_unidade d WHERE lower(trim(d.unidade))=lower(trim(t.unidade)) ORDER BY sk_unidade LIMIT 1),
    (SELECT sk_origem FROM unifecaf.dim_origem d WHERE lower(trim(d.origem))=lower(trim(t.origem)) ORDER BY sk_origem LIMIT 1),
    (SELECT sk_tipo_negocio FROM unifecaf.dim_tipo_negocio d WHERE lower(trim(d.tipo_negocio))=lower(trim(t.tipo_negocio)) ORDER BY sk_tipo_negocio LIMIT 1),
    (SELECT sk_status FROM unifecaf.dim_status d WHERE lower(trim(d.status))=lower(coalesce(nullif(trim(t.status),''),'SEM_STATUS')) ORDER BY sk_status LIMIT 1),
    (SELECT sk_campanha FROM unifecaf.dim_campanha d WHERE lower(trim(d.campanha))=lower(trim(t.campanha)) ORDER BY sk_campanha LIMIT 1),
    (SELECT sk_disparo FROM unifecaf.dim_disparo d WHERE coalesce(d.tipo_disparo,'')=coalesce(nullif(trim(t.tipo_disparo),''),'') AND coalesce(d.peca_disparo,'')=coalesce(nullif(trim(t.peca_disparo),''),'') ORDER BY sk_disparo LIMIT 1),
    (SELECT sk_consultor FROM unifecaf.dim_consultor d WHERE d.tipo='COMERCIAL' AND lower(trim(d.nome))=lower(trim(t.consultor_comercial)) ORDER BY sk_consultor LIMIT 1),
    (SELECT sk_consultor FROM unifecaf.dim_consultor d WHERE d.tipo='DISPARO' AND lower(trim(d.nome))=lower(trim(t.consultor_disparo)) ORDER BY sk_consultor LIMIT 1),
    unifecaf.fn_ts(t.data_inscricao),unifecaf.fn_ts(t.data_matricula),unifecaf.fn_ts(t.data_ultima_interacao),unifecaf.fn_ts(t.data_disparo),coalesce(NULLIF(regexp_replace(coalesce(t.qtd_acionamentos,''),'[^0-9]','','g'),'')::int,0)
  FROM tmp_resolved t WHERE NOT EXISTS (SELECT 1 FROM unifecaf.f_leads f WHERE f.sk_pessoa=t.skp);
  GET DIAGNOSTICS v_insert=ROW_COUNT;

  UPDATE unifecaf.stg_leads SET processado=true,processado_em=now(),erro_processamento=NULL WHERE upload_id=p_upload_id;
  UPDATE unifecaf.logs_importacoes SET status='CONCLUIDO',etapa='FINALIZADO',total_linhas=v_total,linhas_recebidas=v_total,linhas_validas=v_total-v_dup-v_rej,linhas_inseridas=v_insert,linhas_atualizadas=v_update,linhas_ignoradas=CASE WHEN p_atualizar THEN v_dup ELSE v_dup+v_cel+v_cpf END,linhas_rejeitadas=v_rej,atualizado_em=now(),finalizado_em=now(),mensagem='Importação UniFECAF concluída' WHERE upload_id=p_upload_id;

  RETURN QUERY SELECT v_total,v_insert,v_update,v_cel,v_cpf,v_dup,v_rej,format('%s recebidas; %s inseridas; %s atualizadas; %s duplicadas no arquivo; %s rejeitadas.',v_total,v_insert,v_update,v_dup,v_rej);
END; $$;

CREATE OR REPLACE FUNCTION unifecaf.sp_importar_somente_leads_novos(p_upload_id text)
RETURNS TABLE(linhas_recebidas bigint, linhas_inseridas bigint, linhas_atualizadas bigint, existentes_por_celular bigint, existentes_por_cpf bigint, duplicados_no_arquivo bigint, linhas_rejeitadas bigint, mensagem text)
LANGUAGE sql SECURITY DEFINER SET search_path=unifecaf,public AS $$ SELECT * FROM unifecaf.sp_processar_upload(p_upload_id,false); $$;

CREATE OR REPLACE FUNCTION unifecaf.sp_processar_stg_leads(p_upload_id text)
RETURNS TABLE(linhas_recebidas bigint, linhas_inseridas bigint, linhas_atualizadas bigint, existentes_por_celular bigint, existentes_por_cpf bigint, duplicados_no_arquivo bigint, linhas_rejeitadas bigint, mensagem text)
LANGUAGE sql SECURITY DEFINER SET search_path=unifecaf,public AS $$ SELECT * FROM unifecaf.sp_processar_upload(p_upload_id,true); $$;

GRANT USAGE ON SCHEMA unifecaf TO app_paineis;
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA unifecaf TO app_paineis;
GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA unifecaf TO app_paineis;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA unifecaf TO app_paineis;
ALTER DEFAULT PRIVILEGES IN SCHEMA unifecaf GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO app_paineis;
ALTER DEFAULT PRIVILEGES IN SCHEMA unifecaf GRANT USAGE,SELECT ON SEQUENCES TO app_paineis;
ALTER DEFAULT PRIVILEGES IN SCHEMA unifecaf GRANT EXECUTE ON FUNCTIONS TO app_paineis;