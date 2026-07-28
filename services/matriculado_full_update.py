# -*- coding: utf-8 -*-
"""Atualização completa e prioritária para linhas importadas como matriculado=true."""
from __future__ import annotations

from typing import Any, Dict

from . import database as db


def atualizar_matriculados_do_upload(schema: str, upload_id: str) -> Dict[str, Any]:
    """Atualiza todos os campos disponíveis do lead quando a carga confirma matrícula.

    Regras:
    - somente linhas cuja flag recebida seja explicitamente verdadeira;
    - aceita valores booleanos em português e inglês;
    - identifica por CPF e, na ausência, por celular;
    - campos textuais vazios não apagam dados existentes;
    - datas operacionais nunca retrocedem: prevalece a maior data;
    - flag_matriculado é gravada como TRUE.
    """
    schema_ident = db._safe_ident(schema)
    sql = f"""
    WITH origem AS (
        SELECT
            s.*,
            regexp_replace(COALESCE(s.cpf::text, ''), '[^0-9]', '', 'g') AS cpf_limpo,
            regexp_replace(COALESCE(s.celular::text, ''), '[^0-9]', '', 'g') AS celular_limpo,
            ROW_NUMBER() OVER (
                PARTITION BY COALESCE(
                    NULLIF(regexp_replace(COALESCE(s.cpf::text, ''), '[^0-9]', '', 'g'), ''),
                    NULLIF(regexp_replace(COALESCE(s.celular::text, ''), '[^0-9]', '', 'g'), ''),
                    s.id::text
                )
                ORDER BY s.dt_upload DESC NULLS LAST, s.linha_arquivo DESC NULLS LAST, s.id DESC
            ) AS rn
        FROM {schema_ident}.stg_leads_site s
        WHERE s.upload_id = :upload_id
          AND LOWER(BTRIM(COALESCE(s.matriculado, s.flag_matriculado, ''))) IN
              ('true', 't', '1', 'sim', 's', 'yes', 'y', 'matriculado', 'verdadeiro', 'v')
    ), matriculados AS (
        SELECT * FROM origem WHERE rn = 1
    ), atualizados AS (
        UPDATE {schema_ident}.leads_painel_lite l
        SET
            status_inscricao = COALESCE(NULLIF(BTRIM(m.status_inscricao), ''), l.status_inscricao),
            data_inscricao = CASE
                WHEN {schema_ident}.parse_ts_any(m.data_inscricao) IS NULL THEN l.data_inscricao
                WHEN l.data_inscricao IS NULL THEN {schema_ident}.parse_ts_any(m.data_inscricao)
                ELSE GREATEST(l.data_inscricao, {schema_ident}.parse_ts_any(m.data_inscricao))
            END,
            origem = COALESCE(NULLIF(BTRIM(m.origem), ''), l.origem),
            polo = COALESCE(NULLIF(BTRIM(m.unidade), ''), l.polo),
            tipo_negocio = COALESCE(NULLIF(BTRIM(m.tipo_negocio), ''), l.tipo_negocio),
            curso = COALESCE(NULLIF(BTRIM(m.curso), ''), l.curso),
            modalidade = COALESCE(NULLIF(BTRIM(m.modalidade), ''), l.modalidade),
            turno = COALESCE(NULLIF(BTRIM(m.turno), ''), l.turno),
            nome = COALESCE(NULLIF(BTRIM(m.nome), ''), l.nome),
            cpf = COALESCE(NULLIF(BTRIM(m.cpf), ''), l.cpf),
            celular = COALESCE(NULLIF(BTRIM(m.celular), ''), l.celular),
            email = COALESCE(NULLIF(BTRIM(m.email), ''), l.email),
            data_ultima_acao = CASE
                WHEN {schema_ident}.parse_ts_any(m.data_ultima_acao) IS NULL THEN l.data_ultima_acao
                WHEN l.data_ultima_acao IS NULL THEN {schema_ident}.parse_ts_any(m.data_ultima_acao)
                ELSE GREATEST(l.data_ultima_acao, {schema_ident}.parse_ts_any(m.data_ultima_acao))
            END,
            qtd_acionamentos = COALESCE(
                NULLIF(regexp_replace(COALESCE(m.qtd_acionamentos, ''), '[^0-9]', '', 'g'), '')::integer,
                l.qtd_acionamentos
            ),
            status = COALESCE(NULLIF(BTRIM(m.status), ''), l.status),
            data_disparo = CASE
                WHEN {schema_ident}.parse_ts_any(m.data_disparo) IS NULL THEN l.data_disparo
                WHEN l.data_disparo IS NULL THEN {schema_ident}.parse_ts_any(m.data_disparo)
                ELSE GREATEST(l.data_disparo, {schema_ident}.parse_ts_any(m.data_disparo))
            END,
            peca_disparo = COALESCE(NULLIF(BTRIM(m.peca_disparo), ''), l.peca_disparo),
            texto_disparo = COALESCE(NULLIF(BTRIM(m.texto_disparo), ''), l.texto_disparo),
            consultor_disparo = COALESCE(NULLIF(BTRIM(m.consultor_disparo), ''), l.consultor_disparo),
            tipo_disparo = COALESCE(NULLIF(BTRIM(m.tipo_disparo), ''), l.tipo_disparo),
            campanha = COALESCE(NULLIF(BTRIM(m.campanha), ''), l.campanha),
            observacao = COALESCE(NULLIF(BTRIM(m.observacao), ''), l.observacao),
            data_matricula = COALESCE({schema_ident}.parse_ts_any(m.data_matricula), l.data_matricula),
            flag_matriculado = TRUE,
            canal = COALESCE(NULLIF(BTRIM(m.canal), ''), l.canal),
            acao_comercial = COALESCE(NULLIF(BTRIM(m.acao_comercial), ''), l.acao_comercial),
            consultor_comercial = COALESCE(NULLIF(BTRIM(m.consultor_comercial), ''), l.consultor_comercial),
            data_atualizacao = GREATEST(
                COALESCE(l.data_atualizacao, timestamp '1900-01-01'),
                COALESCE(m.dt_upload::timestamp, now()::timestamp)
            ),
            dt_upload = GREATEST(COALESCE(l.dt_upload, m.dt_upload), m.dt_upload)
        FROM matriculados m
        WHERE
            (NULLIF(m.cpf_limpo, '') IS NOT NULL
             AND regexp_replace(COALESCE(l.cpf::text, ''), '[^0-9]', '', 'g') = m.cpf_limpo)
            OR
            (NULLIF(m.cpf_limpo, '') IS NULL
             AND NULLIF(m.celular_limpo, '') IS NOT NULL
             AND regexp_replace(COALESCE(l.celular::text, ''), '[^0-9]', '', 'g') = m.celular_limpo)
        RETURNING l.sk_pessoa_dim
    )
    SELECT COUNT(*)::bigint AS atualizados FROM atualizados
    """
    rows = db._run_gestao_query(sql, {"upload_id": upload_id}, "matriculado_full_update") or []
    return {"atualizados": int((rows[0].get("atualizados") if rows else 0) or 0)}
