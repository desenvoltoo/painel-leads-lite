# -*- coding: utf-8 -*-
"""Cria índices seguros para acelerar Gestão, importações e atualizações."""
from __future__ import annotations

from typing import Any

from . import database as db


def apply_query_performance_guard() -> dict[str, Any]:
    ddl = r"""
    DO $perf$
    DECLARE
      r record;
      idx_name text;
      ddl_sql text;
    BEGIN
      FOR r IN
        SELECT *
        FROM (VALUES
          ('modelo_estrela','leads_painel_lite','data_disparo'),
          ('modelo_estrela','leads_painel_lite','data_matricula'),
          ('modelo_estrela','leads_painel_lite','consultor_disparo'),
          ('modelo_estrela','leads_painel_lite','matriculado'),
          ('modelo_estrela','leads_painel_lite','cpf'),
          ('modelo_estrela','leads_painel_lite','celular'),
          ('modelo_estrela','stg_leads_site','upload_id'),
          ('modelo_estrela','stg_leads_site','cpf'),
          ('modelo_estrela','stg_leads_site','celular'),
          ('modelo_estrela','logs_importacoes','upload_id'),
          ('modelo_estrela','logs_importacoes','status'),
          ('unifecaf','leads_painel_lite','data_disparo'),
          ('unifecaf','leads_painel_lite','data_matricula'),
          ('unifecaf','leads_painel_lite','consultor_disparo'),
          ('unifecaf','leads_painel_lite','matriculado'),
          ('unifecaf','leads_painel_lite','cpf'),
          ('unifecaf','leads_painel_lite','celular'),
          ('unifecaf','stg_leads','upload_id'),
          ('unifecaf','stg_leads','cpf'),
          ('unifecaf','stg_leads','celular'),
          ('unifecaf','logs_importacoes','upload_id'),
          ('unifecaf','logs_importacoes','status')
        ) AS x(schema_name, table_name, column_name)
      LOOP
        IF EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema = r.schema_name
            AND table_name = r.table_name
        ) AND EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = r.schema_name
            AND table_name = r.table_name
            AND column_name = r.column_name
        ) THEN
          idx_name := left('idx_perf_' || r.schema_name || '_' || r.table_name || '_' || r.column_name, 63);
          ddl_sql := format(
            'CREATE INDEX IF NOT EXISTS %I ON %I.%I (%I)',
            idx_name,
            r.schema_name,
            r.table_name,
            r.column_name
          );
          EXECUTE ddl_sql;
        END IF;
      END LOOP;

      IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='modelo_estrela' AND table_name='leads_painel_lite'
      ) THEN
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema='modelo_estrela' AND table_name='leads_painel_lite' AND column_name='consultor_disparo'
        ) AND EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema='modelo_estrela' AND table_name='leads_painel_lite' AND column_name='data_disparo'
        ) THEN
          CREATE INDEX IF NOT EXISTS idx_perf_modelo_consultor_data_disparo
            ON modelo_estrela.leads_painel_lite (consultor_disparo, data_disparo DESC);
        END IF;
      END IF;

      IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='unifecaf' AND table_name='leads_painel_lite'
      ) THEN
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema='unifecaf' AND table_name='leads_painel_lite' AND column_name='consultor_disparo'
        ) AND EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema='unifecaf' AND table_name='leads_painel_lite' AND column_name='data_disparo'
        ) THEN
          CREATE INDEX IF NOT EXISTS idx_perf_unifecaf_consultor_data_disparo
            ON unifecaf.leads_painel_lite (consultor_disparo, data_disparo DESC);
        END IF;
      END IF;
    END;
    $perf$;
    """

    db._run_gestao_query(ddl, {}, "apply_query_performance_guard")
    return {
        "status": "aplicado",
        "escopo": ["gestao", "exportacao", "staging", "logs", "atualizacoes"],
    }
