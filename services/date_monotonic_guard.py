# -*- coding: utf-8 -*-
"""Protege datas operacionais contra regressão durante importações.

A regra é monotônica: um valor existente só pode ser substituído por uma data
mais recente. Valores nulos recebidos nunca apagam uma data já gravada.
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from . import database as db

logger = logging.getLogger(__name__)

CANDIDATES = (
    ("modelo_estrela", "f_lead", ("data_inscricao", "data_disparo", "data_ultima_acao")),
    ("modelo_estrela", "f_leads", ("data_inscricao", "data_disparo", "data_ultima_acao")),
    ("unifecaf", "f_leads", ("data_inscricao", "data_disparo", "data_ultima_interacao")),
)


def _monotonic_expression(column: str) -> str:
    ident = db._safe_ident(column)
    return (
        f"NEW.{ident} := CASE "
        f"WHEN NEW.{ident} IS NULL THEN OLD.{ident} "
        f"WHEN OLD.{ident} IS NULL THEN NEW.{ident} "
        f"ELSE GREATEST(OLD.{ident}, NEW.{ident}) END;"
    )


def apply_date_monotonic_guards() -> list[str]:
    """Cria triggers idempotentes nas tabelas existentes e retorna os alvos."""
    installed: list[str] = []
    engine = db.get_engine()

    with engine.begin() as conn:
        for schema, table, candidates in CANDIDATES:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = :schema AND table_name = :table
                    """
                ),
                {"schema": schema, "table": table},
            ).fetchall()
            columns = {str(row[0]) for row in rows}
            protected = [column for column in candidates if column in columns]
            if not protected:
                continue

            schema_ident = db._safe_ident(schema)
            table_ident = db._safe_ident(table)
            suffix = f"{schema}_{table}".replace("-", "_")
            function_ident = db._safe_ident(f"fn_preservar_datas_recentes_{suffix}")
            trigger_ident = db._safe_ident(f"trg_preservar_datas_recentes_{suffix}")
            assignments = "\n  ".join(_monotonic_expression(column) for column in protected)
            update_columns = ", ".join(db._safe_ident(column) for column in protected)

            conn.execute(
                text(
                    f"""
                    CREATE OR REPLACE FUNCTION {schema_ident}.{function_ident}()
                    RETURNS trigger
                    LANGUAGE plpgsql
                    AS $guard$
                    BEGIN
                      {assignments}
                      RETURN NEW;
                    END;
                    $guard$;

                    DROP TRIGGER IF EXISTS {trigger_ident} ON {schema_ident}.{table_ident};
                    CREATE TRIGGER {trigger_ident}
                    BEFORE UPDATE OF {update_columns}
                    ON {schema_ident}.{table_ident}
                    FOR EACH ROW
                    EXECUTE FUNCTION {schema_ident}.{function_ident}();
                    """
                )
            )
            installed.append(f"{schema}.{table}: {', '.join(protected)}")

    logger.info("date_monotonic_guards_installed targets=%s", installed)
    return installed
