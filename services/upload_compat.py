# -*- coding: utf-8 -*-
from __future__ import annotations

import logging

from . import database as db

logger = logging.getLogger(__name__)


def apply_upload_alias_compat() -> None:
    """Consolida aliases equivalentes antes da preparação do DataFrame.

    A base legada expõe tanto ``matriculado`` quanto ``flag_matriculado`` como
    destinos distintos. Na UniFECAF, ``flag_matriculado`` é renomeado depois
    para ``matriculado``, o que cria duas colunas com o mesmo nome e faz o
    PostgreSQL rejeitar o COPY com DuplicateColumn.

    Mantemos um único destino canônico (``matriculado``) e aceitamos os dois
    nomes de cabeçalho como entrada. A alteração também é segura para a
    Anhanguera, cuja staging já utiliza a coluna ``matriculado``.
    """
    aliases = list(db.UPLOAD_ALIASES.get("matriculado", []))
    aliases.extend(db.UPLOAD_ALIASES.pop("flag_matriculado", []))

    unique_aliases = []
    seen = set()
    for alias in aliases:
        normalized = db._normalize_upload_col(alias)
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique_aliases.append(alias)

    db.UPLOAD_ALIASES["matriculado"] = unique_aliases
    logger.info("upload_alias_compat_applied matriculado_aliases=%s", unique_aliases)
