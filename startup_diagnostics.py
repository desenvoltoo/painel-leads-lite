# -*- coding: utf-8 -*-
"""Helpers para deixar falhas de inicialização explícitas nos logs."""

from __future__ import annotations

import json
import logging
import os
import traceback
from typing import Any, Dict, Iterable

logger = logging.getLogger(__name__)


class StartupConfigError(RuntimeError):
    """Erro de configuração que impede a aplicação de inicializar."""

    def __init__(self, message: str, *, variable: str | None = None, value: str | None = None):
        super().__init__(message)
        self.variable = variable
        self.value = value


def configure_startup_logging() -> None:
    """Garante formato simples em stdout quando o app sobe fora do Gunicorn."""
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def classify_exception(exc: Exception) -> str:
    """Agrupa o erro para facilitar filtro nos logs do Cloud Run."""
    if isinstance(exc, StartupConfigError):
        return "configuration"
    if isinstance(exc, (ValueError, TypeError)):
        return "configuration"
    if isinstance(exc, PermissionError):
        return "permission"
    if isinstance(exc, FileNotFoundError):
        return "filesystem"
    return "runtime"


def build_error_payload(
    exc: Exception,
    *,
    public_message: str,
    phase: str,
    include_trace: bool = True,
) -> Dict[str, Any]:
    """Monta payload padronizado com tipo/categoria do erro."""
    payload: Dict[str, Any] = {
        "ok": False,
        "error": public_message,
        "error_type": type(exc).__name__,
        "error_category": classify_exception(exc),
        "details": str(exc),
        "phase": phase,
    }

    variable = getattr(exc, "variable", None)
    if variable:
        payload["variable"] = variable

    if include_trace:
        payload["trace"] = traceback.format_exc(limit=5)

    return payload


def log_startup_failure(exc: Exception, *, phase: str = "application_startup") -> Dict[str, Any]:
    """Registra falha fatal de inicialização com tipo e categoria do erro."""
    payload = build_error_payload(
        exc,
        public_message="Falha ao inicializar aplicação.",
        phase=phase,
        include_trace=False,
    )
    logger.critical(
        "STARTUP_FAILED %s",
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        exc_info=True,
    )
    return payload


def env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    """Lê inteiro do ambiente e falha com tipo claro quando inválido."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        try:
            value = int(raw.strip())
        except ValueError as exc:
            raise StartupConfigError(
                f"Variável de ambiente {name} deve ser um número inteiro; valor recebido: {raw!r}.",
                variable=name,
                value=raw,
            ) from exc

    if minimum is not None and value < minimum:
        raise StartupConfigError(
            f"Variável de ambiente {name} deve ser maior ou igual a {minimum}; valor recebido: {value}.",
            variable=name,
            value=str(value),
        )
    return value


def env_bool(name: str, default: bool = False) -> bool:
    """Lê booleano do ambiente aceitando formas comuns em PT/EN."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default

    normalized = raw.strip().lower()
    truthy: Iterable[str] = ("1", "true", "yes", "y", "sim", "s", "on")
    falsy: Iterable[str] = ("0", "false", "no", "n", "nao", "não", "off")
    if normalized in truthy:
        return True
    if normalized in falsy:
        return False

    raise StartupConfigError(
        f"Variável de ambiente {name} deve ser booleana (true/false); valor recebido: {raw!r}.",
        variable=name,
        value=raw,
    )
