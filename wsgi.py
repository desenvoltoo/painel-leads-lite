# -*- coding: utf-8 -*-
"""Entrypoint WSGI com diagnóstico explícito de falhas de startup."""

from app import create_app
from startup_diagnostics import configure_startup_logging, log_startup_failure

configure_startup_logging()

try:
    application = create_app()
except Exception as exc:
    log_startup_failure(exc)
    raise
