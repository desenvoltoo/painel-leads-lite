# -*- coding: utf-8 -*-
"""Entrypoint WSGI com diagnóstico explícito de falhas de startup.

Este arquivo não importa ``app`` no topo de propósito. Se um import ou a
criação do Flask falhar, o Gunicorn ainda consegue subir um WSGI mínimo que
mostra o tipo do erro no navegador/API em vez do Cloud Run exibir apenas
"Service Unavailable".
"""

from __future__ import annotations

import html
import json
import os
from typing import Any, Callable, Dict

from startup_diagnostics import (
    build_error_payload,
    configure_startup_logging,
    log_startup_failure,
)

configure_startup_logging()


def _details_enabled() -> bool:
    return os.getenv("STARTUP_ERROR_DETAILS", "true").strip().lower() not in {
        "0",
        "false",
        "no",
        "nao",
        "não",
        "off",
    }


def _startup_error_wsgi_app(payload: Dict[str, Any]) -> Callable:
    """Cria WSGI mínimo para expor falhas de inicialização sem depender do Flask."""
    expose_details = _details_enabled()

    public_payload: Dict[str, Any] = {
        "ok": False,
        "error": payload.get("error", "Falha ao inicializar aplicação."),
        "error_type": payload.get("error_type", "UnknownError"),
        "error_category": payload.get("error_category", "runtime"),
        "phase": payload.get("phase", "application_startup"),
        "message": (
            "A aplicação não inicializou, mas o container está respondendo para diagnóstico. "
            "Corrija o erro abaixo e faça um novo deploy."
        ),
    }
    if expose_details:
        for key in ("details", "variable", "trace"):
            if payload.get(key):
                public_payload[key] = payload[key]
    else:
        public_payload["details"] = (
            "Detalhes ocultos. Defina STARTUP_ERROR_DETAILS=true para mostrar o erro completo."
        )

    json_body = json.dumps(public_payload, ensure_ascii=False, indent=2, sort_keys=True)
    escaped_json = html.escape(json_body)
    html_body = f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Falha ao inicializar aplicação</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 32px; background: #fff7ed; color: #1f2937; }}
    main {{ max-width: 980px; margin: auto; background: white; border: 1px solid #fed7aa; border-radius: 12px; padding: 24px; box-shadow: 0 10px 30px rgba(0,0,0,.08); }}
    h1 {{ color: #c2410c; margin-top: 0; }}
    .badge {{ display: inline-block; background: #ffedd5; color: #9a3412; padding: 6px 10px; border-radius: 999px; font-weight: 700; margin-right: 8px; }}
    pre {{ background: #111827; color: #f9fafb; padding: 16px; border-radius: 8px; overflow-x: auto; white-space: pre-wrap; }}
  </style>
</head>
<body>
  <main>
    <h1>Falha ao inicializar aplicação</h1>
    <p><span class="badge">{html.escape(str(public_payload['error_type']))}</span><span class="badge">{html.escape(str(public_payload['error_category']))}</span></p>
    <p>O Cloud Run chegou até o container, mas o Flask não conseguiu inicializar. Abaixo está o diagnóstico retornado pelo entrypoint WSGI.</p>
    <pre>{escaped_json}</pre>
  </main>
</body>
</html>"""

    def application(environ, start_response):
        wants_json = (
            environ.get("PATH_INFO") == "/health"
            or "application/json" in environ.get("HTTP_ACCEPT", "")
        )
        if wants_json:
            body = json_body.encode("utf-8")
            headers = [("Content-Type", "application/json; charset=utf-8")]
        else:
            body = html_body.encode("utf-8")
            headers = [("Content-Type", "text/html; charset=utf-8")]

        headers.extend(
            [
                ("Content-Length", str(len(body))),
                ("Cache-Control", "no-store"),
            ]
        )
        start_response("503 Service Unavailable", headers)
        return [body]

    return application


try:
    from app import create_app

    application = create_app()
except Exception as exc:
    log_startup_failure(exc)
    diagnostic_payload = build_error_payload(
        exc,
        public_message="Falha ao inicializar aplicação.",
        phase="application_startup",
        include_trace=True,
    )
    application = _startup_error_wsgi_app(diagnostic_payload)
