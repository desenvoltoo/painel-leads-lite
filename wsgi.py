# -*- coding: utf-8 -*-
"""Entrypoint WSGI com diagnóstico explícito de falhas de startup."""
from __future__ import annotations

import html
import json
import os
from typing import Any, Callable, Dict

from startup_diagnostics import build_error_payload, configure_startup_logging, log_startup_failure

configure_startup_logging()


def _details_enabled() -> bool:
    return os.getenv("STARTUP_ERROR_DETAILS", "true").strip().lower() not in {"0", "false", "no", "nao", "não", "off"}


def _startup_error_wsgi_app(payload: Dict[str, Any]) -> Callable:
    expose_details = _details_enabled()
    public_payload: Dict[str, Any] = {
        "ok": False,
        "error": payload.get("error", "Falha ao inicializar aplicação."),
        "error_type": payload.get("error_type", "UnknownError"),
        "error_category": payload.get("error_category", "runtime"),
        "phase": payload.get("phase", "application_startup"),
        "message": "A aplicação não inicializou, mas o container está respondendo para diagnóstico. Corrija o erro abaixo e faça um novo deploy.",
    }
    if expose_details:
        for key in ("details", "variable", "trace"):
            if payload.get(key):
                public_payload[key] = payload[key]
    else:
        public_payload["details"] = "Detalhes ocultos. Defina STARTUP_ERROR_DETAILS=true para mostrar o erro completo."

    json_body = json.dumps(public_payload, ensure_ascii=False, indent=2, sort_keys=True)
    escaped_json = html.escape(json_body)
    html_body = f"""<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Falha ao inicializar aplicação</title><style>body{{font-family:Arial,sans-serif;margin:32px;background:#fff7ed;color:#1f2937}}main{{max-width:980px;margin:auto;background:white;border:1px solid #fed7aa;border-radius:12px;padding:24px}}h1{{color:#c2410c}}pre{{background:#111827;color:#f9fafb;padding:16px;border-radius:8px;overflow:auto;white-space:pre-wrap}}</style></head><body><main><h1>Falha ao inicializar aplicação</h1><pre>{escaped_json}</pre></main></body></html>"""

    def application(environ, start_response):
        wants_json = environ.get("PATH_INFO") == "/health" or "application/json" in environ.get("HTTP_ACCEPT", "")
        body = (json_body if wants_json else html_body).encode("utf-8")
        content_type = "application/json; charset=utf-8" if wants_json else "text/html; charset=utf-8"
        start_response("503 Service Unavailable", [("Content-Type", content_type), ("Content-Length", str(len(body))), ("Cache-Control", "no-store")])
        return [body]

    return application


def _matriculado_explicito(row: Dict[str, Any]) -> bool:
    """Conta matrícula somente quando a flag booleana matriculado é True."""
    return row.get("matriculado") is True


try:
    from services.upload_compat import apply_upload_alias_compat
    from services.upload_queue_compat import apply_upload_queue_compat
    from services.upload_matriculado_patch import apply_matriculado_full_update_patch
    from services.upload_null_guard import apply_upload_null_guard
    from services.import_hardening import apply_import_hardening

    apply_upload_alias_compat()
    apply_upload_queue_compat()
    apply_matriculado_full_update_patch()
    apply_upload_null_guard()

    from app import create_app
    from upload_preview_routes import register_upload_preview_routes
    from upload_new_only_routes import register_upload_new_only_routes
    from upload_update_existing_routes import register_upload_update_existing_routes
    from upload_progress_routes import register_upload_progress_routes
    from institution_routes import register_institution_routes
    from services.gestao_sem_lotes import register_gestao_sem_lotes
    from services.gestao_metricas_corrigidas import register_metricas_corrigidas
    from services import produtividade_export as produtividade_export_module
    from services.produtividade_export import register_produtividade_export
    from services.qualidade_dados import register_qualidade_dados
    from services.date_monotonic_guard import apply_date_monotonic_guards
    from services.gestao_multi_instituicao import apply_multi_institution_metrics
    from services.query_performance_guard import apply_query_performance_guard
    from services.gestao_response_cache import apply_gestao_response_cache
    from services.unifecaf_import_recovery import start_unifecaf_import_recovery
    from services.unifecaf_serial_guard import apply_unifecaf_serial_guard

    produtividade_export_module._is_matriculated = _matriculado_explicito
    multi_result = apply_multi_institution_metrics()
    cache_result = apply_gestao_response_cache()
    serial_result = apply_unifecaf_serial_guard()

    application = create_app()
    application.logger.info("Gestão multi-instituição ativada: %s", multi_result)
    application.logger.info("Cache da Gestão ativado: %s", cache_result)
    application.logger.info("Serialização da SP UniFECAF ativada: %s", serial_result)
    register_institution_routes(application)
    register_upload_preview_routes(application)
    register_upload_new_only_routes(application)
    register_upload_update_existing_routes(application)
    register_upload_progress_routes(application)
    register_gestao_sem_lotes(application)
    register_metricas_corrigidas(application)
    register_produtividade_export(application)
    register_qualidade_dados(application)

    try:
        recovery_result = start_unifecaf_import_recovery()
        application.logger.info("Recuperação automática UniFECAF ativada: %s", recovery_result)
    except Exception:
        application.logger.exception("Não foi possível iniciar a recuperação automática UniFECAF")

    try:
        performance_result = apply_query_performance_guard()
        application.logger.info("Índices de performance aplicados: %s", performance_result)
    except Exception:
        application.logger.exception("Não foi possível aplicar os índices de performance")

    try:
        hardening_result = apply_import_hardening()
        application.logger.info("Endurecimento de importação aplicado: %s", hardening_result)
    except Exception:
        application.logger.exception("Não foi possível aplicar o endurecimento de importação")

    try:
        protected_targets = apply_date_monotonic_guards()
        application.logger.info("Proteção de datas ativada: %s", protected_targets)
    except Exception:
        application.logger.exception("Não foi possível instalar automaticamente a proteção de datas")
except Exception as exc:
    log_startup_failure(exc)
    diagnostic_payload = build_error_payload(exc, public_message="Falha ao inicializar aplicação.", phase="application_startup", include_trace=True)
    application = _startup_error_wsgi_app(diagnostic_payload)
