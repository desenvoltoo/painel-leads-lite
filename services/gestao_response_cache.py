# -*- coding: utf-8 -*-
"""Cache curto e seguro para evitar repetir consultas pesadas da Gestão."""
from __future__ import annotations

import os
import threading
import time
from collections import OrderedDict
from typing import Any

from flask import request

_CACHE: "OrderedDict[tuple[tuple[str, tuple[str, ...]], ...], tuple[float, dict[str, Any]]]" = OrderedDict()
_LOCK = threading.RLock()


def _ttl_seconds() -> int:
    try:
        return max(0, min(300, int(os.getenv("GESTAO_CACHE_TTL_SECONDS", "30"))))
    except Exception:
        return 30


def _max_entries() -> int:
    try:
        return max(8, min(512, int(os.getenv("GESTAO_CACHE_MAX_ENTRIES", "128"))))
    except Exception:
        return 128


def _request_key() -> tuple[tuple[str, tuple[str, ...]], ...]:
    ignored = {"refresh", "_", "cache_bust"}
    pairs = []
    for key in sorted(request.args.keys()):
        if key in ignored:
            continue
        pairs.append((key, tuple(request.args.getlist(key))))
    return tuple(pairs)


def clear_gestao_cache() -> None:
    with _LOCK:
        _CACHE.clear()


def apply_gestao_response_cache() -> dict[str, Any]:
    from . import gestao_sem_lotes

    current = gestao_sem_lotes._consultants_payload
    if getattr(current, "_gestao_response_cache", False):
        return {"status": "ja_aplicado", "ttl_segundos": _ttl_seconds()}

    def cached_payload():
        ttl = _ttl_seconds()
        force_refresh = str(request.args.get("refresh") or "").strip().lower() in {
            "1", "true", "sim", "s", "yes"
        }
        if ttl <= 0 or force_refresh:
            result = current()
            if force_refresh:
                clear_gestao_cache()
            return result

        key = _request_key()
        now = time.monotonic()

        with _LOCK:
            cached = _CACHE.get(key)
            if cached and now - cached[0] < ttl:
                _CACHE.move_to_end(key)
                payload = dict(cached[1])
                payload["cache"] = {
                    "hit": True,
                    "ttl_segundos": ttl,
                    "idade_segundos": round(now - cached[0], 2),
                }
                return payload
            if cached:
                _CACHE.pop(key, None)

        payload = current()
        stored = dict(payload)
        stored["cache"] = {"hit": False, "ttl_segundos": ttl, "idade_segundos": 0}

        with _LOCK:
            _CACHE[key] = (now, stored)
            _CACHE.move_to_end(key)
            while len(_CACHE) > _max_entries():
                _CACHE.popitem(last=False)

        return dict(stored)

    cached_payload._gestao_response_cache = True
    gestao_sem_lotes._consultants_payload = cached_payload

    return {
        "status": "aplicado",
        "ttl_segundos": _ttl_seconds(),
        "max_entradas": _max_entries(),
        "atualizacao_forcada": "?refresh=1",
    }
