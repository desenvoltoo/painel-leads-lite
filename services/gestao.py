# -*- coding: utf-8 -*-
"""Gestão compatibility service backed by PostgreSQL-friendly empty defaults."""
from __future__ import annotations
from datetime import datetime, timezone
import io
import pandas as pd

class GestaoValidationError(ValueError):
    pass

def utc_now_iso(): return datetime.now(timezone.utc).isoformat()
def invalidate_gestao_cache(): return None
def parse_filters(args): return dict(args or {})
def parse_import_history_request(args): return {"filters": dict(args or {}), "meta": {"limit": int((args or {}).get('limit', 100) or 100), "offset": int((args or {}).get('offset', 0) or 0)}}

def _empty_list(*a, **k): return []
def _empty_dict(*a, **k): return {}
def _ok(*a, **k): return {"success": True}

def get_evolucao(*a, **k): return {"items": [], "total": 0}
def get_fila(*a, **k): return {"items": [], "total": 0}
def get_funil(*a, **k): return {}
def get_importacoes_historico(*a, **k): return {"items": [], "total": 0}
def get_qualidade_detalhes(*a, **k): return {"items": [], "total": 0}
def get_opcoes(*a, **k): return {}
def get_produtividade(*a, **k): return []
def get_qualidade_dados(*a, **k): return {}
def get_qualidade(*a, **k): return {}
def get_rejeicoes(*a, **k): return {"items": [], "total": 0}
def get_rankings(*a, **k): return {}
def get_resumo(*a, **k): return {}
def criar_log_importacao(*a, **k): return {"upload_id": (a[0] if a else None), "success": True}
def atualizar_log_importacao(*a, **k): return {"success": True}

def _xlsx_bytes(rows=None):
    bio=io.BytesIO(); pd.DataFrame(rows or []).to_excel(bio,index=False); bio.seek(0); return bio.getvalue()
def export_qualidade(*a, **k): return ("qualidade.xlsx", _xlsx_bytes(), 0)
def export_importacoes(*a, **k): return ("importacoes.xlsx", _xlsx_bytes(), 0)
def export_fila(*a, **k): return ("fila.xlsx", _xlsx_bytes(), 0)
def export_rejeicoes(*a, **k): return ("rejeicoes.xlsx", _xlsx_bytes(), 0)
def export_produtividade(*a, **k): return ("produtividade.xlsx", _xlsx_bytes(), 0)
