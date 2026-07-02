# -*- coding: utf-8 -*-
"""Operational compatibility service without external cloud dependencies."""
from __future__ import annotations
from datetime import datetime, timezone
import csv, io

class DatabaseSchemaError(RuntimeError): pass

def classify_database_error(exc: Exception):
    return {"error_type":"DATABASE_ERROR", "message":"Erro técnico ao consultar o PostgreSQL.", "details": str(exc)}

def parse_operational_request(args=None, json_payload=None): return {"filters": dict(args or {}), "meta": dict(json_payload or {})}
def create_operational_tables(): return {"created": []}
def _items(): return {"items": [], "total": 0}
def get_dashboard(*a, **k): return {}
def get_leads_disponiveis(*a, **k): return _items()
def criar_lote(*a, **k): return {"success": True, "lote_id": None}
def get_lotes(*a, **k): return _items()
def get_lote_detalhe(*a, **k): return {}
def start_lote(*a, **k): return {"success": True}
def finish_lote(*a, **k): return {"success": True}
def get_meus_leads(*a, **k): return _items()
def update_lead_status(*a, **k): return {"success": True}
def liberar_proximos_leads(*a, **k): return {"success": True, "items": []}
def executar_regras_distribuicao(*a, **k): return {"success": True}
def get_esteira_operacional(*a, **k): return _items()
def get_fila_por_prioridade(*a, **k): return _items()
def criar_regra_distribuicao(*a, **k): return {"success": True}
def listar_regras_distribuicao(*a, **k): return _items()
def ativar_desativar_regra(*a, **k): return {"success": True}
def get_lotes_select(*a, **k): return []
def preview_proximo_lote(*a, **k): return {"items": [], "total": 0}
def exportar_proximo_lote(*a, **k): return {"items": [], "total": 0}
def get_lote_csv(*a, **k): return ""
def importar_lote_disparado(*a, **k): return {"success": True}
def importar_novos_leads(*a, **k): return {"success": True}
def get_operacao_logs(*a, **k): return _items()
def cancelar_lote(*a, **k): return {"success": True}
def marcar_lote_disparado(*a, **k): return {"success": True}
def importar_retorno_lote(*a, **k): return {"success": True}
def buscar_leads(*a, **k): return _items()
def get_lead_timeline(*a, **k): return []
def get_lead_lotes(*a, **k): return []
def get_lead_eventos(*a, **k): return []
def get_consultor_momento(*a, **k): return {}
def get_lote_atual_leads(*a, **k): return _items()
def atualizar_lead_lote(*a, **k): return {"success": True}
def listar_usuarios(*a, **k): return _items()
def salvar_usuario(*a, **k): return {"success": True}
def alterar_status_usuario(*a, **k): return {"success": True}
def resetar_senha_usuario(*a, **k): return {"success": True}
def listar_perfis(*a, **k): return []
def auditoria_usuario(*a, **k): return {"success": True}
def buscar_usuario_login(*a, **k): return None
def registrar_login_usuario(*a, **k): return {"success": True}
def atualizar_password_hash_usuario(*a, **k): return {"success": True}
def get_logs_auditoria(*a, **k): return _items()
