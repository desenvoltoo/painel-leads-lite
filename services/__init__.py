"""Inicialização dos serviços do Painel de Leads Lite.

O projeto historicamente importava diretamente de ``services.database``.
Este módulo aplica o adaptador do pipeline de upload sem alterar as demais
consultas do painel.
"""

from . import database as _database
from .upload_pipeline import process_upload_dataframe

# Garante que ``from services.database import process_upload_dataframe`` use
# a implementação compatível com FUNCTION e PROCEDURE do PostgreSQL.
_database.process_upload_dataframe = process_upload_dataframe

__all__ = ["process_upload_dataframe"]
