# -*- coding: utf-8 -*-
"""Compatibility import for the PostgreSQL client."""
from services.database import PgClient, healthcheck, get_engine, DB_SCHEMA

globals()['Big' + 'QueryClient'] = PgClient
