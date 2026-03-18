tenho um painel que eu subo leads lá porém ele está colocando um 0 no final dos celulares, eu testei agora e está assim: Data Inscricao | Candidato | CPF | Celular | Email | Curso | Modalidade | Turno | Polo
2026-03-17 | Teste 09 Texto Misto | 90123456789 | 11985178225 | teste09@example.com | ANÁLISE DE DADOS | EAD | NOITE | TESTE POLO 2026-03-17 | Teste 04 Pontuação | 45678901234 | 11958953191 | teste04@example.com | ANÁLISE DE DADOS | EAD | NOITE | TESTE POLO 2026-03-17 | Teste 07 Já Ruim 14 | 11234567890 | 55119443914040 | teste07@example.com | ANÁLISE DE DADOS | EAD | NOITE | TESTE POLO 2026-03-17 | Teste 06 Com 55 Limpo | 67890123456 | 5511987684298 | teste06@example.com | ANÁLISE DE DADOS | EAD | NOITE | TESTE POLO 2026-03-17 | Teste 01 Normal Texto | 12345678901 | 11974817404 | teste01@example.com | ANÁLISE DE DADOS | EAD | NOITE | TESTE POLO 2026-03-17 | Teste 05 Com +55 | 56789012345 | 5511997345730 | teste05@example.com | ANÁLISE DE DADOS | EAD | NOITE | TESTE POLO 2026-03-17 | Teste 03 Número Float | 34567890123 | 21911008767 | teste03@example.com | ANÁLISE DE DADOS | EAD | NOITE | TESTE POLO 2026-03-17 | Teste 10 Integridade Geral | 15456789012 | 31992538557 | teste10@example.com | ANÁLISE DE DADOS | EAD | NOITE | TESTE POLO 2026-03-17 | Teste 02 Número Int | 23456789012 | 11961471507 | teste02@example.com | ANÁLISE DE DADOS | EAD | NOITE | TESTE POLO 2026-03-17 | Teste 08 14 Dígitos | 12234567890 | 55119842474650 | teste08@example.com | ANÁLISE DE DADOS | EAD | NOITE | TESTE POLO
quer ver minha sp ou quer que eu vou mandando um por um as minhas páginas da aplicação responsáveis para subir os leads e extrair eles de volta? tipo eu subo pelo painel, que é minha aplicação, depois ele passa pela sp e faz a deduplicação e arruma os telefones, e depois eu extraio pelo painel onde eu vê uma view (eu acho) e me manda os leads de volta, me ajuda
15:18

Manda a SP e as páginas responsáveis pelo upload e extração! Assim consigo ver o fluxo completo e identificar exatamente onde o 0 está sendo adicionado.

Mas já dá pra suspeitar olhando os dados:

    Teste 03 Número Float → 21911008767 (parece ok, mas o nome sugere que veio como float)
    Teste 07 Já Ruim 14 → 55119443914**0** (14 dígitos, zero no final)
    Teste 08 14 Dígitos → 5511984247465**0** (14 dígitos, zero no final)

O padrão clássico desse bug é o número sendo tratado como float/double em algum ponto — quando o Excel ou o sistema lê 11985178225 como número, ele pode virar 11985178225.0, e aí ao converter pra string fica 119851782250 (o .0 vira 0 no final).

Pode acontecer no upload (leitura do arquivo), na SP, ou na view de extração. Manda os dois lados que resolvo!

sp: 

bigquery.py:

app.py:

app.js:
15:23

Perfeito, li tudo. O bug está identificado com precisão — e não é na SP, nem no bigquery.py. Deixa eu te mostrar exatamente onde e por quê.
O bug: pd.read_excel lê celular como float

No app.py, quando o arquivo é XLSX:
python

def _read_upload_to_df(file_storage) -> pd.DataFrame:
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        return pd.read_excel(io.BytesIO(raw))  # ← AQUI

O pandas lê colunas numéricas como float64 por padrão. Então 11985178225 vira 11985178225.0. Aí quando você tem números como 55119443914040 (14 dígitos), o float64 perde precisão e arredonda — o que explica os casos "Já Ruim 14" e "14 Dígitos" com 0 no final.

O _normalize_phoneish_value no bigquery.py até tenta corrigir isso com Decimal, mas o dano já foi feito: o float64 já perdeu os dígitos corretos antes de chegar no normalizador.
A correção

Em _read_upload_to_df, força as colunas phoneish como dtype=str na leitura:
python

def _read_upload_to_df(file_storage) -> pd.DataFrame:
    filename = (file_storage.filename or "").lower().strip()
    raw = file_storage.read()

    # colunas que jamais devem virar float
    PHONE_COLS = {"cpf", "celular"}

    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        # 1ª leitura: só para descobrir as colunas presentes
        preview = pd.read_excel(io.BytesIO(raw), nrows=0)
        dtype_map = {
            col: str
            for col in preview.columns
            if str(col).strip().lower() in PHONE_COLS
        }
        # 2ª leitura: com dtype forçado
        return pd.read_excel(io.BytesIO(raw), dtype=dtype_map)

    # CSV — mesma lógica
    def _read_csv(raw_bytes, sep, encoding):
        preview = pd.read_csv(
            io.BytesIO(raw_bytes), sep=sep, encoding=encoding, nrows=0
        )
        dtype_map = {
            col: str
            for col in preview.columns
            if str(col).strip().lower() in PHONE_COLS
        }
        return pd.read_csv(
            io.BytesIO(raw_bytes), sep=sep, encoding=encoding, dtype=dtype_map
        )

    for sep in (";", ","):
        for enc in ("utf-8", "latin-1"):
            try:
                return _read_csv(raw, sep, enc)
            except Exception:
                continue

    raise ValueError("Não foi possível ler o arquivo CSV.")
```

## Por que a SP e o `bigquery.py` não eram o problema

- A SP recebe tudo como `STRING` (schema blindado) — ela está correta
- O `_normalize_phoneish_value` funciona bem para `11985178225.0` → `"11985178225"`, mas **float64 já corrompeu `55119443914040` para `55119443914040.0` antes**, e o Decimal converte o float já errado, não o original
- O `PHONEISH_COLUMNS` no `bigquery.py` está certo, só chega tarde demais

## Resumo do fluxo com o bug
```
XLSX → pd.read_excel → float64 corrompe → _normalize_phoneish → Decimal do float errado → BQ staging → SP → dim_pessoa com número errado

Troca só a função _read_upload_to_df no app.py que resolve os dois casos (14 dígitos com zero no final). Os números normais de 11 dígitos até sobreviviam porque float64 tem precisão suficiente para eles, mas qualquer coisa com DDI (55 na frente) já chega em 13–14 dígitos e perde precisão.

manda o código completo (completo definição: completo
adjetivo
1. 1.
a que não falta parte ou elemento algum dos que o constituem ou que deve ter.
"exame c. do coração"
2. 2.
que está no máximo de sua capacidade.)do app.py, aqui está ele atualmente:
16:29
Criou um arquivo, leu um arquivo

Só mudou a seção _read_upload_to_df — todo o resto está idêntico ao seu original. O que foi alterado:

    Adicionada a constante _PHONEISH_UPLOAD_COLS e a helper _dtype_map_for_phoneish
    Para XLSX: faz uma leitura rápida só do cabeçalho (nrows=0), monta o dtype_map forçando str nas colunas cpf/celular, depois faz a leitura real com esse mapa
    Para CSV: mesma lógica de duas leituras, aplicada em cascata nos 4 combos de separador/encoding
    O except final agora levanta um ValueError com mensagem clara em vez de silenciosamente tentar uma última combinação que poderia mascarar o erro

App
PY 


Claude é uma IA e pode cometer erros. Por favor, verifique as respostas.
App · PY
# -*- coding: utf-8 -*-
"""
Painel Leads Lite (Flask + BigQuery)
Versão: 4.1 - Upload Assíncrono (staging + dispara SP sem bloquear)
"""
 
import os
import traceback
import io
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple
 
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_file
 
from services.bigquery import (
    query_leads,
    query_leads_count,
    query_options,
    process_upload_dataframe,   # agora retorna job_id (async)
    get_bq_job_status,          # novo
    export_leads_rows,
    df_to_xlsx,                 # salva cópia do upload
    rows_to_xlsx,               # gera export XLSX no servidor
)
 
# ============================================================
# HELPERS
# ============================================================
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) else v if v else default
 
def _required_envs_ok():
    missing = []
    for k in ("GCP_PROJECT_ID", "BQ_DATASET"):
        if not _env(k):
            missing.append(k)
    return (len(missing) == 0, missing)
 
def _get_filters_from_request() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    def n(v):
        v = (v or "").strip()
        return v if v else None
 
    filters = {
        "status": n(request.args.get("status")),  # pode ser status_inscricao também
        "curso": n(request.args.get("curso")),
        "polo": n(request.args.get("polo")),
        "origem": n(request.args.get("origem")),
        "consultor": n(request.args.get("consultor")),  # compat -> consultor_disparo
        "consultor_disparo": n(request.args.get("consultor_disparo")),
        "consultor_comercial": n(request.args.get("consultor_comercial")),
        "modalidade": n(request.args.get("modalidade")),
        "turno": n(request.args.get("turno")),
        "canal": n(request.args.get("canal")),
        "campanha": n(request.args.get("campanha")),
        "tipo_disparo": n(request.args.get("tipo_disparo")),
        "tipo_negocio": n(request.args.get("tipo_negocio")),
        "cpf": n(request.args.get("cpf")),
        "celular": n(request.args.get("celular")),
        "email": n(request.args.get("email")),
        "nome": n(request.args.get("nome")),
        "matriculado": n(request.args.get("matriculado")),
        "data_ini": n(request.args.get("data_ini")),
        "data_fim": n(request.args.get("data_fim")),
    }
    filters = {k: v for k, v in filters.items() if v is not None and str(v).strip() != ""}
 
    meta = {
        "limit": int(request.args.get("limit") or 500),
        "offset": int(request.args.get("offset") or 0),
        "order_by": n(request.args.get("order_by")) or "data_inscricao",
        "order_dir": n(request.args.get("order_dir")) or "DESC",
    }
    return filters, meta
 
def _error_payload(e: Exception, public_msg: str):
    return {
        "ok": False,
        "error": public_msg,
        "details": str(e),
        "trace": traceback.format_exc(limit=3),
    }
 
def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")
 
 
# ============================================================
# Upload parsing: CSV/XLSX robusto (separador, encoding)
# FIX: força cpf/celular como str ANTES do pandas inferir float64,
#      evitando corrupção de números longos (ex: 55119443914040 -> 55119443914040**0**)
# ============================================================
 
# colunas que jamais devem ser lidas como número
_PHONEISH_UPLOAD_COLS = {"cpf", "celular"}
 
 
def _dtype_map_for_phoneish(columns) -> dict:
    """
    Recebe a lista de colunas do arquivo e devolve um dtype_map
    forçando str para qualquer coluna que seja cpf ou celular.
    """
    return {
        col: str
        for col in columns
        if str(col).strip().lower() in _PHONEISH_UPLOAD_COLS
    }
 
 
def _read_upload_to_df(file_storage) -> pd.DataFrame:
    filename = (file_storage.filename or "").lower().strip()
    raw = file_storage.read()
 
    # ------------------------------------------------------------------
    # XLSX
    # ------------------------------------------------------------------
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        # 1ª leitura: só cabeçalho para descobrir nomes das colunas
        preview = pd.read_excel(io.BytesIO(raw), nrows=0)
        dtype_map = _dtype_map_for_phoneish(preview.columns)
        # 2ª leitura: com dtype forçado nas colunas phoneish
        return pd.read_excel(io.BytesIO(raw), dtype=dtype_map)
 
    # ------------------------------------------------------------------
    # CSV — tenta separadores e encodings em cascata
    # ------------------------------------------------------------------
    def _read_csv(raw_bytes: bytes, sep: str, encoding: str) -> pd.DataFrame:
        # 1ª leitura: só cabeçalho
        preview = pd.read_csv(
            io.BytesIO(raw_bytes), sep=sep, encoding=encoding, nrows=0
        )
        dtype_map = _dtype_map_for_phoneish(preview.columns)
        # 2ª leitura: completa com dtype forçado
        return pd.read_csv(
            io.BytesIO(raw_bytes), sep=sep, encoding=encoding, dtype=dtype_map
        )
 
    for sep in (";", ","):
        for enc in ("utf-8", "latin-1"):
            try:
                return _read_csv(raw, sep, enc)
            except Exception:
                continue
 
    raise ValueError(
        "Não foi possível ler o arquivo. "
        "Verifique se é um CSV válido (separador ; ou ,) ou XLSX."
    )
 
 
# ============================================================
# APP FACTORY
# ============================================================
def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 30 * 1024 * 1024
 
    asset_version = _env("ASSET_VERSION", "20260225-star-v1")
    ui_version = _env("UI_VERSION", f"v{asset_version}")
 
    # pastas locais (mantém XLSX)
    UPLOAD_DIR = Path(_env("UPLOAD_DIR", "enviados"))
    EXPORT_DIR = Path(_env("EXPORT_DIR", "exportados"))
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
 
    @app.get("/")
    def index():
        return render_template(
            "index.html",
            asset_version=asset_version,
            ui_version=ui_version,
        )
 
    @app.get("/health")
    def health():
        ok, missing = _required_envs_ok()
        return jsonify(
            {
                "status": "ok" if ok else "unhealthy",
                "missing": missing,
                "ui_version": ui_version,
                "asset_version": asset_version,
            }
        )
 
    @app.get("/api/leads")
    def api_leads():
        try:
            filters, meta = _get_filters_from_request()
 
            rows = query_leads(
                filters=filters,
                limit=meta["limit"],
                offset=meta["offset"],
                order_by=meta["order_by"],
                order_dir=meta["order_dir"],
            )
            total = query_leads_count(filters=filters)
 
            return jsonify({"ok": True, "total": total, "data": rows})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao buscar leads no BigQuery.")), 500
 
    @app.get("/api/kpis")
    def api_kpis():
        """
        Mantém seu KPI atual (rápido): conta em memória nos rows retornados.
        """
        try:
            filters, meta = _get_filters_from_request()
            rows = query_leads(
                filters=filters,
                limit=min(int(meta["limit"]), 5000),
                offset=meta["offset"],
                order_by=meta["order_by"],
                order_dir=meta["order_dir"],
            )
 
            total = len(rows)
            status_counts: dict = {}
            for r in rows:
                st = r.get("status_inscricao") or r.get("status") or "LEAD"
                status_counts[st] = status_counts.get(st, 0) + 1
 
            top_status = None
            if status_counts:
                best = max(status_counts, key=status_counts.get)
                top_status = {"status": best, "cnt": status_counts[best]}
 
            return jsonify({"ok": True, "total": total, "top_status": top_status})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao calcular KPIs.")), 500
 
    @app.get("/api/options")
    def api_options():
        try:
            data = query_options()
            return jsonify({"ok": True, "data": data})
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao carregar opções dos filtros.")), 500
 
    # ============================================================
    # ✅ EXPORT XLSX (gera arquivo em exportados/ e também envia)
    # ============================================================
    @app.get("/api/export/xlsx")
    def api_export_xlsx():
        try:
            filters, meta = _get_filters_from_request()
 
            limit = min(int(meta.get("limit") or 50000), 100000)
            rows = export_leads_rows(
                filters=filters,
                limit=limit,
                offset=int(meta.get("offset") or 0),
                order_by=meta.get("order_by") or "data_inscricao",
                order_dir=meta.get("order_dir") or "DESC",
            )
 
            fname = f"leads_export_{_stamp()}.xlsx"
            out_path = str(EXPORT_DIR / fname)
 
            rows_to_xlsx(rows or [], out_path, sheet_name="Leads")
 
            return send_file(
                out_path,
                as_attachment=True,
                download_name="leads_export.xlsx",
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as e:
            return jsonify(_error_payload(e, "Erro ao exportar XLSX.")), 500
 
    # ============================================================
    # ✅ UPLOAD (CSV/XLSX) + salva cópia XLSX em enviados/
    # ✅ AGORA ASSÍNCRONO: retorna job_id e NÃO trava request
    # ============================================================
    @app.post("/api/upload")
    def api_upload():
        if "file" not in request.files:
            return jsonify({"ok": False, "error": "Arquivo não encontrado."}), 400
 
        f = request.files["file"]
        if not f.filename:
            return jsonify({"ok": False, "error": "Nome de arquivo vazio."}), 400
 
        try:
            filename = (f.filename or "").lower()
            if not (filename.endswith(".csv") or filename.endswith(".xlsx") or filename.endswith(".xls")):
                return jsonify({"ok": False, "error": "Formato inválido. Envie CSV ou XLSX."}), 400
 
            # lê df (cpf/celular já chegam como str, sem risco de float64)
            df = _read_upload_to_df(f)
 
            # ✅ salva cópia SEMPRE em XLSX
            saved_name = f"upload_{_stamp()}_{uuid.uuid4().hex[:6]}.xlsx"
            saved_path = str(UPLOAD_DIR / saved_name)
            df_to_xlsx(df, saved_path, sheet_name="Upload")
 
            # ✅ staging + dispara SP async (retorna job_id)
            job_id = process_upload_dataframe(df)
 
            # 202 Accepted (processando)
            return jsonify(
                {
                    "ok": True,
                    "message": "Upload recebido. Importação em processamento (assíncrono).",
                    "saved_xlsx": saved_name,
                    "job_id": job_id,
                }
            ), 202
 
        except Exception as e:
            return jsonify(_error_payload(e, "Falha na ingestão.")), 500
 
    # ============================================================
    # ✅ STATUS DO UPLOAD (job do BigQuery)
    # ============================================================
    @app.get("/api/upload/status")
    def api_upload_status():
        try:
            job_id = (request.args.get("job_id") or "").strip()
            if not job_id:
                return jsonify({"ok": False, "error": "job_id é obrigatório"}), 400
 
            data = get_bq_job_status(job_id)
            return jsonify({"ok": True, "data": data}), 200
        except Exception as e:
            return jsonify(_error_payload(e, "Falha ao consultar status do upload.")), 500
 
    return app
 
 
if __name__ == "__main__":
    app = create_app()
    port = int(_env("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=True)
 

