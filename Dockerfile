FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Cloud Run injecta PORT; manter default
ENV PORT=8080

# ✅ Gunicorn ajustado pra upload/import pesado
# - 1 worker: mais estável (evita duplicar processamento e estourar RAM)
# - threads: ajuda em I/O (upload, rede, BigQuery)
# - timeout alto: evita matar request no meio do import
# - keep-alive: melhora estabilidade em conexões longas
CMD ["gunicorn", "app:create_app()", \
     "--bind", "0.0.0.0:${PORT}", \
     "--workers", "1", \
     "--threads", "8", \
     "--timeout", "1200", \
     "--graceful-timeout", "1200", \
     "--keep-alive", "5", \
     "--access-logfile", "-", \
     "--error-logfile", "-"]
