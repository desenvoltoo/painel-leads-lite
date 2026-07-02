FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000 \
    WEB_CONCURRENCY=2 \
    GUNICORN_THREADS=4 \
    GUNICORN_TIMEOUT=300

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel \
    && python -m pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 8000

CMD exec gunicorn -c gunicorn.conf.py app:app
