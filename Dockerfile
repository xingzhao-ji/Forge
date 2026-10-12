FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir gunicorn==23.0.0 && useradd --uid 10001 --create-home demo
COPY forge ./forge
COPY demo ./demo
USER demo
ENV PORT=8080 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
CMD ["sh", "-c", "exec gunicorn --bind 0.0.0.0:${PORT} --workers 1 --threads 8 --timeout 30 --keep-alive 2 --limit-request-line 2048 --limit-request-fields 20 --limit-request-field_size 2048 demo.server:app"]
