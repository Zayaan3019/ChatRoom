# Chat server image. The server uses only the Python standard library, so the
# image needs no third-party packages (test tools are in requirements-dev.txt).
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Run as a non-root user
RUN useradd --create-home --shell /usr/sbin/nologin chatuser
WORKDIR /app

COPY app/ ./app/
COPY main.py ./
RUN chown -R chatuser:chatuser /app
USER chatuser

# 8888: chat protocol (TCP). 9090: /metrics, /health, /stats (HTTP).
EXPOSE 8888 9090

# Checks the real health endpoint. The slim image has no curl, so use Python.
HEALTHCHECK --interval=15s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/health' % os.getenv('CHAT_METRICS_PORT', '9090'), timeout=4)" || exit 1

# The production server: rate limiting, connection limits, metrics, health.
CMD ["python", "main.py"]
