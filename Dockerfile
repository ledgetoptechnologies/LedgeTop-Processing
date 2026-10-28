FROM python:3.13-slim-bookworm@sha256:e32be0e55814a08977501ca863271d9bd02443710db8f2afaf8372e2c4508530

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PROCESSING_BIND_HOST=0.0.0.0 \
    PROCESSING_PORT=8080

WORKDIR /app
COPY --chown=10001:10001 processing ./processing
RUN mkdir /prototype-state && chown 10001:10001 /prototype-state
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=3).read()"
CMD ["python", "-m", "processing.app"]
