FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app /tmp/streamly \
    && chown -R appuser:appuser /app /tmp/streamly

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py ./
RUN chown appuser:appuser /app/app.py

USER appuser
EXPOSE 8080
CMD ["python", "app.py"]
