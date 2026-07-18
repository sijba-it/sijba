FROM docker.io/library/python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core/ core/
COPY schema/ schema/
COPY plugins-builtin/ plugins-builtin/

# Make builtin plugins executable
RUN chmod +x plugins-builtin/*.sh plugins-builtin/*.py \
    plugins-builtin/*/main.py 2>/dev/null || true

ENV SIJBA_CONFIG_DIR=/config

EXPOSE 8080

CMD ["python", "-m", "core.main"]
