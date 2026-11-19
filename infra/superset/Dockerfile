# Image Superset d'UrbanHub : l'image officielle + le pilote PostgreSQL (psycopg2),
# absent de l'image officielle et nécessaire pour lire nos tables.
FROM apache/superset:5.0.0

USER root
RUN uv pip install --python /app/.venv/bin/python --no-cache psycopg2-binary
USER superset
