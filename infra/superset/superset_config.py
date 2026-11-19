"""Configuration de Superset pour UrbanHub (chargée automatiquement au démarrage)."""
import os

# Clé secrète (obligatoire). Valeur locale définie dans .env.
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Base interne de Superset (utilisateurs, graphiques, dashboards) : la base "superset"
# du conteneur PostgreSQL, séparée de la base "urbanhub" qui contient nos données.
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
    f"@postgres:5432/superset"
)

# Interface en français
BABEL_DEFAULT_LOCALE = "fr"
