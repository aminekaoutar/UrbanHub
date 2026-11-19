"""Lecture des variables d'environnement (fichier .env à la racine, jamais dans Git)."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:  # python-dotenv absent : on lit seulement l'environnement
    pass


def _get(name: str, default: str = "") -> str:
    return os.getenv(name, default)


# MinIO (API S3)
MINIO_ENDPOINT = _get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = _get("MINIO_ROOT_USER", "urbanhub")          # noms des variables : .env.example (infra, Dev 1)
MINIO_SECRET_KEY = _get("MINIO_ROOT_PASSWORD", "urbanhub123")
MINIO_ENABLED = _get("MINIO_ENABLED", "false").lower() == "true"   # false = écriture sur disque local uniquement

# Une couche = un bucket
BUCKET_RAW = _get("BUCKET_RAW", "raw")
BUCKET_CLEAN = _get("BUCKET_CLEAN", "clean")
BUCKET_CURATED = _get("BUCKET_CURATED", "curated")

# Copie locale du Data Lake (même arborescence que MinIO) : data/raw/..., data/clean/..., data/curated/...
LOCAL_LAKE = Path(_get("LOCAL_LAKE", str(ROOT / "data")))

# PostgreSQL (couche serving pour Superset)
POSTGRES_HOST = _get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(_get("POSTGRES_PORT", "5432"))
POSTGRES_DB = _get("POSTGRES_DB", "urbanhub")
POSTGRES_USER = _get("POSTGRES_USER", "urbanhub")
POSTGRES_PASSWORD = _get("POSTGRES_PASSWORD", "urbanhub123")

# Spark : spark://localhost:7077 (cluster Docker) ; local[*] tant que le cluster n'est pas lancé
SPARK_MASTER = _get("SPARK_MASTER_URL", "local[*]")

# CityBikes (Développeur 2)
CITYBIKES_BASE_URL = _get("CITYBIKES_BASE_URL", "https://api.citybik.es/v2")
CITYBIKES_INTERVAL_SECONDS = int(_get("CITYBIKES_INTERVAL_SECONDS", "120"))   # 3 réseaux x 30 appels/h = 90 appels/h (quota : 300/h par IP)

# OpenAQ (Développeur 3)
OPENAQ_API_KEY = _get("OPENAQ_API_KEY", "")


def lake_uri(layer: str, path: str = "") -> str:
    """Chemin d'une couche pour Spark : s3a://<bucket>/<path> si MinIO est actif, sinon le dossier local."""
    bucket = {"raw": BUCKET_RAW, "clean": BUCKET_CLEAN, "curated": BUCKET_CURATED}[layer]
    if MINIO_ENABLED:
        return f"s3a://{bucket}/{path}".rstrip("/")
    return str((LOCAL_LAKE / layer / path)).rstrip("/")
