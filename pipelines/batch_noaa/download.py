"""Téléchargement PARALLÈLE des données météo NOAA Global Hourly pour la France.

Ce que fait ce script, étape par étape :
  1. Télécharge le "carnet d'adresses" des stations (isd-history.csv) et garde
     uniquement les stations françaises (CTRY == "FR").
  2. Pour chaque année demandée, liste les fichiers réellement disponibles chez la NOAA
     et garde ceux des stations françaises (1 fichier = 1 station x 1 année).
  3. Télécharge ces fichiers EN PARALLÈLE (plusieurs à la fois, avec reprises sur erreur)
     et les dépose tels quels dans MinIO :
         raw/noaa/global_hourly/year=<année>/<station>.csv
     Les fichiers déjà présents dans MinIO (même taille) sont sautés : on peut relancer
     le script sans tout retélécharger.

Source : depuis 2025, https://www.ncei.noaa.gov/data/global-hourly/access/ redirige vers
le stockage public Amazon S3 "noaa-global-hourly-pds" (mêmes fichiers). Les données
s'arrêtent au 24/08/2025 (voir docs/decisions.md).

Lancement (depuis la racine du projet, sur le PC) :
    docker compose -f infra/docker-compose.yml --env-file .env exec jupyter python3 -m pipelines.batch_noaa.download

Options utiles :
    --years 2024 2025     années à télécharger (défaut : 2024 2025)
    --workers 16          nombre de téléchargements en parallèle (défaut : 16)
    --limit 5             ne télécharger que 5 fichiers (pour tester)
    --dry-run             afficher ce qui serait téléchargé, sans rien télécharger
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import boto3
import requests
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Réglages
# ---------------------------------------------------------------------------
STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"
NOAA_BUCKET_URL = "https://noaa-global-hourly-pds.s3.amazonaws.com"
DEFAULT_YEARS = [2024, 2025]      # décision d'équipe (docs/decisions.md)
COUNTRY = "FR"

RAW_BUCKET = os.environ.get("BUCKET_RAW", "raw")
RAW_PREFIX = "noaa/global_hourly"           # raw/noaa/global_hourly/year=YYYY/<station>.csv
STATIONS_KEY = "noaa/stations/isd-history.csv"

MAX_RETRIES = 4                             # nombre d'essais par fichier
TIMEOUT = 120                               # secondes avant d'abandonner une requête

log = logging.getLogger("noaa.download")


@dataclass
class NoaaFile:
    year: int
    station_id: str     # ex. "07149099999" (USAF + WBAN)
    size: int           # taille en octets annoncée par la NOAA

    @property
    def url(self) -> str:
        return f"{NOAA_BUCKET_URL}/{self.year}/{self.station_id}.csv"

    @property
    def minio_key(self) -> str:
        return f"{RAW_PREFIX}/year={self.year}/{self.station_id}.csv"


# ---------------------------------------------------------------------------
# Connexion à MinIO (notre Data Lake)
# ---------------------------------------------------------------------------
def minio_client():
    """Client S3 pointant vers MinIO (adresse et identifiants lus dans l'environnement)."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "urbanhub"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "urbanhub123"),
        region_name="us-east-1",
    )


def size_in_minio(s3, key: str) -> int | None:
    """Taille du fichier dans MinIO, ou None s'il n'existe pas."""
    try:
        return s3.head_object(Bucket=RAW_BUCKET, Key=key)["ContentLength"]
    except ClientError as err:
        if err.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


# ---------------------------------------------------------------------------
# Étape 1 : la liste des stations françaises
# ---------------------------------------------------------------------------
def french_stations(session: requests.Session, s3) -> set[str]:
    """Télécharge isd-history.csv, le garde dans MinIO et renvoie les identifiants FR."""
    log.info("Téléchargement de la liste des stations (isd-history.csv)...")
    resp = session.get(STATIONS_URL, timeout=TIMEOUT)
    resp.raise_for_status()
    s3.put_object(Bucket=RAW_BUCKET, Key=STATIONS_KEY, Body=resp.content)

    reader = csv.DictReader(io.StringIO(resp.content.decode("utf-8")))
    ids = {row["USAF"] + row["WBAN"] for row in reader if row["CTRY"] == COUNTRY}
    log.info("%d stations françaises dans le carnet d'adresses", len(ids))
    return ids


# ---------------------------------------------------------------------------
# Étape 2 : les fichiers disponibles pour chaque année
# ---------------------------------------------------------------------------
def list_year(session: requests.Session, year: int) -> dict[str, int]:
    """Liste {station_id: taille} des fichiers disponibles chez la NOAA pour une année.

    Utilise l'API de listing S3 (1000 fichiers par page, on suit les pages).
    """
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    files: dict[str, int] = {}
    token = None
    while True:
        params = {"list-type": "2", "prefix": f"{year}/", "max-keys": "1000"}
        if token:
            params["continuation-token"] = token
        resp = session.get(NOAA_BUCKET_URL + "/", params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        for item in root.findall("s3:Contents", ns):
            key = item.find("s3:Key", ns).text            # ex. "2025/07149099999.csv"
            size = int(item.find("s3:Size", ns).text)
            station_id = key.split("/")[-1].removesuffix(".csv")
            files[station_id] = size
        token_el = root.find("s3:NextContinuationToken", ns)
        if token_el is None:
            return files
        token = token_el.text


def files_to_download(session, years: list[int], stations: set[str]) -> list[NoaaFile]:
    todo: list[NoaaFile] = []
    for year in years:
        available = list_year(session, year)
        french = [NoaaFile(year, sid, size) for sid, size in available.items() if sid in stations]
        total_mb = sum(f.size for f in french) / 1e6
        log.info("%d : %d fichiers français disponibles (%.0f Mo)", year, len(french), total_mb)
        todo.extend(french)
    return todo


# ---------------------------------------------------------------------------
# Étape 3 : téléchargement d'un fichier (exécuté en parallèle par plusieurs "ouvriers")
# ---------------------------------------------------------------------------
def download_one(f: NoaaFile, session: requests.Session, s3) -> str:
    """Télécharge un fichier NOAA et le dépose dans MinIO. Renvoie 'sauté' ou 'téléchargé'."""
    if size_in_minio(s3, f.minio_key) == f.size:
        return "sauté"                                   # déjà là : on ne refait pas

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(f.url, timeout=TIMEOUT)
            resp.raise_for_status()
            if len(resp.content) != f.size:
                raise IOError(f"taille reçue {len(resp.content)} au lieu de {f.size}")
            s3.put_object(Bucket=RAW_BUCKET, Key=f.minio_key, Body=resp.content,
                          ContentType="text/csv")
            return "téléchargé"
        except Exception as err:                         # réseau coupé, serveur occupé...
            if attempt == MAX_RETRIES:
                raise
            wait = 2 ** attempt                          # 2 s, 4 s, 8 s : on patiente de plus en plus
            log.warning("%s (essai %d/%d) : %s -> nouvel essai dans %d s",
                        f.minio_key, attempt, MAX_RETRIES, err, wait)
            time.sleep(wait)
    raise RuntimeError("inaccessible")                   # pour les vérificateurs de type


def download_all(files: list[NoaaFile], workers: int) -> None:
    session = requests.Session()
    # Autant de connexions réseau que d'ouvriers, sinon ils s'attendent entre eux
    adapter = requests.adapters.HTTPAdapter(pool_connections=workers, pool_maxsize=workers)
    session.mount("https://", adapter)
    s3 = minio_client()

    counts = {"téléchargé": 0, "sauté": 0, "erreur": 0}
    failed: list[str] = []
    start = time.time()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(download_one, f, session, s3): f for f in files}
        for i, fut in enumerate(as_completed(futures), start=1):
            f = futures[fut]
            try:
                counts[fut.result()] += 1
            except Exception as err:
                counts["erreur"] += 1
                failed.append(f.minio_key)
                log.error("ÉCHEC %s : %s", f.minio_key, err)
            if i % 25 == 0 or i == len(files):
                log.info("Progression : %d/%d  (téléchargés %d, sautés %d, erreurs %d)",
                         i, len(files), counts["téléchargé"], counts["sauté"], counts["erreur"])

    minutes = (time.time() - start) / 60
    log.info("Terminé en %.1f min : %d téléchargés, %d déjà présents, %d en erreur.",
             minutes, counts["téléchargé"], counts["sauté"], counts["erreur"])
    if failed:
        log.error("Fichiers en erreur (relancer le script pour les reprendre) :\n  %s",
                  "\n  ".join(failed))


# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--years", type=int, nargs="+", default=DEFAULT_YEARS)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")

    session = requests.Session()
    s3 = minio_client()
    stations = french_stations(session, s3)
    files = files_to_download(session, args.years, stations)
    if args.limit:
        files = files[: args.limit]

    total_mb = sum(f.size for f in files) / 1e6
    log.info("À traiter : %d fichiers, %.0f Mo, avec %d téléchargements en parallèle",
             len(files), total_mb, args.workers)
    if args.dry_run:
        for f in files[:10]:
            log.info("  %s  <-  %s", f.minio_key, f.url)
        log.info("(dry-run : rien n'a été téléchargé)")
        return

    download_all(files, args.workers)


if __name__ == "__main__":
    main()
