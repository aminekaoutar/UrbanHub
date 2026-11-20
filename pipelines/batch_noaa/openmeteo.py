"""Complément météo Open-Meteo : du lendemain de la fin des données NOAA (25/08/2025) à aujourd'hui.

Pourquoi : les données NOAA s'arrêtent au 24/08/2025, or les vélos (CityBikes) et la
pollution (OpenAQ) sont collectés maintenant. Sans météo récente, pas d'analyse croisée
(partie 4). Open-Meteo fournit la vraie météo passée, heure par heure, gratuitement et sans clé.

Deux services Open-Meteo sont utilisés pour chaque ville :
  - "archive"  : météo observée/réanalysée, disponible avec ~5 jours de retard ;
  - "forecast" : les 7 derniers jours (pour combler ces 5 jours de retard).
Pour une même heure, la valeur "archive" est prioritaire.

Ce que fait le script :
  1. Télécharge les réponses JSON, les garde telles quelles dans MinIO :
         raw/openmeteo/hourly/year=YYYY/month=MM/day=DD/<ville>_<service>_<horodatage>.json
  2. Les transforme en une table ville x heure (mêmes colonnes et unités que la NOAA) :
         s3a://clean/openmeteo/weather_city_hourly/
     curate.py ajoute ensuite ces lignes à weather_city_hourly, après la dernière heure NOAA.

Limites (à mentionner dans l'interprétation) :
  - Ce sont des données de modèle météo (grille de ~10 km), pas les mesures d'une station.
  - La visibilité n'existe que dans "forecast" : elle est vide (null) pour les dates plus anciennes.

Lancement (depuis la racine du projet, sur le PC) :
    docker compose -f infra/docker-compose.yml --env-file .env exec jupyter python3 -m pipelines.batch_noaa.openmeteo
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone

import boto3
import requests
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from pipelines.batch_noaa.clean import CITIES, get_spark

START_DATE = date(2025, 8, 25)          # lendemain de la dernière donnée NOAA
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OUTPUT = "s3a://clean/openmeteo/weather_city_hourly/"
RAW_BUCKET = "raw"
RAW_PREFIX = "openmeteo/hourly"

# Variable Open-Meteo -> nom de colonne du contrat de données
VARIABLES = {
    "temperature_2m": "temperature",        # °C
    "wind_speed_10m": "wind_speed",         # m/s (grâce à wind_speed_unit=ms)
    "wind_direction_10m": "wind_direction", # degrés
    "pressure_msl": "pressure",             # hPa, ramenée au niveau de la mer (comme SLP NOAA)
    "precipitation": "precipitation",       # mm sur l'heure
    "visibility": "visibility",             # m (seulement dans "forecast")
}
COLUMNS = ["city", "timestamp", "temperature", "wind_speed", "wind_direction",
           "pressure", "precipitation", "visibility", "nb_stations", "source"]

log = logging.getLogger("openmeteo")


def fetch(url: str, params: dict) -> dict:
    """Appel HTTP avec 3 essais. Message clair si la limite d'appels est atteinte."""
    for attempt in range(1, 4):
        resp = requests.get(url, params=params, timeout=60)
        if resp.status_code == 429:
            raise RuntimeError("Open-Meteo : limite d'appels atteinte, réessayer plus tard. "
                               f"Réponse : {resp.text[:200]}")
        if resp.ok:
            return resp.json()
        log.warning("%s : HTTP %s (essai %d/3)", url, resp.status_code, attempt)
        time.sleep(2 ** attempt)
    resp.raise_for_status()
    return {}


def params_for(lat: float, lon: float, variables: list[str]) -> dict:
    return {"latitude": lat, "longitude": lon, "hourly": ",".join(variables),
            "wind_speed_unit": "ms", "timezone": "UTC"}


def save_raw(s3, city: str, service: str, payload: dict, now: datetime) -> None:
    key = (f"{RAW_PREFIX}/year={now:%Y}/month={now:%m}/day={now:%d}/"
           f"{city}_{service}_{now:%Y%m%dT%H%MZ}.json")
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=json.dumps(payload).encode(),
                  ContentType="application/json")


def to_rows(city: str, payload: dict, now: datetime) -> dict[datetime, dict]:
    """Réponse Open-Meteo -> {heure: {colonne: valeur}}, sans les heures futures ni les heures vides."""
    hourly = payload.get("hourly", {})
    rows: dict[datetime, dict] = {}
    for i, t in enumerate(hourly.get("time", [])):
        ts = datetime.strptime(t, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
        if ts > now:
            continue
        values = {col: hourly.get(var, [None] * (i + 1))[i] for var, col in VARIABLES.items()}
        if values["temperature"] is None:       # heure pas encore calculée par Open-Meteo
            continue
        rows[ts] = values
    return rows


def collect_city(s3, city: str, lat: float, lon: float, now: datetime) -> list[Row]:
    end_archive = min(now.date(), date.today()) - timedelta(days=1)
    archive_vars = [v for v in VARIABLES if v != "visibility"]
    archive = fetch(ARCHIVE_URL, {**params_for(lat, lon, archive_vars),
                                  "start_date": START_DATE.isoformat(),
                                  "end_date": end_archive.isoformat()})
    save_raw(s3, city, "archive", archive, now)

    recent = fetch(FORECAST_URL, {**params_for(lat, lon, list(VARIABLES)),
                                  "past_days": 7, "forecast_days": 1})
    save_raw(s3, city, "forecast", recent, now)

    merged = to_rows(city, recent, now)
    merged.update(to_rows(city, archive, now))          # "archive" écrase "forecast"
    # La visibilité n'existe que dans "forecast" : on la récupère quand elle est disponible
    for ts, values in to_rows(city, recent, now).items():
        if merged[ts]["visibility"] is None:
            merged[ts]["visibility"] = values["visibility"]

    log.info("%s : %d heures (%s -> %s)", city, len(merged),
             min(merged).strftime("%Y-%m-%d %H:%M"), max(merged).strftime("%Y-%m-%d %H:%M"))
    return [Row(city=city, timestamp=ts.replace(tzinfo=None), nb_stations=None,
                source="openmeteo", **{c: (float(v) if v is not None else None)
                                       for c, v in values.items()})
            for ts, values in sorted(merged.items())]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "urbanhub"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "urbanhub123"),
        region_name="us-east-1",
    )

    rows: list[Row] = []
    for city, (lat, lon) in CITIES.items():
        rows += collect_city(s3, city, lat, lon, now)

    spark: SparkSession = get_spark("openmeteo")
    schema = ("city string, timestamp timestamp, temperature double, wind_speed double, "
              "wind_direction double, pressure double, precipitation double, visibility double, "
              "nb_stations long, source string")
    df = spark.createDataFrame([[r[c] for c in COLUMNS] for r in rows], schema)
    (df.withColumn("year", F.year("timestamp")).withColumn("month", F.month("timestamp"))
       .repartition("year")
       .write.mode("overwrite").partitionBy("year", "month").parquet(OUTPUT))
    log.info("Terminé : %d lignes écrites dans %s", df.count(), OUTPUT)
    spark.stop()


if __name__ == "__main__":
    main()
