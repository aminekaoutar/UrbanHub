"""Nettoyage des données NOAA : raw (CSV bruts) -> clean (Parquet propre).

Entrée  : s3a://raw/noaa/global_hourly/year=*/<station>.csv   (fichiers NOAA tels quels)
Sortie  : s3a://clean/noaa/weather_hourly/                    (Parquet, partitions year/month)

Ce que fait le nettoyage :
  1. Lecture des CSV. Les fichiers NOAA n'ont pas tous les mêmes colonnes (187 variantes) :
     on les lit par groupes de fichiers ayant le même en-tête, puis on les rassemble.
  2. On écarte les résumés journaliers/mensuels (SOD, SOM) : on ne garde que les observations.
  3. Décodage des champs NOAA "en code" :
       TMP  "+0038,1"          -> temperature   3.8 °C        (÷10)
       DEW  "+0035,1"          -> dew_point     3.5 °C        (÷10)
       WND  "210,1,N,0057,1"   -> wind_direction 210°, wind_speed 5.7 m/s (÷10)
       SLP  "10278,1"          -> pressure      1027.8 hPa    (÷10)
       VIS  "003600,1,9,9"     -> visibility    3600 m
       AA1  "01,0012,9,1"      -> precipitation 1.2 mm sur 1 h (÷10)
  4. Valeurs manquantes : les "faux nombres" NOAA (9999, 99999, 999999...) et les mesures
     marquées douteuses ou fausses (codes qualité 2, 3, 6, 7) deviennent null.
  5. Valeurs impossibles (ex. 70 °C en France) -> null.
  6. Dédoublonnage : plusieurs bulletins au même instant (SYNOP + METAR) -> une seule ligne.
  7. Ajout de city (paris, lyon ou lille si la station est à moins de 30 km, sinon null), source, ingestion_time,
     relative_humidity (calculée à partir de la température et du point de rosée).

Précipitations : la NOAA donne un cumul sur une durée variable (1 h, 3 h, 6 h...).
On garde le cumul sur la plus courte durée disponible et on le ramène à 1 heure
(mm/h). Approximation documentée dans docs/decisions.md.

Lancement (depuis la racine du projet, sur le PC) :
    docker compose -f infra/docker-compose.yml --env-file .env exec jupyter python3 -m pipelines.batch_noaa.clean

Option : --years 2024 2025   années à nettoyer (défaut : 2024 2025)
"""
from __future__ import annotations

import argparse
import logging
import math
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import reduce

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RAW_BUCKET = "raw"
RAW_PREFIX = "noaa/global_hourly/"
OUTPUT = "s3a://clean/noaa/weather_hourly/"

# Colonnes NOAA dont on a besoin (si une colonne manque dans un fichier, elle vaut null)
NOAA_COLUMNS = ["STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME",
                "REPORT_TYPE", "WND", "VIS", "TMP", "DEW", "SLP", "AA1", "AA2", "AA3"]

# Codes qualité NOAA à rejeter : 2/6 = douteux, 3/7 = faux
BAD_QUALITY = ["2", "3", "6", "7"]

# Plages de valeurs plausibles en France (au-delà : erreur de capteur -> null)
VALID_RANGES = {
    "temperature": (-30.0, 46.0),        # record de France : 46,0 °C (2019)
    "dew_point": (-50.0, 40.0),
    "wind_speed": (0.0, 75.0),
    "wind_direction": (1.0, 360.0),
    "pressure": (900.0, 1080.0),
    "visibility": (0.0, 160_000.0),
    "precipitation": (0.0, 200.0),
}

# Années traitées (décision d'équipe : 2024 -> 2026 ; la NOAA s'arrête en août 2025)
DEFAULT_YEARS = [2024, 2025]

# --- TEMPORAIRE : à remplacer par common.cities.assign_city quand Dev 3 l'aura livré ---
# Décision d'équipe : 3 villes. Les autres stations françaises restent dans clean
# (city = null) pour les analyses "France entière".
CITIES = {
    "paris": (48.8566, 2.3522),
    "lyon": (45.7640, 4.8357),
    "lille": (50.6292, 3.0573),
}
MAX_DISTANCE_KM = 30.0


def assign_city(lat: float | None, lon: float | None) -> str | None:
    """Ville du référentiel la plus proche à moins de 30 km, sinon None (formule de haversine)."""
    if lat is None or lon is None:
        return None
    best, best_km = None, MAX_DISTANCE_KM
    for city, (clat, clon) in CITIES.items():
        p1, p2 = math.radians(lat), math.radians(clat)
        dp, dl = p2 - p1, math.radians(clon - lon)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        km = 2 * 6371.0 * math.asin(math.sqrt(a))
        if km <= best_km:
            best, best_km = city, km
    return best
# ----------------------------------------------------------------------------------------

log = logging.getLogger("noaa.clean")


def get_spark(app_name: str) -> SparkSession:
    # TEMPORAIRE : à remplacer par common.spark.get_spark quand Dev 2 l'aura livré
    spark = (SparkSession.builder
             .master(os.environ.get("SPARK_MASTER_URL", "local[*]"))
             .appName(app_name)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------------------
# 1. Lecture des CSV, groupés par en-tête identique
# ---------------------------------------------------------------------------
def group_files_by_header(years: list[int]) -> dict[tuple[str, ...], list[str]]:
    """Lit seulement la 1re ligne de chaque fichier dans MinIO et groupe les fichiers par en-tête."""
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "urbanhub"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "urbanhub123"),
        region_name="us-east-1",
    )
    paginator = s3.get_paginator("list_objects_v2")
    keys = [obj["Key"]
            for year in years
            for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=f"{RAW_PREFIX}year={year}/")
            for obj in page.get("Contents", []) if obj["Key"].endswith(".csv")]

    def header(key: str) -> tuple[str, ...]:
        first_bytes = s3.get_object(Bucket=RAW_BUCKET, Key=key, Range="bytes=0-8191")["Body"].read()
        first_line = first_bytes.decode("utf-8", errors="replace").split("\n")[0].strip()
        return tuple(col.strip('"') for col in first_line.split(","))

    with ThreadPoolExecutor(max_workers=16) as pool:
        headers = list(pool.map(header, keys))

    groups: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for key, cols in zip(keys, headers):
        groups[cols].append(f"s3a://{RAW_BUCKET}/{key}")
    log.info("%d fichiers bruts, %d en-têtes différents", len(keys), len(groups))
    return groups


def read_raw(spark: SparkSession, years: list[int]) -> DataFrame:
    parts = []
    for cols, paths in group_files_by_header(years).items():
        df = spark.read.option("header", True).option("inferSchema", False).csv(paths)
        df = df.select([F.col(c) if c in cols else F.lit(None).cast("string").alias(c)
                        for c in NOAA_COLUMNS])
        parts.append(df)
    return reduce(DataFrame.unionByName, parts)


# ---------------------------------------------------------------------------
# 2-5. Décodage des champs NOAA
# ---------------------------------------------------------------------------
def field(col: str, i: int):
    """i-ème morceau d'un champ NOAA "a,b,c" (0 = premier)."""
    return F.split(F.col(col), ",").getItem(i)


def decode(col: str, value_idx: int, quality_idx: int, missing: str, scale: float):
    """Valeur numérique d'un champ NOAA, ou null si manquante (sentinelle) ou de mauvaise qualité."""
    value, quality = field(col, value_idx), field(col, quality_idx)
    bad = value.isNull() | (value == missing) | quality.isin(BAD_QUALITY)
    return F.when(bad, F.lit(None)).otherwise(value.cast("double") / scale)


def precipitation_rate():
    """Précipitations en mm/h, à partir du champ AA (1, 2 ou 3) couvrant la plus courte durée.

    AA = "pp,dddd,c,q" : pp = durée en heures, dddd = cumul en mm x10, q = qualité.
    """
    candidates = []
    for col in ("AA1", "AA2", "AA3"):
        period = field(col, 0).cast("int")
        depth = field(col, 1)
        quality = field(col, 3)
        ok = (period.between(1, 6) & (depth != "9999") & ~quality.isin(BAD_QUALITY))
        candidates.append(F.when(ok, F.struct(period.alias("p"),
                                              (depth.cast("double") / 10 / period).alias("rate"))))
    # array_min compare les structs sur leur 1er champ : la plus courte durée gagne
    return F.array_min(F.array_compact(F.array(*candidates)))["rate"]


def decode_all(raw: DataFrame) -> DataFrame:
    df = (raw
          .filter(~F.trim(F.col("REPORT_TYPE")).isin("SOD", "SOM"))   # résumés jour/mois
          .select(
              F.col("STATION").alias("station_id"),
              F.to_timestamp("DATE", "yyyy-MM-dd'T'HH:mm:ss").alias("timestamp"),
              F.col("LATITUDE").cast("double").alias("latitude"),
              F.col("LONGITUDE").cast("double").alias("longitude"),
              F.col("ELEVATION").cast("double").alias("elevation"),
              F.col("NAME").alias("station_name"),
              decode("TMP", 0, 1, "+9999", 10).alias("temperature"),
              decode("DEW", 0, 1, "+9999", 10).alias("dew_point"),
              decode("WND", 3, 4, "9999", 10).alias("wind_speed"),
              decode("WND", 0, 1, "999", 1).alias("wind_direction"),
              decode("SLP", 0, 1, "99999", 10).alias("pressure"),
              decode("VIS", 0, 1, "999999", 1).alias("visibility"),
              precipitation_rate().alias("precipitation"),
          )
          .filter(F.col("timestamp").isNotNull()))

    # Valeurs impossibles -> null
    for col, (low, high) in VALID_RANGES.items():
        df = df.withColumn(col, F.when(F.col(col).between(low, high), F.col(col)))

    # Coordonnées 0,0 (station mal renseignée, au milieu de l'océan) -> null
    no_position = (F.col("latitude") == 0) & (F.col("longitude") == 0)
    for col in ("latitude", "longitude"):
        df = df.withColumn(col, F.when(~no_position, F.col(col)))
    return df


# ---------------------------------------------------------------------------
# 6. Dédoublonnage : une ligne par station et par instant
# ---------------------------------------------------------------------------
def deduplicate(df: DataFrame) -> DataFrame:
    measures = ["temperature", "dew_point", "wind_speed", "pressure", "visibility", "precipitation"]
    return (df.groupBy("station_id", "timestamp")
              .agg(F.first("latitude", ignorenulls=True).alias("latitude"),
                   F.first("longitude", ignorenulls=True).alias("longitude"),
                   F.first("elevation", ignorenulls=True).alias("elevation"),
                   F.first("station_name", ignorenulls=True).alias("station_name"),
                   F.first("wind_direction", ignorenulls=True).alias("wind_direction"),
                   *[F.avg(m).alias(m) for m in measures]))


# ---------------------------------------------------------------------------
# 7. Colonnes ajoutées
# ---------------------------------------------------------------------------
def add_columns(spark: SparkSession, df: DataFrame) -> DataFrame:
    # Ville : calculée une fois par station (~200 stations), puis jointure
    stations = (df.groupBy("station_id")
                  .agg(F.first("latitude", ignorenulls=True).alias("lat"),
                       F.first("longitude", ignorenulls=True).alias("lon"))
                  .collect())
    city_map = spark.createDataFrame(
        [(r["station_id"], assign_city(r["lat"], r["lon"])) for r in stations],
        "station_id string, city string")
    log.info("%d stations, dont %d rattachées à une ville du référentiel",
             len(stations), sum(1 for r in city_map.collect() if r["city"]))

    # Humidité relative (formule de Magnus), à partir de la température et du point de rosée
    t, td = F.col("temperature"), F.col("dew_point")
    rh = 100 * F.exp(17.625 * td / (243.04 + td)) / F.exp(17.625 * t / (243.04 + t))

    return (df.join(F.broadcast(city_map), "station_id", "left")
              # least() ignore les null : on teste rh avant, sinon on obtiendrait 100 au lieu de null
              .withColumn("relative_humidity", F.when(rh.isNotNull(), F.least(F.lit(100.0), F.round(rh, 1))))
              .withColumn("source", F.lit("noaa"))
              .withColumn("ingestion_time", F.current_timestamp())
              .withColumn("year", F.year("timestamp"))
              .withColumn("month", F.month("timestamp")))


COLUMNS_ORDER = ["station_id", "timestamp", "temperature", "wind_speed", "wind_direction",
                 "pressure", "precipitation", "visibility", "latitude", "longitude", "city",
                 "source", "ingestion_time", "station_name", "elevation", "dew_point",
                 "relative_humidity", "year", "month"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--years", type=int, nargs="+", default=DEFAULT_YEARS)
    years = parser.parse_args().years

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    spark = get_spark("noaa-clean")

    log.info("Lecture des fichiers bruts (années %s)...", years)
    raw = read_raw(spark, years)
    clean = add_columns(spark, deduplicate(decode_all(raw))).select(COLUMNS_ORDER)

    log.info("Écriture de %s (quelques minutes)...", OUTPUT)
    (clean.repartition("year", "month")
          .write.mode("overwrite")
          .partitionBy("year", "month")
          .parquet(OUTPUT))

    result = spark.read.parquet(OUTPUT)
    log.info("Terminé : %d lignes, %d stations, période %s -> %s",
             result.count(), result.select("station_id").distinct().count(),
             *result.agg(F.min("timestamp"), F.max("timestamp")).first())
    spark.stop()


if __name__ == "__main__":
    main()
