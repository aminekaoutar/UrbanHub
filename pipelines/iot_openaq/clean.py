"""Développeur 3 · raw → clean : clean/openaq/measurements (Parquet, partitions year/month/day).

Une ligne = la moyenne horaire d'un polluant mesurée par un capteur. Colonnes du contrat :
sensor_id, timestamp, latitude, longitude, pollutant, value, unit, city, source, ingestion_time

Étapes :
  1. raw/openaq/locations     → table des capteurs (position, polluant, unité), stations officielles seulement
  2. raw/openaq/sensor_hours  → une ligne par capteur et par heure
  3. jointure sur sensor_id, nettoyage, suppression des doublons, écriture Parquet

    python -m pipelines.iot_openaq.clean
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import config
from common.cities import assign_city_udf
from common.schemas import MEASUREMENTS, POLLUTANT_UNIT, POLLUTANTS
from common.spark import get_spark

RAW_LOCATIONS = "openaq/locations"
RAW_HOURS = "openaq/sensor_hours"
CLEAN = "openaq/measurements"


def read_json(spark: SparkSession, raw_path: str) -> DataFrame:
    # un fichier = une réponse API complète (un objet JSON) ; Spark décompresse le .gz tout seul
    return (
        spark.read.option("multiLine", True)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.json.gz")
        .json(config.lake_uri("raw", raw_path))
        .withColumn("_file", F.input_file_name())
    )


def sensors_table(locations: DataFrame) -> DataFrame:
    """Une ligne par capteur : sensor_id, latitude, longitude, pollutant, unit, city.

    On ne garde que les stations officielles et fixes (décision du 2026-11-19).
    Si un capteur apparaît dans plusieurs fichiers, on garde la description la plus récente.
    """
    loc = locations.select("_file", F.explode("results").alias("l")).filter(
        F.col("l.isMonitor") & ~F.coalesce(F.col("l.isMobile"), F.lit(False))
    )
    s = loc.select(
        "_file",
        F.col("l.coordinates.latitude").cast("double").alias("latitude"),
        F.col("l.coordinates.longitude").cast("double").alias("longitude"),
        F.explode("l.sensors").alias("s"),
    ).select(
        "_file",
        F.col("s.id").cast("string").alias("sensor_id"),
        "latitude",
        "longitude",
        F.col("s.parameter.name").alias("pollutant"),
        F.col("s.parameter.units").alias("unit"),
    )
    latest = Window.partitionBy("sensor_id").orderBy(F.col("_file").desc())
    return (
        s.filter(F.col("pollutant").isin(*POLLUTANTS))
        .withColumn("_rank", F.row_number().over(latest))
        .filter("_rank = 1")
        .drop("_rank", "_file")
        .withColumn("city", assign_city_udf()("latitude", "longitude"))
    )


def hours_table(hours: DataFrame) -> DataFrame:
    """Une ligne par capteur et par heure : sensor_id, timestamp, value, unit, ingestion_time."""
    return hours.select(
        # le collecteur écrit l'id du capteur dans le nom du fichier : sensor_<id>_<début>_<fin>.json.gz
        F.regexp_extract("_file", r"sensor_(\d+)_", 1).alias("sensor_id"),
        F.to_timestamp("ingestion_time").alias("ingestion_time"),
        F.explode("results").alias("r"),
    ).select(
        "sensor_id",
        # début de l'heure mesurée (ex. 07:00 pour la moyenne 07:00 → 08:00), en UTC
        F.to_timestamp("r.period.datetimeFrom.utc").alias("timestamp"),
        F.col("r.value").cast("double").alias("value"),
        F.col("r.parameter.units").alias("hour_unit"),
        "ingestion_time",
    )


def transform(locations: DataFrame, hours: DataFrame) -> DataFrame:
    df = hours_table(hours).join(sensors_table(locations), "sensor_id")  # inner : capteurs officiels seulement
    df = (
        df.filter(F.col("timestamp").isNotNull())
        # contrat : tout en µg/m³ (toutes les stations retenues le sont déjà) ; une autre unité est écartée
        .filter(F.coalesce("hour_unit", "unit") == POLLUTANT_UNIT)
        # une concentration négative est une erreur de capteur → null (pas de sentinelle)
        .withColumn("value", F.when(F.col("value") >= 0, F.col("value")))
        .withColumn("unit", F.lit(POLLUTANT_UNIT))
        .withColumn("source", F.lit("openaq"))
    )
    # le mode live réécrit le jour en cours : on garde la version téléchargée le plus récemment
    newest = Window.partitionBy("sensor_id", "timestamp").orderBy(F.col("ingestion_time").desc())
    df = df.withColumn("_rank", F.row_number().over(newest)).filter("_rank = 1")
    cols = [F.col(f.name).cast(f.dataType) for f in MEASUREMENTS.fields]
    return df.select(
        *cols,
        F.year("timestamp").alias("year"),
        F.month("timestamp").alias("month"),
        F.dayofmonth("timestamp").alias("day"),
    )


def run():
    spark = get_spark("openaq-clean")
    df = transform(read_json(spark, RAW_LOCATIONS), read_json(spark, RAW_HOURS))
    out = config.lake_uri("clean", CLEAN)
    df.repartition("year", "month").write.mode("overwrite").partitionBy("year", "month", "day").parquet(out)
    result = spark.read.parquet(out)
    print(f"clean/{CLEAN} : {result.count():,} lignes -> {out}")
    result.groupBy("city", "pollutant").agg(
        F.countDistinct("sensor_id").alias("capteurs"),
        F.count("*").alias("lignes"),
        F.round(F.avg("value"), 1).alias("moyenne"),
        F.min("timestamp").alias("debut"),
        F.max("timestamp").alias("fin"),
    ).orderBy("city", "pollutant").show(50, truncate=False)


if __name__ == "__main__":
    run()
