"""Développeur 3 · Table croisée urban_city_hourly (une ligne = une ville × une heure).

Jointure des trois tables curated sur la clé du contrat (city, timestamp) :

    curated/noaa/weather_city_hourly       (Dev 1)  ─┐
    curated/citybikes/mobility_city_hourly (Dev 2)  ─┼─► curated/cross/urban_city_hourly + PostgreSQL
    curated/openaq/pollution_city_hourly   (Dev 3)  ─┘

Choix :
  - jointure complète (full outer) : une heure est gardée dès qu'une des trois sources l'a ;
    les vélos ne couvrent que la période de collecte, la météo et la pollution 2024 → 2026.
  - nb_stations existe chez Dev 1 et Dev 2 : renommées nb_weather_stations et nb_bike_stations.
  - colonne en plus du contrat : weather_source (noaa | openmeteo), la colonne `source` de Dev 1.
  - hour (0-23) et day_of_week (1 = lundi ... 7 = dimanche) sont calculés en UTC, comme timestamp.
  - si une table n'existe pas encore, ses colonnes restent vides (le job tourne quand même).

    python -m pipelines.iot_openaq.urban
"""
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException

from common import config
from common.cities import CITIES, START_YEAR
from common.publish import publish
from common.schemas import (
    MOBILITY_CITY_HOURLY,
    POLLUTANTS,
    POLLUTION_CITY_HOURLY,
    URBAN_CITY_HOURLY,
    WEATHER_CITY_HOURLY,
)
from common.spark import get_spark

WEATHER = "noaa/weather_city_hourly"
MOBILITY = "citybikes/mobility_city_hourly"
POLLUTION = "openaq/pollution_city_hourly"
URBAN = "cross/urban_city_hourly"
TABLE = "urban_city_hourly"

KEY = ["city", "timestamp"]
WEATHER_METRICS = ["temperature", "wind_speed", "wind_direction", "pressure", "precipitation", "visibility"]
MOBILITY_METRICS = ["bikes_available", "free_slots", "capacity", "occupancy_rate", "usage_events"]
EXTRA_COLUMNS = ["weather_source"]

# Table météo de Dev 1 : contrat + colonne source
WEATHER_WITH_SOURCE = StructType(WEATHER_CITY_HOURLY.fields + [StructField("source", StringType())])


def _hour_key(df: DataFrame) -> DataFrame:
    """Clé de jointure propre : ville du contrat, timestamp tronqué à l'heure."""
    return df.withColumn("timestamp", F.date_trunc("hour", "timestamp"))


def build(weather: DataFrame, mobility: DataFrame, pollution: DataFrame) -> DataFrame:
    w = _hour_key(weather).select(
        *KEY,
        *WEATHER_METRICS,
        F.col("nb_stations").alias("nb_weather_stations"),
        (F.col("source") if "source" in weather.columns else F.lit(None).cast("string")).alias("weather_source"),
    )
    m = _hour_key(mobility).select(
        *KEY,
        *MOBILITY_METRICS,
        F.col("nb_stations").alias("nb_bike_stations"),
        "nb_empty_stations",
        "nb_full_stations",
    )
    p = _hour_key(pollution).select(*KEY, *POLLUTANTS, "nb_sensors")

    df = (
        w.join(m, KEY, "full")
        .join(p, KEY, "full")
        .filter(F.col("city").isin(*CITIES))
        .filter(F.col("timestamp") >= F.lit(datetime(START_YEAR, 1, 1, tzinfo=timezone.utc)))
        .withColumn("hour", F.hour("timestamp"))
        # Spark : dayofweek 1 = dimanche ; contrat : 1 = lundi ... 7 = dimanche
        .withColumn("day_of_week", (F.dayofweek("timestamp") + 5) % 7 + 1)
        .withColumn("is_weekend", F.col("day_of_week") >= 6)
    )
    contract = [F.col(f.name).cast(f.dataType) for f in URBAN_CITY_HOURLY.fields]
    return df.select(*contract, *EXTRA_COLUMNS)


def read_or_empty(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    uri = config.lake_uri("curated", path)
    try:
        return spark.read.parquet(uri)
    except AnalysisException:
        print(f"[urban] {path} introuvable ({uri}) : colonnes laissées vides")
        return spark.createDataFrame([], schema)


def _write(df: DataFrame) -> str:
    out = config.lake_uri("curated", URBAN)
    (
        df.withColumn("year", F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .repartition("year", "month")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(out)
    )
    return out


def run():
    spark = get_spark("urban-city-hourly")
    df = build(
        read_or_empty(spark, WEATHER, WEATHER_WITH_SOURCE),
        read_or_empty(spark, MOBILITY, MOBILITY_CITY_HOURLY),
        read_or_empty(spark, POLLUTION, POLLUTION_CITY_HOURLY),
    ).cache()
    print(f"curated/{URBAN} : {df.count():,} lignes -> {_write(df)}")
    if publish(df, TABLE):
        print(f"PostgreSQL : table {TABLE} publiée")
    # Combien d'heures ont chaque source, par ville
    df.groupBy("city").agg(
        F.count("*").alias("heures"),
        F.count("temperature").alias("avec_meteo"),
        F.count("bikes_available").alias("avec_velos"),
        F.count("nb_sensors").alias("avec_pollution"),
        F.sum(
            (F.col("temperature").isNotNull() & F.col("bikes_available").isNotNull() & F.col("nb_sensors").isNotNull())
            .cast("int")
        ).alias("avec_les_3"),
        F.min("timestamp").alias("debut"),
        F.max("timestamp").alias("fin"),
    ).orderBy("city").show(truncate=False)


if __name__ == "__main__":
    run()
