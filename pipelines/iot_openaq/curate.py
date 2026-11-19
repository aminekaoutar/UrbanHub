"""Développeur 3 · clean → curated : pollution_city_hourly (une ligne = une ville × une heure).

Colonnes du contrat : city, timestamp, pm25, pm10, no2, o3, co, nb_sensors
  - chaque polluant = moyenne des capteurs de la ville sur l'heure (µg/m³), null si aucun capteur
  - nb_sensors = nombre de capteurs distincts ayant donné au moins une valeur dans l'heure

Écrit dans curated/openaq/pollution_city_hourly (Parquet, partitions year/month)
puis copié dans PostgreSQL (table pollution_city_hourly) si la base est lancée.

    python -m pipelines.iot_openaq.curate
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import config
from common.publish import publish
from common.schemas import POLLUTANTS, POLLUTION_CITY_HOURLY
from common.spark import get_spark

CLEAN = "openaq/measurements"
CURATED = "openaq/pollution_city_hourly"
TABLE = "pollution_city_hourly"


def city_hourly(clean: DataFrame) -> DataFrame:
    df = (
        clean.filter(F.col("city").isNotNull() & F.col("value").isNotNull())
        .withColumn("timestamp", F.date_trunc("hour", "timestamp"))
    )
    # une colonne par polluant (liste fixe : la colonne existe même si aucun capteur ne mesure ce polluant)
    means = df.groupBy("city", "timestamp").pivot("pollutant", list(POLLUTANTS)).agg(F.round(F.avg("value"), 2))
    counts = df.groupBy("city", "timestamp").agg(F.countDistinct("sensor_id").alias("nb_sensors"))
    out = means.join(counts, ["city", "timestamp"])
    return out.select(*[F.col(f.name).cast(f.dataType) for f in POLLUTION_CITY_HOURLY.fields])


def _write(df: DataFrame) -> str:
    out = config.lake_uri("curated", CURATED)
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
    spark = get_spark("openaq-curate")
    clean = spark.read.parquet(config.lake_uri("clean", CLEAN))
    city = city_hourly(clean).cache()
    print(f"curated/{CURATED} : {city.count():,} lignes -> {_write(city)}")
    if publish(city, TABLE):
        print(f"PostgreSQL : table {TABLE} publiée")
    city.groupBy("city").agg(
        F.count("*").alias("heures"),
        *[F.round(F.avg(p), 1).alias(p) for p in POLLUTANTS],
        F.round(F.avg("nb_sensors"), 1).alias("nb_sensors_moy"),
        F.min("timestamp").alias("debut"),
        F.max("timestamp").alias("fin"),
    ).orderBy("city").show(truncate=False)


if __name__ == "__main__":
    run()
