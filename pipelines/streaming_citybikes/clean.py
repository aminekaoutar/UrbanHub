"""Développeur 2 · raw → clean : clean/citybikes/station_status (Parquet, partitions year/month/day).

Une ligne = l'état d'une station à un instant. Colonnes du contrat (section 4.2) :
station_id, station_name, timestamp, latitude, longitude, bikes_available, free_slots, city, source, ingestion_time
+ network_id, capacity, occupancy_rate, ebikes.

    python -m pipelines.streaming_citybikes.clean                 # tout l'historique raw
    python -m pipelines.streaming_citybikes.clean --day 2026-11-18  # une journée (relance horaire)
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import config
from common.spark import get_spark
from pipelines.streaming_citybikes.city import assign_city_col

RAW = "citybikes/networks"
CLEAN = "citybikes/station_status"


def read_raw(spark: SparkSession, day: str = None) -> DataFrame:
    path = config.lake_uri("raw", RAW)
    if day:
        y, m, d = day.split("-")
        path += f"/year={y}/month={m}/day={d}"
    # un fichier = une réponse API complète (un objet JSON) ; Spark décompresse le .gz tout seul
    return (spark.read.option("multiLine", True).option("recursiveFileLookup", True)
            .option("pathGlobFilter", "*.json.gz").json(path)
            .withColumn("_file", F.input_file_name()))


def transform(raw: DataFrame) -> DataFrame:
    s = raw.select(
        F.col("network.id").alias("network_id"),
        # heure de dépôt écrite dans le nom du fichier par le collecteur : <réseau>_YYYYMMDDTHHMMSSZ.json.gz
        F.to_timestamp(F.regexp_extract("_file", r"_(\d{8}T\d{6})Z\.json\.gz$", 1), "yyyyMMdd'T'HHmmss").alias("ingestion_time"),
        F.explode("network.stations").alias("st"),
    )
    df = s.select(
        "network_id", "ingestion_time",
        F.col("st.id").alias("station_id"),
        F.trim(F.col("st.name")).alias("station_name"),
        # l'API renvoie "2026-11-18T21:00:03.062850+00:00Z" : on enlève le Z en trop, puis conversion en UTC
        F.to_timestamp(F.regexp_replace("st.timestamp", r"Z$", "")).alias("timestamp"),
        F.col("st.latitude").cast("double").alias("latitude"),
        F.col("st.longitude").cast("double").alias("longitude"),
        F.col("st.free_bikes").cast("int").alias("bikes_available"),
        F.col("st.empty_slots").cast("int").alias("free_slots"),
        F.col("st.extra.ebikes").cast("int").alias("ebikes"),
    )
    df = (df
          # valeurs impossibles → null (pas de sentinelles dans clean)
          .withColumn("bikes_available", F.when(F.col("bikes_available") >= 0, F.col("bikes_available")))
          .withColumn("free_slots", F.when(F.col("free_slots") >= 0, F.col("free_slots")))
          .withColumn("ebikes", F.when(F.col("ebikes") >= 0, F.col("ebikes")))
          .withColumn("timestamp", F.coalesce("timestamp", "ingestion_time"))
          .filter(F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180))
          .withColumn("capacity", F.col("bikes_available") + F.col("free_slots"))
          .withColumn("occupancy_rate", F.when(F.col("capacity") > 0,
                                               F.round(F.col("bikes_available") / F.col("capacity"), 4)))
          .withColumn("city", assign_city_col())
          .withColumn("source", F.lit("citybikes")))
    # l'API met ses données en cache : deux appels rapprochés renvoient le même relevé → on garde le premier dépôt
    df = df.dropDuplicates(["station_id", "timestamp"])
    cols = ["station_id", "station_name", "timestamp", "latitude", "longitude", "bikes_available", "free_slots",
            "city", "source", "ingestion_time", "network_id", "capacity", "occupancy_rate", "ebikes"]
    return (df.select(*cols)
            .withColumn("year", F.year("timestamp")).withColumn("month", F.month("timestamp"))
            .withColumn("day", F.dayofmonth("timestamp")))


def run(day: str = None):
    spark = get_spark("citybikes-clean")
    df = transform(read_raw(spark, day))
    out = config.lake_uri("clean", CLEAN)
    df.repartition("year", "month", "day").write.mode("overwrite").partitionBy("year", "month", "day").parquet(out)
    n = spark.read.parquet(out).count()
    print(f"clean/{CLEAN} : {n:,} lignes au total -> {out}")
    return n


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", help="YYYY-MM-DD : ne retraiter qu'une journée")
    run(ap.parse_args().day)
