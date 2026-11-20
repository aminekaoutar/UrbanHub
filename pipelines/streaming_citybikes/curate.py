"""Développeur 2 · clean → curated.

1. curated/citybikes/station_hourly       une ligne par station et par heure (sert aux analyses Partie 2)
2. curated/citybikes/mobility_city_hourly une ligne par ville et par heure : LIVRAISON du contrat (section 4.3)
       city, timestamp, bikes_available, free_slots, capacity, occupancy_rate, usage_events,
       nb_stations, nb_empty_stations (< 10 %), nb_full_stations (> 90 %)

Mesure d'usage commune : somme des variations absolues de bikes_available entre deux relevés successifs d'une
station (un vélo pris = -1, un vélo rendu = +1). Les écarts de plus de 15 min entre deux relevés (collecteur arrêté)
ne sont pas comptés, sinon une coupure ferait croire à un pic d'usage.

Les deux tables sont écrites en Parquet (partitions year/month). mobility_city_hourly est publiée dans PostgreSQL
si celui-ci est lancé ; station_hourly seulement avec --with-stations.
Seuls les mois présents dans clean sont réécrits (mode dynamique) : les mois d'historique (history.py) restent.

    python -m pipelines.streaming_citybikes.curate                   # publie mobility_city_hourly
    python -m pipelines.streaming_citybikes.curate --with-stations   # + station_hourly (lourd)
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import config
from common.publish import publish
from common.schemas import MOBILITY_CITY_HOURLY
from common.spark import get_spark

CLEAN = "citybikes/station_status"
STATION_HOURLY = "citybikes/station_hourly"
CITY_HOURLY = "citybikes/mobility_city_hourly"
MAX_GAP_MIN = 15
EMPTY, FULL = 0.10, 0.90


def with_usage(clean: DataFrame) -> DataFrame:
    w = Window.partitionBy("station_id").orderBy("timestamp")
    prev_b, prev_t = F.lag("bikes_available").over(w), F.lag("timestamp").over(w)
    gap_min = (F.col("timestamp").cast("long") - prev_t.cast("long")) / 60
    return clean.withColumn("usage_events", F.when(gap_min <= MAX_GAP_MIN, F.abs(F.col("bikes_available") - prev_b)))


def station_hourly(clean: DataFrame) -> DataFrame:
    df = with_usage(clean).filter(F.col("city").isNotNull()).withColumn("hour", F.date_trunc("hour", "timestamp"))
    return (df.groupBy("station_id", "hour").agg(
                F.first("station_name", ignorenulls=True).alias("station_name"),
                F.first("network_id").alias("network_id"),
                F.first("city").alias("city"),
                F.first("latitude").alias("latitude"), F.first("longitude").alias("longitude"),
                F.round(F.avg("bikes_available"), 2).alias("bikes_available"),
                F.round(F.avg("free_slots"), 2).alias("free_slots"),
                F.round(F.avg("capacity"), 2).alias("capacity"),
                F.round(F.avg("occupancy_rate"), 4).alias("occupancy_rate"),
                F.coalesce(F.sum("usage_events"), F.lit(0)).cast("int").alias("usage_events"),
                F.count("*").alias("nb_readings"))
            .withColumnRenamed("hour", "timestamp")
            .withColumn("is_empty", F.col("occupancy_rate") < EMPTY)
            .withColumn("is_full", F.col("occupancy_rate") > FULL))


def city_hourly(st: DataFrame) -> DataFrame:
    return (st.groupBy("city", "timestamp").agg(
                F.round(F.sum("bikes_available"), 1).alias("bikes_available"),
                F.round(F.sum("free_slots"), 1).alias("free_slots"),
                F.round(F.sum("capacity"), 1).alias("capacity"),
                F.sum("usage_events").cast("long").alias("usage_events"),
                F.countDistinct("station_id").alias("nb_stations"),
                F.sum(F.col("is_empty").cast("int")).alias("nb_empty_stations"),
                F.sum(F.col("is_full").cast("int")).alias("nb_full_stations"))
            .withColumn("occupancy_rate", F.when(F.col("capacity") > 0, F.round(F.col("bikes_available") / F.col("capacity"), 4)))
            # colonnes et types exacts du contrat (common/schemas.py) : les sommes de moyennes sont arrondies à l'entier
            .select(*[(F.round(f.name) if f.name in ("bikes_available", "free_slots", "capacity") else F.col(f.name))
                      .cast(f.dataType).alias(f.name) for f in MOBILITY_CITY_HOURLY.fields]))


def _write(df: DataFrame, path: str):
    out = config.lake_uri("curated", path)
    (df.withColumn("year", F.year("timestamp")).withColumn("month", F.month("timestamp"))
       .repartition("year", "month").write.mode("overwrite").partitionBy("year", "month").parquet(out))
    return out


def run(with_stations: bool = False):
    spark = get_spark("citybikes-curate")
    clean = spark.read.parquet(config.lake_uri("clean", CLEAN))
    st = station_hourly(clean).cache()
    city = city_hourly(st).cache()
    print(f"curated/{STATION_HOURLY} : {st.count():,} lignes -> {_write(st, STATION_HOURLY)}")
    print(f"curated/{CITY_HOURLY} : {city.count():,} lignes -> {_write(city, CITY_HOURLY)}")
    # on publie tout le dossier curated (mois du collecteur + historique de history.py), pas seulement ce calcul :
    # publish() remplace la table PostgreSQL entière.
    # station_hourly (~29 M lignes) n'est publiée qu'à la demande (--with-stations) : trop lourde pour le passage
    # horaire de la machine de démo (8 Go) ; le notebook et le site web la lisent dans le Data Lake.
    tables = [("mobility_city_hourly", CITY_HOURLY)]
    if with_stations:
        tables.insert(0, ("station_hourly", STATION_HOURLY))
    for name, path in tables:
        df = spark.read.parquet(config.lake_uri("curated", path)).drop("year", "month")
        if publish(df, name):
            print(f"PostgreSQL : table {name} publiée")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--with-stations", action="store_true",
                    help="publier aussi station_hourly (~29 M lignes) dans PostgreSQL")
    run(ap.parse_args().with_stations)
