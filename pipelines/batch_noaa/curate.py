"""Agrégation météo : clean (mesures par station) -> curated (moyenne par ville et par heure).

Entrées : s3a://clean/noaa/weather_hourly/               (NOAA, jusqu'au 24/08/2025)
          s3a://clean/openmeteo/weather_city_hourly/     (Open-Meteo, après ; voir openmeteo.py)
Sorties : s3a://curated/noaa/weather_city_hourly/   (Parquet, partitions year/month)
          table PostgreSQL "weather_city_hourly"   (pour Superset)

C'est la table que Dev 1 "livre" aux autres (contrat de données, section 4.3) :
    city, timestamp, temperature, wind_speed, wind_direction, pressure,
    precipitation, visibility, nb_stations
avec timestamp tronqué à l'heure (UTC) et seulement les villes du référentiel
(décision d'équipe : paris, lyon, lille).

Calcul en deux temps, pour qu'une station qui envoie beaucoup de bulletins ne pèse
pas plus qu'une autre :
  1. moyenne par station et par heure ;
  2. moyenne de ces stations, par ville et par heure.
Après la dernière heure NOAA, on ajoute les lignes Open-Meteo (nb_stations = null).
Une colonne en plus, "source" (noaa | openmeteo), indique l'origine de chaque ligne.

La direction du vent est une moyenne d'angles (350° et 10° donnent 0°, pas 180°) :
on fait la moyenne des sinus et des cosinus.

Lancement (depuis la racine du projet, sur le PC) :
    docker compose -f infra/docker-compose.yml --env-file .env exec jupyter python3 -m pipelines.batch_noaa.curate
"""
from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

INPUT = "s3a://clean/noaa/weather_hourly/"
INPUT_OPENMETEO = "s3a://clean/openmeteo/weather_city_hourly/"
OUTPUT = "s3a://curated/noaa/weather_city_hourly/"
PG_TABLE = "weather_city_hourly"

MEASURES = ["temperature", "wind_speed", "pressure", "precipitation", "visibility"]
CONTRACT_COLUMNS = ["city", "timestamp", "temperature", "wind_speed", "wind_direction",
                    "pressure", "precipitation", "visibility", "nb_stations"]
OUTPUT_COLUMNS = CONTRACT_COLUMNS + ["source"]

log = logging.getLogger("noaa.curate")


def get_spark(app_name: str) -> SparkSession:
    # TEMPORAIRE : à remplacer par common.spark.get_spark quand Dev 2 l'aura livré
    spark = (SparkSession.builder
             .master(os.environ.get("SPARK_MASTER_URL", "local[*]"))
             .appName(app_name)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def city_hourly(clean: DataFrame) -> DataFrame:
    rad = F.radians("wind_direction")

    # 1. Moyenne par station et par heure
    per_station = (clean
        .filter(F.col("city").isNotNull())
        .withColumn("timestamp", F.date_trunc("hour", "timestamp"))
        .groupBy("city", "station_id", "timestamp")
        .agg(*[F.avg(m).alias(m) for m in MEASURES],
             F.avg(F.sin(rad)).alias("wind_sin"),
             F.avg(F.cos(rad)).alias("wind_cos")))

    # 2. Moyenne des stations par ville et par heure
    per_city = (per_station
        .groupBy("city", "timestamp")
        .agg(*[F.round(F.avg(m), 2).alias(m) for m in MEASURES],
             F.avg("wind_sin").alias("wind_sin"),
             F.avg("wind_cos").alias("wind_cos"),
             F.countDistinct("station_id").alias("nb_stations")))

    # Angle moyen en degrés, entre 0 et 360
    direction = F.round((F.degrees(F.atan2("wind_sin", "wind_cos")) + 360) % 360, 0)
    return (per_city
        .withColumn("wind_direction", F.when(F.col("wind_sin").isNotNull(), direction))
        .withColumn("source", F.lit("noaa"))
        .select(OUTPUT_COLUMNS))


def add_openmeteo(spark: SparkSession, noaa: DataFrame) -> DataFrame:
    """Ajoute les heures Open-Meteo postérieures à la dernière heure NOAA (si openmeteo.py a tourné)."""
    try:
        om = spark.read.parquet(INPUT_OPENMETEO)
    except Exception:                                   # dossier absent : openmeteo.py pas encore lancé
        log.warning("Pas de données Open-Meteo (%s) : seules les données NOAA sont utilisées",
                    INPUT_OPENMETEO)
        return noaa
    last_noaa = noaa.agg(F.max("timestamp")).first()[0]
    om = (om.filter(F.col("timestamp") > F.lit(last_noaa))
            .select([F.col(c).cast(noaa.schema[c].dataType) for c in OUTPUT_COLUMNS]))
    log.info("Open-Meteo : %d heures ajoutées après %s", om.count(), last_noaa)
    return noaa.unionByName(om)


def publish_postgres(df: DataFrame, table: str) -> None:
    """Copie la table dans PostgreSQL pour Superset.

    TEMPORAIRE : à remplacer par common.publish quand Dev 2 l'aura livré.
    """
    url = (f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:"
           f"{os.environ.get('POSTGRES_PORT', '5432')}/{os.environ.get('POSTGRES_DB', 'urbanhub')}")
    # overwrite = la table est supprimée puis recréée (les graphiques Superset, qui la
    # désignent par son nom, continuent de fonctionner)
    (df.write.mode("overwrite")
       .option("batchsize", 10000)
       .jdbc(url, table, properties={
           "user": os.environ.get("POSTGRES_USER", "urbanhub"),
           "password": os.environ.get("POSTGRES_PASSWORD", "urbanhub123"),
           "driver": "org.postgresql.Driver",
       }))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    spark = get_spark("noaa-curate")

    log.info("Calcul de la météo moyenne par ville et par heure...")
    result = add_openmeteo(spark, city_hourly(spark.read.parquet(INPUT)))

    log.info("Écriture de %s ...", OUTPUT)
    (result.withColumn("year", F.year("timestamp"))
           .withColumn("month", F.month("timestamp"))
           .repartition("year")
           .write.mode("overwrite").partitionBy("year", "month").parquet(OUTPUT))

    curated = spark.read.parquet(OUTPUT).select(OUTPUT_COLUMNS)
    log.info("Copie dans PostgreSQL (table %s)...", PG_TABLE)
    publish_postgres(curated, PG_TABLE)

    log.info("Terminé : %d lignes (ville x heure), %d villes, période %s -> %s",
             curated.count(), curated.select("city").distinct().count(),
             *curated.agg(F.min("timestamp"), F.max("timestamp")).first())
    curated.groupBy("source").agg(F.count("*").alias("lignes"), F.min("timestamp").alias("debut"),
                                  F.max("timestamp").alias("fin")).orderBy("debut").show()
    spark.stop()


if __name__ == "__main__":
    main()
