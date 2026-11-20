"""Profil de chaque station vélo, pour l'onglet Mobilité du dashboard Superset.

La table curated/citybikes/station_hourly (Dev 2, ~29 M lignes) est trop lourde pour PostgreSQL.
Ce job la résume en une ligne par station (~2 250 lignes) sur les 12 derniers mois de données,
et publie le résultat dans PostgreSQL (table station_profile) :

    station_id, station_name, city, latitude, longitude
    capacity            capacité moyenne (vélos + places libres)
    usage_per_day       mouvements de vélos par jour (somme de usage_events / nombre de jours observés)
    occupancy_rate      taux de remplissage moyen (0-1)
    pct_empty, pct_full part des heures où la station est presque vide (< 10 %) / presque pleine (> 90 %)
    occ_night           remplissage moyen la nuit (2 h-5 h, heure de Paris)
    occ_day             remplissage moyen en journée de semaine (10 h-16 h, heure de Paris)
    imbalance           occ_day - occ_night : < 0 la station se vide en journée (quartier résidentiel),
                        > 0 elle se remplit (quartier de bureaux / gares)
    nb_hours, period_start, period_end, computed_at

Lecture seule du Data Lake : ne modifie aucune donnée de Dev 2. Relancé par pipelines/run_pipeline.py,
mais recalculé au plus une fois toutes les 20 h (le profil d'une station bouge peu d'une heure à l'autre).

    python3 -m dashboard.superset.station_profile           # recalcule si le dernier calcul a plus de 20 h
    python3 -m dashboard.superset.station_profile --force   # recalcule maintenant
"""
import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

from common import config
from common.publish import publish
from common.spark import get_spark

SOURCE = "citybikes/station_hourly"
TABLE = "station_profile"
DAYS = 365
MIN_AGE_HOURS = 20


def last_computed() -> datetime | None:
    """Date du dernier calcul publié (None si la table n'existe pas encore)."""
    try:
        import psycopg2
        with psycopg2.connect(host=config.POSTGRES_HOST, port=config.POSTGRES_PORT, dbname=config.POSTGRES_DB,
                              user=config.POSTGRES_USER, password=config.POSTGRES_PASSWORD) as conn:
            with conn.cursor() as cur:
                cur.execute(f"select max(computed_at) from {TABLE}")
                value = cur.fetchone()[0]
                return value.replace(tzinfo=timezone.utc) if value else None
    except Exception:  # noqa: BLE001 - table absente ou PostgreSQL injoignable : on recalcule
        return None


def run(force: bool = False) -> None:
    previous = last_computed()
    if not force and previous and datetime.now(timezone.utc) - previous < timedelta(hours=MIN_AGE_HOURS):
        print(f"{TABLE} : calculé le {previous:%d/%m %H:%M} UTC, rien à faire (--force pour recalculer)")
        return

    spark = get_spark("station-profile")
    st = spark.read.parquet(config.lake_uri("curated", SOURCE))
    end = st.agg(F.max("timestamp")).first()[0]
    start = end - timedelta(days=DAYS)
    st = (st.filter(F.col("year") * 100 + F.col("month") >= start.year * 100 + start.month)  # lecture des seuls mois utiles
            .filter((F.col("timestamp") >= F.lit(start)) & F.col("city").isNotNull()))

    local = F.from_utc_timestamp("timestamp", "Europe/Paris")
    hour, weekday = F.hour(local), F.dayofweek(local)          # dayofweek : 1 = dimanche ... 7 = samedi
    night = hour.between(2, 4)
    workday_day = hour.between(10, 15) & weekday.between(2, 6)

    profile = (st.groupBy("station_id").agg(
                   F.last("station_name", ignorenulls=True).alias("station_name"),
                   F.last("city", ignorenulls=True).alias("city"),
                   F.avg("latitude").alias("latitude"), F.avg("longitude").alias("longitude"),
                   F.round(F.avg("capacity"), 1).alias("capacity"),
                   (F.sum("usage_events") / F.countDistinct(F.to_date(local))).alias("usage_per_day"),
                   F.avg("occupancy_rate").alias("occupancy_rate"),
                   F.avg(F.col("is_empty").cast("double")).alias("pct_empty"),
                   F.avg(F.col("is_full").cast("double")).alias("pct_full"),
                   F.avg(F.when(night, F.col("occupancy_rate"))).alias("occ_night"),
                   F.avg(F.when(workday_day, F.col("occupancy_rate"))).alias("occ_day"),
                   F.count("*").alias("nb_hours"),
                   F.min("timestamp").alias("period_start"), F.max("timestamp").alias("period_end"))
               .withColumn("usage_per_day", F.round("usage_per_day", 1))
               .withColumn("imbalance", F.col("occ_day") - F.col("occ_night"))
               .withColumn("computed_at", F.lit(datetime.now(timezone.utc).replace(tzinfo=None))))

    n = profile.cache().count()
    if publish(profile, TABLE):
        print(f"PostgreSQL : table {TABLE} publiée ({n} stations, {start:%d/%m/%Y} -> {end:%d/%m/%Y})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Profil des stations vélo pour Superset")
    ap.add_argument("--force", action="store_true", help="recalculer même si le dernier calcul est récent")
    run(ap.parse_args().force)
