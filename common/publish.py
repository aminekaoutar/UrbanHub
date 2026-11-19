"""Écriture d'une table curated vers PostgreSQL (couche serving lue par Superset). MinIO reste la source de vérité."""
from pyspark.sql import DataFrame

from common import config


def jdbc_url() -> str:
    return f"jdbc:postgresql://{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DB}"


def publish(df: DataFrame, table: str, mode: str = "overwrite") -> bool:
    """Copie `df` dans la table PostgreSQL `table` (ex. mobility_city_hourly). Renvoie False si PostgreSQL est injoignable."""
    try:
        (df.write.format("jdbc").mode(mode)
         .option("url", jdbc_url()).option("dbtable", table)
         .option("user", config.POSTGRES_USER).option("password", config.POSTGRES_PASSWORD)
         .option("driver", "org.postgresql.Driver")
         .option("truncate", "true")          # overwrite garde la structure et les droits de la table
         .save())
        return True
    except Exception as e:  # noqa: BLE001 - l'infra peut ne pas être lancée : on ne bloque pas le pipeline
        print(f"[publish] {table} non publiée dans PostgreSQL ({type(e).__name__}: {str(e).splitlines()[0][:200]})")
        return False
