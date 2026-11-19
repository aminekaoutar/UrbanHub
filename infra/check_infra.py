"""Vérifie que l'infrastructure UrbanHub fonctionne de bout en bout.

À lancer DANS le conteneur jupyter (depuis la racine du projet, sur le PC) :
    docker compose -f infra/docker-compose.yml --env-file .env exec jupyter python3 infra/check_infra.py

Le test :
  1. démarre une session Spark sur le cluster (spark-master + spark-worker) ;
  2. écrit un petit Parquet dans MinIO (bucket clean) puis le relit ;
  3. copie ces lignes dans PostgreSQL (base urbanhub) via JDBC ;
  4. supprime ensuite les données de test.
"""
import os

import boto3
import psycopg2
from pyspark.sql import SparkSession

TEST_PATH = "s3a://clean/_check_infra/"
TEST_TABLE = "_check_infra"


def main() -> None:
    spark = (
        SparkSession.builder.master(os.environ["SPARK_MASTER_URL"])
        .appName("urbanhub-check-infra")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # 1-2. Spark -> MinIO -> Spark
    df = spark.createDataFrame([("paris", 12.5), ("lyon", 9.1)], ["city", "temperature"])
    df.write.mode("overwrite").parquet(TEST_PATH)
    nb = spark.read.parquet(TEST_PATH).count()
    assert nb == 2, f"attendu 2 lignes, lu {nb}"
    print("[OK] Spark écrit et relit un Parquet dans MinIO")

    # 3. Spark -> PostgreSQL
    pg = {k: os.environ[f"POSTGRES_{k.upper()}"] for k in ("host", "db", "user", "password")}
    url = f"jdbc:postgresql://{pg['host']}:5432/{pg['db']}"
    df.write.mode("overwrite").jdbc(
        url, TEST_TABLE,
        properties={"user": pg["user"], "password": pg["password"], "driver": "org.postgresql.Driver"},
    )
    print("[OK] Spark écrit une table dans PostgreSQL")
    spark.stop()

    # 4. Nettoyage
    with psycopg2.connect(host=pg["host"], dbname=pg["db"], user=pg["user"], password=pg["password"]) as conn:
        conn.cursor().execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")
    s3 = boto3.resource(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )
    s3.Bucket("clean").objects.filter(Prefix="_check_infra/").delete()
    print("[OK] Données de test supprimées")
    print("\nInfrastructure UrbanHub opérationnelle.")


if __name__ == "__main__":
    main()
