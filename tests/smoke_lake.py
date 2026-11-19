"""Jalon 1 (Développeur 2) : vérifie que Spark écrit puis relit un Parquet dans MinIO (s3a://) et, si PostgreSQL
est lancé, qu'une table est publiée.  Prérequis : infra lancée et MINIO_ENABLED=true dans .env.

    python -m tests.smoke_lake
"""
from datetime import datetime, timezone

from common import config, storage
from common.publish import publish
from common.spark import get_spark

KEY = "_smoke/spark_roundtrip"


def main():
    assert config.MINIO_ENABLED, "MINIO_ENABLED=true est requis pour ce test (voir .env)"
    storage.ensure_bucket(config.BUCKET_CLEAN)
    spark = get_spark("smoke-lake")
    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    df = spark.createDataFrame([("paris", now, 1.5), ("lyon", now, 2.5)], "city string, timestamp timestamp, value double")
    path = config.lake_uri("clean", KEY)
    df.write.mode("overwrite").parquet(path)
    back = spark.read.parquet(path)
    assert back.count() == 2 and dict(back.dtypes)["timestamp"] == "timestamp"
    print(f"OK  MinIO : Parquet écrit puis relu via Spark ({path})")
    for k in storage.list_keys("clean", KEY):
        storage.s3().delete_object(Bucket=config.BUCKET_CLEAN, Key=k)
    print("OK  PostgreSQL : table _smoke publiée" if publish(df, "_smoke") else "--  PostgreSQL non joignable (ignoré)")


if __name__ == "__main__":
    main()
