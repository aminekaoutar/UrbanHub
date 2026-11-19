"""get_spark() : session Spark configurée pour lire / écrire le Data Lake (s3a:// MinIO) et publier vers PostgreSQL."""
import os
import sys

from pyspark.sql import SparkSession

from common import config

HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"        # même version que le Hadoop embarqué dans Spark 3.5
POSTGRES_JDBC = "org.postgresql:postgresql:42.7.4"


def get_spark(app_name: str = "urbanhub") -> SparkSession:
    # les workers Python doivent utiliser le même Python que le driver (sinon PYTHON_VERSION_MISMATCH)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # l'image Docker (infra/spark/Dockerfile) contient déjà les connecteurs : on ne les télécharge que hors Docker
    in_image = os.path.exists("/opt/spark/jars/hadoop-aws-3.3.4.jar")
    packages = [] if in_image else [POSTGRES_JDBC] + ([HADOOP_AWS] if config.MINIO_ENABLED else [])
    b = (SparkSession.builder.appName(app_name).master(config.SPARK_MASTER))
    if packages:
        b = b.config("spark.jars.packages", ",".join(packages))
    b = (b
         .config("spark.sql.session.timeZone", "UTC")                       # contrat : tous les timestamps en UTC
         # la JVM aussi en UTC, sinon le JDBC décale les heures vers PostgreSQL du fuseau de la machine (+1 h à Paris)
         .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
         .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")     # réécrit seulement les partitions touchées
         .config("spark.sql.shuffle.partitions", "16")
         .config("spark.driver.memory", "4g"))
    if config.MINIO_ENABLED:
        b = (b.config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT)
             .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY)
             .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config.MINIO_ENDPOINT.startswith("https")).lower())
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
