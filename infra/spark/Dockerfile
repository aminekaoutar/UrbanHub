# Image Spark d'UrbanHub : utilisée par spark-master, spark-worker et jupyter.
# Une seule image pour tous => même version de Python partout (obligatoire pour PySpark).
FROM apache/spark:3.5.9-python3

USER root

# Connecteurs Java :
#  - hadoop-aws + aws-java-sdk-bundle : permettent à Spark de lire/écrire dans MinIO (s3a://)
#  - postgresql : permet à Spark d'écrire les tables curated dans PostgreSQL (JDBC)
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/

# Bibliothèques Python utilisées dans les jobs et les notebooks
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Réglages Spark par défaut (connexion à MinIO)
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# PySpark est fourni par Spark lui-même (pas besoin de "pip install pyspark")
ENV PYTHONPATH=/app:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3

# Dossier personnel pour l'utilisateur "spark" (Jupyter en a besoin)
RUN mkdir -p /home/spark && chown spark:spark /home/spark
ENV HOME=/home/spark

USER spark
