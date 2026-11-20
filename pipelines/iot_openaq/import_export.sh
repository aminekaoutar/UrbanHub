#!/usr/bin/env bash
# Développeur 3 · Import des données OpenAQ dans MinIO (à lancer chez la personne qui a MinIO).
#
# Usage, depuis la racine du repo, dans le terminal du conteneur jupyter (dossier /app) :
#     bash pipelines/iot_openaq/import_export.sh chemin/vers/openaq_export.zip
#
# Étapes :
#   1. dézippe l'export dans data/ (raw/openaq, clean/openaq, curated/openaq)
#   2. envoie chaque couche dans son bucket MinIO
#   3. republie pollution_city_hourly dans PostgreSQL
#   4. reconstruit la table croisée urban_city_hourly
set -euo pipefail

ZIP="${1:-data/openaq_export.zip}"
[ -f "$ZIP" ] || { echo "Fichier introuvable : $ZIP"; exit 1; }

echo "1/4 Décompression de $ZIP dans data/"
mkdir -p data
unzip -oq "$ZIP" -d data

echo "2/4 Envoi vers MinIO"
for layer in raw clean curated; do
    python -m common.storage --layer "$layer" --prefix openaq/
done

echo "3/4 Publication de pollution_city_hourly dans PostgreSQL"
python -m pipelines.iot_openaq.curate

echo "4/4 Construction de urban_city_hourly"
python -m pipelines.iot_openaq.urban

echo "Terminé : vérifie les buckets sur http://localhost:9001"
