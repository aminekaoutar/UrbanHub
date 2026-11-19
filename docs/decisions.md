# Décisions d'équipe

Toute décision qui touche les trois développeurs (contrat de données, villes, période,
stack) est notée ici avec sa date.

## À valider à la réunion de lancement

Propositions par défaut du guide (section 8). Remplacer « à valider » par la décision prise.

| Sujet | Proposition | Statut |
|---|---|---|
| Villes | paris, lyon, marseille, toulouse, nice, nantes, strasbourg, montpellier, bordeaux, lille ; réduites à celles qui ont CityBikes + OpenAQ + une station NOAA proche | à valider |
| Période NOAA | 2022 → 24/08/2025 (fin des données NOAA) ; complément Open-Meteo de 08/2025 à aujourd'hui pour couvrir la période CityBikes / OpenAQ (partie 4) | proposé par Dev 1, à valider |
| Machine de collecte 24 h/24 | PC toujours allumé ou petite VM gratuite | à valider |
| Fréquences | CityBikes 2 min · OpenAQ 15 min · jobs Spark toutes les heures | à valider |
| Assemblage du dashboard | à désigner | à valider |
| Date de rendu | à reporter | à valider |

## Décisions prises

| Date | Décision | Qui |
|---|---|---|
| 2026-11-19 | Structure initiale du dépôt créée (dossiers, `.gitignore`, `.env.example`, `requirements.txt`, `CLAUDE.md`) | Dev 1 |
| 2026-11-19 | Infra Docker : Spark 3.5.9 (`apache/spark`), PostgreSQL 16, Superset 5.0.0, MinIO via `bitnamilegacy/minio:2025.4.22` (MinIO ne publie plus d'image officielle sur Docker Hub ; cette version garde la console web complète). Les buckets `raw`, `clean`, `curated` sont créés au démarrage par `MINIO_DEFAULT_BUCKETS` (remplace le `minio-init.sh` prévu dans le guide). | Dev 1 |
| 2026-11-19 | Les jobs Spark et notebooks tournent dans le conteneur `jupyter` (même image que le cluster), pour éviter d'installer Java/Spark sous Windows. Python y est en 3.10 (version de l'image Spark officielle) ; le driver et les workers doivent avoir la même version. | Dev 1 |
| 2026-11-19 | Source NOAA : `ncei.noaa.gov/data/global-hourly/access/` redirige désormais vers le stockage public Amazon S3 `noaa-global-hourly-pds` (mêmes fichiers). Ces données **s'arrêtent au 24/08/2025** : aucune donnée 2026. Le téléchargement couvre 2022 → 2025 (821 fichiers, ~4 Go, ~200 stations FR). | Dev 1 |
