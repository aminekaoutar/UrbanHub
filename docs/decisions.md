# Décisions d'équipe

Toute décision qui touche les trois développeurs (contrat de données, villes, période,
stack) est notée ici avec sa date.

## À valider à la réunion de lancement

Propositions par défaut du guide (section 8). Remplacer « à valider » par la décision prise.

| Sujet | Proposition | Statut |
|---|---|---|
| Villes | paris, lyon, lille | décidé le 2026-11-19 |
| Période | 2024 → 2026 (année en cours mise à jour en fin de projet) | décidé le 2026-11-19 |
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
| 2026-11-19 | Villes réduites à paris, lyon, lille | Dev 3 (à valider par Dev 1 et Dev 2 dans la PR) |
| 2026-11-19 | Période étudiée : 2024, 2025, 2026 (toutes sources) | Dev 3 (à valider par Dev 1 et Dev 2 dans la PR) |
| 2026-11-19 | CityBikes : collecte limitée aux villes de `common/cities.py` (paris, lyon, lille), relevé toutes les **2 min** = 90 appels/h. Le quota API (300 appels/h) est **par adresse IP** : il est partagé avec tout le wifi (ex. école) ; en cas de blocage (429) le collecteur attend le délai demandé | Dev 2 |
| 2026-11-19 | CityBikes : Marseille (`le-velo`) renvoie 0 vélo dans toutes les stations et Nice (`pony-nice`) 0 station → hors analyses mobilité. Sans impact avec les villes paris, lyon, lille | Dev 2 |
