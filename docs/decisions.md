# Décisions d'équipe

Toute décision qui touche les trois développeurs (contrat de données, villes, période,
stack) est notée ici avec sa date.

## À valider à la réunion de lancement

Propositions par défaut du guide (section 8). Remplacer « à valider » par la décision prise.

| Sujet | Proposition | Statut |
|---|---|---|
| Villes | **paris, lyon, lille** | ✅ décidé en équipe (19/11/2026) |
| Période | **2024 → 2026** pour toutes les sources. Météo : NOAA du 01/01/2024 au 24/08/2025 (fin des données NOAA), puis Open-Meteo du 25/08/2025 à aujourd'hui | ✅ décidé en équipe (19/11/2026) |
| Machine de collecte 24 h/24 | PC toujours allumé ou petite VM gratuite | à valider |
| Fréquences | CityBikes 2 min · OpenAQ 15 min · jobs Spark toutes les heures | à valider |
| Assemblage du dashboard | à désigner | à valider |
| Date de rendu | 21/11/2026 (projet de 2 jours) | ✅ |

## Décisions prises

| Date | Décision | Qui |
|---|---|---|
| 2026-11-19 | Structure initiale du dépôt créée (dossiers, `.gitignore`, `.env.example`, `requirements.txt`, `CLAUDE.md`) | Dev 1 |
| 2026-11-19 | Villes réduites à paris, lyon, lille | Équipe |
| 2026-11-19 | Période étudiée : 2024, 2025, 2026 (toutes sources) | Équipe |
| 2026-11-19 | Infra Docker : Spark 3.5.9 (`apache/spark`), PostgreSQL 16, Superset 5.0.0, MinIO via `bitnamilegacy/minio:2025.4.22` (MinIO ne publie plus d'image officielle sur Docker Hub ; cette version garde la console web complète). Les buckets `raw`, `clean`, `curated` sont créés au démarrage par `MINIO_DEFAULT_BUCKETS` (remplace le `minio-init.sh` prévu dans le guide). | Dev 1 |
| 2026-11-19 | Les jobs Spark et notebooks tournent dans le conteneur `jupyter` (même image que le cluster), pour éviter d'installer Java/Spark sous Windows. Python y est en 3.10 (version de l'image Spark officielle) ; le driver et les workers doivent avoir la même version. | Dev 1 |
| 2026-11-19 | Source NOAA : `ncei.noaa.gov/data/global-hourly/access/` redirige désormais vers le stockage public Amazon S3 `noaa-global-hourly-pds` (mêmes fichiers). Ces données **s'arrêtent au 24/08/2025** : aucune donnée 2026. Le téléchargement couvre 2022 → 2025 (821 fichiers, ~4 Go, ~200 stations FR). | Dev 1 |
| 2026-11-19 | Météo après le 24/08/2025 : **Open-Meteo** (vraies données, gratuit, sans clé) plutôt que recopier une année précédente (données inventées, et analyses croisées faussées). Ce sont des données de modèle (grille ~10 km), pas de station : `nb_stations` = null et `source` = `openmeteo` dans `weather_city_hourly`. Visibilité disponible seulement sur les 7 derniers jours. | Dev 1 |
| 2026-11-19 | `weather_city_hourly` a une colonne en plus du contrat : `source` (`noaa` / `openmeteo`). | Dev 1 |
| 2026-11-19 | Nettoyage NOAA : `clean` garde toutes les stations françaises (analyses « France entière » de la partie 1) ; `city` n'est renseignée que pour les stations à moins de 30 km de paris, lyon ou lille. Précipitations ramenées en mm/h à partir du cumul sur la plus courte durée disponible (1 h ou 3 h). Températures hors de [-30 ; 46] °C rejetées (ex. 49 °C à Chartres une nuit d'avril = capteur défaillant). | Dev 1 |
| 2026-11-19 | CityBikes : collecte limitée aux villes de `common/cities.py` (paris, lyon, lille), relevé toutes les **2 min** = 90 appels/h. Le quota API (300 appels/h) est **par adresse IP** : il est partagé avec tout le wifi (ex. école) ; en cas de blocage (429) le collecteur attend le délai demandé | Dev 2 |
| 2026-11-19 | CityBikes : Marseille (`le-velo`) renvoie 0 vélo dans toutes les stations et Nice (`pony-nice`) 0 station → hors analyses mobilité. Sans impact avec les villes paris, lyon, lille | Dev 2 |
| 2026-11-20 | CityBikes : historique nov. 2024 → sept. 2026 depuis data.citybik.es (même source que le collecteur, changements d'état mis à l'heure par `history.py`). Les mois du collecteur restent prioritaires ; `curate.py` publie désormais tout le dossier curated dans PostgreSQL | Dev 2 |
| 2026-11-20 | Vélos avant nov. 2024 : seul Lyon a un historique public (Métropole de Lyon, `velov_history.py`) → Lyon couvert depuis janv. 2024 ; Paris et Lille commencent en nov. 2024 (aucune source d'historique, rien n'est inventé). Nouvelle table `clean/ilevia/vlille_trips` : trajets V'Lille réels depuis avril 2025 | Dev 2 |
| 2026-11-20 | Site web interactif `dashboard/webapp/` (FastAPI + DuckDB) : chaque graphique interroge le Data Lake en direct, ou PostgreSQL avec `WEBAPP_BACKEND=postgres`. Code commun : à relire par les trois | Dev 2 |
| 2026-11-19 | OpenAQ : seules les stations officielles fixes sont gardées (`isMonitor = true`, réseau EEA) ; capteurs bon marché (AirGradient, HabitatMap) écartés | Dev 3 |
| 2026-11-19 | Machine de démo = PC de Dev 1. Collecte continue par des services Docker (profil `collecte`) : `collecte-pollution` (OpenAQ live, 15 min) et `pipeline` (`pipelines/run_pipeline.py`, toutes les heures : météo + clean/curated/PostgreSQL de chaque source). Vélos à ajouter quand le code de Dev 2 sera dans `main`. | Dev 1 |
| 2026-11-19 | Le worker Spark monte le projet (`/app`, lecture seule, `PYTHONPATH=/app`) : les fonctions Python des jobs (ex. `common.cities.assign_city` dans le nettoyage OpenAQ) s'exécutent sur le worker et doivent y trouver `common`. | Dev 1 |
