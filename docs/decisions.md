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
| 2026-11-19 | OpenAQ : seules les stations officielles fixes sont gardées (`isMonitor = true`, réseau EEA) ; capteurs bon marché (AirGradient, HabitatMap) écartés | Dev 3 |
