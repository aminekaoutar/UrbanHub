# UrbanHub : règles pour Claude

Projet étudiant en équipe de 3 : plateforme Big Data Smart City (météo, vélos, pollution)
sur les grandes villes françaises. Guide complet : `docs/UrbanHub_Guide_Coordination.pdf`.

## Rôles : toujours identifier le rôle avant de répondre

Chaque prompt commence par « Développeur 1 », « Développeur 2 » ou « Développeur 3 ».
Sans rôle, demander lequel avant de répondre.

| Rôle | Mission | Dossiers autorisés |
|---|---|---|
| Dev 1 | Batch NOAA Global Hourly (météo) | `pipelines/batch_noaa/`, `analysis/weather/` |
| Dev 2 | Streaming CityBikes API v2 (vélos) | `pipelines/streaming_citybikes/`, `analysis/mobility/` |
| Dev 3 | IoT OpenAQ API v3 (pollution) + analyses croisées | `pipelines/iot_openaq/`, `analysis/pollution/`, `analysis/cross/` |
| Tous | Code commun | `infra/`, `common/`, `tests/`, `dashboard/`, `docs/`, `CLAUDE.md` |

- Rester dans les dossiers du rôle annoncé. Ne jamais modifier les dossiers d'un autre développeur.
- Toute modification de `common/`, `infra/`, `CLAUDE.md` ou des schémas doit être signalée
  explicitement : elle passe par une pull request relue par les deux autres.
- Pilotes de l'étape 1 : Dev 1 → `infra/docker-compose.yml`, `.env.example` ;
  Dev 2 → `common/config.py`, `spark.py`, `storage.py`, `publish.py` ;
  Dev 3 → `common/cities.py`, `schemas.py`, `docs/data_contract.md`, tests du contrat.
- Les membres de l'équipe débutent : expliquer chaque étape simplement et brièvement.

## Stack

Python 3.11+ · Spark 3.5 (PySpark) + hadoop-aws 3.3.4 · MinIO (S3, accès `s3a://`) ·
PostgreSQL 16 (serving) · Superset · Docker Compose · Jupyter, pandas, scikit-learn.

## Data Lake (MinIO, un bucket par couche)

- `raw` : données brutes telles que reçues (CSV / JSON), partition par date.
- `clean` : nettoyé et typé, Parquet.
- `curated` : agrégats ville × heure, Parquet + copie dans PostgreSQL (via `common/publish.py`).
- Chemins : `<bucket>/<source>/<dataset>/year=.../month=.../day=...`
- Les données ne vont jamais dans Git.

## Contrat de données (ne pas modifier sans accord des trois)

- `timestamp` : UTC, type timestamp Spark (jamais une chaîne), nom exact `timestamp`.
- `latitude` / `longitude` : double, WGS84.
- `city` : minuscules sans accents, parmi paris, lyon, lille (période étudiée : 2024 → 2026) ;
  affectée par `common.cities.assign_city(lat, lon)`
  (ville la plus proche à moins de 30 km, sinon null).
- `source` : `noaa` | `citybikes` | `openaq`. `ingestion_time` : UTC, ajouté par le collecteur.
- Colonnes en snake_case, en anglais. Valeurs manquantes = null (aucune sentinelle 9999).
- Unités : temperature °C · wind_speed m/s · wind_direction degrés 0-360 · pressure hPa ·
  visibility m · precipitation mm · polluants µg/m³.

Tables clean :
- `clean/noaa/weather_hourly` : station_id, timestamp, temperature, wind_speed, wind_direction,
  pressure, precipitation, visibility, latitude, longitude, city, source, ingestion_time.
- `clean/citybikes/station_status` : station_id, station_name, timestamp, latitude, longitude,
  bikes_available, free_slots, city, source, ingestion_time.
- `clean/openaq/measurements` : sensor_id, timestamp, latitude, longitude, pollutant, value,
  unit, city, source, ingestion_time.

Tables curated (timestamp tronqué à l'heure ; clé de jointure = (city, timestamp)) :
- `weather_city_hourly` (Dev 1) : city, timestamp, temperature, wind_speed, wind_direction,
  pressure, precipitation, visibility, nb_stations.
- `mobility_city_hourly` (Dev 2) : city, timestamp, bikes_available, free_slots, capacity,
  occupancy_rate, usage_events, nb_stations, nb_empty_stations (< 10 %), nb_full_stations (> 90 %).
- `pollution_city_hourly` (Dev 3) : city, timestamp, pm25, pm10, no2, o3, co, nb_sensors.
- `urban_city_hourly` (Dev 3) : jointure des trois + hour, day_of_week, is_weekend.

## Git

- `main` doit toujours fonctionner ; personne ne pousse directement dessus.
- Une branche par tâche : `dev1/...`, `dev2/...`, `dev3/...`, `common/...`, puis pull request.
- Jamais de données, de clé API ni de `.env` dans Git.
