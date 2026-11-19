# Contrat de données UrbanHub

Ce document fixe le format des tables partagées entre les trois pipelines.
Le code de référence est dans `common/schemas.py` et `common/cities.py`, vérifié par
`tests/test_contract.py`. **Toute modification passe par une pull request relue par les trois développeurs.**

## 1. Règles générales

| Règle | Détail |
|---|---|
| Noms de colonnes | anglais, `snake_case` |
| `timestamp` | UTC, type Spark `TimestampType` (jamais une chaîne), jamais null |
| `latitude`, `longitude` | `double`, WGS84 |
| `city` | minuscules sans accents, affectée par `assign_city(lat, lon)` |
| `source` | `noaa`, `citybikes` ou `openaq` |
| `ingestion_time` | UTC, heure à laquelle le collecteur a reçu la donnée |
| Valeurs manquantes | `null` uniquement : pas de 9999, -1, chaîne vide, etc. |

### Unités

| Mesure | Unité |
|---|---|
| temperature | °C |
| wind_speed | m/s |
| wind_direction | degrés, 0 à 360 |
| pressure | hPa |
| visibility | m |
| precipitation | mm |
| polluants (pm25, pm10, no2, o3, co) | µg/m³ |

Chaque pipeline convertit vers ces unités dans l'étape raw → clean. Exemple : OpenAQ
peut renvoyer le CO en mg/m³ ou en ppm, il doit être converti en µg/m³.

## 2. Villes et période

Villes : `paris`, `lyon`, `lille`.

Période : années **2024, 2025 et 2026** (`START_YEAR` et `END_YEAR` dans `common/cities.py`).

`assign_city(lat, lon)` renvoie la ville dont le centre est le plus proche, s'il est à
**moins de 30 km** (distance à vol d'oiseau, formule de Haversine). Sinon, elle renvoie `null`.
Les centres des villes sont tous à plus de 60 km les uns des autres : un point ne peut donc
jamais être à moins de 30 km de deux villes.

Dans Spark :

```python
from common.cities import assign_city_udf
df = df.withColumn("city", assign_city_udf()("latitude", "longitude"))
```

## 3. Tables clean (Parquet, une ligne = une mesure)

Chemin : `clean/<source>/<dataset>/year=YYYY/month=MM/day=DD/`. Dans ces tables,
`city` peut être `null` (point à plus de 30 km de toute ville).

**`clean/noaa/weather_hourly`** (Dev 1)

| Colonne | Type | Null ? |
|---|---|---|
| station_id | string | non |
| timestamp | timestamp | non |
| temperature, wind_speed, wind_direction, pressure, precipitation, visibility | double | oui |
| latitude, longitude | double | oui |
| city | string | oui |
| source | string (`noaa`) | non |
| ingestion_time | timestamp | non |

**`clean/citybikes/station_status`** (Dev 2)

| Colonne | Type | Null ? |
|---|---|---|
| station_id | string | non |
| station_name | string | oui |
| timestamp | timestamp | non |
| latitude, longitude | double | oui |
| bikes_available, free_slots | int | oui |
| city | string | oui |
| source | string (`citybikes`) | non |
| ingestion_time | timestamp | non |

**`clean/openaq/measurements`** (Dev 3)

| Colonne | Type | Null ? |
|---|---|---|
| sensor_id | string | non |
| timestamp | timestamp | non |
| latitude, longitude | double | oui |
| pollutant | string : `pm25`, `pm10`, `no2`, `o3`, `co` | non |
| value | double | oui |
| unit | string, toujours `µg/m³` après conversion | non |
| city | string | oui |
| source | string (`openaq`) | non |
| ingestion_time | timestamp | non |

## 4. Tables curated (Parquet + PostgreSQL, une ligne = une ville × une heure)

`timestamp` est tronqué à l'heure (`date_trunc("hour", ...)`). La clé de jointure est
`(city, timestamp)`, et ces deux colonnes ne sont jamais `null`. Les lignes dont `city`
est `null` en clean sont écartées.

**`weather_city_hourly`** (Dev 1) : city, timestamp, temperature, wind_speed,
wind_direction, pressure, precipitation, visibility (double), nb_stations (int).

**`mobility_city_hourly`** (Dev 2) : city, timestamp, bikes_available, free_slots,
capacity, usage_events (int), occupancy_rate (double), nb_stations, nb_empty_stations
(occupation < 10 %), nb_full_stations (occupation > 90 %) (int).

**`pollution_city_hourly`** (Dev 3) : city, timestamp, pm25, pm10, no2, o3, co (double,
µg/m³), nb_sensors (int).

**`urban_city_hourly`** (Dev 3) : jointure des trois tables sur `(city, timestamp)`, plus :

| Colonne | Type | Détail |
|---|---|---|
| nb_weather_stations | int | `nb_stations` de `weather_city_hourly`, renommée |
| nb_bike_stations | int | `nb_stations` de `mobility_city_hourly`, renommée |
| hour | int | 0 à 23, en UTC |
| day_of_week | int | 1 = lundi … 7 = dimanche |
| is_weekend | boolean | vrai le samedi et le dimanche |

Attention : avec Spark, `dayofweek()` renvoie 1 = dimanche. Utiliser plutôt
`(dayofweek(ts) + 5) % 7 + 1` pour obtenir 1 = lundi.
