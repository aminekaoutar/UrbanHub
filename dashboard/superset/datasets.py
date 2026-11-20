"""Datasets Superset du dashboard UrbanHub : tables PostgreSQL + vues SQL (datasets « virtuels »).

Les tables publiées par les pipelines (contrat de données) sont en UTC et par heure. Les vues ci-dessous
ajoutent ce dont le dashboard a besoin sans toucher aux tables : heure de Paris, jour de la semaine,
agrégats journaliers, seuils OMS, classes de température / pluie / vent, corrélations.

Chaque vue garde les colonnes `city` et `timestamp` quand c'est possible : les filtres du dashboard
(Ville, Période, Granularité) s'appliquent alors à tous les graphiques.
"""

# Heure locale (Paris) à partir d'un timestamp UTC sans fuseau
LOCAL = "((timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris')"


def _calendar(local: str = "local") -> str:
    """Colonnes calendrier lisibles et triables (préfixe numérique pour l'ordre des axes)."""
    return f"""
    to_char({local}, 'HH24') || 'h' AS heure,
    extract(hour FROM {local})::int AS hour_local,
    extract(isodow FROM {local})::int || ' · ' ||
        (ARRAY['Lundi','Mardi','Mercredi','Jeudi','Vendredi','Samedi','Dimanche'])[extract(isodow FROM {local})::int] AS jour,
    CASE WHEN extract(isodow FROM {local}) >= 6 THEN 'Week-end' ELSE 'Semaine' END AS type_jour,
    to_char({local}, 'MM') || ' · ' ||
        (ARRAY['Janv.','Févr.','Mars','Avr.','Mai','Juin','Juil.','Août','Sept.','Oct.','Nov.','Déc.'])[extract(month FROM {local})::int] AS mois,
    CASE WHEN extract(month FROM {local}) IN (12, 1, 2) THEN '1 · Hiver'
         WHEN extract(month FROM {local}) IN (3, 4, 5) THEN '2 · Printemps'
         WHEN extract(month FROM {local}) IN (6, 7, 8) THEN '3 · Été'
         ELSE '4 · Automne' END AS saison"""


TEMP_BIN = """CASE WHEN {t} IS NULL THEN NULL
         WHEN {t} < 0 THEN '1 · < 0 °C'      WHEN {t} < 5 THEN '2 · 0-5 °C'
         WHEN {t} < 10 THEN '3 · 5-10 °C'    WHEN {t} < 15 THEN '4 · 10-15 °C'
         WHEN {t} < 20 THEN '5 · 15-20 °C'   WHEN {t} < 25 THEN '6 · 20-25 °C'
         WHEN {t} < 30 THEN '7 · 25-30 °C'   ELSE '8 · ≥ 30 °C' END"""

RAIN_HOUR_BIN = """CASE WHEN {p} IS NULL THEN NULL WHEN {p} = 0 THEN '0 · Sec'
         WHEN {p} < 1 THEN '1 · Faible (< 1 mm/h)' WHEN {p} < 4 THEN '2 · Modérée (1-4 mm/h)'
         ELSE '3 · Forte (≥ 4 mm/h)' END"""

RAIN_DAY_BIN = """CASE WHEN {p} IS NULL THEN NULL WHEN {p} < 0.5 THEN '0 · Sec (< 0,5 mm)'
         WHEN {p} < 5 THEN '1 · Faible (0,5-5 mm)' WHEN {p} < 15 THEN '2 · Modérée (5-15 mm)'
         ELSE '3 · Forte (≥ 15 mm)' END"""

WIND_BIN = """CASE WHEN {w} IS NULL THEN NULL WHEN {w} < 2 THEN '1 · Calme (< 2 m/s)'
         WHEN {w} < 4 THEN '2 · Faible (2-4 m/s)' WHEN {w} < 6 THEN '3 · Modéré (4-6 m/s)'
         WHEN {w} < 9 THEN '4 · Soutenu (6-9 m/s)' ELSE '5 · Fort (≥ 9 m/s)' END"""

# Valeurs guides OMS 2021 (moyenne 24 h, µg/m³). O3 : valeur 8 h, utilisée ici comme repère indicatif.
WHO = "(VALUES ('PM2.5', 15.0), ('PM10', 45.0), ('NO2', 25.0), ('O3', 100.0), ('CO', 4000.0)) AS s(pollutant, seuil)"

POLLUTION_LONG = f"""
SELECT p.city, p.timestamp, v.pollutant, v.value, s.seuil, v.value / s.seuil AS indice,
       {_calendar()}
FROM (SELECT *, {LOCAL} AS local FROM pollution_city_hourly) p
CROSS JOIN LATERAL (VALUES ('PM2.5', p.pm25), ('PM10', p.pm10), ('NO2', p.no2), ('O3', p.o3), ('CO', p.co))
     AS v(pollutant, value)
JOIN {WHO} USING (pollutant)
WHERE v.value IS NOT NULL
"""

URBAN_DAILY = f"""
SELECT city, date_trunc('day', local) AS timestamp,
       avg(temperature) AS temperature, max(temperature) AS tmax, min(temperature) AS tmin,
       sum(precipitation) AS precipitation, avg(wind_speed) AS wind_speed,
       avg(usage_events) AS usage_per_hour, avg(usage_events::float / NULLIF(nb_bike_stations, 0)) AS usage_per_station,
       avg(occupancy_rate) AS occupancy_rate,
       avg(bikes_available) AS bikes_available,
       avg(pm25) AS pm25, avg(pm10) AS pm10, avg(no2) AS no2, avg(o3) AS o3,
       count(usage_events) AS bike_hours,
       CASE WHEN extract(isodow FROM date_trunc('day', local)) >= 6 THEN 'Week-end' ELSE 'Semaine' END AS type_jour
FROM (SELECT *, {LOCAL} AS local FROM urban_city_hourly) u
GROUP BY 1, 2
"""

DATASETS = {
    # ---------------------------------------------------------------- Vue d'ensemble
    "v_data_freshness": """
SELECT 'Météo' AS source, 'weather_city_hourly' AS table_name, city, max(timestamp) AS derniere_heure,
       min(timestamp) AS premiere_heure, count(*) AS nb_lignes FROM weather_city_hourly GROUP BY city
UNION ALL
SELECT 'Vélos', 'mobility_city_hourly', city, max(timestamp), min(timestamp), count(*) FROM mobility_city_hourly GROUP BY city
UNION ALL
SELECT 'Pollution', 'pollution_city_hourly', city, max(timestamp), min(timestamp), count(*) FROM pollution_city_hourly GROUP BY city
UNION ALL
SELECT 'Croisé', 'urban_city_hourly', city, max(timestamp), min(timestamp), count(*) FROM urban_city_hourly GROUP BY city
""",
    "v_data_coverage": """
SELECT 'Météo' AS source, city, timestamp FROM weather_city_hourly WHERE temperature IS NOT NULL
UNION ALL SELECT 'Vélos', city, timestamp FROM mobility_city_hourly
UNION ALL SELECT 'Pollution', city, timestamp FROM pollution_city_hourly
""",
    # ---------------------------------------------------------------- Météo
    "v_weather_hourly": f"""
SELECT city, timestamp, temperature, precipitation, wind_speed, wind_direction, pressure, visibility, source,
       {_calendar()},
       {RAIN_HOUR_BIN.format(p='precipitation')} AS pluie,
       {WIND_BIN.format(w='wind_speed')} AS vent
FROM (SELECT *, {LOCAL} AS local FROM weather_city_hourly) w
""",
    "v_weather_daily": f"""
WITH d AS (
    SELECT city, date_trunc('day', {LOCAL}) AS timestamp,
           avg(temperature) AS tmean, min(temperature) AS tmin, max(temperature) AS tmax,
           sum(precipitation) AS precip_mm, avg(wind_speed) AS wind_mean, max(wind_speed) AS wind_max,
           avg(visibility) AS visibility, avg(pressure) AS pressure, max(source) AS source,
           count(temperature) AS nb_hours
    FROM weather_city_hourly GROUP BY 1, 2
), normale AS (   -- normale du mois sur 2024-2026, par ville
    SELECT city, extract(month FROM timestamp) AS m, avg(tmean) AS t_normale, stddev(tmean) AS t_sd
    FROM d GROUP BY 1, 2
)
SELECT d.*, n.t_normale, d.tmean - n.t_normale AS t_anomalie,
       (d.tmean - n.t_normale) / NULLIF(n.t_sd, 0) AS z_score,
       CASE WHEN (d.tmean - n.t_normale) / NULLIF(n.t_sd, 0) >= 1.5 THEN 'Anormalement chaud'
            WHEN (d.tmean - n.t_normale) / NULLIF(n.t_sd, 0) <= -1.5 THEN 'Anormalement froid'
            ELSE 'Normal' END AS anomalie,
       CASE WHEN extract(isodow FROM d.timestamp) >= 6 THEN 'Week-end' ELSE 'Semaine' END AS type_jour
FROM d JOIN normale n ON n.city = d.city AND n.m = extract(month FROM d.timestamp)
WHERE d.nb_hours >= 12   -- journées assez complètes pour une moyenne fiable
""",
    # ---------------------------------------------------------------- Mobilité
    "v_mobility_hourly": f"""
SELECT city, timestamp, bikes_available, free_slots, capacity, occupancy_rate, usage_events,
       nb_stations, nb_empty_stations, nb_full_stations,
       nb_empty_stations::float / NULLIF(nb_stations, 0) AS pct_empty,
       nb_full_stations::float / NULLIF(nb_stations, 0) AS pct_full,
       usage_events::float / NULLIF(nb_stations, 0) AS usage_per_station,
       {_calendar()}
FROM (SELECT *, {LOCAL} AS local FROM mobility_city_hourly) m
""",
    "v_station_map": """
SELECT station_id, station_name, city, latitude, longitude, capacity, usage_per_day, occupancy_rate,
       pct_empty, pct_full, occ_night, occ_day, imbalance,
       CASE WHEN imbalance <= -0.15 THEN 'Se vide en journée'
            WHEN imbalance >= 0.15 THEN 'Se remplit en journée'
            ELSE 'Équilibrée' END AS profil
FROM station_profile
WHERE nb_hours >= 720 AND capacity > 0 AND imbalance IS NOT NULL
""",
    "v_station_zones": """
SELECT city,
       round(latitude::numeric, 2)::float AS lat_zone, round(longitude::numeric, 2)::float AS lon_zone,
       city || ' ' || round(latitude::numeric, 2) || ' / ' || round(longitude::numeric, 2) AS zone,
       count(*) AS nb_stations, sum(capacity) AS capacity, sum(usage_per_day) AS usage_per_day,
       sum(usage_per_day) / NULLIF(sum(capacity), 0) AS rotation,
       avg(pct_empty) AS pct_empty, avg(pct_full) AS pct_full
FROM station_profile WHERE nb_hours >= 720 AND capacity > 0
GROUP BY 1, 2, 3, 4
""",
    # ---------------------------------------------------------------- Pollution
    "v_pollution_long": POLLUTION_LONG,
    "v_pollution_daily": f"""
SELECT city, date_trunc('day', ((timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris')) AS timestamp,
       pollutant, avg(value) AS moyenne, max(value) AS pic, max(seuil) AS seuil,
       avg(value) / max(seuil) AS indice, (avg(value) > max(seuil))::int AS depassement, count(*) AS nb_hours
FROM ({POLLUTION_LONG}) l
GROUP BY 1, 2, 3
""",
    # ---------------------------------------------------------------- Croisé
    "v_urban_hourly": f"""
SELECT city, timestamp, temperature, precipitation, wind_speed, visibility,
       usage_events, occupancy_rate, bikes_available, pm25, pm10, no2, o3,
       usage_events::float / NULLIF(nb_bike_stations, 0) AS usage_per_station,
       usage_events / NULLIF(avg(usage_events) OVER (PARTITION BY city), 0) AS usage_idx,
       no2 / NULLIF(avg(no2) OVER (PARTITION BY city), 0) AS no2_idx,
       pm25 / NULLIF(avg(pm25) OVER (PARTITION BY city), 0) AS pm25_idx,
       {_calendar()},
       {TEMP_BIN.format(t='temperature')} AS classe_temperature,
       {RAIN_HOUR_BIN.format(p='precipitation')} AS pluie,
       {WIND_BIN.format(w='wind_speed')} AS vent
FROM (SELECT *, {LOCAL} AS local FROM urban_city_hourly) u
""",
    "v_urban_daily": f"""
SELECT d.*,
       {TEMP_BIN.format(t='d.temperature')} AS classe_temperature,
       {RAIN_DAY_BIN.format(p='d.precipitation')} AS pluie,
       {WIND_BIN.format(w='d.wind_speed')} AS vent,
       CASE WHEN d.temperature BETWEEN 12 AND 25 AND d.precipitation < 1 AND d.wind_speed < 6
                 AND coalesce(d.pm25, 0) <= 15 THEN 'Favorable'
            ELSE 'Défavorable' END AS conditions_velo
FROM ({URBAN_DAILY}) d
""",
    "v_correlations": f"""
WITH d AS ({URBAN_DAILY}),
c AS (
    SELECT city,
           corr(temperature, usage_per_hour) AS c1, corr(precipitation, usage_per_hour) AS c2,
           corr(wind_speed, usage_per_hour) AS c3, corr(wind_speed, pm25) AS c4,
           corr(wind_speed, no2) AS c5, corr(temperature, o3) AS c6,
           corr(precipitation, pm10) AS c7, corr(usage_per_hour, no2) AS c8
    FROM d GROUP BY city
)
SELECT c.city, v.relation, v.r
FROM c CROSS JOIN LATERAL (VALUES
    ('Température ↔ usage vélos', c1), ('Pluie ↔ usage vélos', c2), ('Vent ↔ usage vélos', c3),
    ('Vent ↔ PM2.5', c4), ('Vent ↔ NO2', c5), ('Température ↔ O3', c6),
    ('Pluie ↔ PM10', c7), ('Usage vélos ↔ NO2', c8)) AS v(relation, r)
""",
}

# Noms de colonnes affichés dans Superset (tableaux, infobulles)
VERBOSE = {
    "city": "Ville", "timestamp": "Date", "temperature": "Température (°C)",
    "tmean": "Temp. moyenne (°C)", "tmin": "Temp. min (°C)", "tmax": "Temp. max (°C)",
    "precip_mm": "Pluie (mm)", "precipitation": "Pluie (mm)", "wind_mean": "Vent moyen (m/s)",
    "wind_max": "Vent max (m/s)", "wind_speed": "Vent (m/s)", "visibility": "Visibilité (m)",
    "pressure": "Pression (hPa)", "t_normale": "Normale du mois (°C)", "t_anomalie": "Écart à la normale (°C)",
    "z_score": "Écart (en écarts-types)", "anomalie": "Anomalie", "source": "Source",
    "usage_events": "Mouvements de vélos", "usage_per_day": "Mouvements / jour",
    "usage_per_hour": "Mouvements / heure", "occupancy_rate": "Taux de remplissage",
    "bikes_available": "Vélos disponibles", "capacity": "Capacité", "pct_empty": "Part du temps vide",
    "pct_full": "Part du temps pleine", "station_name": "Station", "imbalance": "Déséquilibre jour - nuit",
    "occ_night": "Remplissage la nuit", "occ_day": "Remplissage en journée", "nb_stations": "Stations",
    "rotation": "Rotation (mouvements / place / jour)", "zone": "Zone (~1 km)",
    "pollutant": "Polluant", "value": "Concentration (µg/m³)", "moyenne": "Moyenne du jour (µg/m³)",
    "pic": "Pic horaire (µg/m³)", "seuil": "Seuil OMS (µg/m³)", "indice": "Concentration / seuil OMS",
    "pm25": "PM2.5 (µg/m³)", "pm10": "PM10 (µg/m³)", "no2": "NO2 (µg/m³)", "o3": "O3 (µg/m³)",
    "conditions_velo": "Conditions vélo", "classe_temperature": "Température", "pluie": "Pluie",
    "vent": "Vent", "relation": "Relation", "r": "Corrélation (r)", "derniere_heure": "Dernière heure (UTC)",
    "premiere_heure": "Première heure (UTC)", "nb_lignes": "Lignes", "table_name": "Table PostgreSQL",
    "type_jour": "Type de jour", "profil": "Profil", "usage_per_station": "Mouvements / station / h", "jour": "Jour", "heure": "Heure (Paris)", "mois": "Mois", "saison": "Saison",
}
