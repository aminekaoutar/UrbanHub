"""Schémas Spark des tables clean et curated (contrat de données).

Ne pas modifier sans l'accord des trois développeurs (pull request relue par tous).
Le détail des règles est dans docs/data_contract.md.
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SOURCES = ("noaa", "citybikes", "openaq")
POLLUTANTS = ("pm25", "pm10", "no2", "o3", "co")
POLLUTANT_UNIT = "µg/m³"


def _f(name, dtype, nullable=True):
    return StructField(name, dtype, nullable)


# --- Colonnes communes ------------------------------------------------------

_POSITION = [_f("latitude", DoubleType()), _f("longitude", DoubleType())]
# city est null si le point est à plus de 30 km de toute ville.
_TRACE = [
    _f("city", StringType()),
    _f("source", StringType(), False),
    _f("ingestion_time", TimestampType(), False),
]

# --- Tables clean -----------------------------------------------------------

WEATHER_HOURLY = StructType(
    [
        _f("station_id", StringType(), False),
        _f("timestamp", TimestampType(), False),
        _f("temperature", DoubleType()),
        _f("wind_speed", DoubleType()),
        _f("wind_direction", DoubleType()),
        _f("pressure", DoubleType()),
        _f("precipitation", DoubleType()),
        _f("visibility", DoubleType()),
        *_POSITION,
        *_TRACE,
    ]
)

STATION_STATUS = StructType(
    [
        _f("station_id", StringType(), False),
        _f("station_name", StringType()),
        _f("timestamp", TimestampType(), False),
        *_POSITION,
        _f("bikes_available", IntegerType()),
        _f("free_slots", IntegerType()),
        *_TRACE,
    ]
)

MEASUREMENTS = StructType(
    [
        _f("sensor_id", StringType(), False),
        _f("timestamp", TimestampType(), False),
        *_POSITION,
        _f("pollutant", StringType(), False),
        _f("value", DoubleType()),
        _f("unit", StringType(), False),
        *_TRACE,
    ]
)

# --- Tables curated (ville × heure, clé = city + timestamp) -----------------

_KEY = [_f("city", StringType(), False), _f("timestamp", TimestampType(), False)]

_WEATHER_METRICS = [
    _f("temperature", DoubleType()),
    _f("wind_speed", DoubleType()),
    _f("wind_direction", DoubleType()),
    _f("pressure", DoubleType()),
    _f("precipitation", DoubleType()),
    _f("visibility", DoubleType()),
]
_MOBILITY_METRICS = [
    _f("bikes_available", IntegerType()),
    _f("free_slots", IntegerType()),
    _f("capacity", IntegerType()),
    _f("occupancy_rate", DoubleType()),
    _f("usage_events", IntegerType()),
]
_MOBILITY_COUNTS = [
    _f("nb_empty_stations", IntegerType()),  # occupation < 10 %
    _f("nb_full_stations", IntegerType()),  # occupation > 90 %
]
_POLLUTION_METRICS = [_f(p, DoubleType()) for p in POLLUTANTS]

WEATHER_CITY_HOURLY = StructType(
    [*_KEY, *_WEATHER_METRICS, _f("nb_stations", IntegerType())]
)

MOBILITY_CITY_HOURLY = StructType(
    [*_KEY, *_MOBILITY_METRICS, _f("nb_stations", IntegerType()), *_MOBILITY_COUNTS]
)

POLLUTION_CITY_HOURLY = StructType(
    [*_KEY, *_POLLUTION_METRICS, _f("nb_sensors", IntegerType())]
)

# Jointure des trois tables. Les deux colonnes nb_stations sont renommées
# pour ne pas entrer en collision (météo vs vélos).
URBAN_CITY_HOURLY = StructType(
    [
        *_KEY,
        *_WEATHER_METRICS,
        _f("nb_weather_stations", IntegerType()),
        *_MOBILITY_METRICS,
        _f("nb_bike_stations", IntegerType()),
        *_MOBILITY_COUNTS,
        *_POLLUTION_METRICS,
        _f("nb_sensors", IntegerType()),
        _f("hour", IntegerType(), False),  # 0-23, en UTC
        _f("day_of_week", IntegerType(), False),  # 1 = lundi ... 7 = dimanche
        _f("is_weekend", BooleanType(), False),
    ]
)

CLEAN_SCHEMAS = {
    "noaa/weather_hourly": WEATHER_HOURLY,
    "citybikes/station_status": STATION_STATUS,
    "openaq/measurements": MEASUREMENTS,
}

CURATED_SCHEMAS = {
    "weather_city_hourly": WEATHER_CITY_HOURLY,
    "mobility_city_hourly": MOBILITY_CITY_HOURLY,
    "pollution_city_hourly": POLLUTION_CITY_HOURLY,
    "urban_city_hourly": URBAN_CITY_HOURLY,
}
