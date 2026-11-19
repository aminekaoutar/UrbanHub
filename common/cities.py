"""Villes du projet et affectation d'un point GPS à une ville.

Règle du contrat : un point appartient à la ville la plus proche si son centre
est à moins de 30 km, sinon la ville vaut None (null dans Spark).
"""

from math import asin, cos, radians, sin, sqrt

# Centre de chaque ville (latitude, longitude) en WGS84.
# Noms en minuscules sans accents : ce sont les valeurs exactes de la colonne `city`.
CITIES: dict[str, tuple[float, float]] = {
    "paris": (48.8566, 2.3522),
    "lyon": (45.7640, 4.8357),
    "lille": (50.6292, 3.0573),
}

# Période étudiée (décision du 2026-11-19, voir docs/decisions.md).
START_YEAR = 2024
END_YEAR = 2026

MAX_DISTANCE_KM = 30.0
EARTH_RADIUS_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance à vol d'oiseau entre deux points GPS, en kilomètres."""
    lat1, lon1, lat2, lon2 = map(radians, (lat1, lon1, lat2, lon2))
    a = sin((lat2 - lat1) / 2) ** 2 + cos(lat1) * cos(lat2) * sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_RADIUS_KM * asin(sqrt(a))


def assign_city(lat: float | None, lon: float | None) -> str | None:
    """Renvoie la ville la plus proche à moins de 30 km, sinon None."""
    if lat is None or lon is None:
        return None
    best_city, best_dist = None, MAX_DISTANCE_KM
    for city, (c_lat, c_lon) in CITIES.items():
        dist = haversine_km(lat, lon, c_lat, c_lon)
        if dist < best_dist:
            best_city, best_dist = city, dist
    return best_city


def assign_city_udf():
    """Version Spark de assign_city, à utiliser dans les jobs :

        from common.cities import assign_city_udf
        df = df.withColumn("city", assign_city_udf()("latitude", "longitude"))
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    return udf(assign_city, StringType())
