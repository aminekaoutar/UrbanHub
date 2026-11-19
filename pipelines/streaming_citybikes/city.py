"""Colonne `city` pour Spark.

Le référentiel officiel est common/cities.py (Développeur 3). Tant qu'il n'est pas fusionné, on utilise la même
règle ici : ville du référentiel la plus proche à moins de 30 km, sinon null (contrat, section 4.1).
"""
from pyspark.sql import Column
from pyspark.sql import functions as F

try:
    from common.cities import CITIES  # {"paris": (lat, lon), ...} fourni par le Développeur 3
except ImportError:
    CITIES = {
        "paris": (48.8566, 2.3522), "lyon": (45.7640, 4.8357), "marseille": (43.2965, 5.3698),
        "toulouse": (43.6047, 1.4442), "nice": (43.7102, 7.2620), "nantes": (47.2184, -1.5536),
        "strasbourg": (48.5734, 7.7521), "montpellier": (43.6108, 3.8767), "bordeaux": (44.8378, -0.5792),
        "lille": (50.6292, 3.0573),
    }
MAX_KM = 30.0


def _haversine_km(lat: Column, lon: Column, lat0: float, lon0: float) -> Column:
    dlat, dlon = F.radians(lat - F.lit(lat0)), F.radians(lon - F.lit(lon0))
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(F.lit(lat0))) * F.cos(F.radians(lat)) * F.sin(dlon / 2) ** 2
    return 2 * 6371.0 * F.asin(F.sqrt(a))


def assign_city_col(lat: str = "latitude", lon: str = "longitude") -> Column:
    """Expression Spark native (pas d'UDF Python) : ville la plus proche à moins de 30 km, sinon null."""
    dists = [F.struct(_haversine_km(F.col(lat), F.col(lon), la, lo).alias("d"), F.lit(c).alias("city"))
             for c, (la, lo) in CITIES.items()]
    nearest = F.least(*dists)                      # struct comparé sur le premier champ : la distance
    return F.when(nearest["d"] <= MAX_KM, nearest["city"])
