"""Tests du contrat de données : villes et schémas.

Lancer depuis la racine du projet : pytest
"""

import pytest
from pyspark.sql.types import DoubleType, TimestampType

from common.cities import CITIES, assign_city, haversine_km
from common.schemas import CLEAN_SCHEMAS, CURATED_SCHEMAS, SOURCES

ALL_SCHEMAS = {**CLEAN_SCHEMAS, **CURATED_SCHEMAS}
EXPECTED_CITIES = {"paris", "lyon", "lille"}


# --- Villes -----------------------------------------------------------------

def test_city_list_matches_contract():
    assert set(CITIES) == EXPECTED_CITIES


def test_city_names_are_lowercase_ascii():
    for city in CITIES:
        assert city == city.lower() and city.isascii()


def test_haversine_paris_lyon():
    # Paris - Lyon à vol d'oiseau : environ 392 km.
    assert haversine_km(*CITIES["paris"], *CITIES["lyon"]) == pytest.approx(392, abs=5)


@pytest.mark.parametrize("city", sorted(EXPECTED_CITIES))
def test_city_center_is_assigned_to_itself(city):
    assert assign_city(*CITIES[city]) == city


def test_point_near_city_is_assigned():
    # Versailles, à environ 17 km de Paris.
    assert assign_city(48.8049, 2.1204) == "paris"


def test_point_far_from_cities_is_null():
    # Clermont-Ferrand, à plus de 30 km de toute ville du projet.
    assert assign_city(45.7772, 3.0870) is None


def test_city_outside_project_is_null():
    # Marseille ne fait plus partie des villes étudiées.
    assert assign_city(43.2965, 5.3698) is None


def test_missing_coordinates_are_null():
    assert assign_city(None, 2.35) is None
    assert assign_city(48.85, None) is None


def test_city_zones_do_not_overlap():
    # Deux centres à plus de 60 km : un point ne peut pas hésiter entre deux villes.
    names = list(CITIES)
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            assert haversine_km(*CITIES[a], *CITIES[b]) > 60


# --- Schémas ----------------------------------------------------------------

@pytest.mark.parametrize("name", sorted(ALL_SCHEMAS))
def test_timestamp_is_a_real_timestamp(name):
    field = ALL_SCHEMAS[name]["timestamp"]
    assert isinstance(field.dataType, TimestampType)
    assert not field.nullable


@pytest.mark.parametrize("name", sorted(ALL_SCHEMAS))
def test_columns_are_snake_case_and_unique(name):
    cols = ALL_SCHEMAS[name].fieldNames()
    assert len(cols) == len(set(cols))
    for col in cols:
        assert col == col.lower() and " " not in col and "-" not in col


@pytest.mark.parametrize("name", sorted(CLEAN_SCHEMAS))
def test_clean_tables_have_common_columns(name):
    schema = CLEAN_SCHEMAS[name]
    for col in ("timestamp", "latitude", "longitude", "city", "source", "ingestion_time"):
        assert col in schema.fieldNames()
    assert isinstance(schema["latitude"].dataType, DoubleType)
    assert isinstance(schema["longitude"].dataType, DoubleType)
    assert isinstance(schema["ingestion_time"].dataType, TimestampType)


def test_clean_table_paths_use_known_sources():
    for path in CLEAN_SCHEMAS:
        assert path.split("/")[0] in SOURCES


@pytest.mark.parametrize("name", sorted(CURATED_SCHEMAS))
def test_curated_tables_have_join_key(name):
    schema = CURATED_SCHEMAS[name]
    assert schema.fieldNames()[:2] == ["city", "timestamp"]
    assert not schema["city"].nullable


def test_urban_table_contains_all_metrics():
    urban = set(CURATED_SCHEMAS["urban_city_hourly"].fieldNames())
    for name in ("weather_city_hourly", "mobility_city_hourly", "pollution_city_hourly"):
        metrics = set(CURATED_SCHEMAS[name].fieldNames()) - {"nb_stations"}
        assert metrics <= urban
    assert {"hour", "day_of_week", "is_weekend"} <= urban
