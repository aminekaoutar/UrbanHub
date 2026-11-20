"""Test de la table croisée urban_city_hourly sur de petites données inventées (nécessite Java)."""

import subprocess
from datetime import datetime, timezone

import pytest

if subprocess.run(["java", "-version"], capture_output=True).returncode != 0:
    pytest.skip("Java absent : tests Spark ignorés", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from common.schemas import MOBILITY_CITY_HOURLY, POLLUTION_CITY_HOURLY, URBAN_CITY_HOURLY  # noqa: E402
from pipelines.iot_openaq.urban import WEATHER_WITH_SOURCE, build  # noqa: E402

SAT_10H = datetime(2024, 1, 6, 10, 0, tzinfo=timezone.utc)  # samedi
MON_08H = datetime(2024, 1, 8, 8, 0, tzinfo=timezone.utc)  # lundi
MON_09H = datetime(2024, 1, 8, 9, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def urban(spark):
    weather = spark.createDataFrame(
        [
            ("paris", SAT_10H, 8.0, 3.0, 180.0, 1015.0, 0.0, 10000.0, 4, "noaa"),
            ("paris", MON_08H, 5.0, 2.0, 90.0, 1020.0, 0.2, 8000.0, None, "openmeteo"),
            ("marseille", SAT_10H, 15.0, 5.0, 0.0, 1010.0, 0.0, 9000.0, 2, "noaa"),  # hors projet
        ],
        WEATHER_WITH_SOURCE,
    )
    mobility = spark.createDataFrame(
        [("paris", MON_08H, 300, 700, 1000, 0.3, 120, 50, 3, 1)],
        MOBILITY_CITY_HOURLY,
    )
    pollution = spark.createDataFrame(
        [
            ("paris", SAT_10H, 9.0, 15.0, 20.0, 50.0, None, 10),
            ("paris", MON_08H, 12.0, 20.0, 35.0, 30.0, None, 12),
            ("lyon", MON_09H, 8.0, 14.0, 18.0, 55.0, 200.0, 6),  # ni météo ni vélos
        ],
        POLLUTION_CITY_HOURLY,
    )
    df = build(weather, mobility, pollution)
    rows = {(r.city, r.hour, r.day_of_week): r for r in df.collect()}
    return df, rows


def test_schema_is_contract_plus_weather_source(urban):
    df, _ = urban
    expected = [(f.name, f.dataType) for f in URBAN_CITY_HOURLY.fields]
    assert [(f.name, f.dataType) for f in df.schema.fields][: len(expected)] == expected
    assert df.columns[len(expected):] == ["weather_source"]


def test_full_join_keeps_every_hour_of_project_cities(urban):
    _, rows = urban
    assert set(rows) == {("paris", 10, 6), ("paris", 8, 1), ("lyon", 9, 1)}


def test_three_sources_on_same_hour(urban):
    _, rows = urban
    r = rows[("paris", 8, 1)]
    assert (r.temperature, r.bikes_available, r.no2) == (5.0, 300, 35.0)
    assert (r.nb_weather_stations, r.nb_bike_stations, r.nb_sensors) == (None, 50, 12)
    assert r.weather_source == "openmeteo"


def test_missing_sources_stay_null(urban):
    _, rows = urban
    lyon = rows[("lyon", 9, 1)]
    assert lyon.temperature is None and lyon.bikes_available is None and lyon.pm25 == 8.0


def test_calendar_columns(urban):
    _, rows = urban
    assert rows[("paris", 10, 6)].is_weekend is True  # samedi
    assert rows[("paris", 8, 1)].is_weekend is False  # lundi


def test_runs_with_empty_inputs(spark):
    empty_w = spark.createDataFrame([], WEATHER_WITH_SOURCE)
    empty_m = spark.createDataFrame([], MOBILITY_CITY_HOURLY)
    pollution = spark.createDataFrame([("lille", MON_08H, 7.0, None, None, None, None, 3)], POLLUTION_CITY_HOURLY)
    rows = build(empty_w, empty_m, pollution).collect()
    assert len(rows) == 1 and rows[0].city == "lille" and rows[0].weather_source is None
