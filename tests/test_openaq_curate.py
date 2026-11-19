"""Test du job clean → curated OpenAQ sur de petites données inventées (nécessite Java)."""

import subprocess
from datetime import datetime

import pytest

if subprocess.run(["java", "-version"], capture_output=True).returncode != 0:
    pytest.skip("Java absent : tests Spark ignorés", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from common.schemas import MEASUREMENTS, POLLUTION_CITY_HOURLY  # noqa: E402
from pipelines.iot_openaq.curate import city_hourly  # noqa: E402

T7 = datetime(2024, 1, 2, 7, 0)
T8 = datetime(2024, 1, 2, 8, 0)
NOW = datetime(2026, 11, 19, 10, 0)


@pytest.fixture(scope="module")
def result():
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    def row(sensor, ts, pollutant, value, city="paris"):
        return (sensor, ts, 48.86, 2.35, pollutant, value, "µg/m³", city, "openaq", NOW)

    clean = spark.createDataFrame(
        [
            row("1", T7, "pm25", 10.0),
            row("2", T7, "pm25", 20.0),  # deux capteurs pm25 : moyenne 15
            row("3", T7, "no2", 30.0),
            row("4", T7, "no2", None),  # valeur manquante : ignorée
            row("1", T8, "pm25", 12.0),
            row("9", T7, "pm25", 99.0, city=None),  # hors ville : écarté
            row("5", T7, "o3", 40.0, city="lyon"),
        ],
        MEASUREMENTS,
    )
    df = city_hourly(clean)
    return df, {(r.city, r.timestamp): r for r in df.collect()}


def test_schema_matches_contract(result):
    df, _ = result
    expected = [(f.name, f.dataType) for f in POLLUTION_CITY_HOURLY.fields]
    assert [(f.name, f.dataType) for f in df.schema.fields] == expected


def test_one_row_per_city_and_hour(result):
    _, rows = result
    assert len(rows) == 3


def test_pollutants_are_averaged_across_sensors(result):
    _, rows = result
    r = rows[("paris", T7)]
    assert (r.pm25, r.no2, r.o3, r.pm10, r.co) == (15.0, 30.0, None, None, None)


def test_nb_sensors_counts_sensors_with_a_value(result):
    _, rows = result
    assert rows[("paris", T7)].nb_sensors == 3  # capteurs 1, 2, 3 (le 4 n'a pas de valeur)
    assert rows[("paris", T8)].nb_sensors == 1


def test_rows_without_city_are_dropped(result):
    _, rows = result
    assert {city for city, _ in rows} == {"paris", "lyon"}
