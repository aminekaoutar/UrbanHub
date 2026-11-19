"""Test du job raw → clean OpenAQ sur de petites données inventées (nécessite Java)."""

import gzip
import json
import subprocess

import pytest

# Sur macOS, la commande `java` existe même sans Java installé : on vérifie qu'elle fonctionne.
if subprocess.run(["java", "-version"], capture_output=True).returncode != 0:
    pytest.skip("Java absent : tests Spark ignorés", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from common.schemas import MEASUREMENTS  # noqa: E402
from pipelines.iot_openaq.clean import transform  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(json.dumps(data).encode()))


def _read(spark, folder):
    from pyspark.sql import functions as F

    return (
        spark.read.option("multiLine", True)
        .option("recursiveFileLookup", True)
        .json(str(folder))
        .withColumn("_file", F.input_file_name())
    )


def _station(loc_id, sensor_id, pollutant, is_monitor=True, lat=48.86, lon=2.35):
    return {
        "id": loc_id,
        "isMonitor": is_monitor,
        "isMobile": False,
        "coordinates": {"latitude": lat, "longitude": lon},
        "sensors": [{"id": sensor_id, "parameter": {"name": pollutant, "units": "µg/m³"}}],
    }


def _hour(start, value):
    return {
        "value": value,
        "parameter": {"name": "pm25", "units": "µg/m³"},
        "period": {"datetimeFrom": {"utc": start}},
    }


@pytest.fixture(scope="module")
def clean(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("raw")
    _write(
        root / "locations" / "paris_20261119T100000Z.json.gz",
        {
            "results": [
                _station(1, 101, "pm25"),
                _station(2, 201, "pm25", is_monitor=False),  # capteur bon marché
                _station(3, 301, "so2"),  # polluant hors contrat
            ]
        },
    )
    hours_dir = root / "sensor_hours"
    _write(
        hours_dir / "sensor_101_A.json.gz",
        {
            "ingestion_time": "2026-11-19T10:00:00+00:00",
            "results": [_hour("2024-01-02T07:00:00Z", 12.5), _hour("2024-01-02T08:00:00Z", -3.0)],
        },
    )
    # même heure téléchargée une seconde fois plus tard (mode live) : nouvelle valeur
    _write(
        hours_dir / "sensor_101_B.json.gz",
        {"ingestion_time": "2026-11-19T11:00:00+00:00", "results": [_hour("2024-01-02T07:00:00Z", 13.0)]},
    )
    _write(
        hours_dir / "sensor_201_A.json.gz",
        {"ingestion_time": "2026-11-19T10:00:00+00:00", "results": [_hour("2024-01-02T07:00:00Z", 99.0)]},
    )
    df = transform(_read(spark, root / "locations"), _read(spark, hours_dir))
    return df, df.orderBy("timestamp").collect()


def test_schema_matches_contract(clean):
    df, _ = clean
    contract = [(f.name, f.dataType) for f in MEASUREMENTS.fields]
    assert [(f.name, f.dataType) for f in df.schema.fields][: len(contract)] == contract


def test_only_official_sensor_is_kept(clean):
    _, rows = clean
    assert {r.sensor_id for r in rows} == {"101"}


def test_duplicates_keep_latest_download(clean):
    _, rows = clean
    assert len(rows) == 2
    assert rows[0].value == 13.0


def test_negative_value_becomes_null(clean):
    _, rows = clean
    assert rows[1].value is None


def test_contract_columns_filled(clean):
    df, rows = clean
    r = rows[0]
    assert r.city == "paris" and r.source == "openaq" and r.unit == "µg/m³"
    assert (r.year, r.month, r.day) == (2024, 1, 2)
    # Lu dans Spark (en UTC) : collect() convertirait l'heure dans le fuseau de l'ordinateur.
    first = df.selectExpr("min(date_format(timestamp, 'yyyy-MM-dd HH:mm')) AS t").first().t
    assert first == "2024-01-02 07:00"
